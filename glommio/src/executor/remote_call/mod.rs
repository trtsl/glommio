//! Terminology:
//! A 'request' or 'remote call' is sent 'downstream' from a 'source' / 'src'
//! executor to a 'destination' / 'dst' executor.  The 'destination' executor
//! evaluates the futures it receives via the 'request' and, if required, sends
//! back 'upstream' a 'response' to the 'source' executor.
//!
//! Notes:
//! The implementation uses two meshes (one for downstream requests and one for
//! upstream responses); using a single mesh is prone to deadlocks in the event
//! that the fixed-size channels are filled with requests and no responses can
//! be sent to the upstream executors.  Alternative ways of preventing deadlocks
//! maybe be worth investigating.

// TODO: avoid `futures` as dependency? combine with `futures-lite`?
// TODO: free on same executor as alloc?
// TODO: catch panics?
// TODO: unit tests
// TODO: review `unsafe` and add comments / justifications

use crate::{
    channels::{
        channel_mesh::{Receivers, Senders},
        shared_channel::ConnectedReceiver,
    },
    error::ResourceType,
    free_list::{FreeList, Idx},
    task::JoinHandle,
    GlommioError,
    Local,
    Result,
};
use futures::{self, StreamExt};
// use futures_lite::{self, StreamExt};
use std::{
    cell::RefCell,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt::{self, Debug, Formatter},
    future::Future,
    marker::PhantomData,
    ops::{Index, IndexMut},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

mod future_once;
use future_once::{FutGen, FutureOnce};

#[macro_use]
mod do_drop;
use do_drop::DoDropBox;

mod marker {
    /// A reminder not to derive/impl `Clone` on any object holding this marker
    #[derive(Debug)]
    pub(super) struct NoClone;
}

thread_local! {
    static REGISTRY: Rc<Registry> =
        Rc::new(Registry::default());
}

impl<T> super::Task<T> {
    /// An iterator over the ids of other
    /// [`LocalExecutor`](super::LocalExecutor)s in the same
    /// [`channel_mesh`](crate::channels::channel_mesh) as the current
    /// `LocalExecutor` for meshes created with
    /// [`LocalExecutorPoolBuilder::channel_mesh`](super::LocalExecutorPoolBuilder::channel_mesh).
    /// This may be useful when creating [`RemoteCall`]s.
    pub fn exec_ids() -> impl IntoIterator<Item = usize> {
        let mut idx = 0;
        std::iter::from_fn(move || {
            Registry::senders_with_ref(|s| {
                s.map(|s| {
                    loop {
                        match s.exec_to_mesh_id.nth(idx) {
                            Some((exec_id, Some(_))) => {
                                idx += 1;
                                break Some(exec_id.into());
                            }
                            Some((_, None)) => idx += 1,
                            None => break None,
                        }
                    }
                })
                .flatten()
            })
        })
    }
}

pub(super) enum MsgDownstream {
    RequestAndForget(DoDropBox<dyn Send + FutureOnce<Output = ()>>),
    RequestResponse(DoDropBox<dyn Send + FutureOnce<Output = MsgUpstream>>),
    /// Request to close the sender; other executors are not able to show down
    /// until all senders have been closed.  An executor receiving this request
    /// can delay it and send additional messages, but should track and
    /// eventually close the sender.
    CloseSender,
}

impl MsgDownstream {
    // Safety: the user must ensure `bx_dyn` was created from a `U`
    unsafe fn downcast_unchecked<U>(self) -> U {
        match self {
            MsgDownstream::RequestAndForget(bx_dyn) => bx_dyn.downcast_unchecked::<U>(),
            MsgDownstream::RequestResponse(bx_dyn) => bx_dyn.downcast_unchecked::<U>(),
            MsgDownstream::CloseSender => panic!("not applicable to variant"),
        }
    }
}

impl Debug for MsgDownstream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::RequestAndForget(_) => "MsgDownstream::RequestAndForget(_)",
            Self::RequestResponse(_) => "MsgDownstream::RequestResponse(_)",
            Self::CloseSender => "MsgDownstream::CloseSender",
        };
        f.write_str(s)
    }
}

pub(super) enum MsgUpstream {
    Response {
        /// Return value of the future
        buf: DoDropBox<dyn Send>,
        /// Storage location for the future's output
        idx: Idx<Output>,
    },
}

impl Debug for MsgUpstream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Response { .. } => "MsgUpstream::Response{..}",
        };
        f.write_str(s)
    }
}

#[derive(Debug)]
struct PeerData {
    connected: bool,
    output: FreeList<Output>,
}

impl PeerData {
    fn new(connected: bool) -> Self {
        Self {
            connected,
            output: FreeList::default(),
        }
    }

    fn alloc(&mut self, item: Output) -> Idx<Output> {
        self.output.alloc(item)
    }

    fn dealloc(&mut self, idx: Idx<Output>) -> Output {
        self.output.dealloc(idx)
    }

    fn mark_disconnected(&mut self) {
        self.connected = false;
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn wake_all(&mut self) {
        self.output.find_for_each_mut(|o| {
            if let Some(w) = o.waker.take() {
                w.wake()
            }
        });
    }
}

impl<Idx> Index<Idx> for PeerData
where
    FreeList<Output>: Index<Idx>,
{
    type Output = <FreeList<Output> as Index<Idx>>::Output;
    fn index(&self, index: Idx) -> &Self::Output {
        self.output.index(index)
    }
}
impl<Idx> IndexMut<Idx> for PeerData
where
    FreeList<Output>: IndexMut<Idx>,
{
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        self.output.index_mut(index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MeshId(usize);

impl From<MeshId> for usize {
    fn from(id: MeshId) -> Self {
        id.0
    }
}

impl From<usize> for MeshId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

impl TryFrom<ExecId> for MeshId {
    type Error = ();
    fn try_from(exec_id: ExecId) -> std::result::Result<Self, Self::Error> {
        Registry::senders_with_ref(|s| match s {
            Some(s) => match s.exec_to_mesh_id.get(exec_id) {
                Some(Some(mesh_id)) => Ok(mesh_id.clone()),
                None | Some(None) => Err(()),
            },
            None => Err(()),
        })
    }
}

#[derive(Debug)]
pub struct ExecId(usize);

impl From<usize> for ExecId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

impl From<ExecId> for usize {
    fn from(id: ExecId) -> Self {
        id.0
    }
}

#[derive(Debug)]
struct IdMap {
    ids: Vec<Option<MeshId>>,
    offset: usize,
}

impl IdMap {
    fn new(lo: Option<usize>, hi: Option<usize>) -> Self {
        match (lo, hi) {
            (Some(lo), Some(hi)) => {
                debug_assert!(lo <= hi, "invalid inputs");
                Self {
                    ids: vec![None; 1 + hi - lo],
                    offset: lo,
                }
            }
            (None, None) => Self {
                ids: vec![None; 0],
                offset: 0,
            },
            _ => panic!("invalid inputs"),
        }
    }

    fn get(&self, index: ExecId) -> Option<&Option<MeshId>> {
        let exec_id: usize = index.into();
        if self.offset <= exec_id {
            self.ids.get(exec_id - self.offset)
        } else {
            None
        }
    }

    fn get_mut(&mut self, index: ExecId) -> Option<&mut Option<MeshId>> {
        let exec_id: usize = index.into();
        if self.offset <= exec_id {
            self.ids.get_mut(exec_id - self.offset)
        } else {
            None
        }
    }

    fn nth(&self, index: usize) -> Option<(ExecId, &Option<MeshId>)> {
        (index < self.ids.len()).then(|| (ExecId(self.offset + index), &self.ids[index]))
    }
}

#[derive(Debug)]
struct SendersDownstream {
    senders: Senders<MsgDownstream>,
    exec_to_mesh_id: IdMap,
}

impl SendersDownstream {
    fn new(senders: Senders<MsgDownstream>) -> Self {
        // the senders within a channel mesh are sorted in ascending order of their
        // executor id; we'll use that to establish the range of valid executor
        // ids for this executor's peers within the channel mesh
        fn first_exec_id(
            senders: &Senders<MsgDownstream>,
            range: impl IntoIterator<Item = usize>,
        ) -> Option<usize> {
            range
                .into_iter()
                .filter_map(|idx| senders.executor_id(idx))
                .next()
        }

        let n = senders.nr_consumers();
        let lo = first_exec_id(&senders, 0..n);
        let hi = first_exec_id(&senders, (0..n).rev());

        let mut exec_to_mesh_id = IdMap::new(lo, hi);

        for idx_mesh in 0..senders.nr_consumers() {
            if idx_mesh != senders.peer_id() {
                let exec_id = ExecId(senders.executor_id(idx_mesh).expect("sender not set"));
                *exec_to_mesh_id.get_mut(exec_id).unwrap() = Some(idx_mesh.into());
            }
        }

        Self {
            senders,
            exec_to_mesh_id,
        }
    }

    async fn send_to(&self, id: MeshId, msg: MsgDownstream) -> Result<(), MsgDownstream> {
        self.senders.send_to(id.into(), msg).await
    }

    fn try_send_to(&self, id: MeshId, msg: MsgDownstream) -> Result<(), MsgDownstream> {
        self.senders.try_send_to(id.into(), msg)
    }

    fn nr_consumers(&self) -> usize {
        self.senders.nr_consumers()
    }

    fn mesh_id(&self) -> MeshId {
        self.senders.peer_id().into()
    }

    fn close_mesh_id(&self, id: MeshId) {
        self.senders.close_id(id.into())
    }
}

// `senders` and `output` are in separate `RefCells` because senders may be
// borrowed across `await` points, and because they consume their messages, it
// is not possible to implement a future that releases the `RefCell` between
// `poll`ing the future retuned by `Senders::send`; the implementation must
// ensure that `senders` is not borrowed mutably at a point where senders is
// held at an `await` point `Senders<T>` requires `T: Send` so here `B: Send`
#[derive(Debug)]
pub(super) struct Registry {
    senders: RefCell<Option<SendersDownstream>>,
    /// a map where the key is the channel id of the destination executor
    peer_data: RefCell<HashMap<MeshId, PeerData>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            senders: RefCell::new(None),
            peer_data: RefCell::new(HashMap::new()),
        }
    }
}

impl Registry {
    pub(super) fn set_tls(senders: Senders<MsgDownstream>) {
        REGISTRY.with(|r| *r.senders.borrow_mut() = Some(SendersDownstream::new(senders)));
    }

    fn get() -> Rc<Registry> {
        REGISTRY.with(|r| Rc::clone(r))
    }

    fn senders_with_ref<F, T>(f: F) -> T
    where
        F: FnOnce(Option<&SendersDownstream>) -> T,
    {
        REGISTRY.with(|r| f(r.senders.borrow().as_ref()))
    }

    fn peer_data_with_ref_mut<F, T>(f: F) -> T
    where
        F: FnOnce(&mut HashMap<MeshId, PeerData>) -> T,
    {
        REGISTRY.with(|r| f(&mut *r.peer_data.borrow_mut()))
    }

    pub(super) fn process_inbound(
        request_rx: Receivers<MsgDownstream>,
        response_tx: Senders<MsgUpstream>,
        response_rx: Receivers<MsgUpstream>,
        concurrency_limit: usize,
    ) -> [JoinHandle<()>; 2] {
        let hndl_downstream = {
            // `response_tx` is used in `Local::local` calls which requires `static`
            let response_tx = Rc::new(response_tx);
            let process_msg = {
                let response_tx = Rc::clone(&response_tx);
                move |id, msg| {
                    let response_tx = Rc::clone(&response_tx);
                    async move { Self::process_downstream(&*response_tx, id, msg).await }
                }
            };
            let on_rx_closed = move |id: MeshId| {
                response_tx.close_id(id.into());
            };
            Self::process(request_rx, concurrency_limit, process_msg, on_rx_closed)
        };

        let hndl_uptream = {
            let process_msg = |id, msg| async { Self::process_upstream(id, msg).await };
            let on_rx_closed = |id| {
                Registry::peer_data_with_ref_mut(|peer_data| {
                    if let Some(peer_data) = peer_data.get_mut(&id) {
                        if peer_data.is_connected() {
                            peer_data.mark_disconnected();
                            peer_data.wake_all();
                        }
                    }
                });
            };
            Self::process(response_rx, concurrency_limit, process_msg, on_rx_closed)
        };

        [hndl_downstream, hndl_uptream]
    }

    fn process<P, M, F, C>(
        mut rx: Receivers<M>,
        concurrency_limit: usize,
        process_msg: P,
        on_rx_closed: C,
    ) -> JoinHandle<()>
    where
        P: 'static + FnMut(MeshId, M) -> F + Clone,
        M: 'static + Send,
        F: Future<Output = ()>,
        C: 'static + FnOnce(MeshId) + Clone,
    {
        #[allow(clippy::needless_collect)] // `collect::<Vec<_>>` to start polling the `Task`
        Local::local(async move {
            let handles = rx
                .streams()
                .into_iter()
                .map(|(id, r)| {
                    let process_msg = {
                        let mut process_msg = process_msg.clone();
                        move |msg| process_msg(id.into(), msg)
                    };
                    let on_rx_closed = on_rx_closed.clone();
                    Local::local(async move {
                        match "combinator" {
                            "tasks" => Self::for_each_concurrent_task(r, process_msg).await,
                            "combinator" => {
                                r.for_each_concurrent(concurrency_limit, process_msg).await
                            }
                            _ => unreachable!(),
                        }
                        on_rx_closed(id.into());
                    })
                    .detach()
                })
                .collect::<Vec<_>>();

            for h in handles {
                h.await;
            }
        })
        .detach()
    }

    async fn for_each_concurrent_task<P, M, F>(r: ConnectedReceiver<M>, process_msg: P)
    where
        P: 'static + FnMut(M) -> F + Clone,
        M: 'static + Send,
        F: Future<Output = ()>,
    {
        let handles = Rc::new(RefCell::new(std::collections::VecDeque::new()));
        while let Some(m) = r.recv().await {
            let mut process_msg = process_msg.clone();
            handles.borrow_mut().push_back({
                let handles = Rc::clone(&handles);
                Local::local(async move {
                    process_msg(m).await;

                    // clean up some handes that may have completed previously to prevent `handles`
                    // from growing endlessly; note that items are cleared sequentially, so if
                    // there is one future that hangs, then `handles` will not pop any items after
                    // reaching the future that is hanging
                    struct GetWaker;
                    impl Future for GetWaker {
                        type Output = Waker;
                        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                            Poll::Ready(cx.waker().clone())
                        }
                    }
                    let w = GetWaker.await;
                    let mut cx = std::task::Context::from_waker(&w);

                    // If we are waiting for the final sweep through `handles` to confirm all
                    // `Tasks` have completed (i.e. just prior to calling `on_rx_closed`), then the
                    // `RefCell` is already borrowed mutably, so we just don't perform the sweep
                    // here
                    if let Ok(mut hndls) = handles.try_borrow_mut() {
                        while let Some(mut h) = hndls.pop_front() {
                            let f = Pin::new(&mut h);
                            if Future::poll(f, &mut cx).is_pending() {
                                hndls.push_front(h);
                                break;
                            }
                        }
                    }
                })
                .detach()
            });
        }
        for h in handles.borrow_mut().iter_mut() {
            h.await;
        }
    }

    pub(super) fn process_inbound_select_all(
        request_rx: Receivers<MsgDownstream>,
        response_tx: Senders<MsgUpstream>,
        response_rx: Receivers<MsgUpstream>,
        concurrency_limit: usize,
    ) -> [JoinHandle<()>; 2] {
        let hndl_downstream = {
            let response_tx = Rc::new(response_tx);
            let process_request = {
                let response_tx = Rc::clone(&response_tx);
                move |(id, msg)| {
                    let response_tx = Rc::clone(&response_tx);
                    async move { Self::process_downstream(&*response_tx, id, msg).await }
                }
            };
            let on_rx_closed = move || {
                response_tx.close();
            };
            Self::process_inbound_select_all_impl(
                request_rx,
                concurrency_limit,
                process_request,
                on_rx_closed,
            )
        };

        let hndl_uptream = {
            let process_request = |(id, msg)| async { Self::process_upstream(id, msg).await };
            let on_rx_closed = || {
                Registry::peer_data_with_ref_mut(|peer_data| {
                    peer_data.values_mut().for_each(|peer_data| {
                        if peer_data.is_connected() {
                            peer_data.mark_disconnected();
                            peer_data.wake_all();
                        }
                    })
                });
            };
            Self::process_inbound_select_all_impl(
                response_rx,
                concurrency_limit,
                process_request,
                on_rx_closed,
            )
        };

        [hndl_downstream, hndl_uptream]
    }

    fn process_inbound_select_all_impl<P, F, C, M>(
        mut receivers: Receivers<M>,
        concurrency_limit: usize,
        process_msg: P,
        on_rx_closed: C,
    ) -> JoinHandle<()>
    where
        P: 'static + FnMut((MeshId, M)) -> F + Clone,
        F: Future<Output = ()>,
        C: 'static + FnOnce(),
        M: 'static + Send,
    {
        let receivers = receivers
            .streams()
            .into_iter()
            .map(|(id, r)| r.map(move |s| (id.into(), s)));
        let stream = futures::stream::select_all(receivers);
        Local::local(async move {
            stream
                .for_each_concurrent(concurrency_limit, process_msg)
                .await;
            on_rx_closed();
        })
        .detach()
    }

    async fn process_downstream(
        response_tx: &Senders<MsgUpstream>,
        id: MeshId,
        msg: MsgDownstream,
    ) {
        match msg {
            MsgDownstream::RequestAndForget(mut f) => {
                f.ref_mut_once()
                    .pin_fut()
                    .expect("unreachable: future already polled")
                    .await;
            }
            MsgDownstream::RequestResponse(mut f) => {
                let msg = f
                    .ref_mut_once()
                    .pin_fut()
                    .expect("unreachable: future already polled")
                    .await;
                debug_assert!(matches!(msg, MsgUpstream::Response { .. }));
                response_tx.send_to(id.into(), msg).await.ok();
            }
            MsgDownstream::CloseSender => {
                Registry::senders_with_ref(|s| s.expect("senders not set").close_mesh_id(id));
            }
        }
    }

    async fn process_upstream(id_dst: MeshId, msg: MsgUpstream) {
        match msg {
            MsgUpstream::Response { buf, idx } => {
                Registry::peer_data_with_ref_mut(|peer_data| {
                    if let Some(peer_data) = peer_data.get_mut(&id_dst) {
                        let output = &mut peer_data[idx];
                        match output.waker.take() {
                            Some(w) => {
                                output.buf = Some(buf);
                                w.wake();
                            }
                            // a missing `Waker` indicates that the `RemoteCall` was dropped by the
                            // user, so we clear the entry since it will no longer be accessed
                            None => drop(peer_data.dealloc(idx)),
                        }
                    }
                })
            }
        }
    }

    /// Send requests to close the senders on other executors that are connected
    /// the receivers on this executor
    pub(super) async fn close_request_tx() {
        let reg = Registry::get();
        let senders = reg.senders.borrow();
        if let Some(ref senders) = *senders {
            let id_src = senders.mesh_id();
            for id_dst in (0..senders.nr_consumers()).map(MeshId) {
                if id_src != id_dst {
                    let msg = MsgDownstream::CloseSender;
                    senders.send_to(id_dst.clone(), msg).await.ok();
                    senders.close_mesh_id(id_dst);
                };
            }
        }
    }

    pub(super) fn destroy() {
        REGISTRY.with(|r| {
            r.senders.borrow_mut().take();
            r.peer_data.borrow_mut().clear();
        });
    }
}

pub(super) struct Output {
    buf: Option<DoDropBox<dyn Send>>,
    // a missing `Waker` is used to indicate that the `RemoteCall` waiting on this `Output` has
    // been dropped by the user; therefore, once the `MsgUpstream::Response` is received, the
    // `Output` can simply be cleared
    waker: Option<Waker>,
}

impl Default for Output {
    fn default() -> Self {
        Self {
            buf: None,
            waker: Some(crate::task::waker_fn::dummy_waker()),
        }
    }
}

impl std::fmt::Debug for Output {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("Output {..}")
    }
}

enum RequestInner<F, G> {
    FutGen(F),
    Fut(G),
}

struct Request<F, G> {
    inner: RequestInner<F, G>,
    data_idx: Idx<Output>,
}

impl<F, G> Request<F, G>
where
    F: FnOnce() -> G,
{
    fn new(f: F, data_idx: Idx<Output>) -> Self {
        Self {
            inner: RequestInner::FutGen(f),
            data_idx,
        }
    }

    fn into_fut_gen(self) -> Option<F> {
        match self.inner {
            RequestInner::FutGen(f) => Some(f),
            RequestInner::Fut(_) => None,
        }
    }

    fn set_future(&mut self) {
        if let RequestInner::FutGen(f) = &self.inner {
            unsafe {
                let f = std::ptr::read(f);
                std::ptr::write(&mut self.inner, RequestInner::Fut(f()));
            }
        }
    }
}

impl<F, G> Future for Request<F, G>
where
    F: FnOnce() -> G + Send,
    G: Future,
    G::Output: 'static + Send,
{
    type Output = MsgUpstream;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { Pin::into_inner_unchecked(self) };
        this.set_future();
        let f = match &mut this.inner {
            RequestInner::Fut(f) => f,
            RequestInner::FutGen(_) => unreachable!("set_future not called"),
        };
        unsafe { Pin::new_unchecked(f) }
            .poll(cx)
            .map(|o| MsgUpstream::Response {
                buf: do_drop_box!(o),
                idx: this.data_idx,
            })
    }
}

// `RemoteCall` is not self-referential; `T` is stored in the `Registry`
impl<T> Unpin for RemoteCall<T> {}

/// A facility for sending futures from one
/// [`LocalExecutor`](super::LocalExecutor) to another.
///
/// # Examples
///
/// ```
/// panic!("TODO");
/// ```
#[derive(Debug)]
pub struct RemoteCall<T> {
    id_dst: MeshId,
    data_idx: Option<Idx<Output>>,
    _output: PhantomData<T>,
    _noclone: marker::NoClone,
}

impl RemoteCall<()> {
    /// Send a [`Future`] to another [`LocalExecutor`](super::LocalExecutor) to
    /// be executed there.
    pub async fn submit_and_forget<F, G>(id_dst: usize, f: F) -> Result<(), F>
    where
        F: 'static + Send + FnOnce() -> G,
        G: 'static + Future<Output = ()>,
    {
        let id_dst = ExecId(id_dst);
        let id_dst = match id_dst.try_into() {
            Ok(id_dst) => id_dst,
            Err(_) => return Err(GlommioError::Closed(ResourceType::Channel(f))),
        };

        // CAUTION: `unsafe MsgDownstream::downcast_unchecked` reads raw bytes based on
        // the provided type parameter; if the type of `fut_gen` changes, then
        // the type used in `MsgDownstream::downcast_unchecked` below must be
        // updated as well
        let __caution__: FutGen<F, G> = FutGen::new(f);
        let fut_gen = __caution__;

        let f = do_drop_box!(fut_gen);
        let msg = MsgDownstream::RequestAndForget(f);
        let reg = Registry::get();
        let senders = reg.senders.borrow();
        match senders.as_ref() {
            Some(s) => s.send_to(id_dst, msg).await.map_err(|e| {
                e.map(|msg| {
                    unsafe { msg.downcast_unchecked::<FutGen<F, G>>() }
                        .into_fn()
                        .unwrap()
                })
            }),
            None => Err(GlommioError::Closed(ResourceType::Channel(
                unsafe { msg.downcast_unchecked::<FutGen<F, G>>() }
                    .into_fn()
                    .unwrap(),
            ))),
        }
    }

    /// Try to end a [`Future`] to another
    /// [`LocalExecutor`](super::LocalExecutor) to be executed there.  This
    /// fails if the channel is full or closed.
    pub fn try_submit_and_forget<F, G>(id_dst: usize, f: F) -> Result<(), F>
    where
        F: 'static + Send + FnOnce() -> G,
        G: 'static + Future<Output = ()>,
    {
        let id_dst = ExecId(id_dst);
        let id_dst: MeshId = match id_dst.try_into() {
            Ok(id_dst) => id_dst,
            Err(_) => return Err(GlommioError::Closed(ResourceType::Channel(f))),
        };

        // CAUTION: `unsafe MsgDownstream::downcast_unchecked` reads raw bytes based on
        // the provided type parameter; if the type of `fut_gen` changes, then
        // the type used in `MsgDownstream::downcast_unchecked` below must be
        // updated as well
        let __caution__: FutGen<F, G> = FutGen::new(f);
        let fut_gen = __caution__;

        let f = do_drop_box!(fut_gen);
        let msg = MsgDownstream::RequestAndForget(f);
        Registry::senders_with_ref(|senders| match senders {
            Some(s) => s.try_send_to(id_dst, msg).map_err(|e| {
                unsafe { e.map(|msg| msg.downcast_unchecked::<FutGen<F, G>>()) }
                    .map(|fut_gen| fut_gen.into_fn().unwrap())
            }),
            None => Err(GlommioError::Closed(ResourceType::Channel(
                unsafe { msg.downcast_unchecked::<FutGen<F, G>>() }
                    .into_fn()
                    .unwrap(),
            ))),
        })
    }
}

impl<T> RemoteCall<T>
where
    T: 'static + Send,
{
    fn new(id_dst: MeshId, data_idx: Option<Idx<Output>>) -> Self {
        Self {
            id_dst,
            data_idx,
            _output: PhantomData,
            _noclone: marker::NoClone,
        }
    }

    /// Send a [`Future`] to another [`LocalExecutor`](super::LocalExecutor) to
    /// be executed there.
    ///
    /// The returned [`RemoteCall`] call is a `Future` that can be `await`ed to
    /// retrieve the [`Future::Output`] of the `Future` generated by `f`.
    pub async fn submit_with_response<F, G>(id_dst: usize, f: F) -> Result<Self, F>
    where
        F: 'static + Send + FnOnce() -> G,
        G: 'static + Future<Output = T>,
    {
        let id_dst = ExecId(id_dst);
        Self::generic_submit_with_response(id_dst, f, Self::send_async).await
    }

    /// Try to send a [`Future`] to another
    /// [`LocalExecutor`](super::LocalExecutor) to be executed there.  This
    /// fails if the channel is full or closed.
    ///
    /// The returned [`RemoteCall`] call is a `Future` that can be `await`ed to
    /// retrieve the [`Future::Output`] of the `Future` generated by `f`.
    pub fn try_submit_with_response<F, G>(id_dst: usize, f: F) -> Result<Self, F>
    where
        F: 'static + Send + FnOnce() -> G,
        G: 'static + Future<Output = T>,
    {
        let id_dst = ExecId(id_dst);

        // `Self::generic_submit_with_response` is not actually `async` when its
        // `f_send` argument returns a `Future` that is always `Ready`, so just
        // convert `Self::generic_submit_with_response` to be not `async`
        let waker = crate::task::waker_fn::dummy_waker();
        let mut ctx = std::task::Context::from_waker(&waker);
        let mut fut = Self::generic_submit_with_response(id_dst, f, Self::send_sync);
        let res = unsafe { Pin::new_unchecked(&mut fut).poll(&mut ctx) };
        match res {
            Poll::Ready(o) => o,
            Poll::Pending => unreachable!("send_sync should not be async"),
        }
    }

    async fn send_async(id_dst: MeshId, msg: MsgDownstream) -> Result<(), MsgDownstream> {
        let reg = Registry::get();
        let senders = reg.senders.borrow();
        match senders.as_ref() {
            Some(s) => s.send_to(id_dst, msg).await,
            None => Err(GlommioError::Closed(ResourceType::Channel(msg))),
        }
    }

    async fn send_sync(id_dst: MeshId, msg: MsgDownstream) -> Result<(), MsgDownstream> {
        Registry::senders_with_ref(|senders| match senders {
            Some(s) => s.try_send_to(id_dst, msg),
            None => Err(GlommioError::Closed(ResourceType::Channel(msg))),
        })
    }

    async fn generic_submit_with_response<F, G, S, R>(
        id_dst: ExecId,
        f: F,
        f_send: S,
    ) -> Result<Self, F>
    where
        F: 'static + Send + FnOnce() -> G,
        G: 'static + Future<Output = T>,
        S: FnOnce(MeshId, MsgDownstream) -> R,
        R: Future<Output = Result<(), MsgDownstream>>,
    {
        let id_dst: MeshId = match id_dst.try_into() {
            Ok(id_dst) => id_dst,
            Err(_) => return Err(GlommioError::Closed(ResourceType::Channel(f))),
        };

        let data_idx = Registry::peer_data_with_ref_mut(|peer_data| {
            let peer_data = peer_data
                .entry(id_dst.clone())
                .or_insert_with(|| PeerData::new(true));

            peer_data
                .is_connected()
                .then(|| peer_data.alloc(Output::default()))
        });

        let data_idx = match data_idx {
            Some(data_idx) => data_idx,
            None => return Err(GlommioError::Closed(ResourceType::Channel(f))),
        };

        let req = move || Request::new(f, data_idx);
        let fut_gen = FutGen::new(req);
        match Self::submit_as_dyn(f_send, fut_gen, id_dst.clone()).await {
            Ok(_) => Ok(Self::new(id_dst, Some(data_idx))),
            Err(e) => {
                // deallocate on send error so we don't leak memory
                Registry::peer_data_with_ref_mut(|peer_data| {
                    let peer_data = peer_data
                        .get_mut(&id_dst)
                        .expect("unreachable: peer_data not set");
                    drop(peer_data.dealloc(data_idx));
                    if let GlommioError::Closed(_) = e {
                        peer_data.mark_disconnected();
                    }
                });

                Err(e.map(|fut_gen| {
                    let request = (fut_gen.into_fn().unwrap())();
                    request.into_fut_gen().unwrap()
                }))
            }
        }
    }

    async fn submit_as_dyn<F, G, H, S, R>(
        f_send: S,
        fut_gen: FutGen<F, Request<G, H>>,
        id_dst: MeshId,
    ) -> Result<(), FutGen<F, Request<G, H>>>
    where
        F: 'static + Send + FnOnce() -> Request<G, H>,
        G: 'static + Send + FnOnce() -> H,
        H: 'static + Future<Output = T>,
        S: FnOnce(MeshId, MsgDownstream) -> R,
        R: Future<Output = Result<(), MsgDownstream>>,
    {
        // CAUTION: `unsafe Self::map_submit_err` reads raw bytes based on the provided
        // type parameter; if the type of `fut_gen` changes, then the type used
        // in `Self::map_submit_err` below must be updated as well
        let __caution__: FutGen<F, Request<G, H>> = fut_gen;
        let fut_gen = __caution__;

        let msg = do_drop_box!(fut_gen);
        let msg = MsgDownstream::RequestResponse(msg);
        let res = f_send(id_dst, msg).await;

        res.map_err(|e| unsafe {
            e.map(|msg| msg.downcast_unchecked::<FutGen<F, Request<G, H>>>())
        })
    }
}

impl<T> Drop for RemoteCall<T> {
    fn drop(&mut self) {
        if let Some(idx) = self.data_idx.take() {
            Registry::peer_data_with_ref_mut(|peer_data| {
                if let Some(peer_data) = peer_data.get_mut(&self.id_dst) {
                    // the missing waker indicates that when the response is received, there is no
                    // `RemoteCall` waiting on it, and the entry in `PeerData` should simply be
                    // cleared
                    drop(peer_data[idx].waker.take());
                }
            });
        }
    }
}

impl<T: 'static> Future for RemoteCall<T> {
    type Output = Result<T, ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let reg = Registry::get();
        let mut peer_data = reg.peer_data.borrow_mut();
        // when a `CloseSender` request is received, the `Output` entries for the
        // relevant executor are dropped, so if we arrive here and can't get the
        // `Output` in the `Registry` we return `Ready(Err(Closed(..)))`
        let peer_data = peer_data.get_mut(&self.id_dst).expect("peer_data not set");
        if !peer_data.is_connected() {
            return Poll::Ready(Err(GlommioError::Closed(ResourceType::Channel(()))));
        }

        let idx = self.data_idx.take().expect("missing registry index");
        let output = &mut peer_data[idx];
        match output.buf.take() {
            Some(buf) => {
                let res = unsafe { buf.downcast_unchecked::<T>() };
                peer_data.dealloc(idx);
                Poll::Ready(Ok(res))
            }
            None => {
                output.waker = Some(cx.waker().clone());
                self.data_idx = Some(idx);
                Poll::Pending
            }
        }
    }
}
