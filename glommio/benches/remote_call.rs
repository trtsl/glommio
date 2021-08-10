#![allow(clippy::redundant_clone)]
use futures::{Stream, StreamExt};
use glommio::{
    timer::Timer,
    CpuSet,
    GlommioError,
    Local,
    LocalExecutorPoolBuilder,
    MeshSettings,
    Placement,
    RemoteCall,
    ResourceType,
    Result,
};
use std::{cell::RefCell, future::Future, rc::Rc, time::Duration};

#[derive(Debug, Clone)]
enum Combinator {
    ForEach(usize),
    BufUnordered(usize),
    Buffered(usize),
    AwaitEach,
}

fn main() {
    let m1 = MeshSettings {
        request_channel_size: 1 << 12,
        response_channel_size: 1 << 12,
        concurrency_limit: 1 << 12,
    };
    let m2 = MeshSettings {
        request_channel_size: 1 << 4,
        response_channel_size: 1 << 4,
        concurrency_limit: 1 << 12,
    };
    let m3 = MeshSettings {
        request_channel_size: 1 << 12,
        response_channel_size: 1 << 12,
        concurrency_limit: 1 << 4,
    };
    let m4 = MeshSettings {
        request_channel_size: 1 << 4,
        response_channel_size: 1 << 4,
        concurrency_limit: 1 << 4,
    };

    let nr_cpus = CpuSet::online().unwrap().len();

    run_benches(1 << 12, m1.clone(), 1);
    run_benches(1 << 12, m1.clone(), nr_cpus - 1);
    run_benches(1 << 12, m2.clone(), 1);
    run_benches(1 << 12, m2.clone(), nr_cpus - 1);
    run_benches(1 << 12, m3.clone(), 1);
    run_benches(1 << 12, m3.clone(), nr_cpus - 1);
    run_benches(1 << 8, m4.clone(), 1);
    run_benches(1 << 8, m4.clone(), nr_cpus - 1);
}

fn run_benches(n: usize, m: MeshSettings, d: usize) {
    eprintln!(
        "\n===== submit, n: {}, d: {}, channel_size: {}, concurrency_limit: {} =====",
        n, d, m.request_channel_size, m.concurrency_limit
    );
    run(n, d, m.clone(), submit, Combinator::ForEach(100));
    run(n, d, m.clone(), submit, Combinator::BufUnordered(100));
    run(n, d, m.clone(), submit, Combinator::Buffered(100));
    run(n, d, m.clone(), submit, Combinator::AwaitEach);

    eprintln!(
        "\n===== try_submit, n: {}, d: {}, channel_size: {}, concurrency_limit: {} =====",
        n, d, m.request_channel_size, m.concurrency_limit
    );
    run(n, d, m.clone(), try_submit, Combinator::ForEach(100));
    run(n, d, m.clone(), try_submit, Combinator::BufUnordered(100));
    run(n, d, m.clone(), try_submit, Combinator::Buffered(100));
    run(n, d, m.clone(), try_submit, Combinator::AwaitEach);
}

fn run<F, S, R, E>(n: usize, d: usize, mesh_settings: MeshSettings, f_submit: F, c: Combinator)
where
    F: 'static + FnOnce(usize, usize) -> S + Send + Clone,
    S: Stream<Item = R> + Unpin,
    R: Future<Output = Result<Result<[usize; 3], ()>, E>>,
    E: Future<Output = [usize; 3]>,
{
    LocalExecutorPoolBuilder::new(4)
        .channel_mesh(mesh_settings)
        .placement(Placement::MaxPack(None))
        .on_all_shards(move || async move {
            let nr_ok_ok = RefCell::new(0);
            let nr_ok_err = RefCell::new(0);
            let nr_err = RefCell::new(0);

            let count = |r| match r {
                Ok(Ok(_)) => *nr_ok_ok.borrow_mut() += 1,
                Ok(Err(_)) => *nr_ok_err.borrow_mut() += 1,
                Err(_) => *nr_err.borrow_mut() += 1,
            };

            let t0 = std::time::Instant::now();
            let mut submitted = f_submit(n, d);
            match c {
                Combinator::ForEach(limit) => {
                    submitted
                        .for_each_concurrent(limit, |f| async move { count(f.await) })
                        .await;
                }
                Combinator::BufUnordered(limit) => {
                    let mut s = submitted.buffer_unordered(limit);
                    while let Some(f) = s.next().await {
                        count(f);
                    }
                }
                Combinator::Buffered(limit) => {
                    let mut s = submitted.buffered(limit);
                    while let Some(f) = s.next().await {
                        count(f);
                    }
                }
                Combinator::AwaitEach => {
                    while let Some(f) = submitted.next().await {
                        count(f.await);
                    }
                }
            }
            eprintln!(
                "{:?} on shard {}: {:?} [{} {} {}]",
                c,
                Local::id(),
                t0.elapsed() / n as u32,
                nr_ok_ok.borrow(),
                nr_ok_err.borrow(),
                nr_err.borrow()
            );
            assert_eq!(
                n,
                *nr_ok_ok.borrow() + *nr_ok_err.borrow() + *nr_err.borrow()
            );
        })
        .unwrap()
        .join_all();
}

fn submit(
    n: usize,
    d: usize,
) -> impl Stream<
    Item = impl Future<Output = Result<Result<[usize; 3], ()>, impl Future<Output = [usize; 3]>>>,
> {
    let id_src = Local::id();
    let ids = Rc::new(Local::exec_ids().into_iter().collect::<Vec<_>>());
    futures::stream::iter(0..n).map(move |ii| {
        let ids = Rc::clone(&ids);
        async move {
            let id_dst = id_dst(&*ids, d, ii);
            let payload = move || payload(id_src, id_dst, ii);
            let r = match RemoteCall::submit_with_response(id_dst, payload).await {
                Ok(res) => Ok(res.await),
                Err(res) => Err(res),
            };
            r.map_err(|e| e.map(|f| f()))
        }
    })
}

fn try_submit(
    n: usize,
    d: usize,
) -> impl Stream<
    Item = impl Future<Output = Result<Result<[usize; 3], ()>, impl Future<Output = [usize; 3]>>>,
> {
    let id_src = Local::id();
    let ids = Rc::new(Local::exec_ids().into_iter().collect::<Vec<_>>());
    futures::stream::iter(0..n).map(move |ii| {
        let ids = Rc::clone(&ids);
        async move {
            let id_dst = id_dst(&*ids, d, ii);
            let mut payload = move || payload(id_src, id_dst, ii);
            let r = loop {
                match RemoteCall::try_submit_with_response(id_dst, payload) {
                    Ok(f) => break Ok(f.await),
                    Err(res @ GlommioError::Closed(_)) => break Err(res),
                    Err(GlommioError::WouldBlock(ResourceType::Channel(p))) => {
                        payload = p;
                        Local::later().await;
                        // Timer::new(Duration::from_millis(1)).await;
                    }
                    Err(e) => unreachable!("oops: {:?}", e),
                }
            };
            r.map_err(|e| e.map(|f| f()))
        }
    })
}

async fn payload(id_src: usize, id_dst: usize, ii: usize) -> [usize; 3] {
    if (ii % 100) == 0 {
        Timer::new(Duration::from_millis(ii as u64 % 10)).await;
    }
    debug_assert_eq!(id_dst, Local::id());
    [id_src, id_dst, ii]
}

fn id_dst(ids: &[usize], cardinality: usize, entropy: usize) -> usize {
    ids[entropy % cardinality]
}
