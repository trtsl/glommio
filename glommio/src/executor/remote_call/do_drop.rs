//! The purpose of this module is to be able to enable the following:
//!
//! ```ignore
//! use std::mem::ManuallyDrop;
//! use glommio::executor::remote_call::DoDrop;
//!
//! fn example<T: std::any::Any>(x: T) -> Result<(), T> {
//!     let bx: DoDrop<Box<ManuallyDrop<dyn std::any::Any>>, dyn std::any::Any> =
//!         DoDrop::new(Box::new(ManuallyDrop::new(x)) as _);
//!     send(bx).map_err(|e| unsafe { e.downcast_unchecked::<T>() })
//! }
//!
//! fn send<B>(bx: B) -> Result<(), B> {
//!     todo!()
//! }
//! ```
//!
//! The wrapper ensures that, for `T: ?Sized`:
//! 1. If the entire struct is dropped, then the memory held by the `Box<T>` is
//! freed and `<T as    Drop>::drop` is called
//! 2. If used with `DoDrop::downcast_unchecked`, then memory allocated by
//! `Box<T>` is freed, but    `<T as Drop>::drop` is *not* called

use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

#[cfg(not(feature = "smallbox"))]
pub(crate) type DoDropBox<T> = DoDrop<Box<ManuallyDrop<T>>, T>;

#[cfg(feature = "smallbox")]
pub(crate) type DoDropBox<T> = DoDrop<smallbox::SmallBox<ManuallyDrop<T>, [usize; 8]>, T>;

macro_rules! do_drop_box {
    ($ex: expr) => {{
        use do_drop::DoDrop;
        use std::mem::ManuallyDrop;

        // wrap and coerce unsized if needed
        #[cfg(not(feature = "smallbox"))]
        let x = DoDrop::new(Box::new(ManuallyDrop::new($ex)) as _);

        #[cfg(feature = "smallbox")]
        let x = DoDrop::new(smallbox::smallbox!(ManuallyDrop::new($ex)));

        x
    }};
}

pub(crate) struct DoDrop<B, T>
where
    B: DerefMut<Target = ManuallyDrop<T>>,
    T: ?Sized,
{
    bx: B,
}

impl<B, T> DoDrop<B, T>
where
    B: DerefMut<Target = ManuallyDrop<T>>,
    T: ?Sized,
{
    pub(crate) fn new(bx: B) -> Self {
        Self { bx }
    }

    // manually move `bx` out of `self` since `Drop` prevents doing so automatically
    pub(crate) fn into_box(self) -> B {
        let bx = unsafe { std::ptr::read(&self.bx as *const B) };
        std::mem::forget(self);
        bx
    }

    // Safety: the user must ensure that `T: ?Sized` is of type `U`
    pub(crate) unsafe fn downcast_unchecked<U>(self) -> U {
        let inner: &T = &*self;
        let u = std::ptr::read::<U>(inner as *const T as *const U);
        // we still want to drop the container, which at this point holds a
        // `ManuallyDrop<T>` so it will not call `Drop::drop` on `T` a.k.a `U`
        let drop_b_forget_t: B = DoDrop::into_box(self);
        drop(drop_b_forget_t);
        u
    }
}

impl<B, T> Drop for DoDrop<B, T>
where
    B: DerefMut<Target = ManuallyDrop<T>>,
    T: ?Sized,
{
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.bx) }
    }
}

impl<B, T> Deref for DoDrop<B, T>
where
    B: DerefMut<Target = ManuallyDrop<T>>,
    T: ?Sized,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.bx
    }
}

impl<B, T> DerefMut for DoDrop<B, T>
where
    B: DerefMut<Target = ManuallyDrop<T>>,
    T: ?Sized,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bx
    }
}
