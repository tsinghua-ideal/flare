use std::sync::atomic::{AtomicPtr, Ordering};
use std::{boxed::Box, ptr};
pub struct AtomicPtrWrapper<T> {
    value : AtomicPtr<T>
}

impl<T> AtomicPtrWrapper<T> {
    pub const fn new(p: *mut T) -> Self {
        AtomicPtrWrapper { value: AtomicPtr::new(p) }
    }
    pub fn load(&self, order: Ordering) -> *mut T {
        self.value.load(order)
    }
    pub fn store(&self, ptr: *mut T, order: Ordering) {
        self.value.store(ptr, order)
    }
}


//For variable free 
impl<T> Drop for AtomicPtrWrapper<T> {
    fn drop(&mut self) {
        unsafe { 
            Box::from_raw(self.value.load(Ordering::Acquire));
        }
        self.value.store(ptr::null_mut(), Ordering::Relaxed);
    }
}
