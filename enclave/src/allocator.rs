use core::alloc::{
    GlobalAlloc,
    Layout,
};
use core::cell::Cell;
use sgx_alloc::System;
use sgx_types::*;
use std::cmp;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    static SWITCH: Cell<bool> = Cell::new(false);
    static ALLOC_CNT: Cell<usize> = Cell::new(0);
}

extern "C" {
    pub fn ocall_tc_calloc(nobj: size_t, size: size_t) -> *mut c_void;
    pub fn ocall_tc_malloc(size: size_t) -> *mut c_void;
    pub fn ocall_tc_realloc(p: *mut c_void, size: size_t) -> *mut c_void;
    pub fn ocall_tc_free(p: *mut c_void);
    pub fn ocall_tc_memalign(align: size_t, size: size_t) -> *mut c_void;
}

// The minimum alignment guaranteed by the architecture. This value is used to
// add fast paths for low alignment values. In practice, the alignment is a
// constant at the call site and the branch will be optimized out.
#[cfg(target_arch = "x86")]
const MIN_ALIGN: usize = 8;

// The alignment of sgx tlibc is 16
// https://github.com/intel/linux-sgx/blob/master/sdk/tlibc/stdlib/malloc.c#L541
#[cfg(target_arch = "x86_64")]
const MIN_ALIGN: usize = 16;

pub struct Allocator;

impl Allocator {
    pub fn set_switch(&self, switch: bool) {
        SWITCH.with(|f| f.set(switch));
    }
    
    pub fn get_switch(&self) -> bool {
        SWITCH.with(|f| f.get())
    }

    pub fn get_alloc_cnt(&self) -> usize {
        ALLOC_CNT.with(|f| f.get())
    }

    pub fn reset_alloc_cnt(&self) {
        ALLOC_CNT.with(|f| {
            f.set(0);
        });
    }

}

impl Allocator {
    unsafe fn realloc_fallback(
        &self,
        ptr: *mut u8,
        old_layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        // Docs for GlobalAlloc::realloc require this to be valid:
        let new_layout = Layout::from_size_align_unchecked(new_size, old_layout.align());

        let new_ptr = GlobalAlloc::alloc(self, new_layout);
        if !new_ptr.is_null() {
            let size = cmp::min(old_layout.size(), new_size);
            ptr::copy_nonoverlapping(ptr, new_ptr, size);
            GlobalAlloc::dealloc(self, ptr, old_layout);
        }
        new_ptr
    }

}

unsafe impl GlobalAlloc for Allocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let switch = self.get_switch();
        if switch {
            if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                ocall_tc_malloc(layout.size()) as *mut u8
            } else {
                aligned_malloc(&layout)
            }
        } else {
            ALLOC_CNT.with(|f| {
                f.update(|x| x + 1);
            });
            System.alloc(layout)
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let switch = self.get_switch();
        if switch {
            if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                ocall_tc_calloc(layout.size(), 1) as *mut u8
            } else {
                let ptr = GlobalAlloc::alloc(self, layout);
                if !ptr.is_null() {
                    ptr::write_bytes(ptr, 0, layout.size());
                }
                ptr
            }
        } else {
            ALLOC_CNT.with(|f| {
                f.update(|x| x + 1);
            });
            System.alloc_zeroed(layout)
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let switch = self.get_switch();
        if switch {
            ocall_tc_free(ptr as *mut c_void);
        } else {
            System.dealloc(ptr, layout);
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let switch = self.get_switch();
        if switch {
            if layout.align() <= MIN_ALIGN && layout.align() <= new_size {
                ocall_tc_realloc(ptr as *mut c_void, new_size) as *mut u8
            } else {
                self.realloc_fallback(ptr, layout, new_size)
            }
        } else {
            ALLOC_CNT.with(|f| {
                f.update(|x| x + 1);
            });
            System.realloc(ptr, layout, new_size)
        }
    }

}

#[inline]
unsafe fn aligned_malloc(layout: &Layout) -> *mut u8 {
    ocall_tc_memalign(layout.align(), layout.size()) as *mut u8
}