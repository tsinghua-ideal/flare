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
    static MEM_USAGE: Cell<usize> = Cell::new(0);
    static MAX_MEM_USAGE: Cell<usize> = Cell::new(0);
}

static MEM_TOTAL_USAGE: AtomicUsize = AtomicUsize::new(0);
static MAX_MEM_TOTAL_USAGE: AtomicUsize = AtomicUsize::new(0);

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

    //Memory usage: (local, total)
    pub fn get_memory_usage(&self) -> (usize, usize) {
        (
            MEM_USAGE.with(|f| f.get()),
            MEM_TOTAL_USAGE.load(Ordering::SeqCst),
        )
    }

    pub fn get_max_memory_usage(&self) -> (usize, usize) {
        (
            MAX_MEM_USAGE.with(|f| f.get()),
            MAX_MEM_TOTAL_USAGE.load(Ordering::SeqCst),
        )
    }

    pub fn reset_memory_usage(&self, init_usage: usize) -> (usize, usize) {
        let total_usage = MEM_TOTAL_USAGE.load(Ordering::SeqCst);
        MAX_MEM_TOTAL_USAGE.store(total_usage, Ordering::SeqCst);
        MEM_USAGE.with(|f| {
            f.set(init_usage)
        });
        MAX_MEM_USAGE.with(|f| {
            f.set(init_usage);
        });
        (init_usage, total_usage)
    }

    pub fn reset_max_memory_usage(&self) {
        MAX_MEM_TOTAL_USAGE.store(0, Ordering::SeqCst);
        MAX_MEM_USAGE.with(|f| {
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
            MEM_TOTAL_USAGE.fetch_add(layout.size(), Ordering::Relaxed);
            MAX_MEM_TOTAL_USAGE.fetch_max(MEM_TOTAL_USAGE.load(Ordering::Relaxed), Ordering::Relaxed);
            let usage = MEM_USAGE.with(|f| {
                f.update(|x| x + layout.size())
            });
            MAX_MEM_USAGE.with(|f| {
                f.update(|x| std::cmp::max(x,usage));
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
            MEM_TOTAL_USAGE.fetch_add(layout.size(), Ordering::Relaxed);
            MAX_MEM_TOTAL_USAGE.fetch_max(MEM_TOTAL_USAGE.load(Ordering::Relaxed), Ordering::Relaxed);
            let usage = MEM_USAGE.with(|f| {
                f.update(|x| x + layout.size())
            });
            MAX_MEM_USAGE.with(|f| {
                f.update(|x| std::cmp::max(x,usage));
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
            MEM_TOTAL_USAGE.fetch_sub(layout.size(), Ordering::Relaxed);
            MEM_USAGE.with(|f| {
                let s = layout.size();
                let r = s.saturating_sub(f.get());
                let f = f.update(|x| x.saturating_sub(s));
                f
            });
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
            MEM_TOTAL_USAGE.fetch_add(new_size, Ordering::Relaxed);
            MEM_TOTAL_USAGE.fetch_sub(layout.size(), Ordering::Relaxed);
            MAX_MEM_TOTAL_USAGE.fetch_max(MEM_TOTAL_USAGE.load(Ordering::Relaxed), Ordering::Relaxed);
            let usage = MEM_USAGE.with(|f| {
                let s = layout.size();
                let v = f.update(|x| x + new_size);
                let r = s.saturating_sub(v);
                let f = f.update(|x| x.saturating_sub(s));
                f
            });
            MAX_MEM_USAGE.with(|f| {
                f.update(|x| std::cmp::max(x,usage));
            });
            System.realloc(ptr, layout, new_size)
        }
    }

}

#[inline]
unsafe fn aligned_malloc(layout: &Layout) -> *mut u8 {
    ocall_tc_memalign(layout.align(), layout.size()) as *mut u8
}