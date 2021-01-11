use core::alloc::{
    GlobalAlloc,
    Layout,
};

use sgx_alloc::System;
use sgx_types::*;
use std::cmp;
use std::ptr;
use std::thread;

pub struct Locked<A> {
    inner: spin::Mutex<A>,
}

impl<A> Locked<A> {
    pub const fn new(inner: A) -> Self {
        Locked {
            inner: spin::Mutex::new(inner),
        }
    }

    pub fn lock(&self) -> spin::MutexGuard<A> {
        self.inner.lock()
    }
}

extern "C" {
    pub fn ocall_tc_calloc(nobj: size_t, size: size_t) -> *mut c_void;
    pub fn ocall_tc_malloc(size: size_t) -> *mut c_void;
    pub fn ocall_tc_realloc(p: *mut c_void, size: size_t) -> *mut c_void;
    pub fn ocall_tc_free(p: *mut c_void);
    pub fn ocall_tc_memalign(align: size_t, size: size_t) -> *mut c_void;
}

const TCSNUM: usize = 10;

// The minimum alignment guaranteed by the architecture. This value is used to
// add fast paths for low alignment values. In practice, the alignment is a
// constant at the call site and the branch will be optimized out.
#[cfg(target_arch = "x86")]
const MIN_ALIGN: usize = 8;

// The alignment of sgx tlibc is 16
// https://github.com/intel/linux-sgx/blob/master/sdk/tlibc/stdlib/malloc.c#L541
#[cfg(target_arch = "x86_64")]
const MIN_ALIGN: usize = 16;

pub struct Allocator {
    switch: bool,
    mapper: [sgx_thread_t; TCSNUM],   //match num of TCS
}

impl Allocator {
    pub const fn new() -> Self {
        Allocator {
            switch: false,
            mapper: [0; TCSNUM],
        }
    }

    pub fn set_switch(&mut self, switch: bool) {
        self.switch = switch;
        let cur = thread::rsgx_thread_self(); 
        let (cur_idx, vac_idx) = self.contain_thread_id(cur); 
        if switch {
            if cur_idx == TCSNUM {  // not found
                self.mapper[vac_idx] = cur;
            }
        } else {
            if cur_idx != TCSNUM {
                self.mapper[cur_idx] = 0;
            } 
        }
    }
    
    pub fn get_switch(&self) -> bool {
        let cur = thread::rsgx_thread_self();
        let (cur_idx, _) = self.contain_thread_id(cur);
        let res = (cur_idx != TCSNUM);
        res
    }

    fn contain_thread_id(&self, id: sgx_thread_t) -> (usize, usize) {
        let mut cur_idx: usize = TCSNUM;   //default "not found"
        let mut vac_idx: usize = 0;     //place to insert
        let mut vac_found: bool = false;
        for i in 0..TCSNUM {
            if self.mapper[i] == id {
                cur_idx = i;
            } else if self.mapper[i] == 0 {
                if !vac_found {
                    vac_idx = i;
                    vac_found = true;
                }
            }; 
        }
        (cur_idx, vac_idx)
    }

}

impl Locked<Allocator> {
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

unsafe impl GlobalAlloc for Locked<Allocator> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let bump = self.lock(); // get a mutable reference
        let switch = bump.get_switch();
        if switch {
            if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                ocall_tc_malloc(layout.size()) as *mut u8
            } else {
                aligned_malloc(&layout)
            }
        } else {
            System.alloc(layout)
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let bump = self.lock();
        let switch = bump.get_switch();
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
            System.alloc_zeroed(layout)
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let bump = self.lock();
        let switch = bump.get_switch();
        if switch {
            ocall_tc_free(ptr as *mut c_void);
        } else {
            System.dealloc(ptr, layout);
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let bump = self.lock();
        let switch = bump.get_switch();
        if switch {
            if layout.align() <= MIN_ALIGN && layout.align() <= new_size {
                ocall_tc_realloc(ptr as *mut c_void, new_size) as *mut u8
            } else {
                self.realloc_fallback(ptr, layout, new_size)
            }
        } else {
            System.realloc(ptr, layout, new_size)
        }
    }

}

#[inline]
unsafe fn aligned_malloc(layout: &Layout) -> *mut u8 {
    ocall_tc_memalign(layout.align(), layout.size()) as *mut u8
}