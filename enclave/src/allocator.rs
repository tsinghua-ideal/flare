use core::alloc::{
    AllocRef,
    AllocErr,
    GlobalAlloc,
    Layout,
};
use core::ptr;
use core::sync::atomic::{
    AtomicUsize,
    AtomicPtr,
    Ordering
};
use sgx_alloc::System;
use sgx_types::*;
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
    pub fn ocall_alloc(ret_val: *mut usize,
                       size: usize,
                       align: usize) -> sgx_status_t;
    pub fn ocall_dealloc(ret_val: *mut sgx_status_t,
                         ptr: usize,
                         size: usize,
                         align: usize) -> sgx_status_t;
    pub fn ocall_print_id(ret_val: *mut sgx_status_t, id: usize) -> sgx_status_t;
    pub fn ocall_print_usize(ret_val: *mut sgx_status_t, i: usize) -> sgx_status_t;
    pub fn ocall_print_mapper(ret_val: *mut sgx_status_t, arr: usize) -> sgx_status_t;
}

const TCSNUM: usize = 10;

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
        /*
        let mut rt: sgx_status_t = sgx_status_t::SGX_SUCCESS;
        unsafe { 
            ocall_print_id(&mut rt as *mut sgx_status_t, cur); 
            for i in 0..10 {
                ocall_print_mapper(&mut rt as *mut sgx_status_t, self.mapper[i]);
            }
        }
        */
        let (cur_idx, _) = self.contain_thread_id(cur);
        let res = (cur_idx != TCSNUM);
        /*
        unsafe {
            ocall_print_id(&mut rt as *mut sgx_status_t, cur_idx);
            ocall_print_id(&mut rt as *mut sgx_status_t, res as usize);
        }
        */
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

unsafe impl GlobalAlloc for Locked<Allocator> {
    unsafe fn  alloc(&self, layout: Layout) -> *mut u8 {
        let bump = self.lock(); // get a mutable reference
        let switch = bump.get_switch();
        //let mut rt: sgx_status_t = sgx_status_t::SGX_SUCCESS;
        //ocall_print_usize(&mut rt, switch as usize);
        if switch {
            let mut rt: usize = 0;
            ocall_alloc(&mut rt as *mut usize,
                     layout.size(),
                     layout.align());
            rt as *mut u8
        } else {
            System.alloc(layout)
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let bump = self.lock(); // get a mutable reference
        let switch = bump.get_switch();
        //let mut rt: sgx_status_t = sgx_status_t::SGX_SUCCESS;
        //ocall_print_usize(&mut rt, switch as usize);
        if switch {
            let mut rt: sgx_status_t = sgx_status_t::SGX_SUCCESS;
            ocall_dealloc(&mut rt as *mut sgx_status_t,
                          ptr as usize,
                          layout.size(),
                          layout.align());
        } else {
            System.dealloc(ptr, layout);
        }
    }
}
