// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License..
#![crate_name = "sparkenclave"]
#![crate_type = "staticlib"]

#![feature(box_syntax)]
#![feature(coerce_unsized)]
#![feature(fn_traits)]
#![feature(map_first_last)]
#![feature(raw)]
#![feature(specialization)]
#![feature(unboxed_closures)]
#![feature(unsize)]
#![feature(vec_into_raw_parts)]

#![feature(
    arbitrary_self_types,
    core_intrinsics,
    binary_heap_into_iter_sorted,
)]

#![allow(non_snake_case)]

#![cfg_attr(not(target_env = "sgx"), no_std)]
#![cfg_attr(target_env = "sgx", feature(rustc_private))]

#[cfg(not(target_env = "sgx"))]
#[macro_use]
extern crate sgx_tstd as std;
extern crate sgx_libc as libc;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate downcast_rs; 
use serde_derive::{Deserialize, Serialize};

use sgx_tcrypto::*;
use std::boxed::Box;
use std::cmp::min;
use std::collections::{btree_map::BTreeMap, HashMap};
use std::sync::{atomic::Ordering, Arc, SgxRwLock as RwLock, SgxMutex as Mutex};
use std::thread;
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;

mod allocator;
use allocator::{Allocator, Locked};
mod aggregator;
mod atomicptr_wrapper;
use atomicptr_wrapper::AtomicPtrWrapper;
mod basic;
mod benchmarks;
use benchmarks::*;
mod dependency;
mod partitioner;
mod op;
use op::*;
mod serialization_free;
mod custom_thread;

#[global_allocator]
static ALLOCATOR: Locked<Allocator> = Locked::new(Allocator::new());
static immediate_cout: bool = true;

struct Captured {
    w: f32,
}

lazy_static! {
    static ref CACHE: OpCache = OpCache::new();
    static ref CAVE: Arc<Mutex<HashMap<u64, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    /// loop boundary: (lower bound after maping, upper bound after mapping, iteration number)
    static ref lp_boundary: AtomicPtrWrapper<Vec<(usize, usize, usize)>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(Vec::new()))); 
    static ref opmap: AtomicPtrWrapper<BTreeMap<usize, Arc<dyn OpBase>>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(BTreeMap::new())));
    static ref w: AtomicPtrWrapper<f32> = AtomicPtrWrapper::new(Box::into_raw(Box::new(1.0)));  
    static ref final_id: usize = {
        /* map */
        //map_sec_0()

        /* group_by */
        //group_by_sec_0()

        /* join */
        //join_sec_0()
        //join_sec_1()
        //join_sec_2()
        
        /* reduce */
        //reduce_sec_0()

        /* count */
        count_sec_0()

        /* linear regression */
        /*
        let sc Context::new();
        let nums = sc.make_op(Box::new(|v: Vec<Point>| v), Box::new(|v: Vec<Point>| v));
        //loop 0 boundary
        let iter_num = 3;   //need to manually change now
        let lower_bound = nums.get_id();  //0 
        let g = nums.map(Box::new(|p: Point|
                        p.x*(1f32/(1f32+(-p.y*(unsafe {*w.load(Ordering::Relaxed)} *p.x)).exp())-1f32)*p.y
                    ),
                    Box::new(|v: Vec<f32>| v),
                    Box::new(|v: Vec<f32>| v),
                )
            .reduce(Box::new(|x, y| x+y));
        let upper_bound =g.get_id();   //1
        unsafe{ lp_boundary.load(Ordering::Relaxed).as_mut()}
            .unwrap()
            .push((lower_bound, upper_bound, iter_num));
        //loop 0 boundary
        g.get_id()
        */

    };
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: f32,
    y: f32,
}

#[no_mangle]
pub extern "C" fn get_lp_boundary() -> usize {
    let lp_bd = unsafe{ lp_boundary.load(Ordering::Relaxed).as_ref()}.unwrap();
    ALLOCATOR.lock().set_switch(true);
    let ptr = Box::into_raw(Box::new(lp_bd.to_vec())) as *mut u8 as usize;
    ALLOCATOR.lock().set_switch(false);
    ptr
}

#[no_mangle]
pub extern "C" fn free_lp_boundary(lp_bd_ptr: *mut u8) {
    ALLOCATOR.lock().set_switch(true);
    let lp_bd = unsafe { Box::from_raw(lp_bd_ptr as *mut Vec<(usize, usize, usize)>) };
    drop(lp_bd);
    ALLOCATOR.lock().set_switch(false);
}

#[no_mangle]
pub extern "C" fn secure_executing(tid: u64, 
                                    rdd_ids: *const u8,
                                    cache_meta: CacheMeta,
                                    is_shuffle: u8, 
                                    input: *mut u8, 
                                    captured_vars: *const u8) -> usize 
{
    let rdd_ids = unsafe { (rdd_ids as *const Vec<(usize, usize)>).as_ref() }.unwrap();
    let final_rdd_id = rdd_ids[0].0;
    let captured_vars = unsafe { (captured_vars as *const HashMap<usize, Vec<u8>>).as_ref() }.unwrap();
    println!("inside enclave id = {:?}, is_shuffle = {:?}, thread_id = {:?}", final_rdd_id, is_shuffle, thread::rsgx_thread_self());
    let _gfinal_id = *final_id; //this is necessary to let it accually execute
    //get current stage's captured vars
    
    //TODO
    match captured_vars.get(&final_rdd_id) {
        Some(var) =>  {
            w.store(&mut bincode::deserialize::<f32>(var).unwrap(), Ordering::Relaxed);
        },
        None => (),
    };
    let now = Instant::now();
    println!("opmap = {:?}", load_opmap().len());
    let mut cache_meta = cache_meta.clone();
    let mut call_seq = NextOpId::new(rdd_ids);
    let final_op = call_seq.get_next_op();
    let result_ptr = final_op.iterator_start(tid, &mut call_seq, input, is_shuffle, &mut cache_meta);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Acc max memory usage: {:?}, in enclave {:?} s", ALLOCATOR.lock().get_max_memory_usage(), dur);
    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn free_res_enc(op_id: usize, is_shuffle: u8, input: *mut u8) {
    let op = load_opmap().get(&op_id).unwrap();
    op.call_free_res_enc(input, is_shuffle);
}

#[no_mangle]
pub extern "C" fn priv_free_res_enc(op_id: usize, is_shuffle: u8, input: *mut u8) {
    let op = load_opmap().get(&op_id).unwrap();
    op.call_free_res_enc(input, is_shuffle);
}

#[no_mangle]
pub extern "C" fn get_sketch(rdd_id: usize, 
                            is_shuffle: u8, 
                            p_buf: *mut u8, 
                            p_data_enc: *mut u8)
{
    let op_id = map_id(rdd_id);
    let op = load_opmap().get(&op_id).unwrap();
    op.build_enc_data_sketch(p_buf, p_data_enc, is_shuffle);
}

#[no_mangle]
pub extern "C" fn clone_out(rdd_id: usize, 
                            is_shuffle: u8,
                            p_out: usize, 
                            p_data_enc: *mut u8)
{
    let op_id = map_id(rdd_id);
    let op = load_opmap().get(&op_id).unwrap();
    op.clone_enc_data_out(p_out, p_data_enc, is_shuffle);
}

#[no_mangle]
pub extern "C" fn pre_touching(zero: u8) -> usize 
{
    println!("thread_id = {:?}", thread::rsgx_thread_self());
    let base = std::enclave::get_heap_base();
    let mut p = base as *mut u8;
    unsafe{ *p += zero; }

    for _i in 0..1<<10 {
        unsafe {
            p = p.offset(4*1024);
            *p += zero;
        }
    }
    println!("finish pre_touching");
    return 0;
}

#[no_mangle]
pub extern "C" fn probe_caching(rdd_id: usize,
    part: usize,
) -> usize {
    let cached = CACHE.get_subpid(rdd_id, part);
    ALLOCATOR.lock().set_switch(true);
    let ptr = Box::into_raw(Box::new(cached.clone()));
    ALLOCATOR.lock().set_switch(false);
    ptr as *mut u8 as usize
}

#[no_mangle]
pub extern "C" fn finish_probe_caching(
    cached_sub_parts: *mut u8,
) {
    let cached_sub_parts = unsafe {
        Box::from_raw(cached_sub_parts as *mut Vec<usize>)
    };
    ALLOCATOR.lock().set_switch(true);
    drop(cached_sub_parts);
    ALLOCATOR.lock().set_switch(false);
}

