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
#![feature(drain_filter)]
#![feature(fn_traits)]
#![feature(map_first_last)]
#![feature(once_cell)]
#![feature(raw)]
#![feature(specialization)]
#![feature(type_ascription)]
#![feature(unboxed_closures)]
#![feature(unsize)]
#![feature(vec_into_raw_parts)]
#![feature(cell_update)]
#![feature(slice_group_by)]

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
use serde_closure::Fn;

use std::boxed::Box;
use std::collections::{btree_map::BTreeMap, HashMap};
use std::mem::forget;
use std::string::{String, ToString};
use std::sync::{atomic::Ordering, Arc, SgxRwLock as RwLock, SgxMutex as Mutex};
use std::thread;
use std::time::Instant;
use std::untrusted::time::InstantEx;
use std::vec::Vec;

mod allocator;
use allocator::Allocator;
mod aggregator;
mod atomicptr_wrapper;
use atomicptr_wrapper::AtomicPtrWrapper;
mod basic;
mod benchmarks;
use benchmarks::*;
mod custom_thread;
mod dependency;
mod partitioner;
mod op;
use op::*;
mod serialization_free;
use serialization_free::{Construct, Idx, SizeBuf};
mod utils;

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
static immediate_cout: bool = true;
const NUM_PARTS: usize = 1;

lazy_static! {
    static ref CACHE: OpCache = OpCache::new();
    static ref OP_MAP: AtomicPtrWrapper<BTreeMap<OpId, Arc<dyn OpBase>>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(BTreeMap::new()))); 
    static ref init: Result<()> = {
        /* dijkstra */
        //dijkstra_sec_0()?;

        /* map */
        //map_sec_0()?;
        //map_sec_1()?;

        /* filter */
        //filter_sec_0()?;
        
        /* group_by */
        //group_by_sec_0()?;
        //group_by_sec_1()?;

        /* join */
        //join_sec_0()?;
        //join_sec_1()?;
        //join_sec_2()?;
        
        /* distinct */
        //distinct_sec_0()?;
        //distinct_unsec_0()?;

        /* local file reader */
        //file_read_sec_0()?;

        /* partition_wise_sample */
        //part_wise_sample_sec_0()?;

        /* take */
        //take_sec_0()?;

        /* reduce */
        //reduce_sec_0()?;

        /* count */
        //count_sec_0()?;

        /* union */
        //union_sec_0()?;

        /* zip */
        //zip_sec_0()?;

        /* kmeans */
        //kmeans_sec_0()?;

        /* linear regression */
        //lr_sec()?;

        /* matrix multipilication */
        //mm_sec_0()?;

        /* page rank */
        //pagerank_sec_0()?;

        /* pearson correlation algorithm */
        pearson_sec_0()?;

        /* transitive_closure */
        //transitive_closure_sec_0()?;
        //transitive_closure_sec_1()?;

        /* triangle counting */
        //triangle_counting_sec_0()?;

        // test the speculative execution in loop
        //test0_sec_0()?;

        // topk
        //topk_sec_0()?;

        Ok(())
    };
}

#[no_mangle]
pub extern "C" fn secure_execute_pre(tid: u64,
    op_ids: *const u8,
    part_nums: *const u8,
    dep_info: DepInfo,
) { 
    let _init = *init; //this is necessary to let it accually execute
    let mut op_ids = unsafe { (op_ids as *const Vec<OpId>).as_ref() }.unwrap().clone();
    let mut part_nums = unsafe { (part_nums as *const Vec<usize>).as_ref() }.unwrap().clone();
    println!("in secure_execute_pre, op_ids = {:?}, part_nums = {:?}", op_ids, part_nums);
    if dep_info.dep_type() == 1 {
        assert!(part_nums.len() == op_ids.len()+1);
        let reduce_num = part_nums.remove(0);
        let (parent_id, _) = dep_info.get_op_key();
        let parent = load_opmap().get(&parent_id).unwrap();
        parent.sup_next_shuf_dep(&dep_info, reduce_num);  //set shuf dep if missing when in loop
    }
    //The header is action id
    if part_nums[0] == usize::MAX {
        op_ids.remove(0);
        part_nums.remove(0);
    }
    let len = op_ids.len();
    let final_op = load_opmap().get(&op_ids[0]).unwrap();
    final_op.fix_split_num(part_nums[0]);
    if len > 1 {
        let mut idx = 0;
        while idx < len - 1 {
            let parent_id = op_ids[idx + 1];
            let child_id = op_ids[idx];
            let parent = load_opmap().get(&parent_id).unwrap();
            parent.fix_split_num(part_nums[idx + 1]);
            parent.sup_next_nar_dep(child_id);
            idx += 1;
        }
    }
}

#[no_mangle]
pub extern "C" fn secure_execute(tid: u64, 
    rdd_ids: *const u8,
    op_ids: *const u8,
    part_ids: *const u8,
    cache_meta: CacheMeta,
    dep_info: DepInfo, 
    input: Input, 
    captured_vars: *const u8,
) -> usize {
    let _init = *init; //this is necessary to let it accually execute
    println!("tid: {:?}, at the begining of secure execution", tid);
    let rdd_ids = unsafe { (rdd_ids as *const Vec<usize>).as_ref() }.unwrap().clone();
    let op_ids = unsafe { (op_ids as *const Vec<OpId>).as_ref() }.unwrap().clone();
    let part_ids = unsafe { (part_ids as *const Vec<usize>).as_ref() }.unwrap().clone();
    let captured_vars = unsafe { (captured_vars as *const HashMap<usize, Vec<Vec<u8>>>).as_ref() }.unwrap().clone();
    println!("tid: {:?}, rdd ids = {:?}, op ids = {:?}, dep_info = {:?}, cache_meta = {:?}", tid, rdd_ids, op_ids, dep_info, cache_meta);
    
    let now = Instant::now();
    let mut call_seq = NextOpId::new(tid, rdd_ids, op_ids, part_ids, cache_meta.clone(), captured_vars, &dep_info);
    let final_op = call_seq.get_cur_op();
    let result_ptr = final_op.iterator_start(call_seq, input, &dep_info); //shuffle need dep_info
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("tid: {:?}, secure_execute {:?} s", tid, dur);
    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn free_res_enc(op_id: OpId, dep_info: DepInfo, input: *mut u8) {
    let _init = *init;
    let op = load_opmap().get(&op_id).unwrap();
    op.call_free_res_enc(input, true, &dep_info);
}

#[no_mangle]
pub extern "C" fn priv_free_res_enc(op_id: OpId, dep_info: DepInfo, input: *mut u8) {
    let _init = *init;
    let op = load_opmap().get(&op_id).unwrap();
    op.call_free_res_enc(input, true, &dep_info);
}

#[no_mangle]
pub extern "C" fn get_sketch(op_id: OpId, 
    dep_info: DepInfo, 
    p_buf: *mut u8, 
    p_data_enc: *mut u8)
{
    let _init = *init;
    let op = load_opmap().get(&op_id).unwrap();
    op.build_enc_data_sketch(p_buf, p_data_enc, &dep_info);
}

#[no_mangle]
pub extern "C" fn clone_out(op_id: OpId, 
    dep_info: DepInfo,
    p_out: usize, 
    p_data_enc: *mut u8)
{
    let _init = *init;
    let op = load_opmap().get(&op_id).unwrap();
    op.clone_enc_data_out(p_out, p_data_enc, &dep_info);
}

#[no_mangle]
pub extern "C" fn randomize_in_place(
    op_id: OpId,
    input: *const u8,
    seed: u64,
    is_some: u8,
    num: u64,
) -> usize {
    let _init = *init;
    let sample_op = load_opmap().get(&op_id).unwrap();
    let seed = match is_some {
        0 => None,
        1 => Some(seed),
        _ => panic!("Invalid is_some"),
    };
    let ptr = sample_op.randomize_in_place(input, seed, num);
    ptr as usize
}

#[no_mangle]
pub extern "C" fn etake(
    op_id: OpId,
    input: *const u8,
    should_take: usize,
    have_take: *mut usize,
) -> usize {
    let _init = *init;
    let take_op = load_opmap().get(&op_id).unwrap();
    let have_take = unsafe { have_take.as_mut() }.unwrap();
    let ptr = take_op.etake(input, should_take, have_take);
    ptr as usize
}

#[no_mangle]
pub extern "C" fn set_sampler(
    op_id: OpId,
    with_replacement: u8,
    fraction: f64,
) {
    let _init = *init;
    let sample_op = load_opmap().get(&op_id).unwrap();
    let with_replacement = match with_replacement {
        0 => false,
        1 => true,
        _ => panic!("Invalid with_replacement"),
    };
    sample_op.set_sampler(with_replacement, fraction);
}

#[no_mangle]
pub extern "C" fn tail_compute(input: *mut u8) -> usize {
    let tail_info = unsafe{ (input as *const TailCompInfo).as_ref() }.unwrap();
    let mut tail_info = tail_info.clone();
    kmeans_sec_0_(&mut tail_info).unwrap();
    ALLOCATOR.set_switch(true);
    let ptr = Box::into_raw(Box::new(tail_info.clone()));
    ALLOCATOR.set_switch(false);
    ptr as *mut u8 as usize
}

#[no_mangle]
pub extern "C" fn free_tail_info(input: *mut u8) {
    let tail_info = unsafe {
        Box::from_raw(input as *mut TailCompInfo)
    };
    ALLOCATOR.set_switch(true);
    drop(tail_info);
    ALLOCATOR.set_switch(false);
}

#[no_mangle]
pub extern "C" fn clear_cache() {
    CACHE.clear();
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

