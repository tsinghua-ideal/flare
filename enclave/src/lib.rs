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
#![feature(type_ascription)]
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
use allocator::{Allocator, Locked};
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
static ALLOCATOR: Locked<Allocator> = Locked::new(Allocator::new());
static immediate_cout: bool = true;

lazy_static! {
    static ref BRANCH_OP_HIS: Arc<RwLock<HashMap<OpId, OpId>>> = Arc::new(RwLock::new(HashMap::new())); //(prev_op_id, cur_op_id)
    static ref CACHE: OpCache = OpCache::new();
    static ref CAVE: Arc<Mutex<HashMap<u64, (usize, usize)>>> = Arc::new(Mutex::new(HashMap::new()));  //tid, (remaining_combiner, sorted_max_key)
    static ref OP_MAP: AtomicPtrWrapper<BTreeMap<OpId, Arc<dyn OpBase>>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(BTreeMap::new()))); 
    static ref init: Result<()> = {
        /* dijkstra */
        //dijkstra_sec_0()

        /* map */
        //map_sec_0()
        //map_sec_1()

        /* filter */
        //filter_sec_0()
        
        /* group_by */
        //group_by_sec_0()
        //group_by_sec_1()

        /* join */
        //join_sec_0()
        //join_sec_1()
        //join_sec_2()
        
        /* distinct */
        //distinct_sec_0()
        //distinct_unsec_0()

        /* local file reader */
        //file_read_sec_0()

        /* partition_wise_sample */
        //part_wise_sample_sec_0()

        /* take */
        //take_sec_0()

        /* reduce */
        //reduce_sec_0()

        /* count */
        //count_sec_0()

        /* union */
        //union_sec_0()

        /* kmeans */
        //kmeans_sec_0()
        //kmeans_sec_1()

        /* linear regression */
        //lr_sec()

        /* page rank */
        //pagerank_sec_0()

        /* transitive_closure */
        //transitive_closure_sec_0()
        //transitive_closure_sec_1()
        //transitive_closure_sec_2()

        // test the speculative execution in loop
        //test0_sec_0()

        // topk
        topk_sec_0()
    };
}

#[no_mangle]
pub extern "C" fn secure_execute(tid: u64, 
    rdd_ids: *const u8,
    op_ids: *const u8,
    part_nums: *const u8,
    cache_meta: CacheMeta,
    dep_info: DepInfo, 
    input: Input, 
    captured_vars: *const u8
) -> usize {
    let _init = *init; //this is necessary to let it accually execute
    input.set_init_mem_usage();
    println!("Cur mem: {:?}, at the begining of secure execution", ALLOCATOR.lock().get_memory_usage());
    let rdd_ids = unsafe { (rdd_ids as *const Vec<usize>).as_ref() }.unwrap();
    let op_ids = unsafe { (op_ids as *const Vec<OpId>).as_ref() }.unwrap();
    let part_nums = unsafe { (part_nums as *const Vec<usize>).as_ref() }.unwrap();
    let captured_vars = unsafe { (captured_vars as *const HashMap<usize, Vec<Vec<u8>>>).as_ref() }.unwrap();
    println!("rdd ids = {:?}, part_nums = {:?}, dep_info = {:?}, cache_meta = {:?}", rdd_ids, part_nums, dep_info, cache_meta);
    
    let now = Instant::now();
    let mut call_seq = NextOpId::new(tid, rdd_ids, op_ids, Some(part_nums), cache_meta.clone(), captured_vars.clone(), false);
    let final_op = call_seq.get_cur_op();
    let result_ptr = final_op.iterator_start(&mut call_seq, input, &dep_info); //shuffle need dep_info
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Cur mem: {:?}, secure_execute {:?} s", ALLOCATOR.lock().get_memory_usage(), dur);
    input.set_max_mem_usage();
    println!("Max mem: {:?}", ALLOCATOR.lock().get_max_memory_usage());
    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn exploit_spec_oppty(tid: u64,
    op_ids: *const u8,
    cache_meta: CacheMeta,
    dep_info: DepInfo,  
) -> usize {  //return where has an opportunity, if so, return spec_call_seq
    let _init = *init; //this is necessary to let it accually execute
    let op_ids = unsafe { (op_ids as *const Vec<OpId>).as_ref() }.unwrap();
    if dep_info.dep_type() == 1 {
        let (parent_id, _) = dep_info.get_op_key();
        let parent = load_opmap().get(&parent_id).unwrap();
        parent.sup_next_shuf_dep(&dep_info);  //set shuf dep if missing when in loop
    }
    let len = op_ids.len();
    if len > 1 {
        let mut idx = 0;
        while idx < len - 1 {
            let parent_id = op_ids[idx + 1];
            let child_id = op_ids[idx];
            BRANCH_OP_HIS.write().unwrap().insert(parent_id, child_id);
            let parent = load_opmap().get(&parent_id).unwrap();
            parent.or_insert_nar_child(child_id);
            idx += 1;
        }
    }

    let mut spec_op_id = SpecOpId::new(cache_meta.clone());
    
    //return 0;
    if spec_op_id.is_end() {
        return 0;
    }
    while !spec_op_id.advance(&dep_info) {}

    //The last one is the child of shuffle dependency
    let spec_call_seq = spec_op_id.get_spec_call_seq(&dep_info);
    let ptr;
    if spec_call_seq.0.is_empty() && spec_call_seq.1.is_empty() {
        ptr = 0;
    } else {
        //no need to copy out
        ptr = Box::into_raw(Box::new(spec_call_seq)) as *mut u8 as usize;
    }
    ptr
}

#[no_mangle]
pub extern "C" fn spec_execute(tid: u64, 
    spec_call_seq: usize,
    cache_meta: CacheMeta, 
    hash_ops: *mut u64,
) -> usize {
    println!("Cur mem: {:?}, at the begining of speculative execution", ALLOCATOR.lock().get_memory_usage());
    let now = Instant::now();
    let mut spec_call_seq = unsafe { Box::from_raw(spec_call_seq as *mut u8 as *mut (Vec<usize>, Vec<OpId>)) };
    //should return the hash of op_ids
    let hash_ops = unsafe { hash_ops.as_mut() }.unwrap();
    *hash_ops = default_hash(&spec_call_seq.1);

    //the child rdd id of shuffle dependency
    let child_op_id = {
        spec_call_seq.0.remove(0);
        spec_call_seq.1.remove(0)
    };
    let parent_op_id = spec_call_seq.1[0];
    //identifier is not used, so set it 0 
    let dep_info = DepInfo::new(1, 0, 0, 0, parent_op_id, child_op_id);
    let cache_meta = cache_meta.transform();
    let mut call_seq = NextOpId::new(tid, &spec_call_seq.0, &spec_call_seq.1, None, cache_meta, HashMap::new(), true);
    let final_op = call_seq.get_cur_op();
    let input = Input::padding();
    let result_ptr = final_op.iterator_start(&mut call_seq, input, &dep_info);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Cur mem: {:?}, spec_execute {:?} s", ALLOCATOR.lock().get_memory_usage(), dur);
    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn free_res_enc(op_id: OpId, dep_info: DepInfo, input: *mut u8, is_spec: u8) {
    match is_spec {
        0 => {
            let op = load_opmap().get(&op_id).unwrap();
            op.call_free_res_enc(input, &dep_info);
        },
        1 => {
            let buckets = unsafe {
                Box::from_raw(input as *mut Vec<Vec<u8>>)
            };
            ALLOCATOR.lock().set_switch(true);
            drop(buckets);
            ALLOCATOR.lock().set_switch(false);
        },
        _ => panic!("invalid is_spec"),
    }
}

#[no_mangle]
pub extern "C" fn priv_free_res_enc(op_id: OpId, dep_info: DepInfo, input: *mut u8) {
    let op = load_opmap().get(&op_id).unwrap();
    op.call_free_res_enc(input, &dep_info);
}

#[no_mangle]
pub extern "C" fn get_sketch(op_id: OpId, 
    dep_info: DepInfo, 
    p_buf: *mut u8, 
    p_data_enc: *mut u8,
    is_spec: u8)
{
    match is_spec {
        0 => {
            let op = load_opmap().get(&op_id).unwrap();
            op.build_enc_data_sketch(p_buf, p_data_enc, &dep_info);
        },
        1 => {
            let mut buf = unsafe{ Box::from_raw(p_buf as *mut SizeBuf) };
            let mut idx = Idx::new();
            let buckets_enc = unsafe { Box::from_raw(p_data_enc as *mut Vec<Vec<u8>>) };
            buckets_enc.send(&mut buf, &mut idx);
            forget(buckets_enc);
            forget(buf);
        },
        _ => panic!("invalid is_spec"),
    }

}

#[no_mangle]
pub extern "C" fn clone_out(op_id: OpId, 
    dep_info: DepInfo,
    p_out: usize, 
    p_data_enc: *mut u8,
    is_spec: u8)
{
    match is_spec {
        0 => {
            let op = load_opmap().get(&op_id).unwrap();
            op.clone_enc_data_out(p_out, p_data_enc, &dep_info);
        },
        1 => {
            let mut v_out = unsafe { Box::from_raw(p_out as *mut u8 as *mut Vec<Vec<u8>>) };
            let buckets_enc = unsafe { Box::from_raw(p_data_enc as *mut Vec<Vec<u8>>) };
            v_out.clone_in_place(&buckets_enc);
            forget(v_out);
        },
        _ => panic!("invalid is_spec"),
    }
}

#[no_mangle]
pub extern "C" fn randomize_in_place(
    op_id: OpId,
    input: *const u8,
    seed: u64,
    is_some: u8,
    num: u64,
) -> usize {
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
    //kmeans_sec_1_(&mut tail_info).unwrap();
    ALLOCATOR.lock().set_switch(true);
    let ptr = Box::into_raw(Box::new(tail_info.clone()));
    ALLOCATOR.lock().set_switch(false);
    ptr as *mut u8 as usize
}

#[no_mangle]
pub extern "C" fn free_tail_info(input: *mut u8) {
    let tail_info = unsafe {
        Box::from_raw(input as *mut TailCompInfo)
    };
    ALLOCATOR.lock().set_switch(true);
    drop(tail_info);
    ALLOCATOR.lock().set_switch(false);
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

