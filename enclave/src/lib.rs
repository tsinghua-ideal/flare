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
use serialization_free::{Construct, Idx, SizeBuf};
mod custom_thread;

#[global_allocator]
static ALLOCATOR: Locked<Allocator> = Locked::new(Allocator::new());
static immediate_cout: bool = true;

lazy_static! {
    static ref BRANCH_OP_HIS: Arc<RwLock<HashMap<usize, usize>>> = Arc::new(RwLock::new(HashMap::new())); //(cur_op_id, prev_op_id)
    static ref CACHE: OpCache = OpCache::new();
    static ref CAVE: Arc<Mutex<HashMap<u64, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    /// loop boundary: (lower bound after maping, upper bound after mapping, iteration number)
    static ref lp_boundary: AtomicPtrWrapper<Vec<(usize, usize, usize)>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(Vec::new()))); 
    static ref opmap: AtomicPtrWrapper<BTreeMap<usize, Arc<dyn OpBase>>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(BTreeMap::new()))); 
    static ref final_id: usize = {
        /* map */
        //map_sec_0()

        /* group_by */
        //group_by_sec_0()
        //group_by_sec_1()

        /* join */
        //join_sec_0()
        //join_sec_1()
        //join_sec_2()
        
        /* reduce */
        //reduce_sec_0()

        /* count */
        //count_sec_0()

        /* linear regression */
        //lr_sec()

        // test the speculative execution in loop
        test0_sec_0()
    };
}

#[no_mangle]
pub extern "C" fn get_lp_boundary() -> usize {
    let _gfinal_id = *final_id; //this is necessary to let it accually execute
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
pub extern "C" fn secure_execute(tid: u64, 
    rdd_ids: *const u8,
    cache_meta: CacheMeta,
    dep_info: DepInfo, 
    input: *mut u8, 
    captured_vars: *const u8
) -> usize {
    println!("Cur mem: {:?}, at the begining of secure execution", ALLOCATOR.lock().get_memory_usage());
    let rdd_ids = unsafe { (rdd_ids as *const Vec<(usize, usize)>).as_ref() }.unwrap();
    let final_rdd_id = rdd_ids[0].0;
    let captured_vars = unsafe { (captured_vars as *const HashMap<usize, Vec<Vec<u8>>>).as_ref() }.unwrap();
    println!("rdd id = {:?}, dep_info = {:?}, cache_meta = {:?}", final_rdd_id, dep_info, cache_meta);
    
    let now = Instant::now();
    let mut call_seq = NextOpId::new(rdd_ids, cache_meta.clone(), captured_vars.clone(), false);
    let final_op = call_seq.get_cur_op();
    let result_ptr = final_op.iterator_start(tid, &mut call_seq, input, &dep_info); //shuffle need dep_info
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Cur mem: {:?}, secure_execute {:?} s", ALLOCATOR.lock().get_memory_usage(), dur);
    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn exploit_spec_oppty(tid: u64, 
    start_rdd_id: usize,
    cache_meta: CacheMeta,
    dep_info: DepInfo,  
) -> usize {  //return where has an opportunity, if so, return spec_call_seq
    let _gfinal_id = *final_id; //this is necessary to let it accually execute
    let start_op_id = map_id(start_rdd_id);
    let start_op = load_opmap().get(&start_op_id).unwrap();
    if dep_info.dep_type() == 1 {
        start_op.sup_next_shuf_dep(&dep_info);  //set shuf dep if missing when in loop
    }

    let mut spec_op_id = SpecOpId::new(start_rdd_id, cache_meta.clone());
    
    //return 0;
    if spec_op_id.is_end() {
        return 0;
    }

    let mut span = 0;
    let lp_bd = unsafe{ lp_boundary.load(Ordering::Relaxed).as_ref()}.unwrap();
    for (lower, upper, iter_num) in lp_bd {
        if start_op_id > *lower && start_op_id <= *upper {
            span = upper - lower;
            break;
        }
    }

    for _ in 0..span {
        if spec_op_id.advance() {
            break;
        }
    }

    //The last one is the child of shuffle dependency
    let spec_call_seq = spec_op_id.get_spec_call_seq(&dep_info);
    let ptr;
    if spec_call_seq.is_empty() {
        ptr = 0;
    } else {
        //no need to copy out
        ptr = Box::into_raw(Box::new(spec_call_seq.clone())) as *mut u8 as usize;
    }
    ptr
}

#[no_mangle]
pub extern "C" fn spec_execute(tid: u64, 
    spec_call_seq: usize,
    cache_meta: CacheMeta,
    input: *mut u8, 
    parent_rdd_id: *mut usize,
    child_rdd_id: *mut usize,
) -> usize {
    println!("Cur mem: {:?}, at the begining of speculative execution", ALLOCATOR.lock().get_memory_usage());
    let now = Instant::now();
    let mut spec_call_seq = unsafe { Box::from_raw(spec_call_seq as *mut u8 as *mut Vec<usize>) };
    spec_call_seq.reverse();
    let parent_rdd_id = unsafe { parent_rdd_id.as_mut() }.unwrap();
    let child_rdd_id = unsafe { child_rdd_id.as_mut() }.unwrap();
    //the child rdd id of shuffle dependency
    *child_rdd_id = spec_call_seq.remove(0);
    *parent_rdd_id = spec_call_seq[0];
    //identifier is not used, so set it 0 
    let dep_info = DepInfo::new(11, 0, *parent_rdd_id, *child_rdd_id);
    let mut rdd_ids = vec![(*parent_rdd_id, *parent_rdd_id)];
    let mut cur_cont_seg = 0;
    for rdd_id in spec_call_seq.into_iter() {
        let lower = &mut rdd_ids[cur_cont_seg].1;
        if rdd_id == *lower - 1 || rdd_id == *lower {
            *lower = rdd_id;
        } else {
            rdd_ids.push((rdd_id, rdd_id));
            cur_cont_seg += 1;
        }
    }
    let cache_meta = cache_meta.transform();
    let mut call_seq = NextOpId::new(&rdd_ids, cache_meta, HashMap::new(), true);
    let final_op = call_seq.get_cur_op();
    let result_ptr = final_op.iterator_start(tid, &mut call_seq, input, &dep_info);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Cur mem: {:?}, spec_execute {:?} s", ALLOCATOR.lock().get_memory_usage(), dur);
    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn free_res_enc(op_id: usize, dep_info: DepInfo, input: *mut u8, is_spec: u8) {
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
pub extern "C" fn priv_free_res_enc(op_id: usize, dep_info: DepInfo, input: *mut u8) {
    let op = load_opmap().get(&op_id).unwrap();
    op.call_free_res_enc(input, &dep_info);
}

#[no_mangle]
pub extern "C" fn get_sketch(rdd_id: usize, 
    dep_info: DepInfo, 
    p_buf: *mut u8, 
    p_data_enc: *mut u8,
    is_spec: u8)
{
    match is_spec {
        0 => {
            let op_id = map_id(rdd_id);
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
pub extern "C" fn clone_out(rdd_id: usize, 
    dep_info: DepInfo,
    p_out: usize, 
    p_data_enc: *mut u8,
    is_spec: u8)
{
    match is_spec {
        0 => {
            let op_id = map_id(rdd_id);
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

