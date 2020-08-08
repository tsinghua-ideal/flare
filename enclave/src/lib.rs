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
#![feature(coerce_unsized)]
#![feature(fn_traits)]
#![feature(specialization)]
#![feature(unboxed_closures)]
#![feature(unsize)]

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
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
use serde_derive::{Deserialize, Serialize};

use sgx_types::*;
use sgx_tcrypto::*;
use sgx_rand::Rng;
use std::boxed::Box;
use std::cell::Cell;
use std::cmp::min;
use std::collections::{btree_map::BTreeMap, HashMap};
use std::slice;
use std::string::String;
use std::sync::{atomic::{AtomicPtr, Ordering}, Arc, SgxRwLock as RwLock};
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;
use bytes::{Bytes, BytesMut};

mod aggregator;
mod basic;
mod dependency;
mod downcast_rs;
mod dyn_clone;
mod partitioner;
mod op;

use crate::basic::{AnyData, Arc as SerArc};
use crate::op::{Context, Op, OpBase, Pair};

struct Captured {
    w: f32,
}

lazy_static! {
    static ref sig: RwLock<i32> = RwLock::new(0);
    static ref sc: Arc<Context> = Context::new();
    /*
    static ref w: RwLock<f32> = {
        // error: TCS policy - bound
        //let mut rng = sgx_rand::thread_rng();
        //let mut _w = rng.gen::<f32>();
        let _w: f32 = 1.0;
        RwLock::new(_w)
    };
    */
    static ref w: AtomicPtr<f32> = AtomicPtr::new(&mut 1.0);  
   
    static ref opmap: BTreeMap<usize, Arc<dyn OpBase>> = {
        let mut tempHM = BTreeMap::new();
        /* map */
        /*
        let col = sc.make_op::<i32>();
        tempHM.insert(col.get_id(), col.get_op_base());
        let g = col.map(|i| i+1 );
        tempHM.insert(g.get_id(), g.get_op_base()); 
        */

        /* join */
        /*
        let col1 = sc.make_op::<(i32, (String, String))>();
        tempHM.insert(col1.get_id(), col1.get_op_base());
        let col2 = sc.make_op::<(i32, String)>();
        tempHM.insert(col2.get_id(), col2.get_op_base());
        let g = col2.join(col1.clone(), 4);
        tempHM.insert(g.get_id(), g.get_op_base());
        */

        /* group_by */
        /*
        let r = sc.make_op::<(i32, i32)>();
        tempHM.insert(r.get_id(), r.get_op_base());
        let g = r.group_by_key(4);
        tempHM.insert(g.get_id(), g.get_op_base());
        */

        /* reduce */
        /*
        let nums = sc.make_op::<i32>();
        tempHM.insert(nums.get_id(), nums.get_op_base());
        let g = nums.reduce(|x, y| x+y);
        tempHM.insert(g.get_id(), g.get_op_base());
        */

        /* linear regression */
        let nums = sc.make_op::<Point>();
        let iter_num = 3;
        for i in 0..iter_num {
            //make op
            let g = nums.map(|p: Point|
                        p.x*(1f32/(1f32+(-p.y*(unsafe {*w.load(Ordering::Relaxed)} *p.x)).exp())-1f32)*p.y
                    )
                .reduce(|x, y| x+y);
            if i == iter_num - 1 {
                tempHM.insert(g.get_id(), g.get_op_base());
            }
        }
        tempHM
    };   
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: f32,
    y: f32,
}

#[no_mangle]
pub extern "C" fn secure_executing(id: usize, 
                                   is_shuffle: u8, 
                                   input: *const u8, 
                                   input_idx: *const usize,
                                   idx_len: usize, 
                                   output: *mut u8, 
                                   output_idx: *mut usize,
                                   captured_vars: *const u8) -> usize 
{
    //println!("inside enclave id = {:?}, is_shuffle = {:?}", id, is_shuffle);
    let ser_data_idx = unsafe { slice::from_raw_parts(input_idx as *const usize, idx_len)};
    let ser_data = unsafe { slice::from_raw_parts(input as *const u8, ser_data_idx[idx_len-1]) };
    //get current stage's captured vars
    let captured_vars = unsafe { (captured_vars as *const HashMap<usize, Vec<u8>>).as_ref() }.unwrap();
   
    //TODO
    match captured_vars.get(&id) {
        Some(var) =>  {
            w.store(&mut bincode::deserialize::<f32>(var).unwrap(), Ordering::Relaxed);
        },
        None => (),
    };
   
    let now = Instant::now();
    let op = match opmap.iter().find(|&x| x.0>= &id ) {
        Some((k,v)) => v,
        None => panic!("Invalid op id"),
    };
    let (ser_result, ser_result_idx) = op.iterator(ser_data, ser_data_idx, is_shuffle);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("in enclave {:?} s", dur);

    let out_idx_len = ser_result_idx.len();
    let out_idx = unsafe { slice::from_raw_parts_mut(output_idx as * mut usize, out_idx_len as usize) }; 
    out_idx.copy_from_slice(ser_result_idx.as_slice());
    let out = unsafe { slice::from_raw_parts_mut(output as * mut u8, out_idx[out_idx_len-1] as usize) }; 
    out.copy_from_slice(ser_result.as_slice());
    return out_idx_len
}

#[no_mangle]
pub extern "C" fn pre_touching(zero: u8) -> usize 
{
    // 4KB per page
    // 1MB = 1<<8 = 2^8 pages
    let mut p: *mut u8 = std::ptr::null_mut();
    let mut tmp_p: *mut u8 = std::ptr::null_mut();
    {
        // malloc < 256B
        p = Box::into_raw(Box::new(1)); 
        //println!("<256B -> {:?}", p);
    }
    {
        // 256B < malloc <256KB, e.g., 4K: one_page
        tmp_p = Vec::with_capacity(4*1024).as_mut_ptr();
        //println!("4KB -> {:?}", tmp_p);
        p = min(tmp_p, p);
    }
    {
        // malloc > 256KB, e.g., 1M
        tmp_p = Vec::with_capacity(1024 * 1024).as_mut_ptr();
        //println!("1MB -> {:?}", tmp_p);
        p = min(tmp_p, p);
    }
    unsafe{ *p += zero; }

    for _i in (0..1<<10) {
        unsafe {
            p = p.offset(4*1024);
            *p += zero;
        }
    }
    println!("finish pre_touching");
    return 0;
}
