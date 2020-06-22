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
use std::collections::HashMap;
use std::slice;
use std::string::String;
use std::sync::Arc;
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

lazy_static! {
    static ref SC: Arc<Context> = Context::new();
    static ref opmap: HashMap<usize, Arc<dyn OpBase>> = {
        let mut tempHM = HashMap::new();
        let sc = SC.clone();
        /* map */
        let col = sc.make_op::<i32>();
        tempHM.insert(col.get_id(), col.get_op_base());
        let g = col.map(|i| i+1 );
        tempHM.insert(g.get_id(), g.get_op_base()); 
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

    /* join */
    /*
    let sc = Context::new();
    let col1 = sc.make_op::<(i32, (String, String))>();
    let col2 = sc.make_op::<(i32, String)>();
    let g = col2.join(col1.clone(), 4);
    */

    /* group_by */
    /*
    let sc = Context::new();
    let r = sc.make_op::<(i32, i32)>();
    let g = r.group_by_key(4);
    */

    /* reduce */
    /*
    let sc = Context::new();
    let nums = sc.make_op::<i32>();
    let g = nums.reduce(|x, y| x+y);
    */

    /*linear regression */
    /*let sc = Context::new();
    let nums = sc.make_op::<f32>();
    let iter_num = 1000;
    for i in 0..iter_num {
        let gradient = nums.map(|p: Point|
            p.x*(1/(1+(-p.y*(w*p.x)).exp())-1)*p.y
        ).reduce(|x, y| x+y);
    */
    let op = match opmap.get(&id) {
        Some(op) => op,
        None => panic!("Invalid op id"),
    };
    let (ser_result, ser_result_idx) = op.iterator(ser_data, ser_data_idx, is_shuffle);

    let out_idx_len = ser_result_idx.len();
    let out_idx = unsafe { slice::from_raw_parts_mut(output_idx as * mut usize, out_idx_len as usize) }; 
    out_idx.copy_from_slice(ser_result_idx.as_slice());
    let out = unsafe { slice::from_raw_parts_mut(output as * mut u8, out_idx[out_idx_len-1] as usize) }; 
    out.copy_from_slice(ser_result.as_slice());
    return out_idx_len
}


