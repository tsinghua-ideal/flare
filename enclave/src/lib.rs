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

use sgx_types::*;
use sgx_tcrypto::*;
use std::slice;
use std::string::String;
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

use crate::basic::AnyData;
use crate::op::{Context, Op, Pair};

#[no_mangle]
pub extern "C" fn secure_executing(id: usize, 
                                   is_shuffle: u8, 
                                   input: *const u8, 
                                   input_idx: *const usize,
                                   idx_len: usize, 
                                   output: *mut u8, 
                                   output_idx: *mut usize) -> usize 
{
    //println!("inside enclave id = {:?}, is_shuffle = {:?}", id, is_shuffle);
    let ser_data_idx = unsafe { slice::from_raw_parts(input_idx as *const usize, idx_len)};
    let ser_data = unsafe { slice::from_raw_parts(input as *const u8, ser_data_idx[idx_len-1]) };

    /* join */
    let sc = Context::new();
    let col1 = sc.make_op::<(i32, (String, String))>();
    let col2 = sc.make_op::<(i32, String)>();
    let g = col2.join(col1.clone(), 4);

    /* group_by
    let sc = Context::new();
    let r = sc.make_op::<(i32, i32)>();
    let g = r.group_by_key(4);
    */

    /* map
    let col = sc.make_op::<i32>();
    let g = col.map(|i| i+1 );
    */

    let (ser_result, ser_result_idx) = g.compute_by_id(ser_data, ser_data_idx, id, is_shuffle);

    let out_idx_len = ser_result_idx.len();
    let out_idx = unsafe { slice::from_raw_parts_mut(output_idx as * mut usize, out_idx_len as usize) }; 
    out_idx.copy_from_slice(ser_result_idx.as_slice());
    let out = unsafe { slice::from_raw_parts_mut(output as * mut u8, out_idx[out_idx_len-1] as usize) }; 
    out.copy_from_slice(ser_result.as_slice());
    out_idx_len

}


