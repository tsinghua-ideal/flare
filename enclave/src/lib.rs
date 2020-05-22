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
#![feature(unboxed_closures)]
#![feature(unsize)]
#![feature(coerce_unsized)]

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
use bytes::{Bytes, BytesMut};

mod basic;
mod common;
mod parallel_collection;
mod mapper;
use crate::common::Common;

#[no_mangle]
pub extern "C" fn secure_executing(ecall_ids: *const u8, id_len: usize, input: *const u8, in_len: usize, output: *mut u8 ) -> usize {
    println!("inside enclave");
    let input_slice = unsafe { slice::from_raw_parts(input, in_len) };
    let id_len = id_len/std::mem::size_of::<usize>();
    let ecall_ids = unsafe { slice::from_raw_parts(ecall_ids as *const usize, id_len) };    
    
    let sc = common::Context::new();
    let op = sc.make_op::<i32>();
    let op1 = op.map(|x| x+1);
    let ser_result = op1.compute_by_id(input_slice, ecall_ids[0]);

    let out_len = ser_result.len();
    let output_slice = unsafe { slice::from_raw_parts_mut( output as * mut u8, out_len as usize) }; 
    output_slice.copy_from_slice(ser_result.as_slice());
    out_len 
}

