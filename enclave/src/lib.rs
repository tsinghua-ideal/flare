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

#![cfg_attr(not(target_env = "sgx"), no_std)]
#![cfg_attr(target_env = "sgx", feature(rustc_private))]

#[cfg(not(target_env = "sgx"))]
#[macro_use]
extern crate sgx_tstd as std;
#[macro_use]
extern crate serde_derive;

use sgx_types::*;
use sgx_tcrypto::*;
use sgx_trts::memeq::ConsttimeMemEq;
use std::slice;
use std::ptr;
use std::vec::Vec;
use std::string::String;
use std::boxed::Box;
use bytes::{Bytes, BytesMut};

/*
fn run_function(data: Vec<i32>, ecall_ids: Vec<usize>) -> Vec<i32> {
    let data_iter = data.into_iter();
    for id in &ecall_ids {
        match id {
            1 => data_iter.map(|i| i+1 ), 
            2 => data_iter.map(|i| (0..i).collect::<Vec<_>> ()), //expected closure, found a different closure
        }
    }
    data_iter.collect::<Vec<_>>()
}
*/

#[no_mangle]
pub extern "C" fn secure_executing(ecall_ids: *const u8, id_len: usize, input: *const u8, in_len: usize, output: *mut u8 ) -> usize {
    println!("inside enclave");
    let input_slice = unsafe { slice::from_raw_parts(input, in_len) };
    //TODO: get ecall_id

    //TODO: i32 needs to be consistent with type of item in the initial RDD.
    let data: Vec<i32> = bincode::deserialize(input_slice).unwrap();

    //TODO: This closures needs to be consistent with other parts
    let result = data.into_iter()
        .map(|i| i+1 )
        .collect::<Vec<_>>();
    
    let serialized_result: Vec<u8> = bincode::serialize(&result).unwrap();
    let out_len = serialized_result.len();
    let output_slice = unsafe { slice::from_raw_parts_mut( output as * mut u8, out_len as usize) }; 
    //output_slice.copy_from_slice(input_slice);
    output_slice.copy_from_slice(serialized_result.as_slice());
    out_len 
}

