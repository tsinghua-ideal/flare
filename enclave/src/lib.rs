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
#![feature(map_first_last)]
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
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate downcast_rs; 
use serde_derive::{Deserialize, Serialize};

use sgx_types::*;
use sgx_tcrypto::*;
use std::boxed::Box;
use std::cmp::min;
use std::collections::{btree_map::BTreeMap, HashMap};
use std::string::String;
use std::sync::{atomic::Ordering, Arc, SgxMutex as Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;

mod aggregator;
mod atomicptr_wrapper;
use atomicptr_wrapper::AtomicPtrWrapper;
mod basic;
mod dependency;
mod partitioner;
mod op;
use op::*;
mod serialization_free;

struct Captured {
    w: f32,
}

lazy_static! {
    static ref CAVE: Arc<Mutex<HashMap<u64, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    /// loop boundary: (lower bound after maping, upper bound after mapping, iteration number)
    static ref lp_boundary: AtomicPtrWrapper<Vec<(usize, usize, usize)>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(Vec::new()))); 
    static ref opmap: AtomicPtrWrapper<BTreeMap<usize, Arc<dyn OpBase>>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(BTreeMap::new())));
    static ref sc: Arc<Context> = Context::new();
    static ref w: AtomicPtrWrapper<f32> = AtomicPtrWrapper::new(Box::into_raw(Box::new(1.0)));  
    static ref final_id: usize = {
        /* map */
        /*
        let fe = Box::new(|vp: Vec<i32>| {
            let buf0 = ser_encrypt::<>(vp);
            buf0
        });
    
        let fd = Box::new(|ve: Vec<u8>| {
            let buf0 = ve;
            let pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            pt0
        });
        let rdd0 = sc.make_op(fe.clone(), fd.clone());
        let rdd1 = rdd0.map(Box::new(|i: i32| i * 4399 / (i % 71 + 1)), fe, fd);
        rdd1.get_id()
        */

        /* group_by */
        /*
        let fe = Box::new(|vp: Vec<(String, i32)>| {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            (buf0, buf1)
        });
    
        let fd = Box::new(|ve: (Vec<u8>, Vec<u8>)| {
            let (buf0, buf1) = ve;
            let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<i32> = ser_decrypt::<>(buf1);
            let len = pt0.len() | pt1.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
        });
    
        let fe_gb = Box::new(|vp: Vec<(String, Vec<i32>)>| {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            (buf0, buf1)
        });
    
        let fd_gb = Box::new(|ve: (Vec<u8>, Vec<u8>)| {
            let (buf0, buf1) = ve;
            let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<Vec<i32>> = ser_decrypt::<>(buf1);
            let len = pt0.len() | pt1.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
        });
        let rdd0 = sc.make_op(fe, fd);
        let rdd1 = rdd0.group_by_key(fe_gb, fd_gb, 4);
        rdd1.get_id()
        */

        /* join */
        /*
        let rdd0_fe = Box::new(|vp: Vec<(i32, (String, String))> | -> (Vec<u8>, (Vec<u8>, Vec<u8>)) {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            let mut buf2 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1.0);
                buf2.push(i.1.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            let buf2 = ser_encrypt::<>(buf2);
            (buf0, (buf1, buf2))
        });
    
        let rdd0_fd = Box::new(|ve: (Vec<u8>, (Vec<u8>, Vec<u8>))| -> Vec<(i32, (String, String))> {
            let (buf0, (buf1, buf2)) = ve;
            let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
            let mut pt2: Vec<String> = ser_decrypt::<>(buf2);
            let len = pt0.len() | pt1.len() | pt2.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt2.resize_with(len, Default::default);
            pt0.into_iter()
                .zip(pt1.into_iter()
                    .zip(pt2.into_iter())
                ).collect::<Vec<_>>() 
        });

        let rdd1_fe = Box::new(|vp: Vec<(i32, String)> | -> (Vec<u8>, Vec<u8>) {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            (buf0, buf1)
        });
    
        let rdd1_fd = Box::new(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(i32, String)> {
            let (buf0, buf1) = ve;
            let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
            let len = pt0.len() | pt1.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
        });

        let fe_jn = Box::new(|vp: Vec<(i32, (String, (String, String)))>| {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            let mut buf2 = Vec::with_capacity(len);
            let mut buf3 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1.0);
                buf2.push(i.1.1.0);
                buf3.push(i.1.1.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            let buf2 = ser_encrypt::<>(buf2);
            let buf3 = ser_encrypt::<>(buf3);
            (buf0, (buf1, (buf2, buf3)))
        });
    
        let fd_jn = Box::new(|ve: (Vec<u8>, (Vec<u8>, (Vec<u8>, Vec<u8>)))| {
            let (buf0, (buf1, (buf2, buf3))) = ve;
            let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
            let mut pt2: Vec<String> = ser_decrypt::<>(buf2);
            let mut pt3: Vec<String> = ser_decrypt::<>(buf3);
            let len = pt0.len() | pt1.len() | pt2.len() | pt3.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt2.resize_with(len, Default::default);
            pt3.resize_with(len, Default::default);
            pt0.into_iter()
                .zip(pt1.into_iter()
                    .zip(pt2.into_iter()
                        .zip(pt3.into_iter())
                    )
                )
                .collect::<Vec<_>>() 
        });
        let rdd0 = sc.make_op(rdd0_fe, rdd0_fd);
        let rdd1 = sc.make_op(rdd1_fe, rdd1_fd);
        let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
        rdd2.get_id()
        */
        
        /*
        let rdd0_fe = Box::new(|vp: Vec<(i32, (String, String))> | -> (Vec<i32>, (Vec<String>, Vec<String>)) {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            let mut buf2 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1.0);
                buf2.push(i.1.1);
            }
            (buf0, (buf1, buf2))
        });
    
        let rdd0_fd = Box::new(|ve: (Vec<i32>, (Vec<String>, Vec<String>))| -> Vec<(i32, (String, String))> {
            let (mut pt0, (mut pt1, mut pt2)) = ve;
            let len = pt0.len() | pt1.len() | pt2.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt2.resize_with(len, Default::default);
            pt0.into_iter()
                .zip(pt1.into_iter()
                    .zip(pt2.into_iter())
                ).collect::<Vec<_>>() 
        });
    
        let rdd1_fe = Box::new(|vp: Vec<(i32, String)> | -> (Vec<i32>, Vec<String>) {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            (buf0, buf1)
        });
    
        let rdd1_fd = Box::new(|ve: (Vec<i32>, Vec<String>)| -> Vec<(i32, String)> {
            let (mut pt0, mut pt1) = ve;
            let len = pt0.len() | pt1.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
        });
    
        let fe_jn = Box::new(|vp: Vec<(i32, (String, (String, String)))>| {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            let mut buf2 = Vec::with_capacity(len);
            let mut buf3 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1.0);
                buf2.push(i.1.1.0);
                buf3.push(i.1.1.1);
            }
            (buf0, (buf1, (buf2, buf3)))
        });
    
        let fd_jn = Box::new(|ve: (Vec<i32>, (Vec<String>, (Vec<String>, Vec<String>)))| {
            let (mut pt0, (mut pt1, (mut pt2, mut pt3))) = ve;
            let len = pt0.len() | pt1.len() | pt2.len() | pt3.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt2.resize_with(len, Default::default);
            pt3.resize_with(len, Default::default);
            pt0.into_iter()
                .zip(pt1.into_iter()
                    .zip(pt2.into_iter()
                        .zip(pt3.into_iter())
                    )
                )
                .collect::<Vec<_>>() 
        });
        let rdd0 = sc.make_op(rdd0_fe, rdd0_fd);
        let rdd1 = sc.make_op(rdd1_fe, rdd1_fd);
        let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
        rdd2.get_id()
        */
        
        /*
        let fe = Box::new(|vp: Vec<(i32, i32)> | -> (Vec<u8>, Vec<u8>) {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            (buf0, buf1)
        });
    
        let fd = Box::new(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(i32, i32)> {
            let (buf0, buf1) = ve;
            let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<i32> = ser_decrypt::<>(buf1);
            let len = pt0.len() | pt1.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
        });
      
        let fe_jn = Box::new(|vp: Vec<(i32, (i32, i32))>| {
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            let mut buf2 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1.0);
                buf2.push(i.1.1);
            }
            let buf0 = ser_encrypt::<>(buf0);
            let buf1 = ser_encrypt::<>(buf1);
            let buf2 = ser_encrypt::<>(buf2);
            (buf0, (buf1, buf2))
        });
    
        let fd_jn = Box::new(|ve: (Vec<u8>, (Vec<u8>, Vec<u8>))| {
            let (buf0, (buf1, buf2)) = ve;
            let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            let mut pt1: Vec<i32> = ser_decrypt::<>(buf1);
            let mut pt2: Vec<i32> = ser_decrypt::<>(buf2);
            let len = pt0.len() | pt1.len() | pt2.len();
            pt0.resize_with(len, Default::default);
            pt1.resize_with(len, Default::default);
            pt2.resize_with(len, Default::default);
            pt0.into_iter()
                .zip(pt1
                    .into_iter()
                    .zip(pt2.into_iter())
                )
                .collect::<Vec<_>>() 
        });

        let rdd0 = sc.make_op(fe.clone(), fd.clone());
        let rdd1 = sc.make_op(fe.clone(), fd.clone());
        let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
        rdd2.get_id()
        */      

        /* reduce */
        
        let fe = Box::new(|vp: Vec<i32>| {
            let buf0 = ser_encrypt::<>(vp);
            buf0
        });
    
        let fd = Box::new(|ve: Vec<u8>| {
            let buf0 = ve;
            let pt0: Vec<i32> = ser_decrypt::<>(buf0); 
            pt0
        });

        let rdd0 = sc.make_op(fe, fd);
        let rdd1 = rdd0.reduce(Box::new(|x, y| x+y));
        rdd1.get_id()
              

        /* linear regression */
        /*
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
pub extern "C" fn secure_executing(id: usize,
                                   tid: u64,
                                   is_shuffle: u8, 
                                   input: *mut u8, 
                                   captured_vars: *const u8) -> usize 
{
    println!("inside enclave id = {:?}, is_shuffle = {:?}, thread_id = {:?}", id, is_shuffle, thread::rsgx_thread_self());
    let _final_id = *final_id; //this is necessary to let it accually execute
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
    let mapped_id = map_id(id);
    println!("mapped_id = {:?}", mapped_id);
    println!("opmap = {:?}", load_opmap().len());
    let op = load_opmap().get(&mapped_id).unwrap();
    let result_ptr = op.iterator(tid, input, is_shuffle);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("in enclave {:?} s", dur);

    return result_ptr as usize
}

#[no_mangle]
pub extern "C" fn get_sketch(id: usize, 
                            is_shuffle: u8, 
                            p_buf: *mut u8, 
                            p_data_enc: *mut u8)
{
    let mapped_id = map_id(id);
    let op = load_opmap().get(&mapped_id).unwrap();
    op.build_enc_data_sketch(p_buf, p_data_enc, is_shuffle);
}

#[no_mangle]
pub extern "C" fn clone_out(id: usize, 
                            is_shuffle: u8,
                            p_out: usize, 
                            p_data_enc: *mut u8)
{
    let mapped_id = map_id(id);
    let op = load_opmap().get(&mapped_id).unwrap();
    op.clone_enc_data_out(p_out, p_data_enc, is_shuffle);
}

#[no_mangle]
pub extern "C" fn pre_touching(zero: u8) -> usize 
{
    println!("thread_id = {:?}", thread::rsgx_thread_self());
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

    for _i in 0..1<<10 {
        unsafe {
            p = p.offset(4*1024);
            *p += zero;
        }
    }
    println!("finish pre_touching");
    return 0;
}
