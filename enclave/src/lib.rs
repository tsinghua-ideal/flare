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

#![feature(allocator_api)]
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
#[macro_use]
extern crate downcast_rs; 
use serde_derive::{Deserialize, Serialize};

use sgx_types::*;
use sgx_tcrypto::*;
use sgx_rand::Rng;
use std::boxed::Box;
use std::cell::Cell;
use std::cmp::min;
use std::collections::{btree_map::BTreeMap, HashMap};
use std::string::String;
use std::sync::{atomic::Ordering, Arc, SgxRwLock as RwLock};
use std::thread;
use std::time::{Duration, Instant};
use std::untrusted::time::InstantEx;
use std::vec::Vec;

mod aggregator;
mod allocator;
use allocator::{Allocator, Locked};
mod atomicptr_wrapper;
use atomicptr_wrapper::AtomicPtrWrapper;
mod basic;
mod dependency;
mod partitioner;
mod op;

use crate::op::{Context, Op, OpE, OpBase, Pair, encrypt, decrypt, divide_ct, recover_ct, map_id, load_opmap};

#[global_allocator]
static ALLOCATOR: Locked<Allocator> = Locked::new(Allocator::new());

struct Captured {
    w: f32,
}

lazy_static! {
    /// loop boundary: (lower bound after maping, upper bound after mapping, iteration number)
    static ref lp_boundary: AtomicPtrWrapper<Vec<(usize, usize, usize)>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(Vec::new()))); 
    static ref opmap: AtomicPtrWrapper<BTreeMap<usize, Arc<dyn OpBase>>> = AtomicPtrWrapper::new(Box::into_raw(Box::new(BTreeMap::new())));
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
    static ref w: AtomicPtrWrapper<f32> = AtomicPtrWrapper::new(Box::into_raw(Box::new(1.0)));  
    static ref final_id: usize = {
        /* map */
        /*
        let rdd0 = sc.make_op(Box::new(|i: Vec<(i32, i32)>| i), Box::new(|i: Vec<(i32, i32)>| i));
        let rdd1 = rdd0.map(Box::new(|i: (i32, i32)| (i.0 + 1, i.1 + 1)), 
            Box::new(|vp: Vec<(i32, i32)>| -> Vec<(Option<Vec<u8>>, i32)>{
                let len = vp.len();
                let mut buf0 = Vec::with_capacity(len);
                let mut buf1 = Vec::with_capacity(len);
                for i in vp {
                    buf0.push(i.0);
                    buf1.push(i.1);
                }
                //in case of group_by
                {
                    let mut buf0_tmp = buf0.clone();
                    buf0_tmp.dedup();
                    if buf0_tmp.len() == 1 {
                        buf0 = buf0_tmp;
                    }
                }
                let enc0 = encrypt::<>(bincode::serialize(&buf0).unwrap().as_ref());
                //let enc1 = encrypt(bincode::serialize(buf1.as_ref()).unwrap().as_ref());
                let buf0 = divide_ct::<>(enc0, len);
                //let buf1 = divide_ct::<>(enc1, len);
                buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
            }), // the second f should be passed in for encryption
            Box::new(|ve: Vec<(Option<Vec<u8>>, i32)>| -> Vec<(i32, i32)> {
                let len = ve.len();
                let mut buf0 = Vec::with_capacity(len*4); //common length 
                let mut buf1 = Vec::with_capacity(len*4); 
                for i in ve {
                    buf0.push(i.0);
                    buf1.push(i.1);
                }
                let enc0 = recover_ct::<>(buf0);
                let mut buf0: Vec<i32> = bincode::deserialize(decrypt::<>(enc0.as_ref()).as_ref()).unwrap(); 
                //let enc1 = recover_ct::<>(buf1);
                //let buf1: Vec<i32> = bincode::deserialize(decrypt::<>(enc1.as_ref()).as_ref()).unwrap(); 
                if buf0.len() == 1 {
                    buf0.resize(buf1.len(), buf0[0].clone());
                }
                buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
            })
        );
        rdd1.get_id()
        */

        /* group_by */
        
        /*
        let rdd0 = sc.make_op(Box::new(|i: Vec<(String, i32)>| i), Box::new(|i: Vec<(String, i32)>| i));
        let rdd1 = rdd0.group_by_key(4);
        rdd1.get_id()
        */

        /* join */
        /*
        let rdd0_fe = Box::new(|vp: Vec<(i32, (String, String))>| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>{
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            // in case of group_by
            {
                let mut buf0_tmp = buf0.clone();
                buf0_tmp.dedup();
                if buf0_tmp.len() == 1 {
                    buf0 = buf0_tmp;
                }
            }
            let enc0 = encrypt::<>(bincode::serialize(&buf0).unwrap().as_ref());
            let enc1 = encrypt(bincode::serialize(&buf1).unwrap().as_ref());
            let buf0 = divide_ct::<>(enc0, len);
            let buf1 = divide_ct::<>(enc1, len);
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        });
    
        let rdd0_fd = Box::new(|ve: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>| -> Vec<(i32, (String, String))> {
            let len = ve.len();
            let mut buf0 = Vec::with_capacity(len*4); //common length 
            let mut buf1 = Vec::with_capacity(len*4); 
            for i in ve {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let enc0 = recover_ct::<>(buf0);
            let mut buf0: Vec<i32> = bincode::deserialize(decrypt::<>(enc0.as_ref()).as_ref()).unwrap(); 
            let enc1 = recover_ct::<>(buf1);
            let buf1: Vec<(String, String)> = bincode::deserialize(decrypt::<>(enc1.as_ref()).as_ref()).unwrap(); 
            // in case of group_by
            if buf0.len() == 1 {
                buf0.resize(buf1.len(), buf0[0].clone());
            }
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        });

        let rdd0 = sc.make_op(rdd0_fe, rdd0_fd);

        let rdd1_fe = Box::new(|vp: Vec<(i32, String)>| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>{
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            // in case of group_by
            {
                let mut buf0_tmp = buf0.clone();
                buf0_tmp.dedup();
                if buf0_tmp.len() == 1 {
                    buf0 = buf0_tmp;
                }
            }
            let enc0 = encrypt::<>(bincode::serialize(&buf0).unwrap().as_ref());
            let enc1 = encrypt(bincode::serialize(&buf1).unwrap().as_ref());
            let buf0 = divide_ct::<>(enc0, len);
            let buf1 = divide_ct::<>(enc1, len);
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        });
    
        let rdd1_fd = Box::new(|ve: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>| -> Vec<(i32, String)> {
            let len = ve.len();
            let mut buf0 = Vec::with_capacity(len*4); //common length 
            let mut buf1 = Vec::with_capacity(len*4); 
            for i in ve {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let enc0 = recover_ct::<>(buf0);
            let mut buf0: Vec<i32> = bincode::deserialize(decrypt::<>(enc0.as_ref()).as_ref()).unwrap(); 
            let enc1 = recover_ct::<>(buf1);
            let buf1: Vec<String> = bincode::deserialize(decrypt::<>(enc1.as_ref()).as_ref()).unwrap(); 
            // in case of group_by
            if buf0.len() == 1 {
                buf0.resize(buf1.len(), buf0[0].clone());
            }
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        });

        let rdd1 = sc.make_op(rdd1_fe, rdd1_fd);
        let rdd2 = rdd1.join(rdd0.clone(), 1);
        rdd2.get_id()
        */

        let fe = Box::new(|vp: Vec<(i32, i32)>| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>{
            let len = vp.len();
            let mut buf0 = Vec::with_capacity(len);
            let mut buf1 = Vec::with_capacity(len);
            for i in vp {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            // in case of group_by
            {
                let mut buf0_tmp = buf0.clone();
                buf0_tmp.dedup();
                if buf0_tmp.len() == 1 {
                    buf0 = buf0_tmp;
                }
            }
            let enc0 = encrypt::<>(bincode::serialize(&buf0).unwrap().as_ref());
            let enc1 = encrypt::<>(bincode::serialize(&buf1).unwrap().as_ref());
            let buf0 = divide_ct::<>(enc0, len);
            let buf1 = divide_ct::<>(enc1, len);
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        });

        let fd = Box::new(|ve: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>| -> Vec<(i32, i32)> {
            let len = ve.len();
            let mut buf0 = Vec::with_capacity(len*4); //common length 
            let mut buf1 = Vec::with_capacity(len*4); 
            for i in ve {
                buf0.push(i.0);
                buf1.push(i.1);
            }
            let enc0 = recover_ct::<>(buf0);
            let mut buf0: Vec<i32> = bincode::deserialize(decrypt::<>(enc0.as_ref()).as_ref()).unwrap(); 
            let enc1 = recover_ct::<>(buf1);
            let buf1: Vec<i32> = bincode::deserialize(decrypt::<>(enc1.as_ref()).as_ref()).unwrap(); 
            // in case of group_by
            if buf0.len() == 1 {
                buf0.resize(buf1.len(), buf0[0].clone());
            }
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        });

        let rdd0 = sc.make_op(fe.clone(), fd.clone());
        let rdd1 = sc.make_op(fe.clone(), fd.clone());
        let rdd2 = rdd1.join(rdd0.clone(), 1);
        rdd2.get_id()
        
        /* reduce */
        /*
        let rdd0 = sc.make_op(Box::new(|i: Vec<i32>| i), Box::new(|i: Vec<i32>| i));
        let rdd1 = rdd0.reduce(Box::new(|x, y| x+y));
        rdd1.get_id()
        */

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
    let result_ptr = op.iterator(input, is_shuffle);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("in enclave {:?} s", dur);

    return result_ptr as usize
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
