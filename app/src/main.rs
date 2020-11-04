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
#![feature(proc_macro_hygiene)]

use std::time::{Duration, Instant};
use vega::*;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};

mod allocator;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: f32,
    y: f32,
}


fn main() -> Result<()> {
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.

    /* map */
    /*
    let sc = Context::new()?;
    let now = Instant::now();
    let rdd0 = sc.make_rdd(vec![], vec![(0, 0), (1, 1)] , Fn!(|i| i), Fn!(|i| i), 1);
    let rdd1 = rdd0.map(Fn!(|i: (i32, i32)| (i.0 + 1, i.1 + 1)), 
        Fn!(|vp: Vec<(i32, i32)>| -> Vec<(Option<Vec<u8>>, i32)>{
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
            //let enc1 = encrypt(bincode::serialize(&buf1).unwrap().as_ref());
            let buf0 = divide_ct::<>(enc0, len);
            //let buf1 = divide_ct::<>(enc1, len);
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        }), // the second f should be passed in for encryption
        Fn!(|ve: Vec<(Option<Vec<u8>>, i32)>| -> Vec<(i32, i32)> {
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
            // in case of group_by
            if buf0.len() == 1 {
                buf0.resize(buf1.len(), buf0[0].clone());
            }
            buf0.into_iter().zip(buf1.into_iter()).collect::<Vec<_>>() 
        })
    );  
    let res = rdd1.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", rdd1.get_fd()(res));
    */

    /* group_by */
    
    /*
    let sc = Context::new()?;
    let vec = vec![
        ("x".to_string(), 1),
        ("x".to_string(), 2),
        ("x".to_string(), 3),
        ("x".to_string(), 4),
        ("x".to_string(), 5),
        ("x".to_string(), 6),
        ("x".to_string(), 7),
        ("y".to_string(), 1),
        ("y".to_string(), 2),
        ("y".to_string(), 3),
        ("y".to_string(), 4),
        ("y".to_string(), 5),
        ("y".to_string(), 6),
        ("y".to_string(), 7),
        ("y".to_string(), 8),
    ];
    let rdd0 = sc.make_rdd(vec![], vec, Fn!(|i| i), Fn!(|i| i), 4);
    let rdd1 = rdd0.group_by_key(4);
    let res = rdd1.secure_collect().unwrap();
    println!("result: {:?}", res);
    */

    /*
    let sc = Context::new()?;
    let len = 1_000_000;
    let mut vec: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for i in (0..len) {
        vec.push((rng.gen(), rng.gen()));
    }
    let r = sc.make_rdd(vec, 1, true);
    let g = r.group_by_key(4);
    let res = g.collect().unwrap();
    println!("result: {:?}", res[0]);
    */

    /* join */
    /*
    let sc = Context::new()?;
    let col0 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    //encrypt first for carrying out experiment
    let rdd0_fe = Fn!(|vp: Vec<(i32, (String, String))>| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>{
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

    let rdd0_fd = Fn!(|ve: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>| -> Vec<(i32, (String, String))> {
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
    let col0_enc = rdd0_fe(col0);

    let rdd0 = sc.parallelize(vec![], 
        col0_enc,
        rdd0_fe,
        rdd0_fd,
        1);
    let col1 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];

    let rdd1_fe = Fn!(|vp: Vec<(i32, String)>| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>{
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

    let rdd1_fd = Fn!(|ve: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>| -> Vec<(i32, String)> {
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

    let col1_enc = rdd1_fe(col1);

    let rdd1 = sc.parallelize(vec![], 
        col1_enc,  
        rdd1_fe,
        rdd1_fd,
        1);
    let rdd2 = rdd1.join(rdd0.clone(), 1);
    let res = rdd2.secure_collect().unwrap();
    println!("result: {:?}", rdd2.get_fd()(res));
    */

    let sc = Context::new()?;
    let len = 1_0000;
    let mut vec0: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut vec1: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec0.push((rng.gen::<i32>() % 100, rng.gen()));
        vec1.push((rng.gen::<i32>() % 100, rng.gen()));
    }

    //println!("vec0 = {:?}, \nvec1 = {:?}", vec0, vec1);

    //encrypt first for carrying out experiment
    let fe = Fn!(|vp: Vec<(i32, i32)>| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>{
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

    let fd = Fn!(|ve: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>| -> Vec<(i32, i32)> {
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
  
    let mut vec0_enc = Vec::with_capacity(len);
    let mut cur = 0;
    while cur < len {
        let next = match cur + MAX_ENC_BL  > len {
            true => len,
            false => cur + MAX_ENC_BL ,
        };
        vec0_enc.append(&mut fe((&vec0[cur..next]).to_vec()));
        cur = next;
    }

    let mut vec1_enc = Vec::with_capacity(len);
    let mut cur = 0;
    while cur < len {
        let next = match cur + MAX_ENC_BL  > len {
            true => len,
            false => cur + MAX_ENC_BL ,
        };
        vec1_enc.append(&mut fe((&vec1[cur..next]).to_vec()));
        cur = next;
    }

    let rdd0 = sc.parallelize(vec![], vec0_enc, fe.clone(), fd.clone(), 1);
    let rdd1 = sc.parallelize(vec![], vec1_enc, fe.clone(), fd.clone(), 1);
    let rdd2 = rdd1.join(rdd0.clone(), 1);
    let res = rdd2.secure_collect().unwrap();
    //println!("result: {:?}", rdd2.batch_decrypt(&res).len());
    //println!("result: {:?}", rdd2.batch_decrypt(&res));

    /* reduce */
    /*
    let sc = Context::new()?;
    let rdd0 = sc.make_rdd(vec![], vec![1i32, 2, 3, 4], Fn!(|i| i), Fn!(|i| i), 2);
    let res = rdd0.secure_reduce(Fn!(|x: i32, y: i32| x + y))?;
    println!("result: {:?}", res);
    */

    /* linear regression */
    /*
    let mut rng = rand::thread_rng();
    let point_num = 1_000_000;
    let mut points: Vec<Point> = Vec::with_capacity(point_num);
    for i in 0..point_num { 
        let point = Point { x: rng.gen(), y: rng.gen() };
        points.push(point);
    } 
    let sc = Context::new()?;
    let points_rdd = sc.make_rdd(vec![], points, Fn!(|i| i), Fn!(|i| i), 1);
    let mut w = rng.gen::<f32>();
    let iter_num = 3;
    let now = Instant::now();
    for i in 0..iter_num {
        let g = points_rdd.map(Fn!(move |p: Point| 
                p.x * (1f32/(1f32+(-p.y * (w * p.x)).exp())-1f32) * p.y
            ),
            Fn!(|v: Vec<f32>|
                v
            ),
            Fn!(|v: Vec<f32>|
                v
            )
        ).secure_reduce(Fn!(|x, y| x+y)).unwrap();
        w -= g.unwrap();
        println!("{:?}: w = {:?}", i, w);
    } 
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("w = {:?}", w);
    */

    Ok(())
}
