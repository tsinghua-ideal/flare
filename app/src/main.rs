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

use std::time::Instant;
use vega::*;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};

pub mod benchmarks;
use benchmarks::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: f32,
    y: f32,
}


fn main() -> Result<()> {
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.

    /* map */
    //map_sec_0()?;
    //map_unsec_0()?;

    /* group_by */
    //group_by_sec_0()?;
    //group_by_sec_1()?;

    /* join */
    //join_sec_0()?;
    //join_sec_1()?;
    //join_sec_2()?;
    //join_unsec_2()?;

    /* reduce */
    //reduce_sec_0()?;

    /* count */
    //count_unsec_0()?;
    count_sec_0()?;

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
