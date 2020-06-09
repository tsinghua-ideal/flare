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

use vega::*;
use rand::Rng;

fn main() -> Result<()> {
    //group_by
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
    
    //join
    /*
    let sc = Context::new()?;
    let col1 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    let col1 = sc.parallelize(col1, 1, true);
    let col2 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];
    let col2 = sc.parallelize(col2, 1, true);
    let inner_joined_rdd = col2.join(col1.clone(), 4);
    let res = inner_joined_rdd.collect().unwrap();
    println!("result: {:?}", res);
    */

    //map
    /*
    let sc = Context::new()?;
    let col = sc.make_rdd((0..100).collect::<Vec<_>>(), 1, true);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| i+1 ));
    let res = vec_iter.collect().unwrap();
    println!("result: {:?}", res.last());
    */

    //reduce
    let sc = Context::new()?;
    let nums = sc.make_rdd(vec![1i32, 2, 3, 4], 2, true);
    let res = nums.reduce(Fn!(|x: i32, y: i32| x + y))?;
    println!("result: {:?}", res);
    Ok(())
}
