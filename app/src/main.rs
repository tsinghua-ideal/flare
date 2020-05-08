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

fn main() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 12, true);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let col1 = col.map(Fn!(|i| i+1 ));
    let vec_iter = col1.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect().unwrap();
    println!("result: {:?}", res);
    Ok(())
}
