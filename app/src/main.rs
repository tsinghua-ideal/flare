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
#![feature(specialization)]

use vega::*;

pub mod benchmarks;
use benchmarks::*;



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

    /* distinct */
    //distinct_sec_0()?;
    //distinct_unsec_0()?;

    /* reduce */
    //reduce_sec_0()?;

    /* count */
    //count_sec_0()?;
    //count_unsec_0()?;

    /* linear regression */
    //lr_sec()?;
    //lr_unsec()?;

    /* transitive_closure */
    transitive_closure_sec()?;
    //transitive_closure_unsec()?;

    // test the speculative execution in loop
    //test0_sec_0()?;

    Ok(())
}
