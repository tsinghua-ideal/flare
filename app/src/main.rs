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
#![feature(map_first_last)]
#![feature(proc_macro_hygiene)]
#![feature(specialization)]
use std::sync::atomic;

use vega::*;

pub mod benchmarks;
use benchmarks::*;

const NUM_PARTS: usize = 1;

macro_rules! numin {
    () => {{
        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        input.trim().parse().unwrap()
    }};
}

fn main() -> Result<()> {
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    /* dijkstra */
    //dijkstra_sec_0()?;
    //dijkstra_unsec_0()?;

    /* map */
    //map_sec_0()?;
    //map_sec_1()?;
    //map_unsec_0()?;
    //map_unsec_1()?;

    /* filter */
    //filter_sec_0()?;
    //filter_unsec_0()?;

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

    /* local file reader */
    //file_read_sec_0()?;
    //file_read_unsec_0()?;

    /* partition_wise_sample */
    //part_wise_sample_sec_0()?;
    //part_wise_sample_unsec_0()?;
    //part_wise_sample_unsec_1()?;
    //part_wise_sample_unsec_2()?;

    /* take */
    //take_sec_0()?;
    //take_unsec_0()?;

    /* reduce */
    //reduce_sec_0()?;

    /* count */
    //count_sec_0()?;
    //count_unsec_0()?;

    /* union */
    //union_sec_0()?;
    //union_unsec_0()?;

    /* zip */
    //zip_sec_0()?;

    /* kmeans */
    //kmeans_sec_0()?;
    //kmeans_unsec_0()?;

    /* linear regression */
    //lr_sec()?;
    //lr_unsec()?;

    /* matrix multipilication */
    //mm_sec_0()?;
    //mm_unsec_0()?;

    /* page rank */
    //pagerank_sec_0()?;
    //pagerank_unsec_0()?;

    /* pearson correlation algorithm */
    pearson_sec_0()?;
    //pearson_unsec_0()?;

    /* transitive_closure */
    //transitive_closure_sec_0()?;
    //transitive_closure_sec_1()?;
    //transitive_closure_unsec_0()?;
    //transitive_closure_unsec_1()?;

    /* triangle counting */
    //triangle_counting_sec_0()?;
    //triangle_counting_unsec_0()?;

    // test the speculative execution in loop
    //test0_sec_0()?;

    //topk
    //topk_sec_0()?;
    //topk_unsec_0()?;

    Ok(())
}
