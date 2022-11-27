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

use ordered_float::OrderedFloat;
use vega::*;

pub mod benchmarks;
use benchmarks::*;

const NUM_PARTS: usize = 1;
const NUM_PARTS_LOCAL: u64 = 1;

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

    /* sort */
    //sort_by_sec()?;
    //sort_by_unsec()?;

    /* big data bench */
    // aggregate_sec()?;
    // filter_sec()?;
    // cross_project_sec()?;
    // aggregate_unsec()?;
    // filter_unsec()?;
    // cross_project_unsec()?;

    /* join on tpc-h */
    // te1_sec()?;
    // te2_sec()?;
    // te3_sec()?;
    // te1_unsec()?;
    // te2_unsec()?;
    // te3_unsec()?;

    /* tpc-h query */
    // q1_sec()?;
    // q2_sec()?;
    // q3_sec()?;
    // q4_sec()?;
    // q5_sec()?;
    // q6_sec()?;
    // q7_sec()?;
    // q8_sec()?;
    // q9_sec()?;
    // q10_sec()?;
    // q11_sec()?;
    // q12_sec()?;
    // q13_sec()?;
    // q14_sec()?;
    // q15_sec()?;
    // q16_sec()?;
    // q17_sec()?;
    // q18_sec()?;
    // q19_sec()?;
    // q20_sec()?;
    // q21_sec()?;
    // q22_sec()?;
    // q1_unsec()?;
    // q2_unsec()?;
    // q3_unsec()?;
    // q4_unsec()?;
    // q5_unsec()?;
    // q6_unsec()?;
    // q7_unsec()?;
    // q8_unsec()?;
    // q9_unsec()?;
    // q10_unsec()?;
    // q11_unsec()?;
    // q12_unsec()?;
    // q13_unsec()?;
    // q14_unsec()?;
    // q15_unsec()?;
    // q16_unsec()?;
    // q17_unsec()?;
    // q18_unsec()?;
    // q19_unsec()?;
    // q20_unsec()?;
    // q21_unsec()?;
    // q22_unsec()?;

    /* social graph */
    // se1_sec()?;
    // se2_sec()?;
    // se3_sec()?;
    // se1_unsec()?;
    // se2_unsec()?;
    // se3_unsec()?;

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
