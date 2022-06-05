use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use crate::*;

use serde_derive::{Deserialize, Serialize};

fn parse_vector(line: String) -> Vec<f64> {
    line.split(' ')
        .map(|number| number.parse::<f64>().unwrap())
        .collect()
}

fn squared_distance(p: &Vec<f64>, center: &Vec<f64>) -> f64 {
    assert_eq!(p.len(), center.len());
    let sum = p.iter().zip(center.iter()).fold(0 as f64, |acc, (x, y)| {
        let delta = *y - *x;
        acc + delta * delta
    });
    sum.sqrt()
}

fn closest_point(p: &Vec<f64>, centers: &Vec<Vec<f64>>) -> usize {
    let mut best_index = 0;
    let mut closest = f64::MAX;

    for (index, center) in centers.iter().enumerate() {
        let temp_dist = squared_distance(p, center);
        if temp_dist < closest {
            closest = temp_dist;
            best_index = index
        }
    }

    best_index
}

fn merge_results(a: (Vec<f64>, i32), b: (Vec<f64>, i32)) -> (Vec<f64>, i32) {
    (
        a.0.iter().zip(b.0.iter()).map(|(x, y)| x + y).collect(),
        a.1 + b.1,
    )
}

// secure mode
pub fn kmeans_sec_0() -> Result<()> {
    let sc = Context::new()?;


    // TODO: need to change dir
    let dir = PathBuf::from("/opt/data/ct_km_41065");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>  
    }));

    let data_rdd = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
            None,
            Some(deserializer),
        )
        .map(Fn!(|line: String| { parse_vector(line) }));


    let k = 10;
    
    let mut k_points = data_rdd.take(k).unwrap();
    
    
    sc.enter_loop();
    //let k_points_ct = k_points.get_ct();
    let k_points_ = k_points.get_pt();
    let closest = data_rdd.map(Fn!(move |p| {
        //need to manually write decryption function
        //let k_points = k_points_ct.clone().into_iter()
        //    .flat_map(|ct| ser_decrypt::<Vec<f64>>(ct).into_iter())
        //    .collect::<Vec<_>>();
        (closest_point(&p, &k_points_), (p, 1))
    }));
    let point_stats = closest.reduce_by_key(Fn!(|(a, b)| merge_results(a, b)), 1);
    let new_points = point_stats
        .map(Fn!(|pair: (usize, (Vec<f64>, i32))| (
            pair.0,
            pair.1
                .0
                .iter()
                .map(|x| x * 1.0 / pair.1 .1 as f64)
                .collect::<Vec<_>>()
        )))
        .collect()
        .unwrap();

    
    
    
    
    
    
    
    
    
    
    
    
    
    

    sc.leave_loop();
    
    
    Ok(())
}