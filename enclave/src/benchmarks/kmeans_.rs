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
pub fn kmeans_sec_0_(tail_info: &mut TailCompInfo) -> Result<()> {
    let sc = Context::new()?;

    let fe = Fn!(|vp: Vec<Vec<f64>>|{
        ser_encrypt::<>(vp)    
    });

    let fd = Fn!(|ve: Vec<u8>|{  //ItemE = Vec<u8>
        let data: Vec<Vec<f64>> = ser_decrypt::<>(ve);
        data
    });

    let fe_mp = Fn!(|vp: Vec<(usize, (Vec<f64>, i32))>| {
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

    let fd_mp = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<usize> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<(Vec<f64>, i32)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp1 = Fn!(|vp: Vec<(usize, Vec<f64>)>| {
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

    let fd_mp1 = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<usize> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<f64>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    // TODO: need to change dir
    let dir = PathBuf::from("/opt/data/ct_km_41065");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>  
    }));

    let data_rdd = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), None, Some(deserializer), fe, fd);
    

    let k = 10;
    let converge_dist = OText::<f64>::rec(tail_info, None, None);
    let mut k_points = data_rdd.secure_take(k, tail_info).unwrap();
    
    let mut temp_dist = OText::rec(tail_info, None, None);
    
    //let k_points_ct = k_points.get_ct();
    let k_points_ = k_points.get_pt();
    let closest = data_rdd.map(
        Fn!(move |p| {
            //need to manually write decryption function
            //let k_points = k_points_ct.clone().into_iter()
            //    .flat_map(|ct| ser_decrypt::<Vec<f64>>(ct).into_iter())
            //    .collect::<Vec<_>>();
            (closest_point(&p, &k_points_), (p, 1))
        }), 
        fe_mp.clone(), 
        fd_mp.clone());
    let point_stats = closest.reduce_by_key(Fn!(|(a, b)| merge_results(a, b)), 1, fe_mp.clone(), fd_mp.clone());
    let new_points = point_stats.map(
        Fn!(|pair: (usize, (Vec<f64>, i32))|
            (pair.0, pair.1.0.iter().map(|x| x * 1.0 / pair.1.1 as f64).collect::<Vec<_>>())
        ), fe_mp1.clone(), fd_mp1.clone(), 
    ).secure_collect(tail_info).unwrap();
    
    let new_points = (*new_points).clone().into_iter().collect::<HashMap<usize, Vec<f64>>>();
    *temp_dist = 0.0;
    for i in 0..k as usize {
        if new_points.get(&i).is_some() {
            *temp_dist += squared_distance(&k_points[i], &new_points[&i]);
        }
    }

    for (idx, point) in new_points {
        k_points[idx] = point;
    }
        
    tail_info.clear();
    tail_info.insert(&k_points);
    tail_info.insert(&temp_dist);
    
    
    Ok(())
}

pub fn kmeans_sec_1_(tail_info: &mut TailCompInfo) -> Result<()> {
    let sc = Context::new()?;

    let fe = Fn!(|vp: Vec<Vec<u8>>|{
        ser_encrypt::<>(vp)    
    });

    let fd = Fn!(|ve: Vec<u8>|{  //ItemE = Vec<u8>
        let data: Vec<Vec<u8>> = ser_decrypt::<>(ve);
        data
    });

    let fe_mp0 = Fn!(|vp: Vec<Vec<String>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_mp0 = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_fmp = Fn!(|vp: Vec<Vec<f64>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_fmp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<f64>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_mp = Fn!(|vp: Vec<(usize, (Vec<f64>, i32))>| {
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

    let fd_mp = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<usize> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<(Vec<f64>, i32)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp1 = Fn!(|vp: Vec<(usize, Vec<f64>)>| {
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

    let fd_mp1 = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<usize> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<f64>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    // TODO: need to change dir
    let dir = PathBuf::from("/opt/data/ct_km_41065");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>  
    }));

    let lines = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), None, Some(deserializer), fe, fd)
        .map(Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
        }), fe_mp0, fd_mp0);
    let data_rdd = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().map(|line| {
            parse_vector::<>(line)
        })) as Box<dyn Iterator<Item = _>>
    }), fe_fmp, fd_fmp);
    

    let k = 10;
    let converge_dist = OText::<f64>::rec(tail_info, None, None);
    let mut k_points = data_rdd.secure_take_sample(false, k, Some(42), tail_info).unwrap();
    
    let mut temp_dist = OText::rec(tail_info, None, None);
    
    let k_points_ct = k_points.get_ct();
    let closest = data_rdd.map(
        Fn!(move |p| {
            //need to manually write decryption function
            let k_points = k_points_ct.clone().into_iter()
                .flat_map(|ct| ser_decrypt::<Vec<f64>>(ct).into_iter())
                .collect::<Vec<_>>();
            (closest_point(&p, &k_points), (p, 1))
        }), 
        fe_mp.clone(), 
        fd_mp.clone());
    let point_stats = closest.reduce_by_key(Fn!(|(a, b)| merge_results(a, b)), 1, fe_mp.clone(), fd_mp.clone());
    let new_points = point_stats.map(
        Fn!(|pair: (usize, (Vec<f64>, i32))|
            (pair.0, pair.1.0.iter().map(|x| x * 1.0 / pair.1.1 as f64).collect::<Vec<_>>())
        ), fe_mp1.clone(), fd_mp1.clone(), 
    ).secure_collect(tail_info).unwrap();
    
    let new_points = (*new_points).clone().into_iter().collect::<HashMap<usize, Vec<f64>>>();
    *temp_dist = 0.0;
    for i in 0..k as usize {
        if new_points.get(&i).is_some() {
            *temp_dist += squared_distance(&k_points[i], &new_points[&i]);
        }
    }

    for (idx, point) in new_points {
        k_points[idx] = point;
    }
        
    tail_info.clear();
    tail_info.insert(&k_points);
    tail_info.insert(&temp_dist);
    
    
    Ok(())
}

