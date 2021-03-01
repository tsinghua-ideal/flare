use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use vega::*;
use rand::Rng;

pub fn union_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let num_edges = 1;
    let num_vertices = 20;
    let mut rng = rand::thread_rng();

    let mut hset = HashSet::new();
    let mut count_edges = 0;
    while count_edges < num_edges {
        let from = rng.gen_range::<_, u32, u32>(0, num_vertices);
        let to = rng.gen_range::<_, u32, u32>(0, num_vertices);
        if from != to {
            count_edges += 1;
            hset.insert((from, to));
        }
    }
    let mut data = hset.into_iter().collect::<Vec<_>>();


    let fe = Fn!(|vp: Vec<(u32, u32)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(u32, u32)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<u32> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_gb = Fn!(|vp: Vec<(u32, Vec<u32>)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd_gb = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(u32, Vec<u32>)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<u32>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let mut len = data.len();
    let mut data_enc = Vec::with_capacity(len);
    while len >= MAX_ENC_BL {
        len -= MAX_ENC_BL;
        let remain = data.split_off(MAX_ENC_BL);
        let input = data;
        data = remain;
        data_enc.push(fe(input));
    }
    if len != 0 {
        data_enc.push(fe(data));
    }
    let now = Instant::now();
    let mut tc = sc.parallelize(vec![], data_enc.clone(), fe.clone(), fd.clone(), 1);
    let tc0 = tc.clone();
    let mut next_count = tc.secure_count().unwrap();
    let mut idx = 0;
    while idx < 100 {
        tc = tc.union(tc0.clone().into());
            //.distinct_with_num_partitions(1);
            //.group_by_key(fe_gb.clone(), fd_gb.clone(), 1)
            //.map(Fn!(|i: (u32, Vec<u32>)| (i.0, i.1[0])), fe.clone(), fd.clone());
            //.map(Fn!(|i: (u32, u32)| (i.0, i.1)), fe.clone(), fd.clone());
        tc.cache();
        next_count = tc.secure_count().unwrap();
        idx += 1; println!("idx = {:?}", idx);
        sc.set_num(0);
    }
    //next_count = tc.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("next count = {:?}, total time {:?}s", next_count, dur);
    Ok(())
}

pub fn union_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let num_edges = 50;
    let num_vertices = 20;
    let mut rng = rand::thread_rng();
    
    let mut hset = HashSet::new();
    let mut count_edges = 0;
    while count_edges < num_edges {
        let from = rng.gen_range::<_, u32, u32>(0, num_vertices);
        let to = rng.gen_range::<_, u32, u32>(0, num_vertices);
        if from != to {
            count_edges += 1;
            hset.insert((from, to));
        }
    }
    let data = hset.into_iter().collect::<Vec<_>>();
    

    let fe = Fn!(|vp: Vec<(u32, u32)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(u32, u32)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<u32> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });
    let now = Instant::now();
    let mut tc = sc.parallelize(data, vec![], fe.clone(), fd.clone(), 1);
    let mut next_count = tc.count().unwrap();
    println!("next_count = {:?}", next_count);
    while next_count < 1000 {
        tc = tc.union(tc.clone().into());
        tc.cache();
        next_count = tc.count().unwrap();
        println!("next_count = {:?}, tc = {:?}", next_count, tc.collect().unwrap());
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}