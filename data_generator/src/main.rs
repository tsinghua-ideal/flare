#![feature(specialization)]

use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::io::prelude::*;
use vega::*;
use rand::Rng;

#[allow(unused_must_use)]
fn set_up(data: Vec<u8>, dir: PathBuf, file_num: usize) {
    println!("Creating tests in dir: {}", (&dir).to_str().unwrap());
    create_dir_all(&dir);

    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file_name = path.as_path().to_str().unwrap();
        if !std::path::Path::new(file_name).exists() {
            let mut f = File::create(file_name).unwrap();
            f.write_all(&data).unwrap();
        }
    });

}

fn main() {
    //micro_file_reader
    let fixture =
        b"This is some textual test data.\nCan be converted to strings and there are two lines.".to_vec();

    let iteme = ser_encrypt(vec![fixture.clone()]);
    println!("ItemE = {:?}", iteme);
    let bytes = bincode::serialize(&vec![iteme]).unwrap();
    set_up(bytes, PathBuf::from("/tmp/ct_lf"), 10);
    set_up(fixture, PathBuf::from("/tmp/pt_lf"), 10);
    
    //k_means
    let (bytes, bytes_enc) = generate_kmeans_data(2_000_000, 5);
    set_up(bytes_enc, PathBuf::from("/tmp/ct_km"), 1);
    set_up(bytes, PathBuf::from("/tmp/pt_km"), 1);

    //pagerank
    let (bytes, bytes_enc) = generate_pagerank_data(1_000_000);
    set_up(bytes_enc, PathBuf::from("/tmp/ct_pr"), 1);
    set_up(bytes, PathBuf::from("/tmp/pt_pr"), 1);

    //tc
    let num_edges = 200;
    let num_vertices = 100;
    let (bytes, bytes_enc) = generate_tc_data(num_edges, num_vertices);
    set_up(bytes_enc, PathBuf::from("/tmp/ct_tc"), 1);
    set_up(bytes, PathBuf::from("/tmp/pt_tc"), 1);
    
    let num_edges = 1_000;
    let num_vertices = 1_000;
    let (bytes, bytes_enc) = generate_tc_data(num_edges, num_vertices);
    set_up(bytes_enc, PathBuf::from("/tmp/ct_tc_1"), 1);
    set_up(bytes, PathBuf::from("/tmp/pt_tc_1"), 1);

}

fn generate_kmeans_data(num_points: usize, dimension: usize) -> (Vec<u8>, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let vals: Vec<Vec<f64>> = (0..num_points).map(|_| (0..dimension).map(|_| rng.gen_range(0 as f64, 20 as f64)).collect()).collect();
    let mut iter = vals.chunks(MAX_ENC_BL);
    let mut batch = iter.next();
    let mut data_enc = Vec::new();
    while batch.is_some() {
        let part = batch.unwrap()
            .iter()
            .map(|n| n.iter()
                .map(|val| val.to_string())
                .collect::<Vec<_>>()
                .join(" ")
            ).collect::<Vec<_>>()
            .join("\n")
            .as_bytes()
            .to_vec();
        data_enc.push(ser_encrypt(vec![part]));
        batch = iter.next();
    }

    let strings = vals.into_iter()
        .map(|n| n.into_iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(" ")
        ).collect::<Vec<_>>()
        .join("\n")
        .as_bytes()
        .to_vec();
    let bytes = bincode::serialize(&data_enc).unwrap();
    (strings, bytes)
}

fn generate_pagerank_data(len: usize) -> (Vec<u8>, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let vals: Vec<Vec<u32>> = (0..len).map(|_| (0..2).map(|_| rng.gen_range(0 as u32, 2_000_000 as u32)).collect()).collect();
    let mut iter = vals.chunks(MAX_ENC_BL);
    let mut batch = iter.next();
    let mut data_enc = Vec::new();
    while batch.is_some() {
        let part = batch.unwrap()
            .iter()
            .map(|n| n.iter()
                .map(|val| val.to_string())
                .collect::<Vec<_>>()
                .join(" ")
            ).collect::<Vec<_>>()
            .join("\n")
            .as_bytes()
            .to_vec();
        data_enc.push(ser_encrypt(vec![part]));
        batch = iter.next();
    }

    let strings = vals.into_iter()
        .map(|n| n.iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(" ")
        ).collect::<Vec<_>>()
        .join("\n")
        .as_bytes()
        .to_vec();

    let bytes = bincode::serialize(&data_enc).unwrap();
    (strings, bytes)
}

fn generate_tc_data(num_edges: u32, num_vertices: u32) -> (Vec<u8>, Vec<u8>) {
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
    let bytes = bincode::serialize(&data).unwrap();

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

    let bytes_enc = bincode::serialize(&data_enc).unwrap();
    (bytes, bytes_enc)
}