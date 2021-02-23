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
    let mut rng = rand::thread_rng();
    let vals: Vec<Vec<f64>> = (0..2_000_000).map(|_| (0..5).map(|_| rng.gen_range(0 as f64, 20 as f64)).collect()).collect();
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
    set_up(bytes, PathBuf::from("/tmp/ct_km"), 1);
    set_up(strings, PathBuf::from("/tmp/pt_km"), 1);

    //pagerank
    let vals: Vec<Vec<u32>> = (0..1_000_000).map(|_| (0..2).map(|_| rng.gen_range(0 as u32, 2_000_000 as u32)).collect()).collect();
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
    set_up(bytes, PathBuf::from("/tmp/ct_pr"), 1);
    set_up(strings, PathBuf::from("/tmp/pt_pr"), 1);

}
