use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::io::prelude::*;
use vega::*;
use rand::Rng;

#[allow(unused_must_use)]
fn set_up(data: Vec<u8>, dir: PathBuf) {
    println!("Creating tests in dir: {}", (&dir).to_str().unwrap());
    create_dir_all(&dir);

    (0..10).for_each(|idx| {
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

    set_up(ser_encrypt(fixture.clone()), PathBuf::from("/tmp/ct_lf"));
    set_up(fixture, PathBuf::from("/tmp/pt_lf"));

    //k_means
    let mut rng = rand::thread_rng();
    let vals: Vec<Vec<f64>> = (0..100).map(|_| (0..5).map(|_| rng.gen_range(0 as f64, 20 as f64)).collect()).collect();

    let strings = vals.into_iter()
        .map(|n| n.into_iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(" ")
        ).collect::<Vec<_>>()
        .join("\n")
        .as_bytes()
        .to_vec();

    set_up(ser_encrypt(strings.clone()), PathBuf::from("/tmp/ct_km"));
    set_up(strings, PathBuf::from("/tmp/pt_km"));

    //pagerank
    let vals: Vec<&str> = vec!["1 2", "1 3", "1 4", "2 1", "3 1", "4 1"];

    let strings = vals.join("\n")
        .as_bytes()
        .to_vec();

    set_up(ser_encrypt(strings.clone()), PathBuf::from("/tmp/ct_pr"));
    set_up(strings, PathBuf::from("/tmp/pt_pr"));

}
