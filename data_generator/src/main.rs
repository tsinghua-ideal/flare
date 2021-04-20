#![feature(specialization)]
use std::error::Error;
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::io::prelude::*;
use vega::{Fn, ser_encrypt, ser_decrypt, MAX_ENC_BL};
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
    /*
    //micro_file_reader
    let fixture =
        b"This is some textual test data.\nCan be converted to strings and there are two lines.".to_vec();

    let iteme = ser_encrypt(vec![fixture.clone()]);
    println!("ItemE = {:?}", iteme);
    let bytes = bincode::serialize(&vec![iteme]).unwrap();
    set_up(bytes, PathBuf::from("/opt/data/ct_lf"), 10);
    set_up(fixture, PathBuf::from("/opt/data/pt_lf"), 10);
    
    //k_means
    let (bytes, bytes_enc) = generate_kmeans_data(2_000_000, 5);
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_km"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_km"), 1);

    //pagerank
    let (bytes, bytes_enc) = generate_pagerank_data(1_000_000, true);
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_pr"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_pr"), 1);
    
    let (bytes, bytes_enc) = generate_pagerank_data(10_000_000, false);
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_pr_1"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_pr_1"), 1);

    let (bytes, bytes_enc) = generate_pagerank_data(4_000_000, false);
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_pr_2"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_pr_2"), 1);
    */

    //adapt data of pagerank to opaque
    /* 
    let data = convert_pagerank_data(PathBuf::from("/opt/data/pt_pr"), 1);
    set_up(data, PathBuf::from("/opt/data/pr_opaque"), 1);
    */

    /*
    //tc
    let num_edges = 500;
    let num_vertices = 300;
    let (bytes, bytes_enc) = generate_tc_data(num_edges, num_vertices);
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_tc"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_tc"), 1);
    
    let num_edges = 10_000;
    let num_vertices = 1_000;
    let (bytes, bytes_enc) = generate_tc_data(num_edges, num_vertices);
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_tc_1"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_tc_1"), 1);
    */

    //adapt data of tc to opaque
    let data = convert_tc_data(PathBuf::from("/opt/data/pt_tc_1"), 1);
    set_up(data, PathBuf::from("/opt/data/tc_opaque_1"), 1);

    //dijkstra 
    /*
    let (bytes, bytes_enc) = generate_dijkstra_data();
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_dij"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_dij"), 1);
    */

    //topk
    /* 
    let (bytes, bytes_enc) = generate_topk_data();
    set_up(bytes_enc, PathBuf::from("/opt/data/ct_topk"), 1);
    set_up(bytes, PathBuf::from("/opt/data/pt_topk"), 1);
    */

}

fn generate_dijkstra_data() -> (Vec<u8>, Vec<u8>) {
    /*
    let mut data: Vec<(usize, usize, Option<String>)> = vec![
        (1, 0, Some(String::from("2,10:3,5:"))),
        (2, 999, Some(String::from("3,2:4,1:"))),
        (3, 999, Some(String::from("2,3:4,9:5,2:"))),
        (4, 999, Some(String::from("5,4:"))),
        (5, 999, Some(String::from("1,7:4,6:"))),
    ];
    */
    fn custom_split_nodes_textfile(node: String) -> (usize, usize, Option<String>) {
        let s = node.split(' ').collect::<Vec<_>>();
        if s.len() < 3 {
            let v = s.into_iter().map(|v| v.parse::<usize>().unwrap()).collect::<Vec<_>>();
            (v[0], v[1], None)
        } else {
            (s[0].parse::<usize>().unwrap(), s[1].parse::<usize>().unwrap(), Some(s[2].to_string()))
        }
    }
    let mut f = match File::open("/opt/data/facebook/fb.dat") {
        // `io::Error` 的 `description` 方法返回一个描述错误的字符串。
        Err(why) => panic!("couldn't open for {}", why.to_string()),
        Ok(file) => file,
    };
    let lines = io::BufReader::new(f).lines();
    let mut data = lines.into_iter().map(|line| {
        let line = line.unwrap();
        custom_split_nodes_textfile(line)
    }).collect::<Vec<_>>();

    let bytes = bincode::serialize(&data).unwrap();
    let fe = Fn!(|vp: Vec<(usize, usize, Option<String>)>|{
        ser_encrypt::<>(vp)    
    });
    let fd = Fn!(|ve: Vec<u8>|{  //ItemE = Vec<u8>
        let data: Vec<(usize, usize, Option<String>)> = ser_decrypt::<>(ve);
        data
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

fn generate_pagerank_data(len: usize, is_random: bool) -> (Vec<u8>, Vec<u8>) {
    let vals = match is_random {
        true => {
            let mut rng = rand::thread_rng();
            (0..len).map(|_| (0..2).map(|_| rng.gen_range(0 as u32, 2_000_000 as u32)).collect()).collect()
        },
        false => {
            let sq = (len as f64).sqrt().floor() as u32;
            let mut vals = Vec::new();
            for i in 0..sq {
                let mut v = (0..sq).map(|v| vec![i, v]).collect::<Vec<_>>();
                vals.append(&mut v);
            }
            vals
        }
    };
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

fn convert_pagerank_data(dir: PathBuf, file_num: i32) -> Vec<u8> {
    let mut content = vec![];
    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file = fs::File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        reader.read_to_end(&mut content).unwrap();
    });
    let mut m = HashSet::new();
    let mut lines = String::from_utf8(content)
        .unwrap()
        .lines()
        .map(|s| s.to_string())
        .map(|line| {
            let parts = line.split(" ")
                .collect::<Vec<_>>();
            let src = parts[0].parse::<u32>().unwrap();
            let dst = parts[1].parse::<u32>().unwrap();
            m.insert(src);
            m.insert(dst);
            vec![src, dst, 0]
        }).collect::<Vec<_>>();
    for i in m {
        lines.push(vec![i, i, 1]);
    }
    lines.into_iter()
        .map(|n| n.iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(" ")
        ).collect::<Vec<_>>()
        .join("\n")
        .as_bytes()
        .to_vec()
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

fn convert_tc_data(dir: PathBuf, file_num: i32) -> Vec<u8> {
    let mut content = vec![];
    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file = fs::File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        reader.read_to_end(&mut content).unwrap();
    });
    let data: Vec<(u32, u32)> = bincode::deserialize(&content).unwrap();
    data.into_iter()
        .map(|(from, to)| {
            let v = vec![from.to_string(), to.to_string()];
            v.join(" ")
        }).collect::<Vec<_>>()
        .join("\n")
        .as_bytes()
        .to_vec()
}

fn generate_topk_data() -> (Vec<u8>, Vec<u8>) {
    let mut f = match File::open("/opt/data/ml/ratings.dat") {
        // `io::Error` 的 `description` 方法返回一个描述错误的字符串。
        Err(why) => panic!("couldn't open for {}", why.to_string()),
        Ok(file) => file,
    };
    let lines = io::BufReader::new(f).lines();
    let mut data = lines.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();
    let bytes = bincode::serialize(&data).unwrap();

    let mut len = data.len();
    let mut data_enc = Vec::with_capacity(len);
    while len >= MAX_ENC_BL {
        len -= MAX_ENC_BL;
        let remain = data.split_off(MAX_ENC_BL);
        let input = data;
        data = remain;
        data_enc.push(ser_encrypt(input));
    }
    if len != 0 {
        data_enc.push(ser_encrypt(data));
    }

    let bytes_enc = bincode::serialize(&data_enc).unwrap();
    (bytes, bytes_enc)
}