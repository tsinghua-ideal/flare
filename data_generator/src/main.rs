#![feature(specialization)]
use std::error::Error;
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::io::prelude::*;
use vega::{Data, Fn, ser_encrypt, ser_decrypt, batch_encrypt, MAX_ENC_BL};
use rand::Rng;
use rand_distr::{Normal, Distribution};
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: Vec<f32>,
    y: f32,
}

fn into_file_parts<T: Data>(mut data: Vec<T>, file_num: usize) -> Vec<Vec<u8>> {
    let len_per_file = (data.len() - 1) / file_num + 1;
    let mut ser = Vec::new();
    for i in 0..file_num {
        let mut data_per_file = data.split_off(std::cmp::min(len_per_file, data.len())); 
        std::mem::swap(&mut data_per_file, &mut data);
        ser.push(bincode::serialize(&data_per_file).unwrap());
    }
    ser
}

fn into_file_parts_str(mut data: Vec<String>, file_num: usize) -> Vec<Vec<u8>> {
    let len_per_file = (data.len() - 1) / file_num + 1;
    let mut bytes = Vec::new();
    for i in 0..file_num {
        let mut data_per_file = data.split_off(std::cmp::min(len_per_file, data.len())); 
        std::mem::swap(&mut data_per_file, &mut data);
        bytes.push(data_per_file
            .join("\n")
            .as_bytes()
            .to_vec());
    }
    bytes
}

#[allow(unused_must_use)]
fn set_up(mut data: Vec<Vec<u8>>, dir: PathBuf) {
    println!("Creating tests in dir: {}", (&dir).to_str().unwrap());
    create_dir_all(&dir);
    
    (0..data.len()).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file_name = path.as_path().to_str().unwrap();
        if !std::path::Path::new(file_name).exists() {
            let mut f = File::create(file_name).unwrap();
            f.write_all(&data[idx]).unwrap();
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
    set_up(vec![bytes], PathBuf::from("/opt/data/ct_lf"));
    set_up(vec![fixture], PathBuf::from("/opt/data/pt_lf"));
    */

    /* logistic regression */
    /*
    generate_logistic_regression_data(20_000_000, 1, "lr_20");
    */

    /* matrix multiplication */
    //generate_mm_data(500, "mm_a_500");
    //generate_mm_data(500, "mm_b_500");

    //k_means
    /*
    generate_kmeans_data(2_000_000, 5, "km");
    let data = convert_kmeans_data(PathBuf::from("/opt/data/pt_km"), 16);
    set_up(vec![data], PathBuf::from("/opt/data/km_opaque"));
    */
    
    //pagerank
    /*
    generate_pagerank_data(1_000_000, true, "pr");
    generate_pagerank_data(10_000_000, false, "pr_1");
    generate_pagerank_data(4_000_000, false, "pr_2");
    */
    //adapt data of pagerank to opaque
    /*
    let data = convert_pagerank_data(PathBuf::from("/opt/data/pt_pr_2"), 16);
    set_up(vec![data], PathBuf::from("/opt/data/pr_opaque_2"));
    */

    //pearson
    //generate_pearson_data(10_000_000, "pe_a_107");
    //generate_pearson_data(10_000_000, "pe_b_107");
    generate_pearson_data(10, "pe_a_101");
    generate_pearson_data(10, "pe_b_101");

    //tc
    /*
    let num_edges = 500;
    let num_vertices = 300;
    generate_tc_data(num_edges, num_vertices, "tc");
    
    let num_edges = 10_000;
    let num_vertices = 1_000;
    generate_tc_data(num_edges, num_vertices, "tc_1");
    */
    //adapt data of tc to opaque
    /*
    let data = convert_tc_data(PathBuf::from("/opt/data/pt_tc"), 16);
    set_up(vec![data], PathBuf::from("/opt/data/tc_opaque"));
    */    
    /*
    //dijkstra 
    generate_dijkstra_data("dij_1");
    
    //topk
    generate_topk_data("topk");
    */
}

fn generate_dijkstra_data(s: &str) {
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
    let f = match File::open("/opt/data/facebook/fb.dat") {
        // `io::Error` 的 `description` 方法返回一个描述错误的字符串。
        Err(why) => panic!("couldn't open for {}", why.to_string()),
        Ok(file) => file,
    };
    let lines = io::BufReader::new(f).lines();
    let mut data = lines.into_iter().map(|line| {
        let line = line.unwrap();
        custom_split_nodes_textfile(line)
    }).collect::<Vec<_>>();
    //generate new data
    let mut hs = HashSet::new();
    let mut rng = rand::thread_rng();
    for _ in 0..1_000_000 {
        let pair = (rng.gen_range(4039 as usize, 100_000 as usize), rng.gen_range(4039 as usize, 100_000 as usize));
        hs.insert(pair);
    }
    let mut hm = HashMap::new();
    for pair in hs {
        let e = hm.entry(pair.0).or_insert((3000 as usize, vec![]));
        e.1.push((pair.1, rng.gen_range(1 as usize, 3000 as usize)));
    }
    for i in hm {
        let (p0, (p1, p2)) = i;
        let v = p2.into_iter().map(|(x, y)| {
            let mut s = x.to_string();
            s.push_str(",");
            s.push_str(&y.to_string());
            s
        }).collect::<Vec<_>>();
        let mut s = v.join(":");
        s.push_str(":");
        data.push((p0, p1, Some(s)));
    }
    let zero = &mut data[0];
    let zero_s = zero.2.as_mut().unwrap();
    let mut hs = HashSet::new();
    for _ in 0..100 {
        hs.insert(rng.gen_range(4039 as usize, 100_000 as usize));
    } 
    for i in hs {
        zero_s.push_str(&i.to_string());
        zero_s.push_str(",");
        zero_s.push_str(&rng.gen_range(1 as usize, 3000 as usize).to_string());
        zero_s.push_str(":");
    }

    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts(data.clone(), 16) , PathBuf::from(pt_path));
    let fe = Fn!(|vp: Vec<(usize, usize, Option<String>)>|{
        ser_encrypt::<>(vp)    
    });
    let fd = Fn!(|ve: Vec<u8>|{  //ItemE = Vec<u8>
        let data: Vec<(usize, usize, Option<String>)> = ser_decrypt::<>(ve);
        data
    });
    let data_enc =batch_encrypt(data, fe);
    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));
}

fn generate_logistic_regression_data(num_points: usize, dimension: usize, s: &str) {
    let fe = Fn!(|vp: Vec<Point>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Point> = ser_decrypt::<>(buf0); 
        pt0
    });
    
    let mut rng = rand::thread_rng();
    let normal = Normal::new(0.0 as f32, 1.0).unwrap();
    let mut data: Vec<Point> = Vec::with_capacity(num_points);
    for i in 0..num_points {
        let mut x = Vec::with_capacity(dimension); 
        for j in 0..dimension {
            x.push(normal.sample(&mut rng));
        }    
        let y =  match i % 2 {0 => -1.0, 1 => 1.0, _ => panic!("should not happen")};
        let point = Point {x, y};
        data.push(point);
    } 

    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts(data.clone(), 16) , PathBuf::from(pt_path));

    let data_enc = batch_encrypt(data, fe);
    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));
}

fn generate_mm_data(n: u32, s: &str) {
    let mut rng = rand::thread_rng();
    
    let mut data = Vec::with_capacity((n*n) as usize);
    for i in 0..n {
        data.append(&mut (0..n).map(|j| ((i, j), rng.gen::<f64>())).collect::<Vec<_>>());
    }
    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts(data.clone(), 16), PathBuf::from(pt_path));

    let fe = Fn!(|vp: Vec<((u32, u32), f64)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<((u32, u32), f64)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<(u32, u32)> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let data_enc = batch_encrypt(data, fe);
    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));
}

fn generate_kmeans_data(num_points: usize, dimension: usize, s: &str) {
    let mut rng = rand::thread_rng();
    let mut data: Vec<Vec<f64>> = (0..num_points).map(|_| (0..dimension).map(|_| rng.gen_range(0 as f64, 20 as f64)).collect()).collect();
    let vals = data.clone();
    let data_enc = batch_encrypt(data, Fn!(|x| ser_encrypt(x)));
    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));

    let strings = vals.into_iter()
        .map(|n| n.into_iter()
            .map(|val| val.to_string())
            .collect::<Vec<_>>()
            .join(" ")
        ).collect::<Vec<_>>();

    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts_str(strings, 16), PathBuf::from(pt_path));
}

fn convert_kmeans_data(dir: PathBuf, file_num: i32) -> Vec<u8> {
    let mut lines = Vec::new();
    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file = fs::File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let mut content = vec![];
        reader.read_to_end(&mut content).unwrap();
        lines.append(&mut String::from_utf8(content)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>());
    });
    lines.join("\n")
        .as_bytes()
        .to_vec()
}

fn generate_pagerank_data(len: usize, is_random: bool, s: &str) {
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
        ).collect::<Vec<_>>();

    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));

    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts_str(strings, 16), PathBuf::from(pt_path));
}

fn generate_pearson_data(n: usize, s: &str) {
    let mut rng = rand::thread_rng();

    let data = (0..n).map(|_| rng.gen::<f64>()).collect::<Vec<_>>();
    
    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts(data.clone(), 16), PathBuf::from(pt_path));

    let fe = Fn!(|vp: Vec<f64> | -> Vec<u8> {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| -> Vec<f64> {
        ser_decrypt::<>(ve)
    });

    let data_enc = batch_encrypt(data, fe);
    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));
}

fn convert_pagerank_data(dir: PathBuf, file_num: i32) -> Vec<u8> {
    let mut lines = Vec::new();
    let mut m = HashSet::new();
    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file = fs::File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let mut content = vec![];
        reader.read_to_end(&mut content).unwrap();
        lines.append(&mut String::from_utf8(content)
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
            }).collect::<Vec<_>>());
    });
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

fn generate_tc_data(num_edges: u32, num_vertices: u32, s: &str) {
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
    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts(data.clone(), 16), PathBuf::from(pt_path));

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

    let data_enc = batch_encrypt(data, fe);
    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));
}

fn convert_tc_data(dir: PathBuf, file_num: i32) -> Vec<u8> {
    let mut lines = Vec::new();
    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file = fs::File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let mut content = vec![];
        reader.read_to_end(&mut content).unwrap();
        let data: Vec<(u32, u32)> = bincode::deserialize(&content).unwrap();
        lines.append(&mut data.into_iter()
            .map(|(from, to)| {
                let v = vec![from.to_string(), to.to_string()];
                v.join(" ")
            }).collect::<Vec<_>>());
    });
    lines.join("\n")
        .as_bytes()
        .to_vec()
}

fn generate_topk_data(s: &str) {
    let mut f = match File::open("/opt/data/ml/ratings.dat") {
        // `io::Error` 的 `description` 方法返回一个描述错误的字符串。
        Err(why) => panic!("couldn't open for {}", why.to_string()),
        Ok(file) => file,
    };
    let lines = io::BufReader::new(f).lines();
    let mut data = lines.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();
    let mut pt_path = String::from("/opt/data/pt_");
    pt_path.push_str(s);
    set_up(into_file_parts(data.clone(), 16), PathBuf::from(pt_path));

    let data_enc = batch_encrypt(data, Fn!(|x| ser_encrypt(x)));

    let mut ct_path = String::from("/opt/data/ct_");
    ct_path.push_str(s);
    set_up(into_file_parts(data_enc, 16), PathBuf::from(ct_path));
}