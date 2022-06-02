#![feature(specialization)]
use rand::seq::SliceRandom;
use rand::Rng;
use rand_distr::{Distribution, Normal};
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::fs::{create_dir_all, File};
use std::io::prelude::*;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use vega::{batch_encrypt, ser_decrypt, ser_encrypt, Data, Fn, MAX_ENC_BL};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: Vec<f32>,
    y: f32,
}

fn into_file_parts<T: Data>(data: &Vec<T>, file_num: usize) -> Vec<Vec<u8>> {
    let len_per_file = (data.len() - 1) / file_num + 1;
    data.chunks(len_per_file)
        .map(|x| bincode::serialize(x).unwrap())
        .collect::<Vec<_>>()
}

fn from_file_parts<T: Data>(dir: PathBuf, file_num: usize) -> Vec<T> {
    let mut data = Vec::new();
    (0..file_num).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        let file = fs::File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let mut content = vec![];
        reader.read_to_end(&mut content).unwrap();
        let mut block: Vec<T> = bincode::deserialize(&content).unwrap();
        data.append(&mut block);
    });
    data
}

fn encode_str(data: Vec<String>) -> Vec<u8> {
    data.join("\n").as_bytes().to_vec()
}

fn save_encrypted_file<T: Data>(data: &Vec<T>, dir: PathBuf, file_num: usize) {
    let data_enc = batch_encrypt(&data);
    set_up(into_file_parts(&data_enc, file_num), dir);
}

fn save_plain_file<T: Data>(data: &Vec<T>, dir: PathBuf, file_num: usize) {
    set_up(into_file_parts(data, file_num), dir);
}

#[allow(unused_must_use)]
fn set_up(data: Vec<Vec<u8>>, dir: PathBuf) {
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
    /* logistic regression */
    // let data = generate_logistic_regression_data(1_000_000, 2);
    // println!("lr data = {:?}", &data[0..10]);
    // save_plain_file(&data, PathBuf::from("/opt/data/pt_lr_1062"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_lr_1062"), 16);
    // set_up(
    //     vec![encode_str(convert_lr_data(data))],
    //     PathBuf::from("/opt/data/lr_opaque_1062"),
    // );

    /* matrix multiplication */
    //generate_mm_data(2000, 20);
    // let data = from_file_parts::<((u32, u32), f64)>(PathBuf::from("/opt/data/pt_mm_a_2000_20"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_mm_a_2000_20"), 16);
    // let data = from_file_parts::<((u32, u32), f64)>(PathBuf::from("/opt/data/pt_mm_b_20_2000"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_mm_b_20_2000"), 16);
    // let data = from_file_parts::<((u32, u32), f64)>(PathBuf::from("/opt/data/pt_mm_a_100"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_mm_a_100"), 16);
    // let data = from_file_parts::<((u32, u32), f64)>(PathBuf::from("/opt/data/pt_mm_b_100"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_mm_b_100"), 16);

    //k_means

    // let data = generate_kmeans_data(4_000_000, 5);
    // //for fairness, parse vector should be performed
    // let data = convert_kmeans_data(data);
    // save_plain_file(&data, PathBuf::from("/opt/data/pt_km_41065"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_km_41065"), 16);
    // set_up(
    //     vec![encode_str(data)],
    //     PathBuf::from("/opt/data/km_opaque_41065"),
    // );

    //pearson
    // generate_pearson_data(10_000_000);
    // let data = from_file_parts::<f64>(PathBuf::from("/opt/data/pt_pe_a_105"), 16);
    // println!("pe data = {:?}", &data[0..10]);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_pe_a_105"), 16);
    // let data = from_file_parts::<f64>(PathBuf::from("/opt/data/pt_pe_b_105"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_pe_b_105"), 16);
    // let data = from_file_parts::<f64>(PathBuf::from("/opt/data/pt_pe_a_108"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_pe_a_108"), 16);
    // let data = from_file_parts::<f64>(PathBuf::from("/opt/data/pt_pe_b_108"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_pe_b_108"), 16);

    //pagerank
    // let data = generate_pagerank_data(Some("/opt/data/pr_opaque_cit-Patents/opaque"), 0, false);
    // println!("pr data = {:?}", &data[0..10]);
    // let data = convert_pagerank_data(data, false);
    // save_plain_file(&data, PathBuf::from("/opt/data/pt_pr_cit-Patents"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_pr_cit-Patents"), 16);
    // set_up(
    //     vec![encode_str(data)],
    //     PathBuf::from("/opt/data/pr_opaque_cit-Patents"),
    // );

    // let data = generate_pagerank_data(Some("/opt/data/pr_opaque/opaque"), 0, false);
    // let data = convert_pagerank_data(data, false);
    // save_plain_file(&data, PathBuf::from("/opt/data/pt_pr"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_pr"), 16);
    // set_up(vec![encode_str(data)], PathBuf::from("/opt/data/pr_opaque"));

    //tc
    //generate_tc_data(Some("musae_facebook_edges.txt"), 0, 0);
    // let data = from_file_parts::<(u32, u32)>(PathBuf::from("/opt/data/pt_tc_2"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_tc_2"), 16);
    // let data = from_file_parts::<(u32, u32)>(PathBuf::from("/opt/data/pt_tc_7"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_tc_7"), 16);
    // let data = from_file_parts::<(u32, u32)>(PathBuf::from("/opt/data/pt_tc_fb"), 16);
    // save_encrypted_file(&data, PathBuf::from("/opt/data/ct_tc_fb"), 16);
    // let data =
    //     from_file_parts::<(u32, u32)>(PathBuf::from("/opt/data/pt_tri_soc-Slashdot0811"), 16);
    // save_encrypted_file(
    //     &data,
    //     PathBuf::from("/opt/data/ct_tri_soc-Slashdot0811"),
    //     16,
    // );

    //dijkstra
    let data = generate_dijkstra_data(Some(
        "/opt/data/dij_opaque_fb/processed_facebook_combined.txt",
    ));
    save_plain_file(&data, PathBuf::from("/opt/data/pt_dij_fb"), 16);
    save_encrypted_file(&data, PathBuf::from("/opt/data/ct_dij_fb"), 16);

    //topk
    //generate_topk_data();
}

fn generate_dijkstra_data(true_data: Option<&str>) -> Vec<(usize, usize, Option<String>)> {
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
            let v = s
                .into_iter()
                .map(|v| v.parse::<usize>().unwrap())
                .collect::<Vec<_>>();
            (v[0], v[1], None)
        } else {
            (
                s[0].parse::<usize>().unwrap(),
                s[1].parse::<usize>().unwrap(),
                Some(s[2].to_string()),
            )
        }
    }
    match true_data {
        Some(path) => {
            let file = fs::File::open(PathBuf::from(path)).unwrap();
            let mut reader = BufReader::new(file);
            let mut content = vec![];
            reader.read_to_end(&mut content).unwrap();
            String::from_utf8(content)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .map(|line| custom_split_nodes_textfile(line))
                .collect::<Vec<_>>()
        }
        None => {
            let f = match File::open("/opt/data/true_data/processed_facebook_combined.txt") {
                // `io::Error` 的 `description` 方法返回一个描述错误的字符串。
                Err(why) => panic!("couldn't open for {}", why.to_string()),
                Ok(file) => file,
            };
            let lines = io::BufReader::new(f).lines();
            let mut data = lines
                .into_iter()
                .map(|line| {
                    let line = line.unwrap();
                    custom_split_nodes_textfile(line)
                })
                .collect::<Vec<_>>();
            //generate new data
            let mut hs = HashSet::new();
            let mut rng = rand::thread_rng();
            for _ in 0..1_000_000 {
                let pair = (
                    rng.gen_range(4039 as usize, 100_000 as usize),
                    rng.gen_range(4039 as usize, 100_000 as usize),
                );
                hs.insert(pair);
            }
            let mut hm = HashMap::new();
            for pair in hs {
                let e = hm.entry(pair.0).or_insert((3000 as usize, vec![]));
                e.1.push((pair.1, rng.gen_range(1 as usize, 3000 as usize)));
            }
            for i in hm {
                let (p0, (p1, p2)) = i;
                let v = p2
                    .into_iter()
                    .map(|(x, y)| {
                        let mut s = x.to_string();
                        s.push_str(",");
                        s.push_str(&y.to_string());
                        s
                    })
                    .collect::<Vec<_>>();
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
            data
        }
    }
}

fn generate_logistic_regression_data(num_points: usize, dimension: usize) -> Vec<Point> {
    let mut rng = rand::thread_rng();
    let normal = Normal::new(0.0 as f32, 1.0).unwrap();
    let mut data: Vec<Point> = Vec::with_capacity(num_points);
    for i in 0..num_points {
        let mut x = Vec::with_capacity(dimension);
        for j in 0..dimension {
            x.push(normal.sample(&mut rng));
        }
        let y = match i % 2 {
            0 => -1.0,
            1 => 1.0,
            _ => panic!("should not happen"),
        };
        let point = Point { x, y };
        data.push(point);
    }
    data
}

fn convert_lr_data(data: Vec<Point>) -> Vec<String> {
    data.into_iter()
        .map(|p| {
            let mut v = p.x.iter().map(|v| v.to_string()).collect::<Vec<_>>();
            v.push(p.y.to_string());
            v.join(" ")
        })
        .collect::<Vec<_>>()
}

fn generate_mm_data(row: u32, col: u32) -> Vec<((u32, u32), f64)> {
    let mut rng = rand::thread_rng();
    let mut data = Vec::with_capacity((row * col) as usize);
    for i in 0..row {
        data.append(
            &mut (0..col)
                .map(|j| ((i, j), rng.gen::<f64>()))
                .collect::<Vec<_>>(),
        );
    }
    data
}

fn convert_mm_data(data: Vec<((u32, u32), f64)>) -> Vec<String> {
    data.into_iter()
        .map(|((i, j), v)| {
            let v = vec![i.to_string(), j.to_string(), v.to_string()];
            v.join(" ")
        })
        .collect::<Vec<_>>()
}

fn generate_kmeans_data(num_points: usize, dimension: usize) -> Vec<Vec<f64>> {
    let mut rng = rand::thread_rng();
    let data: Vec<Vec<f64>> = (0..num_points)
        .map(|_| {
            (0..dimension)
                .map(|_| rng.gen_range(0 as f64, 20 as f64))
                .collect()
        })
        .collect();
    data
}

fn convert_kmeans_data(data: Vec<Vec<f64>>) -> Vec<String> {
    data.into_iter()
        .map(|n| {
            n.into_iter()
                .map(|val| val.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
}

fn generate_pagerank_data(true_data: Option<&str>, len: usize, is_random: bool) -> Vec<Vec<u32>> {
    match true_data {
        Some(path) => {
            let file = fs::File::open(PathBuf::from(path)).unwrap();
            let mut reader = BufReader::new(file);
            let mut content = vec![];
            reader.read_to_end(&mut content).unwrap();
            String::from_utf8(content)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .map(|line| {
                    let parts = line.split(" ").collect::<Vec<_>>();
                    let src = parts[0].parse::<u32>().unwrap();
                    let dst = parts[1].parse::<u32>().unwrap();
                    vec![src, dst]
                })
                .filter(|x| x[0] != x[1])
                .collect::<Vec<_>>()
        }
        None => match is_random {
            true => {
                let mut rng = rand::thread_rng();
                (0..len)
                    .map(|_| {
                        (0..2)
                            .map(|_| rng.gen_range(0 as u32, 2_000_000 as u32))
                            .collect()
                    })
                    .collect()
            }
            false => {
                let sq = (len as f64).sqrt().floor() as u32;
                let mut vals = Vec::new();
                for i in 0..sq {
                    let mut v = (0..sq).map(|v| vec![i, v]).collect::<Vec<_>>();
                    vals.append(&mut v);
                }
                vals
            }
        },
    }
}

fn generate_pearson_data(n: usize) -> Vec<f64> {
    let mut rng = rand::thread_rng();
    (0..n).map(|_| rng.gen::<f64>()).collect::<Vec<_>>()
}

fn convert_pagerank_data(data: Vec<Vec<u32>>, is_opaque: bool) -> Vec<String> {
    if is_opaque {
        let mut m = HashSet::new();
        for i in data.iter() {
            for j in i.iter() {
                m.insert(*j);
            }
        }
        data.into_iter()
            .map(|mut n| {
                n.push(0);
                n
            })
            .chain(m.into_iter().map(|x| vec![x, x, 1]))
            .map(|n| {
                n.iter()
                    .map(|val| val.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
    } else {
        data.into_iter()
            .map(|n| {
                n.iter()
                    .map(|val| val.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
    }
}

fn generate_tc_data(true_data: Option<&str>, num_edges: u32, num_vertices: u32) -> Vec<(u32, u32)> {
    match true_data {
        Some(path) => {
            let file = fs::File::open(PathBuf::from(path)).unwrap();
            let mut reader = BufReader::new(file);
            let mut content = vec![];
            reader.read_to_end(&mut content).unwrap();
            String::from_utf8(content)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .map(|line| {
                    let parts = line.split(" ").collect::<Vec<_>>();
                    let src = parts[0].parse::<u32>().unwrap();
                    let dst = parts[1].parse::<u32>().unwrap();
                    (src, dst)
                })
                .collect::<Vec<_>>()
        }
        None => {
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
            hset.into_iter().collect::<Vec<_>>()
        }
    }
}

fn convert_tc_data(data: Vec<(u32, u32)>) -> Vec<String> {
    data.into_iter()
        .map(|(from, to)| {
            let v = vec![from.to_string(), to.to_string()];
            v.join(" ")
        })
        .collect::<Vec<_>>()
}

fn generate_topk_data() -> Vec<String> {
    let mut f = match File::open("/opt/data/ml/ratings.dat") {
        // `io::Error` 的 `description` 方法返回一个描述错误的字符串。
        Err(why) => panic!("couldn't open for {}", why.to_string()),
        Ok(file) => file,
    };
    let lines = io::BufReader::new(f).lines();
    lines.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>()
}
