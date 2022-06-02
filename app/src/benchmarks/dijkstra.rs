use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

fn custom_split_nodes_text_file(
    node: (usize, usize, Option<String>),
) -> (usize, (usize, Option<Vec<String>>, String)) {
    let neighbors = node.2.map(|s| {
        let mut v = s.split(":").map(|x| x.to_string()).collect::<Vec<_>>();
        v.split_off(v.len() - 1);
        v
    });
    let path = node.0.to_string();
    (node.0, (node.1, neighbors, path))
}

fn custom_split_neighbor(
    parent_path: &String,
    parent_distance: usize,
    neighbor: &String,
) -> (usize, (usize, Option<Vec<String>>, String)) {
    let neighbor = neighbor.split(',').collect::<Vec<_>>();
    let (nid, mut distance): (usize, usize) =
        (neighbor[0].parse().unwrap(), neighbor[1].parse().unwrap());
    distance += parent_distance;
    let mut path = parent_path.to_string();
    path.push_str("->");
    path.push_str(&nid.to_string());
    (nid, (distance, None, path))
}

fn custom_split_nodes_iterative(
    node: (usize, (usize, Option<Vec<String>>, String)),
) -> (usize, (usize, Option<Vec<String>>, String)) {
    let (nid, (distance, neighbors, mut path)) = node;
    let elements = path.split("->");
    if elements.last().unwrap().parse::<usize>().unwrap() != nid {
        path.push_str("->");
        path.push_str(&nid.to_string());
    }
    (nid, (distance, neighbors, path))
}

fn min_distance(
    node_value0: (usize, Option<Vec<String>>, String),
    node_value1: (usize, Option<Vec<String>>, String),
) -> (usize, Option<Vec<String>>, String) {
    let mut neighbors = None;
    let mut distance = 0;
    let mut path = String::new();
    let (dist0, neighbors0, path0) = node_value0;
    let (dist1, neighbors1, path1) = node_value1;
    if neighbors0.is_some() {
        neighbors = neighbors0;
    } else {
        neighbors = neighbors1;
    }
    if dist0 <= dist1 {
        distance = dist0;
        path = path0;
    } else {
        distance = dist1;
        path = path1;
    }
    (distance, neighbors, path)
}

pub fn dijkstra_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let dir = PathBuf::from("/opt/data/ct_dij_CA");
    let mut nodes = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
            None,
            Some(deserializer),
        )
        .map(Fn!(|node| custom_split_nodes_text_file(node)));
    let mut old = nodes
        .secure_aggregate(
            0,
            Fn!(
                |local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| local_sum
                    + node.1 .0
            ),
            Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),
        )
        .unwrap()
        .get_pt();
    let mut new = old.clone();
    let mut iterations = 0;
    while (iterations == 0 || old != new) && iterations < 5 {
        iterations += 1;
        old = new;
        let mapper = nodes.flat_map(Fn!(|node: (
            usize,
            (usize, Option<Vec<String>>, String)
        )| {
            let (nid, (data0, data1, data2)) = node.clone();
            let mut res = Vec::new();
            res.push(node);
            if let Some(d) = data1 {
                res.append(
                    &mut d
                        .into_iter()
                        .map(|neighbor: String| custom_split_neighbor(&data2, data0, &neighbor))
                        .collect::<Vec<_>>(),
                );
            }
            Box::new(res.into_iter()) as Box<dyn Iterator<Item = _>>
        }));
        let reducer = mapper.reduce_by_key(Fn!(|(x, y)| min_distance(x, y)), 1);
        nodes = reducer.map(Fn!(|node| custom_split_nodes_iterative(node)));
        nodes.cache();
        new = nodes
            .secure_aggregate(
                0,
                Fn!(
                    |local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| {
                        local_sum + node.1 .0
                    }
                ),
                Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),
            )
            .unwrap()
            .get_pt();
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("new = {:?}, elapsed time = {:?}", new, dur);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!(
        "Finished after {:?} iterations, total time {:?}s",
        iterations, dur
    );
    Ok(())
}

// unsecure mode
pub fn dijkstra_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(usize, usize, Option<String>)>>(&file).unwrap()
        //Item = (u32, u32)
    }));

    let dir = PathBuf::from("/opt/data/pt_dij_CA");
    let mut nodes = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(|nodes: Vec<(usize, usize, Option<String>)>| Box::new(
            nodes
                .into_iter()
                .map(|node| custom_split_nodes_text_file(node))
        )
            as Box<dyn Iterator<Item = _>>));
    let mut old = nodes
        .aggregate(
            0,
            Fn!(
                |local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| local_sum
                    + node.1 .0
            ),
            Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),
        )
        .unwrap();
    let mut new = old.clone();
    let mut iterations = 0;
    //let mut result = Vec::new();
    while (iterations == 0 || old != new) && iterations < 5 {
        iterations += 1;
        old = new;
        let mapper = nodes.flat_map(Fn!(|node: (
            usize,
            (usize, Option<Vec<String>>, String)
        )| {
            let (nid, (data0, data1, data2)) = node.clone();
            let mut res = Vec::new();
            res.push(node);
            if let Some(d) = data1 {
                res.append(
                    &mut d
                        .into_iter()
                        .map(|neighbor: String| custom_split_neighbor(&data2, data0, &neighbor))
                        .collect::<Vec<_>>(),
                );
            }
            Box::new(res.into_iter()) as Box<dyn Iterator<Item = _>>
        }));
        let reducer = mapper.reduce_by_key(Fn!(|(x, y)| min_distance(x, y)), 1);
        nodes = reducer.map(Fn!(|node| custom_split_nodes_iterative(node)));
        //result = nodes.collect().unwrap();
        //nodes = sc.parallelize(result.clone(), vec![], fe_mp.clone(), fd_mp.clone(), 1);
        nodes.cache();
        new = nodes
            .aggregate(
                0,
                Fn!(
                    |local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| {
                        local_sum + node.1 .0
                    }
                ),
                Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),
            )
            .unwrap();
        println!("new = {:?}", new);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!(
        "Finished after {:?} iterations, total time {:?}s",
        iterations, dur
    );

    Ok(())
}
