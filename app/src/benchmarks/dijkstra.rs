use std::collections::{HashMap, BTreeMap};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

fn custom_split_nodes_text_file(node: (usize, usize, Option<String>)) -> (usize, (usize, Option<Vec<String>>, String)) {
    let neighbors = node.2.map(|s| 
        s.split(":")
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    );
    let path = node.0.to_string();
    (node.0, (node.1, neighbors, path))
}

fn custom_split_neighbor(parent_path: &String, parent_distance: usize, neighbor: &String) -> (usize, (usize, Option<Vec<String>>, String)) {
    let neighbor = neighbor.split(',').collect::<Vec<_>>();
    let (nid, mut distance): (usize, usize) = (neighbor[0].parse().unwrap(), neighbor[1].parse().unwrap());
    distance += parent_distance;
    let mut path = parent_path.to_string();
    path.push_str("->");
    path.push_str(&nid.to_string());
    (nid, (distance, None, path))
}

fn custom_split_nodes_iterative(node: (usize, (usize, Option<Vec<String>>, String))) -> (usize, (usize, Option<Vec<String>>, String)) {
    let (nid, (distance, neighbors, mut path)) = node;
    let elements = path.split("->");
    if elements.last().unwrap().parse::<usize>().unwrap() != nid {
        path.push_str("->");
        path.push_str(&nid.to_string());
    }
    (nid, (distance, neighbors, path))
}

fn min_distance(node_value0: (usize, Option<Vec<String>>, String), node_value1: (usize, Option<Vec<String>>, String)) -> (usize, Option<Vec<String>>, String) {
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

// secure mode
pub fn dijkstra_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
    let fe = Fn!(|vp: Vec<(usize, usize, Option<String>)>|{
        ser_encrypt::<>(vp)    
    });

    let fd = Fn!(|ve: Vec<u8>|{  //ItemE = Vec<u8>
        let data: Vec<(usize, usize, Option<String>)> = ser_decrypt::<>(ve);
        data
    });

    let fe_mp = Fn!(|vp: Vec<(usize, (usize, Option<Vec<String>>, String))>| {
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
        let mut pt1: Vec<(usize, Option<Vec<String>>, String)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp1 = Fn!(|vp: Vec<(usize, Option<Vec<String>>, String)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
        }
        let buf0 = ser_encrypt::<>(buf0);
        buf0
    });

    let fd_mp1 = Fn!(|ve: Vec<u8>| {
        let mut pt0: Vec<(usize, Option<Vec<String>>, String)> = ser_decrypt::<>(ve);
        pt0
    });

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));

    let dir = PathBuf::from("/opt/data/ct_dij");
    let mut nodes = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), fe, fd)
        .map(Fn!(|node| custom_split_nodes_text_file(node)), fe_mp.clone(), fd_mp.clone());
    let mut old = nodes.secure_aggregate(
        0,
        Fn!(|local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| local_sum + node.1.0),
        Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),  
        Fn!(|v| v), 
        Fn!(|v| v)
    ).unwrap().to_plain();
    let mut new = old.clone();
    let mut iterations = 0;
    let mut result = Text::<_, _>::new(Vec::new(), None, None);
    while iterations == 0 || old[0] != new[0] {
        iterations += 1;
        old = new;
        let node_values = nodes.map(Fn!(|x: (usize, (usize, Option<Vec<String>>, String))| x.1), fe_mp1.clone(), fd_mp1.clone());
        let neighbors = node_values.filter(Fn!(|v: &(usize, Option<Vec<String>>, String)| v.1.is_some()))
            .flat_map(Fn!(|node: (usize, Option<Vec<String>>, String)| {
                let (data0, data1, data2) = node;
                Box::new(data1.unwrap().into_iter()
                    .map(|neighbor: String| custom_split_neighbor(&data2, data0, &neighbor))
                    .collect::<Vec<_>>()
                    .into_iter())
                as Box<dyn Iterator<Item = _>>
            }), fe_mp.clone(), fd_mp.clone());
        let mapper = nodes.union(neighbors.into());
        let reducer = mapper.reduce_by_key(Fn!(|(x, y)| min_distance(x, y)), 1, fe_mp.clone(), fd_mp.clone());
        nodes = reducer.map(Fn!(|node| custom_split_nodes_iterative(node)), fe_mp.clone(), fd_mp.clone());
        result = nodes.secure_collect().unwrap();
        nodes = sc.parallelize(vec![], (*result).clone(), fe_mp.clone(), fd_mp.clone(), 1);
        nodes.cache();
        new = nodes.secure_aggregate(
            0,
            Fn!(|local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| local_sum + node.1.0),
            Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),  
            Fn!(|v| v), 
            Fn!(|v| v)
        ).unwrap().to_plain();
        println!("new = {:?}", new[0]);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Finished after {:?} iterations, time {:?}s", iterations, dur);
    println!("res = {:?}", result.to_plain());
    Ok(())
}
    
// unsecure mode
pub fn dijkstra_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
    let lfe = Fn!(|vp: Vec<Vec<(usize, usize, Option<String>)>>| {
        vp
    });
    let lfd = Fn!(|ve: Vec<Vec<(usize, usize, Option<String>)>>| {
        ve
    });

    let fe_mp = Fn!(|vp: Vec<(usize, (usize, Option<Vec<String>>, String))>| {
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
        let mut pt1: Vec<(usize, Option<Vec<String>>, String)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp1 = Fn!(|vp: Vec<(usize, Option<Vec<String>>, String)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
        }
        let buf0 = ser_encrypt::<>(buf0);
        buf0
    });

    let fd_mp1 = Fn!(|ve: Vec<u8>| {
        let mut pt0: Vec<(usize, Option<Vec<String>>, String)> = ser_decrypt::<>(ve);
        pt0
    });

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(usize, usize, Option<String>)>>(&file).unwrap()  //Item = (u32, u32)
    }));

    let dir = PathBuf::from("/opt/data/pt_dij");
    let mut nodes = sc.read_source(LocalFsReaderConfig::new(dir), Some(deserializer), None, lfe, lfd)
        .flat_map(Fn!(|nodes: Vec<(usize, usize, Option<String>)>| Box::new(nodes.into_iter().map(|node| custom_split_nodes_text_file(node))) as Box<dyn Iterator<Item = _>>), fe_mp.clone(), fd_mp.clone());
    let mut old = nodes.aggregate(
        0,
        Fn!(|local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| local_sum + node.1.0),
        Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1),
    ).unwrap();
    let mut new = old.clone();
    let mut iterations = 0;
    let mut result = Vec::new();
    while iterations == 0 || old != new {
        iterations += 1;
        old = new;
        let node_values = nodes.map(Fn!(|x: (usize, (usize, Option<Vec<String>>, String))| x.1), fe_mp1.clone(), fd_mp1.clone());
        let neighbors = node_values.filter(Fn!(|v: &(usize, Option<Vec<String>>, String)| v.1.is_some()))
            .flat_map(Fn!(|node: (usize, Option<Vec<String>>, String)| {
                let (data0, data1, data2) = node;
                Box::new(data1.unwrap().into_iter()
                    .map(|neighbor: String| custom_split_neighbor(&data2, data0, &neighbor))
                    .collect::<Vec<_>>()
                    .into_iter())
                as Box<dyn Iterator<Item = _>>
            }), fe_mp.clone(), fd_mp.clone());
        let mapper = nodes.union(neighbors.into());
        let reducer = mapper.reduce_by_key(Fn!(|(x, y)| min_distance(x, y)), 1, fe_mp.clone(), fd_mp.clone());
        nodes = reducer.map(Fn!(|node| custom_split_nodes_iterative(node)), fe_mp.clone(), fd_mp.clone());
        result = nodes.collect().unwrap();
        nodes = sc.parallelize(result.clone(), vec![], fe_mp.clone(), fd_mp.clone(), 1);
        nodes.cache();
        new = nodes.aggregate(
            0,
            Fn!(|local_sum: usize, node: (usize, (usize, Option<Vec<String>>, String))| local_sum + node.1.0),
            Fn!(|local_sum0, local_sum1| local_sum0 + local_sum1)
        ).unwrap();
        println!("new = {:?}", new);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Finished after {:?} iterations, time {:?}s", iterations, dur);
    println!("res = {:?}", result);
    Ok(())
}

