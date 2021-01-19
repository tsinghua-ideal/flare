use std::collections::HashSet;
use std::time::Instant;
use vega::*;
use rand::Rng;

pub fn transitive_closure() -> Result<()> {
    let sc = Context::new()?;
    let num_edges = 200;
    let num_vertices = 100;
    let mut rng = rand::thread_rng();

    let mut hset = HashSet::new();
    let mut count_edges = 0;
    while count_edges < num_edges {
        let from = rng.gen::<u32>() & num_vertices;
        let to = rng.gen::<u32>() & num_vertices;
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
  
    let fe_jn = Fn!(|vp: Vec<(u32, (u32, u32))>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        let mut buf2 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1.0);
            buf2.push(i.1.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        let buf2 = ser_encrypt::<>(buf2);
        (buf0, (buf1, buf2))
    });

    let fd_jn = Fn!(|ve: (Vec<u8>, (Vec<u8>, Vec<u8>))| {
        let (buf0, (buf1, buf2)) = ve;
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<u32> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<u32> = ser_decrypt::<>(buf2);
        let len = pt0.len() | pt1.len() | pt2.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1
                .into_iter()
                .zip(pt2.into_iter())
            )
            .collect::<Vec<_>>() 
    });
    

    let mut tc = sc.parallelize(data, vec![], fe.clone(), fd.clone(), 2);
    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).
    
    // Because join() joins on keys, the edges are stored in reversed order.
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());
    
    // This join is iterated until a fixed point is reached.
    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        tc = tc.union(
            tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
                .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone())
                .distinct().into()
        ).unwrap();
        next_count = tc.count().unwrap();
    }

    Ok(())
}