use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use vega::*;
use rand::Rng;

pub fn transitive_closure_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
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
    
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(Vec<u8>, Vec<u8>)>>(&file).unwrap()  //ItemE = (Vec<u8>, Vec<u8>)
    }));

    let dir = PathBuf::from("/tmp/ct_tc_1");
    let mut tc = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), fe, fd);
    tc.cache();
    let mut data_enc = tc.secure_collect().unwrap().clone();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());
    
    let mut old_count = 0;
    let mut next_count = tc.secure_count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        let jn = tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
            .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone());
        data_enc.append(&mut jn.secure_collect().unwrap());
        tc = sc.parallelize(vec![], data_enc, fe.clone(), fd.clone(), 1)
            .distinct();
        tc.cache();
        next_count = tc.secure_count().unwrap();
        data_enc = tc.secure_collect().unwrap().clone();
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}

pub fn transitive_closure_sec_1() -> Result<()> {  //may lead to core dump, but don't know why
    let sc = Context::new()?;
    let now = Instant::now();
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

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(Vec<u8>, Vec<u8>)>>(&file).unwrap()  //ItemE = (Vec<u8>, Vec<u8>)
    }));

    let dir = PathBuf::from("/tmp/ct_tc");
    let mut tc = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), fe, fd);
    tc.cache();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());
    
    let mut old_count = 0;
    let mut next_count = tc.secure_count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        tc = tc.union(
            tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
                .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone())
                .into()
        ).distinct();
        tc.cache();
        next_count = tc.secure_count().unwrap();
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}

pub fn transitive_closure_sec_2() -> Result<()> {
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
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());

    let mut old_count = 0;
    let mut next_count = tc.secure_count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        let jn = tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
            .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone());
        data_enc.append(&mut jn.secure_collect().unwrap());
        tc = sc.parallelize(vec![], data_enc, fe.clone(), fd.clone(), 1)
            .distinct();
        tc.cache();
        next_count = tc.secure_count().unwrap();
        data_enc = tc.secure_collect().unwrap().clone();
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}

pub fn transitive_closure_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
    let lfe = Fn!(|vp: Vec<Vec<(u32, u32)>>| {
        vp
    });
    let lfd = Fn!(|ve: Vec<Vec<(u32, u32)>>| {
        ve
    });
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

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u32, u32)>>(&file).unwrap()  //Item = (u32, u32)
    }));

    let dir = PathBuf::from("/tmp/pt_tc_1");
    let mut tc = sc.read_source(LocalFsReaderConfig::new(dir), Some(deserializer), None, lfe, lfd)
        .flat_map(Fn!(|v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>), fe.clone(), fd.clone());
    tc.cache();
    let mut data = tc.collect().unwrap();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());
    
    // This join is iterated until a fixed point is reached.
    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        let jn = tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
            .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone());
        data.append(&mut jn.collect().unwrap());
        tc = sc.parallelize(data, vec![], fe.clone(), fd.clone(), 1)
            .distinct();
        tc.cache();
        next_count = tc.count().unwrap();
        data = tc.collect().unwrap();
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}

pub fn transitive_closure_unsec_1() -> Result<()> {
    let sc = Context::new()?;
    let num_edges = 100;
    let num_vertices = 50;
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
    let now = Instant::now();
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
                .into()
        ).distinct();
        tc.cache();
        next_count = tc.count().unwrap();
        println!("next_count = {:?}, tc = {:?}", next_count, tc.collect().unwrap());
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}