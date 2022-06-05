use rand::Rng;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

pub fn transitive_closure_sec_0() -> Result<()> {
    //may lead to core dump, but don't know why
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = (Vec<u8>)
    }));

    let dir = PathBuf::from("/opt/data/ct_tc_fb");
    let mut tc = sc.read_source(
        LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
        None,
        Some(deserializer),
    );
    tc.cache();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    let mut old_count = 0;
    let mut next_count = tc.secure_count().unwrap();
    let mut iter = 0;
    while next_count != old_count && iter < 5 {
        old_count = next_count;
        tc = tc
            .union(
                tc.join(edges.clone(), 1)
                    .map(Fn!(|x: (u32, (u32, u32))| (x.1 .1, x.1 .0)))
                    .into(),
            )
            .distinct_with_num_partitions(1);
        tc.cache();
        next_count = tc.secure_count().unwrap();
        iter += 1;
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}

pub fn transitive_closure_sec_1() -> Result<()> {
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
    let data_enc = batch_encrypt(&hset.into_iter().collect::<Vec<_>>());

    let now = Instant::now();
    let mut tc = sc.parallelize(vec![], data_enc.clone(), 1);
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    let mut old_count = 0;
    let mut next_count = tc.secure_count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        tc = tc
            .union(
                tc.join(edges.clone(), 1)
                    .map(Fn!(|x: (u32, (u32, u32))| (x.1 .1, x.1 .0)))
                    .into(),
            )
            .distinct_with_num_partitions(1);
        tc.cache();
        next_count = tc.secure_count().unwrap();
        println!("next_count = {:?}", next_count);
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}

pub fn transitive_closure_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u32, u32)>>(&file).unwrap() //Item = (u32, u32)
    }));

    let dir = PathBuf::from("/opt/data/pt_tc_fb");
    let mut tc = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ));
    tc.cache();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    // This join is iterated until a fixed point is reached.
    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();
    let mut iter = 0;
    while next_count != old_count && iter < 5 {
        old_count = next_count;
        tc = tc
            .union(
                tc.join(edges.clone(), 1)
                    .map(Fn!(|x: (u32, (u32, u32))| (x.1 .1, x.1 .0)))
                    .into(),
            )
            .distinct_with_num_partitions(1);
        tc.cache();
        next_count = tc.count().unwrap();
        iter += 1;
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

    let now = Instant::now();
    let mut tc = sc.parallelize(data, vec![], 1);
    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    // This join is iterated until a fixed point is reached.
    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();
    while next_count != old_count {
        old_count = next_count;
        tc = tc
            .union(
                tc.join(edges.clone(), 1)
                    .map(Fn!(|x: (u32, (u32, u32))| (x.1 .1, x.1 .0)))
                    .into(),
            )
            .distinct();
        tc.cache();
        next_count = tc.count().unwrap();
        println!(
            "next_count = {:?}, tc = {:?}",
            next_count,
            tc.collect().unwrap()
        );
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?}s", dur);
    Ok(())
}
