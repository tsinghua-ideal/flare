use crate::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

pub fn se1_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/ct_social_graph_100m_popular");
    let dir1 = PathBuf::from("/opt/data/ct_social_graph_100m_inactive");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u32, u32)| (x.1, x.0)));
    let table1 = sc
        .read_source::<_, _, (u32, u32)>(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u32, u32)| x));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res);

    Ok(())
}

pub fn se2_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/ct_social_graph_1m_popular");
    let dir1 = PathBuf::from("/opt/data/ct_social_graph_10m_normal");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u32, u32)| (x.1, x.0)));
    let table1 = sc
        .read_source::<_, _, (u32, u32)>(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u32, u32)| x));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res);

    Ok(())
}

pub fn se3_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/ct_social_graph_100k_popular");
    let dir1 = PathBuf::from("/opt/data/ct_social_graph_100k_normal");
    let table0 = sc
        .read_source::<_, _, (u32, u32)>(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u32, u32)| x));
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res);

    Ok(())
}

pub fn se1_unsec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u32, u32)>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/pt_social_graph_100m_popular");
    let dir1 = PathBuf::from("/opt/data/pt_social_graph_100m_inactive");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer.clone()),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ))
        .map(Fn!(|x: (u32, u32,)| (x.1, x.0)));
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res);

    Ok(())
}

pub fn se2_unsec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u32, u32)>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/pt_social_graph_1m_popular");
    let dir1 = PathBuf::from("/opt/data/pt_social_graph_10m_normal");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer.clone()),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ))
        .map(Fn!(|x: (u32, u32,)| (x.1, x.0)));
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res);

    Ok(())
}

pub fn se3_unsec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u32, u32)>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/pt_social_graph_100k_popular");
    let dir1 = PathBuf::from("/opt/data/pt_social_graph_100k_normal");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer.clone()),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ));
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u32, u32)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ))
        .map(Fn!(|x: (u32, u32,)| (x.1, x.0)));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res);

    Ok(())
}
