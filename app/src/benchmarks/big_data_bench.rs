use crate::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

// BigDataBench_V5.0_BigData_MicroBenchmark/Hive/Interactive_Query/e-commerce-aggregation.sql
pub fn aggregate_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir = PathBuf::from("/opt/data/ct_bdb_ec_order_items_1g");
    let table = sc.read_source(
        LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
        None,
        Some(deserializer.clone()),
    );
    let keyed = table
        .key_by(Fn!(|x: &(u64, u64, u64, u64, f32, f32)| x.2))
        .map(Fn!(|x: (u64, (u64, u64, u64, u64, f32, f32))| (
            x.0, x.1 .3
        )));
    let agg = keyed.reduce_by_key(Fn!(|(x, y): (u64, u64)| x + y), NUM_PARTS);
    let res = agg.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res.get_pt().len());

    Ok(())
}

// BigDataBench_V5.0_BigData_MicroBenchmark/Hive/Interactive_MicroBenchmark/BigOP-e-commerce-filter.sql
pub fn filter_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir = PathBuf::from("/opt/data/ct_bdb_ec_order_items_1g");
    let table = sc.read_source(
        LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
        None,
        Some(deserializer.clone()),
    );
    let filtered = table.filter(Fn!(|x: &(u64, u64, u64, u64, f32, f32)| x.5 > 750000.0));
    let res = filtered.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res.get_pt().len());

    Ok(())
}

// BigDataBench_V5.0_BigData_MicroBenchmark/Hive/Interactive_MicroBenchmark/BigOP-e-commerce-crossproject.sql
pub fn cross_project_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/ct_bdb_ec_order_items_1g");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &(u64, u64, u64, u64, f32, f32)| x.1));

    let dir1 = PathBuf::from("/opt/data/ct_bdb_ec_order_1g");
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: (u64, u64, String)| (x.0, (x.1, x.2))));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res.get_pt().len());

    Ok(())
}

pub fn aggregate_unsec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u64, u64, u64, u64, f32, f32)>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir = PathBuf::from("/opt/data/pt_bdb_ec_order_items_1g");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u64, u64, u64, u64, f32, f32)>| Box::new(v.into_iter())
                as Box<dyn Iterator<Item = _>>
        ));
    let keyed = table
        .key_by(Fn!(|x: &(u64, u64, u64, u64, f32, f32)| x.2))
        .map(Fn!(|x: (u64, (u64, u64, u64, u64, f32, f32))| (
            x.0, x.1 .3
        )));
    let agg = keyed.reduce_by_key(Fn!(|(x, y): (u64, u64)| x + y), NUM_PARTS);
    let res = agg.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res.len());

    Ok(())
}

pub fn filter_unsec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u64, u64, u64, u64, f32, f32)>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir = PathBuf::from("/opt/data/pt_bdb_ec_order_items_1g");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer.clone()),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u64, u64, u64, u64, f32, f32)>| Box::new(v.into_iter())
                as Box<dyn Iterator<Item = _>>
        ));
    let filtered = table.filter(Fn!(|x: &(u64, u64, u64, u64, f32, f32)| x.5 > 750000.0));
    let res = filtered.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res.len());

    Ok(())
}

pub fn cross_project_unsec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer0 = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u64, u64, u64, u64, f32, f32)>>(&file).unwrap()
    }));
    let deserializer1 = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<(u64, u64, String)>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/pt_bdb_ec_order_items_1g");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer0),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u64, u64, u64, u64, f32, f32)>| Box::new(v.into_iter())
                as Box<dyn Iterator<Item = _>>
        ))
        .key_by(Fn!(|x: &(u64, u64, u64, u64, f32, f32)| x.1));

    let dir1 = PathBuf::from("/opt/data/pt_bdb_ec_order_1g");
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            Some(deserializer1),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<(u64, u64, String,)>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ))
        .map(Fn!(|x: (u64, u64, String,)| (x.0, (x.1, x.2))));

    let joined = table0.join(table1, NUM_PARTS);
    let res = joined.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("res.len() = {:?}", res.len());

    Ok(())
}
