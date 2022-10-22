use crate::*;
use std::collections::HashMap;
use std::path::PathBuf;


pub fn te1_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));


    let dir0 = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir1 = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &(
            u64,
            String,
            String,
            u32,
            String,
            f32,
            String
        )| x.3))
        .map(Fn!(|x: (
            u32,
            (u64, String, String, u32, String, f32, String)
        )| (x.0, x.1 .0)));
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &(
            u64,
            String,
            String,
            u32,
            String,
            f32,
            String,
            String
        )| x.3))
        .map(Fn!(|x: (
            u32,
            (u64, String, String, u32, String, f32, String, String)
        )| (x.0, x.1 .0)));

    let joined = table0.join(table1, NUM_PARTS);
    let _res = joined.count().unwrap();




    Ok(())
}

pub fn te2_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));


    let dir = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .key_by(Fn!(|x: &(
            u64,
            String,
            String,
            u32,
            String,
            f32,
            String
        )| x.3))
        .map(Fn!(|x: (
            u32,
            (u64, String, String, u32, String, f32, String)
        )| (x.0, x.1 .0)));

    let joined = table.join(table.clone(), NUM_PARTS);
    let _res = joined.count().unwrap();




    Ok(())
}

pub fn te3_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));


    let dir = PathBuf::from("/opt/data/ct_tpch_200m_customer");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .key_by(Fn!(|x: &(
            u64,
            String,
            String,
            u32,
            String,
            f32,
            String,
            String
        )| x.3))
        .map(Fn!(|x: (
            u32,
            (u64, String, String, u32, String, f32, String, String)
        )| (x.0, x.1 .0)));

    let joined = table.join(table.clone(), NUM_PARTS);
    let _res = joined.count().unwrap();




    Ok(())
}