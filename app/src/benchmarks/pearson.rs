use rand::Rng;
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

// secure mode
pub fn pearson_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/ct_pe_a_108");
    let dir1 = PathBuf::from("/opt/data/ct_pe_b_108");
    let x = sc.read_source(
        LocalFsReaderConfig::new(dir0).num_partitions_per_executor(1),
        None,
        Some(deserializer.clone()),
    );
    let y = sc.read_source(
        LocalFsReaderConfig::new(dir1).num_partitions_per_executor(1),
        None,
        Some(deserializer),
    );

    let mx = x.secure_reduce(Fn!(|a: f64, b| a + b)).unwrap().get_pt()
        / x.secure_count().unwrap() as f64;
    let my = y.secure_reduce(Fn!(|a: f64, b| a + b)).unwrap().get_pt()
        / y.secure_count().unwrap() as f64;

    let (upper, lowerx, lowery) = x
        .zip(y.into())
        .map(Fn!(move |pair: (f64, f64)| {
            let up = (pair.0 - mx) * (pair.1 - my);
            let lowx = (pair.0 - mx) * (pair.0 - mx);
            let lowy = (pair.1 - my) * (pair.1 - my);
            (up, lowx, lowy)
        }))
        .secure_reduce(Fn!(|a: (f64, f64, f64), b: (f64, f64, f64)| (
            a.0 + b.0,
            a.1 + b.1,
            a.2 + b.2
        )))
        .unwrap()
        .get_pt();
    let r = upper / (f64::sqrt(lowerx) * f64::sqrt(lowery));
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?} s, r = {:?}", dur, r);
    Ok(())
}

pub fn pearson_unsec_0() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<f64>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/pt_pe_a_108");
    let dir1 = PathBuf::from("/opt/data/pt_pe_b_108");
    let x = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(1),
            Some(deserializer.clone()),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<f64>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ));
    let y = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(1),
            Some(deserializer),
            None,
        )
        .flat_map(Fn!(
            |v: Vec<f64>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>
        ));

    let mx = x.reduce(Fn!(|a, b| a + b)).unwrap().unwrap() / x.count().unwrap() as f64;
    let my = y.reduce(Fn!(|a, b| a + b)).unwrap().unwrap() / y.count().unwrap() as f64;

    let (upper, lowerx, lowery) = x
        .zip(y.into())
        .map(Fn!(move |pair: (f64, f64)| {
            let up = (pair.0 - mx) * (pair.1 - my);
            let lowx = (pair.0 - mx) * (pair.0 - mx);
            let lowy = (pair.1 - my) * (pair.1 - my);
            (up, lowx, lowy)
        }))
        .reduce(Fn!(|a: (f64, f64, f64), b: (f64, f64, f64)| (
            a.0 + b.0,
            a.1 + b.1,
            a.2 + b.2
        )))
        .unwrap()
        .unwrap();
    let r = upper / (f64::sqrt(lowerx) * f64::sqrt(lowery));
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?} s, r = {:?}", dur, r);
    Ok(())
}
