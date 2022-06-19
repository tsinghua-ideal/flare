use std::path::PathBuf;

use crate::*;


// secure mode
pub fn pearson_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));


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

    let mx = x.reduce(Fn!(|a: f64, b| a + b)).unwrap().get_pt()
        / x.count().unwrap() as f64;
    let my = y.reduce(Fn!(|a: f64, b| a + b)).unwrap().get_pt()
        / y.count().unwrap() as f64;

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
        .get_pt();
    let _r = upper / (f64::sqrt(lowerx) * f64::sqrt(lowery));


    Ok(())
}