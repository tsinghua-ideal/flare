use std::path::PathBuf;
use std::time::Instant;
use vega::*;
use rand::Rng;

// secure mode
pub fn pearson_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let fe = Fn!(|vp: Vec<f64>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<f64> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_zip = Fn!(|vp: Vec<(f64, f64)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd_zip = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(f64, f64)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<f64> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_map = Fn!(|vp: Vec<(f64, f64, f64)>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_map = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<(f64, f64, f64)> = ser_decrypt::<>(buf0); 
        pt0
    });
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/ct_pe_a_108");
    let dir1 = PathBuf::from("/opt/data/ct_pe_b_108");
    let x = sc.read_source(LocalFsReaderConfig::new(dir0).num_partitions_per_executor(1), None, Some(deserializer.clone()), fe.clone(), fd.clone());
    let y = sc.read_source(LocalFsReaderConfig::new(dir1).num_partitions_per_executor(1), None, Some(deserializer), fe.clone(), fd.clone());

    let mx = (*x.secure_reduce(Fn!(|a, b| a+b), Fn!(|x| x), Fn!(|x| x)).unwrap())/
        x.secure_count().unwrap() as f64;
    let my = (*y.secure_reduce(Fn!(|a, b| a+b), Fn!(|x| x), Fn!(|x| x)).unwrap())/
        y.secure_count().unwrap() as f64;

    let (upper, lowerx, lowery) = *x.zip(y.into(), fe_zip, fd_zip)
        .map(Fn!(move |pair: (f64, f64)| {
            let up = (pair.0 - mx) * (pair.1 - my);
            let lowx = (pair.0 - mx) * (pair.0 - mx);
            let lowy = (pair.1 - my) * (pair.1 - my);
            (up, lowx, lowy)
        }), fe_map, fd_map)
        .secure_reduce(Fn!(|a: (f64, f64, f64), b: (f64, f64, f64)| (a.0+b.0, a.1+b.1, a.2+b.2)), Fn!(|x| x), Fn!(|x| x)).unwrap();
    let r = upper / (f64::sqrt(lowerx) * f64::sqrt(lowery));
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, r = {:?}", dur, r);
    Ok(())
}

pub fn pearson_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let lfe = Fn!(|vp: Vec<Vec<f64>>| {
        vp
    });
    let lfd = Fn!(|ve: Vec<Vec<f64>>| {
        ve
    });
    let fe = Fn!(|vp: Vec<f64>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<f64> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_zip = Fn!(|vp: Vec<(f64, f64)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd_zip = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(f64, f64)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<f64> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_map = Fn!(|vp: Vec<(f64, f64, f64)>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_map = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<(f64, f64, f64)> = ser_decrypt::<>(buf0); 
        pt0
    });
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<f64>>(&file).unwrap()
    }));

    let now = Instant::now();
    let dir0 = PathBuf::from("/opt/data/pt_pe_a_108");
    let dir1 = PathBuf::from("/opt/data/pt_pe_b_108");
    let x = sc.read_source(LocalFsReaderConfig::new(dir0).num_partitions_per_executor(1), Some(deserializer.clone()), None, lfe.clone(), lfd.clone())
        .flat_map(Fn!(|v: Vec<f64>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>), fe.clone(), fd.clone());
    let y = sc.read_source(LocalFsReaderConfig::new(dir1).num_partitions_per_executor(1), Some(deserializer), None, lfe.clone(), lfd.clone())
        .flat_map(Fn!(|v: Vec<f64>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>), fe.clone(), fd.clone());

    let mx = x.reduce(Fn!(|a, b| a+b)).unwrap().unwrap()/
        x.count().unwrap() as f64;
    let my = y.reduce(Fn!(|a, b| a+b)).unwrap().unwrap()/
        y.count().unwrap() as f64;

    let (upper, lowerx, lowery) = x.zip(y.into(), fe_zip, fd_zip)
        .map(Fn!(move |pair: (f64, f64)| {
            let up = (pair.0 - mx) * (pair.1 - my);
            let lowx = (pair.0 - mx) * (pair.0 - mx);
            let lowy = (pair.1 - my) * (pair.1 - my);
            (up, lowx, lowy)
        }), fe_map, fd_map)
        .reduce(Fn!(|a: (f64, f64, f64), b: (f64, f64, f64)| (a.0+b.0, a.1+b.1, a.2+b.2))).unwrap().unwrap();
    let r = upper / (f64::sqrt(lowerx) * f64::sqrt(lowery));
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, r = {:?}", dur, r);
    Ok(())
}
