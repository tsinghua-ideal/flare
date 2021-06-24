use std::collections::{HashMap, BTreeMap};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

// secure mode
pub fn mm_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
    let fe = Fn!(|vp: Vec<((u32, u32), f64)>|{
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)|{  //ItemE = (Vec<u8>, Vec<u8>)
        let (buf0, buf1) = ve;
        let mut pt0: Vec<(u32, u32)> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp = Fn!(|vp: Vec<(u32, (u32, f64))>|{
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
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<(u32, f64)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn = Fn!(|vp: Vec<(u32, ((u32, f64), (u32, f64)))>| {
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
        let mut pt1: Vec<(u32, f64)> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<(u32, f64)> = ser_decrypt::<>(buf2);
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
        bincode::deserialize::<Vec<(Vec<u8>, Vec<u8>)>>(&file).unwrap()  //ItemE = Vec<u8>  
    }));

    let dir_a = PathBuf::from("/opt/data/ct_mm_a_500");
    let dir_b = PathBuf::from("/opt/data/ct_mm_b_500");
    let ma = sc.read_source(LocalFsReaderConfig::new(dir_a).num_partitions_per_executor(1), None, Some(deserializer.clone()), fe.clone(), fd.clone())
        .map(Fn!(|a: ((u32, u32), f64)| {
            (a.0.1, (a.0.0, a.1))
        }), fe_mp, fd_mp);
    let mb = sc.read_source(LocalFsReaderConfig::new(dir_b).num_partitions_per_executor(1), None, Some(deserializer), fe.clone(), fd.clone())
        .map(Fn!(|b: ((u32, u32), f64)| {
            (b.0.0, (b.0.1, b.1))
        }), fe_mp, fd_mp);

    let temp = ma.join(mb, fe_jn, fd_jn, 1)
        .map(Fn!(|n: (u32, ((u32, f64), (u32, f64)))| ((n.1.0.0, n.1.1.0), n.1.0.1 * n.1.1.1)), fe.clone(), fd.clone());

    let mc = temp.reduce_by_key(Fn!(|(x, y)| x + y), 1, fe, fd);

    let output = mc.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("count = {:?}, total time = {:?}", output, dur);
    Ok(())
}
    
// unsecure mode
pub fn mm_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
    let lfe = Fn!(|vp: Vec<Vec<((u32, u32), f64)>>| {
        vp
    });
    let lfd = Fn!(|ve: Vec<Vec<((u32, u32), f64)>>| {
        ve
    });
    let fe = Fn!(|vp: Vec<((u32, u32), f64)>|{
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)|{  //ItemE = (Vec<u8>, Vec<u8>)
        let (buf0, buf1) = ve;
        let mut pt0: Vec<(u32, u32)> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp = Fn!(|vp: Vec<(u32, (u32, f64))>|{
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
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<(u32, f64)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn = Fn!(|vp: Vec<(u32, ((u32, f64), (u32, f64)))>| {
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
        let mut pt1: Vec<(u32, f64)> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<(u32, f64)> = ser_decrypt::<>(buf2);
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
        bincode::deserialize::<Vec<((u32, u32), f64)>>(&file).unwrap()  //Item = ((u32, u32), f64)
    }));

    let dir_a = PathBuf::from("/opt/data/pt_mm_a_500");
    let dir_b = PathBuf::from("/opt/data/pt_mm_b_500");
    let ma = sc.read_source(LocalFsReaderConfig::new(dir_a).num_partitions_per_executor(1), Some(deserializer.clone()), None, lfe.clone(), lfd.clone())
        .flat_map(Fn!(|va: Vec<((u32, u32), f64)>| {
            Box::new(va.into_iter().map(|a| 
                (a.0.1, (a.0.0, a.1))
            )) as Box<dyn Iterator<Item = _>>
        }), fe_mp, fd_mp);
    let mb = sc.read_source(LocalFsReaderConfig::new(dir_b).num_partitions_per_executor(1), Some(deserializer), None, lfe.clone(), lfd.clone())
        .flat_map(Fn!(|vb: Vec<((u32, u32), f64)>| {
            Box::new(vb.into_iter().map(|b|
                (b.0.0, (b.0.1, b.1))
            )) as Box<dyn Iterator<Item = _>>
        }), fe_mp, fd_mp);

    let temp = ma.join(mb, fe_jn, fd_jn, 1)
        .map(Fn!(|n: (u32, ((u32, f64), (u32, f64)))| ((n.1.0.0, n.1.1.0), n.1.0.1 * n.1.1.1)), fe.clone(), fd.clone());

    let mc = temp.reduce_by_key(Fn!(|(x, y)| x + y), 1, fe, fd);

    let output = mc.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("count = {:?}, total time = {:?}", output, dur);
    Ok(())
}

