use std::collections::HashSet;
use std::path::PathBuf;

use crate::*;


pub fn transitive_closure_sec_0() -> Result<()> {
    let sc = Context::new()?;
    
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

    let dir = PathBuf::from("/opt/data/ct_tc");
    let mut tc = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), fe, fd);
    
    let mut data_enc = tc.collect().unwrap();
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());
    
    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();

    old_count = next_count;
    let _jn = tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
        .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone());

    tc = sc.make_op(fe.clone(), fd.clone(), 1)
        .distinct();
    data_enc = tc.collect().unwrap();
    tc = sc.make_op(fe.clone(), fd.clone(), 1);
    
    next_count = tc.count().unwrap();

    
    
   
    Ok(())
}

pub fn transitive_closure_sec_1() -> Result<()> {
    let sc = Context::new()?;
    
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

    let dir = PathBuf::from("/opt/data/ct_tc");
    let mut tc = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), fe, fd);
    
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());

    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();

    old_count = next_count;
    tc = tc.union(
        tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
            .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone())
            .into()
    ).distinct();

    next_count = tc.count().unwrap();

    
    
   
    Ok(())
}

pub fn transitive_closure_sec_2() -> Result<()> {
    let sc = Context::new()?;

















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














    let mut tc = sc.make_op(fe.clone(), fd.clone(), 1);
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)), fe.clone(), fd.clone());

    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();

    old_count = next_count;
    let _jn = tc.join(edges.clone(), fe_jn.clone(), fd_jn.clone(), 1)
        .map(Fn!(|x: (u32, (u32, u32))| (x.1.1, x.1.0)), fe.clone(), fd.clone());

    tc = sc.make_op(fe.clone(), fd.clone(), 1)
        .distinct();

    next_count = tc.count().unwrap();





    Ok(())
}