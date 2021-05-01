use std::collections::HashSet;
use std::path::PathBuf;

use crate::*;


pub fn triangle_counting_sec_0() -> Result<()> {
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

    let fe_mp = Fn!(|vp: Vec<((u32, u32), i32)> | -> (Vec<u8>, Vec<i32>) {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        (buf0, buf1)
    });

    let fd_mp = Fn!(|ve: (Vec<u8>, Vec<i32>)| -> Vec<((u32, u32), i32)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<(u32, u32)> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<i32> = buf1;
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn1 = Fn!(move |vp: Vec<((u32, u32), ((u32, (u32, u32)), i32))>| {
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
        let buf1 = (fe_jn.clone())(buf1);
        (buf0, (buf1, buf2))
    });

    let fd_jn1 = Fn!(move |ve: (Vec<u8>, ((Vec<u8>, (Vec<u8>, Vec<u8>)), Vec<i32>))| {
        let (buf0, (buf1, buf2)) = ve;
        let mut pt0: Vec<(u32, u32)> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<(u32, (u32, u32))> = (fd_jn.clone())(buf1);
        let mut pt2 = buf2;
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

    let dir = PathBuf::from("/opt/data/ct_tc_1");
    let graph = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), None, Some(deserializer), fe.clone(), fd.clone())
        .filter(Fn!(|edge: &(u32, u32)| edge.0 != edge.1))
        .map(Fn!(|edge: (u32, u32)| match edge.0 < edge.1 {
            true => edge,
            false => (edge.1, edge.0)
        }), fe.clone(), fd.clone()).distinct();
    
    let count = graph.join(graph.clone(), fe_jn, fd_jn, 1)
        .key_by(Fn!(|item: &(u32, (u32, u32))| item.1))
        .join(graph.clone()
            .map(Fn!(|edge| (edge, 1 as i32)), fe_mp, fd_mp),
            fe_jn1, fd_jn1, 1
        ).count().unwrap();
    
   
    
    Ok(())
}