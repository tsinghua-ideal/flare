use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;
use crate::*;


pub fn union_sec_0() -> Result<()> {
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

    let fe_gb = Fn!(|vp: Vec<(u32, Vec<u32>)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd_gb = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(u32, Vec<u32>)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<u32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<u32>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });













    
    let mut tc = sc.make_op(fe.clone(), fd.clone(), 1);
    let tc0 = tc.clone();
    let mut next_count = tc.count().unwrap();
    

    tc = tc.union(tc0.clone().into());
        //.distinct_with_num_partitions(1);
        //.group_by_key(fe_gb.clone(), fd_gb.clone(), 1)
        //.map(Fn!(|i: (u32, Vec<u32>)| (i.0, i.1[0])), fe.clone(), fd.clone());
        //.map(Fn!(|i: (u32, u32)| (i.0, i.1)), fe.clone(), fd.clone());

    next_count = tc.count().unwrap();
    
    
    
    //next_count = tc.count().unwrap();
    
    
    Ok(())
}