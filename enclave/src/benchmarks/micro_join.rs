use std::boxed::Box;
use std::string::String;
use std::vec::Vec;
use crate::op::*;
use crate::Fn;

pub fn join_sec_0() -> usize {
    let sc = Context::new();
    let rdd0_fe = Fn!(|vp: Vec<(i32, (String, String))> | -> (Vec<u8>, (Vec<u8>, Vec<u8>)) {
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

    let rdd0_fd = Fn!(|ve: (Vec<u8>, (Vec<u8>, Vec<u8>))| -> Vec<(i32, (String, String))> {
        let (buf0, (buf1, buf2)) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<String> = ser_decrypt::<>(buf2);
        let len = pt0.len() | pt1.len() | pt2.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1.into_iter()
                .zip(pt2.into_iter())
            ).collect::<Vec<_>>() 
    });

    let rdd1_fe = Fn!(|vp: Vec<(i32, String)> | -> (Vec<u8>, Vec<u8>) {
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

    let rdd1_fd = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(i32, String)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn = Fn!(|vp: Vec<(i32, (String, (String, String)))>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        let mut buf2 = Vec::with_capacity(len);
        let mut buf3 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1.0);
            buf2.push(i.1.1.0);
            buf3.push(i.1.1.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        let buf2 = ser_encrypt::<>(buf2);
        let buf3 = ser_encrypt::<>(buf3);
        (buf0, (buf1, (buf2, buf3)))
    });

    let fd_jn = Fn!(|ve: (Vec<u8>, (Vec<u8>, (Vec<u8>, Vec<u8>)))| {
        let (buf0, (buf1, (buf2, buf3))) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<String> = ser_decrypt::<>(buf2);
        let mut pt3: Vec<String> = ser_decrypt::<>(buf3);
        let len = pt0.len() | pt1.len() | pt2.len() | pt3.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt3.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1.into_iter()
                .zip(pt2.into_iter()
                    .zip(pt3.into_iter())
                )
            )
            .collect::<Vec<_>>() 
    });
    let rdd0 = sc.make_op(rdd0_fe, rdd0_fd, 1);
    let rdd1 = sc.make_op(rdd1_fe, rdd1_fd, 1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
    rdd2.get_id()
}
        
pub fn join_sec_1() -> usize {
    let sc = Context::new();
    let rdd0_fe = Fn!(|vp: Vec<(i32, (String, String))> | -> (Vec<i32>, (Vec<String>, Vec<String>)) {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        let mut buf2 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1.0);
            buf2.push(i.1.1);
        }
        (buf0, (buf1, buf2))
    });

    let rdd0_fd = Fn!(|ve: (Vec<i32>, (Vec<String>, Vec<String>))| -> Vec<(i32, (String, String))> {
        let (mut pt0, (mut pt1, mut pt2)) = ve;
        let len = pt0.len() | pt1.len() | pt2.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1.into_iter()
                .zip(pt2.into_iter())
            ).collect::<Vec<_>>() 
    });

    let rdd1_fe = Fn!(|vp: Vec<(i32, String)> | -> (Vec<i32>, Vec<String>) {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        (buf0, buf1)
    });

    let rdd1_fd = Fn!(|ve: (Vec<i32>, Vec<String>)| -> Vec<(i32, String)> {
        let (mut pt0, mut pt1) = ve;
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn = Fn!(|vp: Vec<(i32, (String, (String, String)))>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        let mut buf2 = Vec::with_capacity(len);
        let mut buf3 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1.0);
            buf2.push(i.1.1.0);
            buf3.push(i.1.1.1);
        }
        (buf0, (buf1, (buf2, buf3)))
    });

    let fd_jn = Fn!(|ve: (Vec<i32>, (Vec<String>, (Vec<String>, Vec<String>)))| {
        let (mut pt0, (mut pt1, (mut pt2, mut pt3))) = ve;
        let len = pt0.len() | pt1.len() | pt2.len() | pt3.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt3.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1.into_iter()
                .zip(pt2.into_iter()
                    .zip(pt3.into_iter())
                )
            )
            .collect::<Vec<_>>() 
    });
    let rdd0 = sc.make_op(rdd0_fe, rdd0_fd, 1);
    let rdd1 = sc.make_op(rdd1_fe, rdd1_fd, 1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
    rdd2.get_id()
    
}

pub fn join_sec_2() -> usize {
    let sc = Context::new();
    let fe = Fn!(|vp: Vec<(i32, i32)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(i32, i32)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<i32> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });
    
    let fe_jn = Fn!(|vp: Vec<(i32, (i32, i32))>| {
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
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<i32> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<i32> = ser_decrypt::<>(buf2);
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

    let rdd0 = sc.make_op(fe.clone(), fd.clone(), 1);
    let rdd1 = sc.make_op(fe.clone(), fd.clone(), 1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
    rdd2.get_id()   
}