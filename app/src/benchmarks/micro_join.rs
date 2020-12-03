use std::time::Instant;
use vega::*;
use rand::Rng;

// test simple case  
pub fn join_sec_0() -> Result<()> {
    let sc = Context::new()?;
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
    let col0 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    let col0_enc = vec![rdd0_fe(col0)];
    let rdd0 = sc.parallelize(vec![], 
        col0_enc,
        rdd0_fe,
        rdd0_fd,
        1);
    let col1 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];
    let col1_enc = vec![rdd1_fe(col1)];

    let rdd1 = sc.parallelize(vec![], 
        col1_enc,  
        rdd1_fe,
        rdd1_fd,
        1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn,1);
    let res = rdd2.secure_collect().unwrap();
    println!("result: {:?}", rdd2.batch_decrypt(res));
    Ok(())
}
   
//test with identical map
pub fn join_sec_1() -> Result<()> {

    let sc = Context::new()?;
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

    let col0 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    let col0_enc = vec![rdd0_fe(col0)];
    let rdd0 = sc.parallelize(vec![], 
        col0_enc,
        rdd0_fe,
        rdd0_fd,
        1);
    let col1 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];
    let col1_enc = vec![rdd1_fe(col1)];

    let rdd1 = sc.parallelize(vec![], 
        col1_enc,  
        rdd1_fe,
        rdd1_fd,
        1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn,1);
    let res = rdd2.secure_collect().unwrap();
    println!("result: {:?}", rdd2.batch_decrypt(res));
    Ok(())
}
    

pub fn join_unsec_2 () -> Result<()> {
    let sc = Context::new()?;
    let len = 1_0000;
    let mut vec0: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut vec1: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec0.push((rng.gen::<i32>() % 100, rng.gen()));
        vec1.push((rng.gen::<i32>() % 100, rng.gen()));
    }

    //encrypt first for carrying out experiment
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

    let now = Instant::now();
    let rdd0 = sc.parallelize(vec0, vec![], fe.clone(), fd.clone(), 1);
    let rdd1 = sc.parallelize(vec1, vec![], fe.clone(), fd.clone(), 1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
    let _res = rdd2.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", _res.len());
    Ok(())
}
    
//massive data
pub fn join_sec_2() -> Result<()> {
    let sc = Context::new()?;
    let len = 1_0000;
    let mut vec0: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut vec1: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec0.push((rng.gen::<i32>() % 100, rng.gen()));
        vec1.push((rng.gen::<i32>() % 100, rng.gen()));
    }

    //encrypt first for carrying out experiment
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

    let mut vec0_enc = Vec::with_capacity(len);
    let mut cur = 0;
    while cur < len {
        let next = match cur + MAX_ENC_BL  > len {
            true => len,
            false => cur + MAX_ENC_BL ,
        };
        vec0_enc.push(fe((&vec0[cur..next]).to_vec()));
        cur = next;
    }

    let mut vec1_enc = Vec::with_capacity(len);
    let mut cur = 0;
    while cur < len {
        let next = match cur + MAX_ENC_BL  > len {
            true => len,
            false => cur + MAX_ENC_BL ,
        };
        vec1_enc.push(fe((&vec1[cur..next]).to_vec()));
        cur = next;
    }

    let now = Instant::now();
    let rdd0 = sc.parallelize(vec![], vec0_enc, fe.clone(), fd.clone(), 1);
    let rdd1 = sc.parallelize(vec![], vec1_enc, fe.clone(), fd.clone(), 1);
    let rdd2 = rdd1.join(rdd0.clone(), fe_jn, fd_jn, 1);
    let _res = rdd2.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", rdd2.batch_decrypt(_res).len());
    Ok(())
}