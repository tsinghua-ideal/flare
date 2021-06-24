use std::time::Instant;
use vega::*;
use rand::Rng;

// secure mode
pub fn zip_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let fe0 = Fn!(|vp: Vec<i32>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd0 = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe1 = Fn!(|vp: Vec<String>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd1 = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<String> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_zip = Fn!(|vp: Vec<(i32, String)> | -> (Vec<u8>, Vec<u8>) {
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

    let fd_zip = Fn!(|ve: (Vec<u8>, Vec<u8>)| -> Vec<(i32, String)> {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let col0 = vec![1, 2, 3, 4, 5];
    let col1 = vec![
        "5a".to_string(),
        "4b".to_string(),
        "3c".to_string(),
        "2d".to_string(),
        "1a".to_string(),
    ];

    let col0 = batch_encrypt(col0, fe0.clone());
    let col1 = batch_encrypt(col1, fe1.clone());

    let now = Instant::now();
    let rdd0 = sc.make_rdd(vec![], col0, fe0, fd0, 1);
    let rdd1 = sc.make_rdd(vec![], col1, fe1, fd1, 1);
    let zipped_rdd = rdd0.zip(rdd1.into(), fe_zip, fd_zip); 
    let res = zipped_rdd.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.to_plain().len());
    Ok(())
}
