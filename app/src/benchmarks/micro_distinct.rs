use std::time::Instant;
use vega::*;
use rand::Rng;

// secure mode
pub fn distinct_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let fe = Fn!(|vp: Vec<i32>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        pt0
    });

    let mut data = vec![1; 100];
    let mut len = data.len();
    let mut data_enc = Vec::with_capacity(len);
    while len >= MAX_ENC_BL {
        len -= MAX_ENC_BL;
        let remain = data.split_off(MAX_ENC_BL);
        let input = data;
        data = remain;
        data_enc.push(fe(input));
    }
    if len != 0 {
        data_enc.push(fe(data));
    }

    let now = Instant::now();
    let rdd0 = sc.make_rdd(vec![], data_enc , fe.clone(), fd.clone(), 1);
    let rdd1 = rdd0.distinct();  
    let res = rdd1.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.to_plain().len());
    Ok(())
}
    
// unsecure mode
pub fn distinct_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let fe = Fn!(|vp: Vec<i32>| {
        vp
    });
    let fd = Fn!(|ve: Vec<i32>| {
        ve
    });
    let data = vec![1; 100];
    let now = Instant::now();
    let rdd0 = sc.make_rdd(data, vec![] , fe.clone(), fd.clone(), 1);
    let rdd1 = rdd0.distinct();
    let res = rdd1.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.len());
    Ok(())
}

