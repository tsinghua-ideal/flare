use std::time::Instant;
use vega::*;
use rand::Rng;
    
pub fn reduce_sec_0() -> Result<()> {
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

    let mut data = (0..10).collect::<Vec<_>>();
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

    let rdd0 = sc.make_rdd(vec![], data_enc, fe, fd, 1);
    let res = *rdd0.secure_reduce(Fn!(|x: i32, y: i32| x + y), Fn!(|x| x), Fn!(|x| x))?;
    println!("result: {:?}", res);
    Ok(())
}