
use crate::*;


// secure mode
pub fn map_sec_0() -> Result<()> {
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













    
    
    
    let rdd0 = sc.make_op(fe.clone(), fd.clone(), 1);
    let rdd1 = rdd0.map(Fn!(|i: i32|  i % (1 << 20) * 4399 / (i % 71 + 1) ), fe, fd);
    let _res = rdd1.collect().unwrap();
    
    
    
    Ok(())
}

