
use crate::*;


// secure mode
pub fn filter_sec_0() -> Result<()> {
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
    let rdd1 = rdd0.filter(Fn!(|i: &i32|  i % 5 == 0));
    let _res = rdd1.collect().unwrap();
    
    
    
    Ok(())
}