
use crate::*;


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

    let fe_rd = Fn!(|vp: Vec<i32>| {
        vp
    });
    let fd_rd = Fn!(|ve: Vec<i32>| {
        ve
    });

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    let rdd0 = sc.make_op(fe, fd, 1);
    let _res = rdd0.reduce(Fn!(|x, y| x+y), fe_rd, fd_rd)?;
    
    Ok(()) 
}