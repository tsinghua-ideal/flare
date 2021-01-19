use std::boxed::Box;
use std::vec::Vec;
use crate::op::*;

// secure mode
pub fn map_sec_0() -> usize {
    let sc = Context::new();
    let fe = Box::new(|vp: Vec<i32>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Box::new(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        pt0
    });
    let rdd0 = sc.make_op(fe.clone(), fd.clone());
    let rdd1 = rdd0.map(Box::new(|i: i32|  i % (1 << 20) * 4399 / (i % 71 + 1) ), fe, fd);
    rdd1.get_id()
}
