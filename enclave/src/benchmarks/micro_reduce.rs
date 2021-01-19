use std::boxed::Box;
use std::vec::Vec;
use crate::op::*;
use crate::Fn;

pub fn reduce_sec_0() -> usize {
    let sc = Context::new();
    let fe = Fn!(|vp: Vec<i32>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        pt0
    });

    let rdd0 = sc.make_op(fe, fd);
    let rdd1 = rdd0.reduce(Fn!(|x, y| x+y));
    rdd1.get_id() 
}