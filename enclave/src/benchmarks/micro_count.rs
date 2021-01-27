use std::boxed::Box;
use std::default::Default;
use std::vec::Vec;
use crate::op::*;
use crate::Fn;

// secure mode
pub fn count_sec_0() -> usize {
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
    let rdd0 = sc.make_op(fe.clone(), fd.clone(), 1);
    let rdd1 = rdd0.map(Fn!(|i: i32|  i % (1 << 10) * 4399 / (i % 71 + 1) ), fe.clone(), fd.clone());
    let _res = rdd1.count();
    let rdd2 = rdd1.map(Fn!(|i: i32|  i % (1 << 10) * 8765 / (i % 97 + 1) ), fe, fd);
    let _res = rdd2.count();
    rdd2.get_id()
}