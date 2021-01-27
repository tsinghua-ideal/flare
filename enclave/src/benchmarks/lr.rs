use std::sync::atomic::Ordering;
use std::vec::Vec;

use crate::op::*;
use crate::Fn;

use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Point {
    x: f32,
    y: f32,
}

// secure mode
pub fn lr_sec() -> usize {
    let sc = Context::new();
    let fe = Fn!(|vp: Vec<Point>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Point> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_mp = Fn!(|vp: Vec<f32>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_mp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<f32> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_rd = Fn!(|vp: Vec<f32>| {
        vp
    });
    let fd_rd = Fn!(|ve: Vec<f32>| {
        ve
    });

    let nums = sc.make_op(fe, fd, 1);
    let w = 0 as f32;
    //loop 0 boundary
    let iter_num = 3;   //need to manually change now
    let lower_bound = nums.get_id();  //0 
    let g = nums.map(Fn!(move |p: Point|
                    p.x*(1f32/(1f32+(-p.y*(w * p.x)).exp())-1f32)*p.y
                ),
                fe_mp,
                fd_mp,
            )
        .reduce(Fn!(|x, y| x+y), fe_rd, fd_rd);
    let upper_bound =g.get_id();   //1
    unsafe{ crate::lp_boundary.load(Ordering::Relaxed).as_mut()}
        .unwrap()
        .push((lower_bound, upper_bound, iter_num));
    //loop 0 boundary
    g.get_id()
}

