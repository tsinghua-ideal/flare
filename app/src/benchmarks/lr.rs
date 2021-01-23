use std::time::Instant;
use vega::*;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    x: f32,
    y: f32,
}

// secure mode
pub fn lr_sec() -> Result<()> {
    let sc = Context::new()?;
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

    let mut rng = rand::thread_rng();
    let point_num = 1_000_000;
    let mut data: Vec<Point> = Vec::with_capacity(point_num);
    for _ in 0..point_num { 
        let point = Point { x: rng.gen(), y: rng.gen() };
        data.push(point);
    } 
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

    let points_rdd = sc.make_rdd(vec![], data_enc, fe, fd, 1);
    let mut w = rng.gen::<f32>();
    let iter_num = 3;
    let now = Instant::now();
    for i in 0..iter_num {
        let g = points_rdd.map(Fn!(move |p: Point| 
                p.x * (1f32/(1f32+(-p.y * (w * p.x)).exp())-1f32) * p.y
            ),
            fe_mp, 
            fd_mp
        ).secure_reduce(Fn!(|x, y| x+y), fe_rd, fd_rd).unwrap();
        w -= g.unwrap()[0];
        println!("{:?}: w = {:?}", i, w);
    } 
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("w = {:?}", w);

    Ok(())
}
    
// unsecure mode
pub fn lr_unsec() -> Result<()> {
    let sc = Context::new()?;
    let fe = Fn!(|vp: Vec<Point>| {
        vp
    });

    let fd = Fn!(|ve: Vec<Point>| {
        ve
    });

    let fe_mp = Fn!(|vp: Vec<f32>| {
        vp
    });

    let fd_mp = Fn!(|ve: Vec<f32>| {
        ve
    });

    let mut rng = rand::thread_rng();
    let point_num = 1_000_000;
    let mut data: Vec<Point> = Vec::with_capacity(point_num);
    for _ in 0..point_num { 
        let point = Point { x: rng.gen(), y: rng.gen() };
        data.push(point);
    } 

    let points_rdd = sc.make_rdd(data, vec![], fe, fd, 1);
    let mut w = rng.gen::<f32>();
    let iter_num = 3;
    let now = Instant::now();
    for i in 0..iter_num {
        let g = points_rdd.map(Fn!(move |p: Point| 
                p.x * (1f32/(1f32+(-p.y * (w * p.x)).exp())-1f32) * p.y
            ),
            fe_mp, 
            fd_mp
        ).reduce(Fn!(|x, y| x+y)).unwrap();
        w -= g.unwrap();
        println!("{:?}: w = {:?}", i, w);
    } 
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("w = {:?}", w);

    Ok(())
}

