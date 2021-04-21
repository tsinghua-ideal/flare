
use crate::*;


use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
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

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    let nums = sc.make_op(fe, fd, 1);
    let mut w = 0 as f32;

    sc.enter_loop();
    let _g = nums.map(Fn!(move |p: Point|
                    p.x*(1f32/(1f32+(-p.y*(w * p.x)).exp())-1f32)*p.y
                ),
                fe_mp,
                fd_mp,
            ).reduce(Fn!(|x, y| x+y), fe_rd, fd_rd).unwrap();
   
    
    sc.leave_loop();
    
    
    
    
    Ok(())
}

