
use std::path::PathBuf;
use crate::*;


use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Point {
    x: Vec<f32>,
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

    let fe_mp = Fn!(|vp: Vec<Vec<f32>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_mp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<f32>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = (Vec<u8>, Vec<u8>)
    }));
    
    
    let dim = 5;
    let dir = PathBuf::from("/opt/data/ct_lr_3_5");
    let mut points_rdd = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), None, Some(deserializer), fe, fd);
    let mut w = Vec::new();

    sc.enter_loop();
    let w_c = w.clone();
    let g = (*points_rdd.map(Fn!(move |p: Point| {
            let y = p.y;
            p.x.iter().zip(w.iter())
                .map(|(&x, &w): (&f32, &f32)| x * (1f32/(1f32+(-y * (w * x)).exp())-1f32) * y)
                .collect::<Vec<_>>()
        }),
        fe_mp, 
        fd_mp
    ).reduce(Fn!(|x: Vec<f32>, y: Vec<f32>| x.into_iter()
            .zip(y.into_iter())
            .map(|(x, y)| x + y)
            .collect::<Vec<_>>()
        ), 
        Fn!(|x| x), 
        Fn!(|x| x)).unwrap()).clone();
   
    



    sc.leave_loop();
    
    
    
    
    Ok(())
}

