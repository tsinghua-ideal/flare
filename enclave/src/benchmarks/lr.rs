use deepsize::DeepSizeOf;

use std::path::PathBuf;
use crate::*;


use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, DeepSizeOf, Default, Clone, Debug)]
pub struct Point {
    x: Vec<f32>,
    y: f32,
}

// secure mode
pub fn lr_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = (Vec<u8>, Vec<u8>)
    }));
    
    
    let dim = 2;
    let dir = PathBuf::from("/opt/data/ct_lr_51072");
    let points_rdd = sc.read_source(
        LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
        None,
        Some(deserializer),
    );
    let mut w = Vec::new();

    sc.enter_loop();
    let w_c = w.clone();
    let g = points_rdd
        .map(Fn!(move |p: Point| {
            let y = p.y;
            p.x.iter()
                .zip(w.iter())
                .map(|(&x, &w): (&f32, &f32)| {
                    x * (1f32 / (1f32 + (-y * (w * x)).exp()) - 1f32) * y
                })
                .collect::<Vec<_>>()
        }))
        .reduce(Fn!(|x: Vec<f32>, y: Vec<f32>| x
            .into_iter()
            .zip(y.into_iter())
            .map(|(x, y)| x + y)
            .collect::<Vec<_>>()))
        .unwrap()
        .get_pt();
   
    



        
    sc.leave_loop();
    
    
    
    
    Ok(())
}

