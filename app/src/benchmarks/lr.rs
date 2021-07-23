use std::time::Instant;
use std::path::PathBuf;
use vega::*;
use rand::Rng;
use rand_distr::{Normal, Distribution};
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

    let mut rng = rand::thread_rng();
    let dim = 5;
    let dir = PathBuf::from("/opt/data/ct_lr_51072");
    let mut points_rdd = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), None, Some(deserializer), fe, fd);
    let mut w = (0..dim).map(|_| rng.gen::<f32>()).collect::<Vec<_>>();  //TODO: wrapper with Ciphertext? 
    let now = Instant::now();
    for i in 0..3 {
        let w_c = w.clone();
        let g = (*points_rdd.map(Fn!(move |p: Point| {
                let y = p.y;
                p.x.iter().zip(w.iter())
                    .map(|(&x, &w): (&f32, &f32)| x * (1f32/(1f32+(-y * (w * x)).exp())-1f32) * y)
                    .collect::<Vec<_>>()
            }),
            fe_mp,
            fd_mp
        ).secure_reduce(Fn!(|x: Vec<f32>, y: Vec<f32>| x.into_iter()
                .zip(y.into_iter())
                .map(|(x, y)| x + y)
                .collect::<Vec<_>>()
            ), 
            Fn!(|x| x), 
            Fn!(|x| x)).unwrap()).clone();
        w = w_c.into_iter()
            .zip(g.into_iter())
            .map(|(w, g)| w-g)
            .collect::<Vec<_>>();
        println!("{:?}: w = {:?}", i, w);
    } 
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?} s", dur);
    println!("w = {:?}", w);

    Ok(())
}
    
// unsecure mode
pub fn lr_unsec() -> Result<()> {
    let sc = Context::new()?;

    let lfe = Fn!(|vp: Vec<Vec<Point>>| {
        vp
    });
    let lfd = Fn!(|ve: Vec<Vec<Point>>| {
        ve
    });

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

    let mut rng = rand::thread_rng();
    let dim = 5;
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Point>>(&file).unwrap()  //Item = Point
    }));

    let dir = PathBuf::from("/opt/data/pt_lr_51072");
    let mut points_rdd = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), Some(deserializer), None, lfe, lfd)
        .flat_map(Fn!(|v: Vec<Point>| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>), fe.clone(), fd.clone());
    let mut w = (0..dim).map(|_| rng.gen::<f32>()).collect::<Vec<_>>();  
    let iter_num = 3;
    let now = Instant::now();
    for i in 0..iter_num {
        let w_c = w.clone();
        let g = points_rdd.map(Fn!(move |p: Point| {
                let y = p.y;    
                p.x.iter().zip(w.iter())
                    .map(|(&x, &w): (&f32, &f32)| x * (1f32/(1f32+(-y * (w * x)).exp())-1f32) * y)
                    .collect::<Vec<_>>()
            }),
            fe_mp, 
            fd_mp)
        .reduce(Fn!(|x: Vec<f32>, y: Vec<f32>| x.into_iter()
            .zip(y.into_iter())
            .map(|(x, y)| x + y)
            .collect::<Vec<_>>()))
        .unwrap();
        w = w_c.into_iter()
            .zip(g.unwrap().into_iter())
            .map(|(x, y)| x-y)
            .collect::<Vec<_>>();
        println!("{:?}: w = {:?}", i, w);
    } 
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("total time {:?} s", dur);
    println!("w = {:?}", w);

    Ok(())
}

