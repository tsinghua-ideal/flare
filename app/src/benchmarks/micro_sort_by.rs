use crate::*;
use rand::Rng;
use std::time::Instant;

pub fn sort_by_sec() -> Result<()> {
    let sc = Context::new()?;
    let len = 1_000_000;
    let mut vec: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec.push((rng.gen::<i32>(), rng.gen()));
    }
    let vec_enc = batch_encrypt(&vec);

    let now = Instant::now();
    let rdd0 = sc.parallelize::<(i32, i32), _, _>(vec![], vec_enc, NUM_PARTS);
    let rdd1 = rdd0.sort_by(true, NUM_PARTS, Fn!(|x: &(i32, i32)| x.0));
    let res = rdd1.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    vec.sort_by_key(|x| x.0);
    assert_eq!(
        res.get_pt().into_iter().map(|x| x.0).collect::<Vec<_>>(),
        vec.into_iter().map(|x| x.0).collect::<Vec<_>>()
    );
    Ok(())
}

pub fn sort_by_unsec() -> Result<()> {
    let sc = Context::new()?;
    let len = 1_000_000;
    let mut vec: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec.push((rng.gen::<i32>(), rng.gen()));
    }

    let now = Instant::now();
    let rdd0 = sc.parallelize(vec.clone(), vec![], NUM_PARTS);
    let rdd1 = rdd0.sort_by(true, NUM_PARTS, Fn!(|x: &(i32, i32)| x.0));
    let res = rdd1.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    vec.sort_by_key(|x| x.0);
    assert_eq!(
        res.into_iter().map(|x| x.0).collect::<Vec<_>>(),
        vec.into_iter().map(|x| x.0).collect::<Vec<_>>()
    );
    Ok(())
}
