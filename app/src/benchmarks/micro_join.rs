use rand::Rng;
use std::time::Instant;
use vega::*;

// test simple case
pub fn join_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let col0 = vec![
        (1, ("A".to_string(), "B".to_string())),
        (2, ("C".to_string(), "D".to_string())),
        (3, ("E".to_string(), "F".to_string())),
        (4, ("G".to_string(), "H".to_string())),
    ];
    let col0_enc = batch_encrypt(&col0);
    let rdd0 = sc.parallelize::<(i32, (String, String)), _, _>(vec![], col0_enc, 1);
    let col1 = vec![
        (1, "A1".to_string()),
        (1, "A2".to_string()),
        (2, "B1".to_string()),
        (2, "B2".to_string()),
        (3, "C1".to_string()),
        (3, "C2".to_string()),
    ];
    let col1_enc = batch_encrypt(&col1);

    let rdd1 = sc.parallelize::<(i32, String), _, _>(vec![], col1_enc, 1);
    let rdd2 = rdd1.join(rdd0.clone(), 1);
    let res = rdd2.secure_collect().unwrap();
    println!("result: {:?}", res.get_pt());
    Ok(())
}

//massive data
pub fn join_sec_2() -> Result<()> {
    let sc = Context::new()?;
    let len = 1_0000;
    let mut vec0: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut vec1: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec0.push((rng.gen::<i32>() % 100, rng.gen()));
        vec1.push((rng.gen::<i32>() % 100, rng.gen()));
    }
    let vec0_enc = batch_encrypt(&vec0);
    let vec1_enc = batch_encrypt(&vec1);

    let now = Instant::now();
    let rdd0 = sc.parallelize::<(i32, i32), _, _>(vec![], vec0_enc, 1);
    let rdd1 = sc.parallelize::<(i32, i32), _, _>(vec![], vec1_enc, 1);
    let rdd2 = rdd1.join(rdd0.clone(), 1);
    let _res = rdd2.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", _res.get_pt().len());
    Ok(())
}

pub fn join_unsec_2() -> Result<()> {
    let sc = Context::new()?;
    let len = 1_0000;
    let mut vec0: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut vec1: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec0.push((rng.gen::<i32>() % 100, rng.gen()));
        vec1.push((rng.gen::<i32>() % 100, rng.gen()));
    }

    let now = Instant::now();
    let rdd0 = sc.parallelize(vec0, vec![], 1);
    let rdd1 = sc.parallelize(vec1, vec![], 1);
    let rdd2 = rdd1.join(rdd0.clone(), 1);
    let _res = rdd2.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", _res.len());
    Ok(())
}
