use rand::Rng;
use std::time::Instant;
use vega::*;

pub fn group_by_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let data_enc = batch_encrypt(&vec![
        ("x".to_string(), 1i32),
        ("x".to_string(), 2),
        ("x".to_string(), 3),
        ("x".to_string(), 4),
        ("x".to_string(), 5),
        ("x".to_string(), 6),
        ("x".to_string(), 7),
        ("y".to_string(), 1),
        ("y".to_string(), 2),
        ("y".to_string(), 3),
        ("y".to_string(), 4),
        ("y".to_string(), 5),
        ("y".to_string(), 6),
        ("y".to_string(), 7),
        ("y".to_string(), 8),
    ]);
    let rdd0 = sc.make_rdd::<(String, i32), _, _>(vec![], data_enc, 1);
    let rdd1 = rdd0.group_by_key(1);
    let res = rdd1.secure_collect().unwrap();
    println!("result: {:?}", res.get_pt());
    Ok(())
}

pub fn group_by_sec_1() -> Result<()> {
    let sc = Context::new()?;
    let mut len = 1_000_000;
    let mut vec: Vec<(i32, i32)> = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    for _ in 0..len {
        vec.push((rng.gen(), rng.gen()));
    }
    let data_enc = batch_encrypt(&vec);
    let now = Instant::now();
    let r = sc.make_rdd::<(i32, i32), _, _>(vec![], data_enc, 4);
    let g = r.group_by_key(4);
    let res = g.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result.len(): {:?}", res.get_pt().len());
    Ok(())
}
