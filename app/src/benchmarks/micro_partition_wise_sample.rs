use rand::Rng;
use std::time::Instant;
use vega::*;

// secure mode
pub fn part_wise_sample_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let data_enc = batch_encrypt(&(0..10_000).collect::<Vec<_>>());
    let now = Instant::now();
    let rdd0 = sc.make_rdd::<i32, _, _>(vec![], data_enc, 1);
    let res = rdd0.secure_take_sample(false, 200, Some(123))?;
    let res = res.get_pt();
    assert!(res.len() == 200);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    Ok(())
}

// unsecure mode
pub fn part_wise_sample_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let data = vec![1, 2, 3, 4, 5];
    let now = Instant::now();
    let rdd0 = sc.make_rdd(data, vec![], 6);
    let res = rdd0.take_sample(false, 6, Some(123))?;
    assert!(res.len() == 5);
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    Ok(())
}
