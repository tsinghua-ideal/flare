use rand::Rng;
use std::time::Instant;
use vega::*;

// secure mode
pub fn take_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let data_enc = batch_encrypt(&(0..100_000).collect::<Vec<_>>());
    let now = Instant::now();
    let rdd0 = sc.make_rdd::<i32, _, _>(vec![], data_enc, 1);
    let res = rdd0.secure_take(10).unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, res = {:?}", dur, res.get_pt());
    Ok(())
}

// unsecure mode
pub fn take_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let data = (0..100_000).collect::<Vec<_>>();
    let now = Instant::now();
    let rdd0 = sc.make_rdd(data, vec![], 1);
    let res = rdd0.take(10).unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, res = {:?}", dur, res);
    Ok(())
}
