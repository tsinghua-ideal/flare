use rand::Rng;
use std::time::Instant;
use vega::*;

// secure mode
pub fn filter_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let data_enc = batch_encrypt(&(0..10_000_000).collect::<Vec<_>>());
    let now = Instant::now();
    let rdd0 = sc.make_rdd(vec![], data_enc, 1);
    let rdd1 = rdd0.filter(Fn!(|i: &i32| i % 5 == 0));
    let res = rdd1.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.get_pt().len());
    Ok(())
}

// unsecure mode
pub fn filter_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let data = (0..10_000_000).collect::<Vec<_>>();
    let now = Instant::now();
    let rdd0 = sc.make_rdd(data, vec![], 1);
    let rdd1 = rdd0.filter(Fn!(|i: &i32| i % 5 == 0));
    let res = rdd1.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.len());
    Ok(())
}
