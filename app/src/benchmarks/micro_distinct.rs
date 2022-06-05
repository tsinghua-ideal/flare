use rand::Rng;
use std::time::Instant;
use vega::*;

// secure mode
pub fn distinct_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let data_enc = batch_encrypt(&vec![1i32; 100]);
    let now = Instant::now();
    let rdd0 = sc.make_rdd::<i32, _, _>(vec![], data_enc, 1);
    let rdd1 = rdd0.distinct();
    let res = rdd1.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.get_pt().len());
    Ok(())
}

// unsecure mode
pub fn distinct_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let data = vec![1; 100];
    let now = Instant::now();
    let rdd0 = sc.make_rdd(data, vec![], 1);
    let rdd1 = rdd0.distinct();
    let res = rdd1.collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.len());
    Ok(())
}
