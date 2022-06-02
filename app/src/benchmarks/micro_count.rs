use rand::Rng;
use std::time::Instant;
use vega::*;

// secure mode
pub fn count_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let data_enc = batch_encrypt(&(0..500_000).collect::<Vec<_>>());
    let now = Instant::now();
    let rdd0 = sc.make_rdd(vec![], data_enc, 1);
    let rdd1 = rdd0.map(Fn!(|i: i32| i % (1 << 10) * 4399 / (i % 71 + 1)));
    rdd1.cache();
    let res = rdd1.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, res = {:?}", dur, res);
    let now = Instant::now();
    let rdd2 = rdd1.map(Fn!(|i: i32| i % (1 << 10) * 8765 / (i % 97 + 1)));
    let res = rdd2.secure_count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, res = {:?}", dur, res);
    Ok(())
}

// unsecure mode
pub fn count_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let data = (0..100_000).collect::<Vec<_>>();
    let now = Instant::now();
    let rdd0 = sc.make_rdd(data, vec![], 1);
    let rdd1 = rdd0.map(Fn!(|i: i32| i % (1 << 10) * 4399 / (i % 71 + 1)));
    rdd1.cache();
    let res = rdd1.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, res = {:?}", dur, res);
    let now = Instant::now();
    let rdd2 = rdd1.map(Fn!(|i: i32| i % (1 << 10) * 8765 / (i % 97 + 1)));
    let res = rdd2.count().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s, res = {:?}", dur, res);
    Ok(())
}
