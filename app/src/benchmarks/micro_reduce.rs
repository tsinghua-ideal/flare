use crate::*;
use rand::Rng;
use std::time::Instant;

pub fn reduce_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let data_enc = batch_encrypt(&(0..10).collect::<Vec<_>>());
    let rdd0 = sc.make_rdd(vec![], data_enc, NUM_PARTS);
    let res = *rdd0.secure_reduce(Fn!(|x: i32, y: i32| x + y))?;
    println!("result: {:?}", res);
    Ok(())
}
