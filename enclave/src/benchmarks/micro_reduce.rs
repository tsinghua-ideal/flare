
use crate::*;


pub fn reduce_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let rdd0 = sc.make_op(NUM_PARTS);
    let _res = *rdd0.reduce(Fn!(|x: i32, y: i32| x + y))?;
    
    Ok(()) 
}