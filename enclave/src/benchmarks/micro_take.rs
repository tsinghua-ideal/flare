
use crate::*;


// secure mode
pub fn take_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    let rdd0 = sc.make_op::<i32>(1);
    let _res = rdd0.take(10).unwrap();

    Ok(())
}