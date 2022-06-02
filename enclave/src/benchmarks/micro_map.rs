
use crate::*;


// secure mode
pub fn map_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    let rdd0 = sc.make_op(1);
    let rdd1 = rdd0.map(Fn!(|i: i32|  i % (1 << 20) * 4399 / (i % 71 + 1) ));
    let _res = rdd1.collect().unwrap();
    
    
    
    Ok(())
}