
use crate::*;


// secure mode
pub fn count_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    
    let rdd0 = sc.make_op(1);
    let rdd1 = rdd0.map(Fn!(|i: i32|  i % (1 << 10) * 4399 / (i % 71 + 1) ));
    
    let _res = rdd1.count().unwrap();
    
    
    
    let rdd2 = rdd1.map(Fn!(|i: i32|  i % (1 << 10) * 8765 / (i % 97 + 1) ));
    let _res = rdd2.count().unwrap();
    
    
    Ok(())
}