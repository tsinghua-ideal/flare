
use crate::*;


// secure mode
pub fn distinct_sec_0() -> Result<()> {
    let sc = Context::new()?;

    

    let rdd0 = sc.make_op::<i32>(1);
    let rdd1 = rdd0.distinct();
    let _res = rdd1.collect().unwrap();
    
    
    
    Ok(())
}

