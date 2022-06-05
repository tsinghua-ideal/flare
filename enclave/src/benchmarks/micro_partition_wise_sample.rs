
use crate::*;


// secure mode
pub fn part_wise_sample_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    let rdd0 = sc.make_op::<i32>(1);
    let _res = rdd0.take_sample(false, 200, Some(123))?;
    
    
    
    
    Ok(())
}

