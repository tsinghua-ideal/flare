
use crate::*;


pub fn group_by_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    
    
    
    
    
    
    
    








    let rdd0 = sc.make_op::<(String, i32)>(NUM_PARTS);
    let rdd1 = rdd0.group_by_key(NUM_PARTS);
    let _res = rdd1.collect().unwrap();
    
    Ok(())
}

pub fn group_by_sec_1() -> Result<()> {
    let sc = Context::new()?;


    
    
    
    
    

    let r = sc.make_op::<(i32, i32)>(NUM_PARTS);
    let g = r.group_by_key(NUM_PARTS);
    let _res = g.collect().unwrap();
    
    
    
    Ok(())
}