
use crate::*;



// test simple case
pub fn join_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    
    
    
    
  



    
    let rdd0 = sc.make_op::<(i32, (String, String))>(NUM_PARTS);
    
    
    
    
    
    
    
    
    
    
    
    
    
    let rdd1 = sc.make_op::<(i32, String)>(NUM_PARTS);
    let rdd2 = rdd1.join(rdd0.clone(), NUM_PARTS);
    let _res = rdd2.collect().unwrap();
    
    Ok(())
}
   
//massive data
pub fn join_sec_2() -> Result<()> {
    let sc = Context::new()?;

    
    
    








    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    let rdd0 = sc.make_op::<(i32, i32)>(NUM_PARTS);
    let rdd1 = sc.make_op::<(i32, i32)>(NUM_PARTS);
    let rdd2 = rdd1.join(rdd0.clone(), NUM_PARTS);
    let _res = rdd2.collect().unwrap();
    
    
    
    
    
    
    Ok(())   
}