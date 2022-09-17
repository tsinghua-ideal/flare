
use crate::*;



// test simple case
pub fn join_sec_0() -> Result<()> {
    let sc = Context::new()?;

    
    
    
    
    
  



    
    let rdd0 = sc.make_op::<(i32, (String, String))>(2);
    
    
    
    
    
    
    
    
    
    


    
    let rdd1 = sc.make_op::<(i32, String)>(2);
    let rdd2 = rdd1.join(rdd0.clone(), 2);
    let _res = rdd2.collect().unwrap();
    
    Ok(())
}
   
//massive data
pub fn join_sec_2() -> Result<()> {
    let sc = Context::new()?;

    
    
    






























    let rdd0 = sc.make_op::<(i32, i32)>(8);
    let rdd1 = sc.make_op::<(i32, i32)>(8);
    let rdd2 = rdd1.join(rdd0.clone(), 8);
    let _res = rdd2.collect().unwrap();
    
    
    
    
    
    
    Ok(())   
}