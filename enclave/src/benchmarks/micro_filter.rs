
use crate::*;


// secure mode
pub fn filter_sec_0() -> Result<()> {
    let sc = Context::new()?;



    let rdd0 = sc.make_op(1);
    let rdd1 = rdd0.filter(Fn!(|i: &i32|  i % 5 == 0));
    let _res = rdd1.collect().unwrap();
    
    
    
    Ok(())
}