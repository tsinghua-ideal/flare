
use crate::*;


// secure mode
pub fn zip_sec_0() -> Result<()> {
    let sc = Context::new()?;











    let rdd0 = sc.make_op::<i32>(NUM_PARTS);
    let rdd1 = sc.make_op::<String>(NUM_PARTS);
    let zipped_rdd = rdd0.zip(rdd1.into()); 
    let _res = zipped_rdd.collect().unwrap();



    Ok(())
}
