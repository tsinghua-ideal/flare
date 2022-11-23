use crate::*;



pub fn sort_by_sec() -> Result<()> {
    let sc = Context::new()?;








    let now = Instant::now();
    let rdd0 = sc.make_op::<(i32, i32)>(NUM_PARTS);
    let rdd1 = rdd0.sort_by(true, NUM_PARTS, Fn!(|x: &(i32, i32)| x.0));
    let _res = rdd1.collect().unwrap();






    
    Ok(())
}
