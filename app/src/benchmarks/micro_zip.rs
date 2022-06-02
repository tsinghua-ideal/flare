use rand::Rng;
use std::time::Instant;
use vega::*;

// secure mode
pub fn zip_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let col0 = vec![1, 2, 3, 4, 5];
    let col1 = vec![
        "5a".to_string(),
        "4b".to_string(),
        "3c".to_string(),
        "2d".to_string(),
        "1a".to_string(),
    ];
    let col0 = batch_encrypt(&col0);
    let col1 = batch_encrypt(&col1);
    let now = Instant::now();
    let rdd0 = sc.make_rdd::<i32, _, _>(vec![], col0, 1);
    let rdd1 = sc.make_rdd::<String, _, _>(vec![], col1, 1);
    let zipped_rdd = rdd0.zip(rdd1.into());
    let res = zipped_rdd.secure_collect().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("Total time {:?} s", dur);
    println!("result: {:?}", res.get_pt().len());
    Ok(())
}
