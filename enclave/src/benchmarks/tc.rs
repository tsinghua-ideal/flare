use std::collections::HashSet;
use std::path::PathBuf;

use crate::*;


pub fn transitive_closure_sec_0() -> Result<()> {

    let sc = Context::new()?;
    

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));

    let dir = PathBuf::from("/opt/data/ct_tc_fb");
    let mut tc = sc.read_source(
        LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
        None,
        Some(deserializer),
    );
    
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();

    sc.enter_loop();
    old_count = next_count;
    tc = tc
        .union(
            tc.join(edges.clone(), NUM_PARTS)
                .map(Fn!(|x: (u32, (u32, u32))| (x.1 .1, x.1 .0)))
                .into(),
        )
        .distinct_with_num_partitions(NUM_PARTS);

    next_count = tc.count().unwrap();


    sc.leave_loop();
    
   
    Ok(())
}

pub fn transitive_closure_sec_1() -> Result<()> {
    let sc = Context::new()?;

















    let mut tc = sc.make_op(NUM_PARTS);
    let edges = tc.map(Fn!(|x: (u32, u32)| (x.1, x.0)));

    let mut old_count = 0;
    let mut next_count = tc.count().unwrap();
    sc.enter_loop();
    old_count = next_count;
    tc = tc
        .union(
            tc.join(edges.clone(), NUM_PARTS)
                .map(Fn!(|x: (u32, (u32, u32))| (x.1 .1, x.1 .0)))
                .into(),
        )
        .distinct_with_num_partitions(NUM_PARTS);

    next_count = tc.count().unwrap();

    sc.leave_loop();


    Ok(())
}