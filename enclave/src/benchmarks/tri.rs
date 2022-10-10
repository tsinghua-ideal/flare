use std::collections::HashSet;
use std::path::PathBuf;

use crate::*;


pub fn triangle_counting_sec_0() -> Result<()> {
    let sc = Context::new()?;
    

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));

    let dir = PathBuf::from("/opt/data/ct_tri_soc-Slashdot0811");
    let graph = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .filter(Fn!(|edge: &(u32, u32)| edge.0 != edge.1))
        .map(Fn!(|edge: (u32, u32)| match edge.0 < edge.1 {
            true => edge,
            false => (edge.1, edge.0),
        }))
        .distinct(); 
    
    let count = graph
        .join(graph.clone(), NUM_PARTS) //8
        .key_by(Fn!(|item: &(u32, (u32, u32))| item.1)) //9
        .join(
            graph
                .clone() //11
                .map(Fn!(|edge| (edge, 1 as i32))), //10
            NUM_PARTS,
        )
        .count()
        .unwrap();
    
   
    
    Ok(())
}