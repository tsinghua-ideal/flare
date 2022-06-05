use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use crate::*;

// secure mode
pub fn pagerank_sec_0() -> Result<()> {
    let sc = Context::new()?;


    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let iters = 1;
    let dir = PathBuf::from("/opt/data/ct_pr_cit-Patents");
    let links = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(1),
            None,
            Some(deserializer),
        )
        .map(Fn!(|line: String| {
            let parts = line.split(" ").collect::<Vec<_>>();
            (parts[0].to_string(), parts[1].to_string())
        }))
        .distinct_with_num_partitions(1)
        .group_by_key(1);
    
    let mut ranks = links.map_values(Fn!(|_| 1.0));

    sc.enter_loop();
        let contribs =
            links
                .join(ranks, 1)
                .values()
                .flat_map(Fn!(|(urls, rank): (Vec<String>, f64)| {
                    let size = urls.len() as f64;
                    Box::new(urls.into_iter().map(move |url| (url, rank / size)))
                        as Box<dyn Iterator<Item = _>>
                }));
        ranks = contribs
            .reduce_by_key(Fn!(|(x, y)| x + y), 1)
            .map_values(Fn!(|v| 0.15 + 0.85 * v));
    sc.leave_loop();

    let _output = ranks
        .reduce(Fn!(|x: (String, f64), y: (String, f64)| {
            if x.1 > y.1 {
                x
            } else {
                y
            }
        }))
        .unwrap()
        .get_pt();
    
    
    

        
    Ok(())
}