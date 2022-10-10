use crate::*;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Instant;

// secure mode
pub fn pagerank_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let iters = 1;
    let dir = PathBuf::from("/opt/data/ct_pr_cit-Patents");
    let links = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .map(Fn!(|line: String| {
            let parts = line.split(" ").collect::<Vec<_>>();
            (parts[0].to_string(), parts[1].to_string())
        }))
        .distinct_with_num_partitions(NUM_PARTS)
        .group_by_key(NUM_PARTS);
    links.cache();
    let mut ranks = links.map_values(Fn!(|_| 1.0));

    for _ in 0..iters {
        let contribs = links
            .join(ranks, NUM_PARTS)
            .values()
            .flat_map(Fn!(|(urls, rank): (Vec<String>, f64)| {
                let size = urls.len() as f64;
                Box::new(urls.into_iter().map(move |url| (url, rank / size)))
                    as Box<dyn Iterator<Item = _>>
            }));
        ranks = contribs
            .reduce_by_key(Fn!(|(x, y)| x + y), NUM_PARTS)
            .map_values(Fn!(|v| 0.15 + 0.85 * v));
    }

    let output = ranks
        .secure_reduce(Fn!(|x: (String, f64), y: (String, f64)| {
            if x.1 > y.1 {
                x
            } else {
                y
            }
        }))
        .unwrap()
        .get_pt();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!(
        "{:?} rank first with {:?}, total time = {:?}",
        output.0, output.1, dur
    );
    Ok(())
}

// unsecure mode
pub fn pagerank_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<String>>(&file).unwrap()
    }));

    let iters = 1; //7 causes core dump, why? some hints: converge when 6
    let dir = PathBuf::from("/opt/data/pt_pr_cit-Patents");
    let lines = sc.read_source(
        LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
        Some(deserializer),
        None,
    );
    let links = lines
        .flat_map(Fn!(|lines: Vec<String>| {
            Box::new(lines.into_iter().map(|line| {
                let parts = line.split(" ").collect::<Vec<_>>();
                (parts[0].to_string(), parts[1].to_string())
            })) as Box<dyn Iterator<Item = _>>
        }))
        .distinct_with_num_partitions(NUM_PARTS)
        .group_by_key(NUM_PARTS);
    links.cache();
    let mut ranks = links.map_values(Fn!(|_| 1.0));

    for _ in 0..iters {
        let contribs = links
            .join(ranks, NUM_PARTS)
            .values()
            .flat_map(Fn!(|(urls, rank): (Vec<String>, f64)| {
                let size = urls.len() as f64;
                Box::new(urls.into_iter().map(move |url| (url, rank / size)))
                    as Box<dyn Iterator<Item = _>>
            }));
        ranks = contribs
            .reduce_by_key(Fn!(|(x, y)| x + y), NUM_PARTS)
            .map_values(Fn!(|v| 0.15 + 0.85 * v));
    }

    let output = ranks
        .reduce(Fn!(|x: (String, f64), y: (String, f64)| {
            if x.1 > y.1 {
                x
            } else {
                y
            }
        }))
        .unwrap();
    let output = output.unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!(
        "{:?} rank first with {:?}, total time = {:?}",
        output.0, output.1, dur
    );

    Ok(())
}
