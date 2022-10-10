use std::path::PathBuf;

use crate::*;


// secure mode
pub fn topk_sec_0() -> Result<()> {
    let sc = Context::new()?;


    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));

    let dir = PathBuf::from("/opt/data/ct_topk");
    let rdd0 = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer));
    let rdd1 = rdd0.map(Fn!(|i: String| {
        let s = i.split("::").collect::<Vec<_>>();
        (s[0].to_string(), s[1].to_string(), s[2].to_string())
    }));
    
    let _count = rdd1.count().unwrap();
    
    let rdd2 = rdd1
        .map(Fn!(|i: (String, String, String)| (
            i.1,
            (i.2.parse::<i64>().unwrap(), 1)
        )))
        .reduce_by_key(
            Fn!(|(v1, v2): ((i64, i64), (i64, i64))| (v1.0 + v2.0, v1.1 + v2.1)),
            NUM_PARTS,
        )
        .map(Fn!(|x: (String, (i64, i64))| (
            ((x.1 .0 as f64) / (x.1 .1 as f64)) as i64,
            x.0
        )));
    let _res = rdd2
        .aggregate(
            vec![(0, String::new()); 10],
            Fn!(|mut v1: Vec<(i64, String)>, i: (i64, String)| {
                for x in v1.iter_mut() {
                    if x.0 < i.0 {
                        *x = i;
                        break;
                    }
                }
                v1
            }),
            Fn!(|mut v1: Vec<(i64, String)>, mut v2: Vec<(i64, String)>| {
                v1.append(&mut v2);
                v1.sort_unstable();
                v1.split_off(v1.len() - 10)
            }),
        )
        .unwrap()
        .get_pt();
    
    
    Ok(())
}