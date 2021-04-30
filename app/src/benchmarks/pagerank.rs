use std::collections::{HashMap, BTreeMap};
use std::path::PathBuf;
use std::time::Instant;
use vega::*;

// secure mode
pub fn pagerank_sec_0() -> Result<()> {
    let sc = Context::new()?;
    let now = Instant::now();
    let fe = Fn!(|vp: Vec<Vec<u8>>|{
        ser_encrypt::<>(vp)    
    });

    let fd = Fn!(|ve: Vec<u8>|{  //ItemE = Vec<u8>
        let data: Vec<Vec<u8>> = ser_decrypt::<>(ve);
        data
    });

    let fe_mp = Fn!(|vp: Vec<Vec<String>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_mp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_fmp = Fn!(|vp: Vec<(String, String)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_fmp = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_gbk = Fn!(|vp: Vec<(String, Vec<String>)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_gbk = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<String>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mv = Fn!(|vp: Vec<(String, f64)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_mv = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn = Fn!(|vp: Vec<(String, (Vec<String>, f64))>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        let mut buf2 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1.0);
            buf2.push(i.1.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        let buf2 = ser_encrypt::<>(buf2);
        (buf0, (buf1, buf2))
    });

    let fd_jn = Fn!(|ve: (Vec<u8>, (Vec<u8>, Vec<u8>))| {
        let (buf0, (buf1, buf2)) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<String>> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<f64> = ser_decrypt::<>(buf2);
        let len = pt0.len() | pt1.len() | pt2.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1
                .into_iter()
                .zip(pt2.into_iter())
            )
            .collect::<Vec<_>>() 
    });

    let fe_v = Fn!(|vp: Vec<(Vec<String>, f64)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_v = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>  
    }));

    let iters = 1;
    let dir = PathBuf::from("/opt/data/ct_pr_4");
    let lines = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), None, Some(deserializer), fe, fd)
        .map(Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
        }), fe_mp, fd_mp);
    let links = lines.flat_map(Fn!(|lines: Vec<String>| {
            Box::new(lines.into_iter().map(|line| {
                let parts = line.split(" ")
                    .collect::<Vec<_>>();
                (parts[0].to_string(), parts[1].to_string())
            })) as Box<dyn Iterator<Item = _>>
        }), fe_fmp, fd_fmp).distinct_with_num_partitions(1)
        .group_by_key(fe_gbk, fd_gbk, 1);
    links.cache();
    let mut ranks = links.map_values(Fn!(|_| 1.0), fe_mv.clone(), fd_mv.clone());

    for _ in 0..iters {
        let contribs = links.join(ranks, fe_jn, fd_jn, 1)
            .values(fe_v, fd_v)
            .flat_map(Fn!(|(urls, rank): (Vec<String>, f64)| {
                let size = urls.len() as f64;
                Box::new(urls.into_iter().map(move |url| (url, rank / size))) 
                    as Box<dyn Iterator<Item = _>>
                }), fe_mv.clone(), fd_mv.clone(),
            );
        ranks = contribs.reduce_by_key(Fn!(|(x, y)| x + y), 1, fe_mv.clone(), fd_mv.clone())
            .map_values(Fn!(|v| 0.15 + 0.85 * v), fe_mv.clone(), fd_mv.clone());
    }

    let output = ranks.secure_collect().unwrap();
    /*
    for (url, rank) in output.to_plain() {
        println!("url {:?} has rank {:?}", url, rank);
    }
    */
    let output = output.to_plain().into_iter().map(|(k, v)| (ordered_float::OrderedFloat(v), k)).collect::<BTreeMap<_, _>>();
    let (v, k) = output.last_key_value().unwrap();
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("{:?} rank first with {:?}, total time = {:?}", k, v, dur);
    Ok(())
}
    
// unsecure mode
pub fn pagerank_unsec_0() -> Result<()> {
    let sc = Context::new()?;
    let fe = Fn!(|vp: Vec<Vec<String>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_fmp = Fn!(|vp: Vec<(String, String)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_fmp = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_gbk = Fn!(|vp: Vec<(String, Vec<String>)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_gbk = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<String>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mv = Fn!(|vp: Vec<(String, f64)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_mv = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_jn = Fn!(|vp: Vec<(String, (Vec<String>, f64))>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        let mut buf2 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1.0);
            buf2.push(i.1.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        let buf2 = ser_encrypt::<>(buf2);
        (buf0, (buf1, buf2))
    });

    let fd_jn = Fn!(|ve: (Vec<u8>, (Vec<u8>, Vec<u8>))| {
        let (buf0, (buf1, buf2)) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<String>> = ser_decrypt::<>(buf1);
        let mut pt2: Vec<f64> = ser_decrypt::<>(buf2);
        let len = pt0.len() | pt1.len() | pt2.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt2.resize_with(len, Default::default);
        pt0.into_iter()
            .zip(pt1
                .into_iter()
                .zip(pt2.into_iter())
            )
            .collect::<Vec<_>>() 
    });

    let fe_v = Fn!(|vp: Vec<(Vec<String>, f64)>| {
        let len = vp.len();
        let mut buf0 = Vec::with_capacity(len);
        let mut buf1 = Vec::with_capacity(len);
        for i in vp {
            buf0.push(i.0);
            buf1.push(i.1);
        }
        let buf0 = ser_encrypt::<>(buf0);
        let buf1 = ser_encrypt::<>(buf1);
        (buf0, buf1)
    });

    let fd_v = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<f64> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    }));

    let iters = 1; //7 causes core dump, why? some hints: converge when 6
    let dir = PathBuf::from("/opt/data/pt_pr_4");
    let lines = sc.read_source(LocalFsReaderConfig::new(dir).num_partitions_per_executor(1), Some(deserializer), None, fe, fd);
    let links = lines.flat_map(Fn!(|lines: Vec<String>| {
            Box::new(lines.into_iter().map(|line| {
                let parts = line.split(" ")
                    .collect::<Vec<_>>();
                (parts[0].to_string(), parts[1].to_string())
            })) as Box<dyn Iterator<Item = _>>
        }), fe_fmp, fd_fmp).distinct()
        .group_by_key(fe_gbk, fd_gbk, 1);
    links.cache();
    let mut ranks = links.map_values(Fn!(|_| 1.0), fe_mv.clone(), fd_mv.clone());

    for _ in 0..iters {
        let contribs = links.join(ranks, fe_jn, fd_jn, 1)
            .values(fe_v, fd_v)
            .flat_map(Fn!(|(urls, rank): (Vec<String>, f64)| {
                let size = urls.len() as f64;
                Box::new(urls.into_iter().map(move |url| (url, rank / size))) 
                    as Box<dyn Iterator<Item = _>>
                }), fe_mv.clone(), fd_mv.clone(),
            );
        ranks = contribs.reduce_by_key(Fn!(|(x, y)| x + y), 1, fe_mv.clone(), fd_mv.clone())
            .map_values(Fn!(|v| 0.15 + 0.85 * v), fe_mv.clone(), fd_mv.clone());
    }

    let output = ranks.collect().unwrap().into_iter().map(|(k, v)| (ordered_float::OrderedFloat(v), k)).collect::<BTreeMap<_, _>>();
    let (v, k) = output.last_key_value().unwrap();
    println!("{:?} rank first with {:?}", k, v);

    Ok(())
}

