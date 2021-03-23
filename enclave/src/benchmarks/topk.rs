use std::path::PathBuf;

use crate::*;


// secure mode
pub fn topk_sec_0() -> Result<()> {
    let sc = Context::new()?;
    
    let lfe = Fn!(|vp: Vec<String>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let lfd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<String> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe = Fn!(|vp: Vec<(String, String, String)>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<(String, String, String)> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_mp = Fn!(|vp: Vec<(String, (i64, i64))>| {
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

    let fd_mp = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<String> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<(i64, i64)> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_mp1 = Fn!(|vp: Vec<(i64, String)>| {
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

    let fd_mp1 = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<i64> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<String> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });


    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>
    }));

    let dir = PathBuf::from("/opt/data/ct_topk");
    let rdd0 = sc.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), lfe, lfd);
    let rdd1 = rdd0.map(Fn!(|i: String| {
        let s = i.split("::").collect::<Vec<_>>();
        (s[0].to_string(), s[1].to_string(), s[2].to_string())
    }), fe.clone(), fd.clone());
    
    let _count = rdd1.count().unwrap();
    
    let rdd2 = rdd1.map(Fn!(|i: (String, String, String)|  (i.1, (i.2.parse::<i64>().unwrap(), 1))), fe_mp.clone(), fd_mp.clone())
        .reduce_by_key(Fn!(|(v1, v2): ((i64, i64), (i64, i64))| (v1.0 + v2.0, v1.1 + v2.1)), 1, fe_mp, fd_mp)
        .map(Fn!(|x: (String, (i64, i64))| (((x.1.0 as f64)/(x.1.1 as f64)) as i64, x.0)), fe_mp1, fd_mp1);
    let _res = rdd2.aggregate(vec![(0, String::new()); 10], 
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
        }), Fn!(|x| x), Fn!(|x| x)
    ).unwrap();
    
    
    Ok(())
}