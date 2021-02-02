
use crate::*;


pub fn test0_sec_0() -> Result<()> {
    let sc = Context::new()?;

    let fe = Fn!(|vp: Vec<(i32, i32)>| {
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

    let fd = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<i32> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });

    let fe_gb = Fn!(|vp: Vec<(i32, Vec<i32>)>| {
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

    let fd_gb = Fn!(|ve: (Vec<u8>, Vec<u8>)| {
        let (buf0, buf1) = ve;
        let mut pt0: Vec<i32> = ser_decrypt::<>(buf0); 
        let mut pt1: Vec<Vec<i32>> = ser_decrypt::<>(buf1);
        let len = pt0.len() | pt1.len();
        pt0.resize_with(len, Default::default);
        pt1.resize_with(len, Default::default);
        pt0.into_iter().zip(pt1.into_iter()).collect::<Vec<_>>() 
    });




















    let mut rdd0 = sc.make_op(fe.clone(), fd.clone(), 1);

    let rdd1 = rdd0.group_by_key(fe_gb, fd_gb, 1);
    rdd0 = rdd1.flat_map(Fn!(|(k, vv): (i32, Vec<i32>)| {
            let mut res = Vec::with_capacity(vv.len());
            for item in vv {
                let res_k = k * item / 4399 + 165;
                res.push((item, res_k));
            }
            Box::new(res.into_iter()) as Box<dyn Iterator<Item = (i32, i32)>>
        }),
        fe.clone(),
        fd.clone()
    );

    let _count = rdd0.count().unwrap();




    Ok(())
}