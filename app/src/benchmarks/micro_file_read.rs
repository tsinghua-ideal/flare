use std::path::PathBuf;
use vega::io::*;
use vega::*;


pub fn file_read_sec_0() -> Result<()> {
    let context = Context::new()?;

    let fe = Fn!(|vp: Vec<Vec<String>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_fmp = Fn!(|vp: Vec<String>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_fmp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<String> = ser_decrypt::<>(buf0); 
        pt0
    });

    let dir = PathBuf::from("/tmp/ct_lf");
    let deserializer = Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    });

    let lines = context.read_source(LocalFsReaderConfig::new(dir, true), deserializer, fe, fd);
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().flat_map(|line| {
            line.split(' ')
                .map(|word| word.to_string())
                .collect::<Vec<String>>()
                .into_iter()
        })) as Box<dyn Iterator<Item = _>>
    }), fe_fmp, fd_fmp);
    let uniline = line.distinct();
    let res = uniline.secure_collect().unwrap();
    println!("result: {:?}", res.to_plain());
    Ok(())
}

pub fn file_read_unsec_0() -> Result<()> {
    let context = Context::new()?;

    let dir = PathBuf::from("/tmp/pt_lf");
    let deserializer = Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    });

    let fe = Fn!(|vp: Vec<Vec<String>>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<Vec<String>> = ser_decrypt::<>(buf0); 
        pt0
    });

    let fe_fmp = Fn!(|vp: Vec<String>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_fmp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<String> = ser_decrypt::<>(buf0); 
        pt0
    });


    let lines = context.read_source(LocalFsReaderConfig::new(dir, false), deserializer, fe, fd);
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().flat_map(|line| {
            line.split(' ')
                .map(|word| word.to_string())
                .collect::<Vec<String>>()
                .into_iter()
        })) as Box<dyn Iterator<Item = _>>
    }), fe_fmp, fd_fmp);
    let uniline = line.distinct();
    let res = uniline.collect().unwrap();
    println!("result: {:?}", res);
    Ok(())
}