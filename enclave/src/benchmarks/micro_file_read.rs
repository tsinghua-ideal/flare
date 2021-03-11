use std::path::PathBuf;

use crate::*;


pub fn file_read_sec_0() -> Result<()> {
    let context = Context::new()?;

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

    let fe_fmp = Fn!(|vp: Vec<String>| {
        let buf0 = ser_encrypt::<>(vp);
        buf0
    });

    let fd_fmp = Fn!(|ve: Vec<u8>| {
        let buf0 = ve;
        let pt0: Vec<String> = ser_decrypt::<>(buf0); 
        pt0
    });

    let dir = PathBuf::from("/opt/data/ct_lf");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap()  //ItemE = Vec<u8>  
    }));

    let lines = context.read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer), fe, fd)
        .map(Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
        }), fe_mp, fd_mp);
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

    Ok(())
}