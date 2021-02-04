use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::io::prelude::*;
use vega::io::*;
use vega::*;

#[allow(unused_must_use)]
fn set_up(file_name: &str, secure: bool) {
    let mut dir = PathBuf::from("/tmp");
    let mut fixture =
        b"This is some textual test data.\nCan be converted to strings and there are two lines.".to_vec();
    if secure {
        dir = dir.join("ct_dir");
        fixture = ser_encrypt(fixture);
    } else {
        dir = dir.join("pt_dir");
    }
    println!("Creating tests in dir: {}", (&dir).to_str().unwrap());
    create_dir_all(&dir);

    if !std::path::Path::new(file_name).exists() {
        let mut f = File::create(dir.join(file_name)).unwrap();
        f.write_all(&fixture).unwrap();
    }
}

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

    // Multiple files test
    let dir = PathBuf::from("/tmp").join("ct_dir");
    (0..10).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        set_up(path.as_path().to_str().unwrap(), true);
    });

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
    println!("result: {:?}", uniline.batch_decrypt(res));
    Ok(())
}

pub fn file_read_unsec_0() -> Result<()> {
    let context = Context::new()?;

    // Multiple files test
    let dir = PathBuf::from("/tmp").join("pt_dir");
    (0..10).for_each(|idx| {
        let f_name = format!("test_file_{}", idx);
        let path = dir.join(f_name.as_str());
        set_up(path.as_path().to_str().unwrap(), false);
    });

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