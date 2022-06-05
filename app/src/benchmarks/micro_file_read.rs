use std::path::PathBuf;
use vega::io::*;
use vega::*;

pub fn file_read_sec_0() -> Result<()> {
    let context = Context::new()?;

    let dir = PathBuf::from("/opt/data/ct_lf");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    let lines = context
        .read_source(LocalFsReaderConfig::new(dir), None, Some(deserializer))
        .map(Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        }));
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().flat_map(|line| {
            line.split(' ')
                .map(|word| word.to_string())
                .collect::<Vec<String>>()
                .into_iter()
        })) as Box<dyn Iterator<Item = _>>
    }));
    let uniline = line.distinct();
    let res = uniline.secure_collect().unwrap();
    println!("result: {:?}", res.get_pt());
    Ok(())
}

pub fn file_read_unsec_0() -> Result<()> {
    let context = Context::new()?;

    let dir = PathBuf::from("/opt/data/pt_lf");
    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    }));

    let lines = context.read_source(LocalFsReaderConfig::new(dir), Some(deserializer), None);
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().flat_map(|line| {
            line.split(' ')
                .map(|word| word.to_string())
                .collect::<Vec<String>>()
                .into_iter()
        })) as Box<dyn Iterator<Item = _>>
    }));
    let uniline = line.distinct();
    let res = uniline.collect().unwrap();
    println!("result: {:?}", res);
    Ok(())
}
