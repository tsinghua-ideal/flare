use crate::*;
use std::collections::HashMap;
use std::path::PathBuf;


type TPart = (
    i32,
    String,
    String,
    String,
    String,
    i32,
    String,
    f32,
    String,
);
type TSupplier = (i32, String, String, i32, String, f32, String);
//Default and Debug are implemented for tuples up to twelve items long
type TLineItem = (
    (i32, i32, i32, i32, i32, f32, f32, f32),
    String,
    String,
    Date,
    Date,
    Date,
    String,
    String,
    String,
);
type TPartSupp = (i32, i32, i32, f32, String);
type TOrders = (i32, i32, String, f32, Date, String, String, i32, String);
type TNation = (i32, String, i32, String);
type TRegion = (i32, String, String);
type TCustomer = (i32, String, String, i32, String, f32, String, String);

pub fn te1_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));


    let dir0 = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir1 = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let table0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir0).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &TSupplier| x.3))
        .map(Fn!(|x: (i32, TSupplier)| (x.0, x.1 .0)));
    let table1 = sc
        .read_source(
            LocalFsReaderConfig::new(dir1).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &TCustomer| x.3))
        .map(Fn!(|x: (i32, TCustomer)| (x.0, x.1 .0)));

    let joined = table0.join(table1, NUM_PARTS);
    let _res = joined.count().unwrap();




    Ok(())
}

pub fn te2_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));


    let dir = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .key_by(Fn!(|x: &TSupplier| x.3))
        .map(Fn!(|x: (i32, TSupplier)| (x.0, x.1 .0)));

    let joined = table.join(table.clone(), NUM_PARTS);
    let _res = joined.count().unwrap();




    Ok(())
}

pub fn te3_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));


    let dir = PathBuf::from("/opt/data/ct_tpch_200m_customer");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .key_by(Fn!(|x: &TCustomer| x.3))
        .map(Fn!(|x: (i32, TCustomer)| (x.0, x.1 .0)));

    let joined = table.join(table.clone(), NUM_PARTS);
    let _res = joined.count().unwrap();




    Ok(())
}

pub fn q1_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .filter(Fn!(|x: &TLineItem| {
            let mut date = Date::new(1998, 12, 1).unwrap();
            date.subtract_days(90);
            x.3 <= date
        }))
        .map(Fn!(|x: TLineItem| (
            (x.1, x.2),
            (x.0 .4, x.0 .5, x.0 .6, x.0 .7, 1f32, 0f32)
        )))
        .reduce_by_key(
            Fn!(|(x, y): (
                (i32, f32, f32, f32, f32, f32),
                (i32, f32, f32, f32, f32, f32)
            )| {
                (
                    x.0 + y.0,
                    x.1 + y.1,
                    x.1 * (1.0 - x.2) + y.1 * (1.0 - y.2),
                    x.1 * (1.0 - x.2) * (1.0 + x.3) + y.1 * (1.0 - y.2) * (1.0 + y.3),
                    x.3 + y.3,
                    (x.4 + y.4),
                )
            }),
            NUM_PARTS,
        )
        .map(Fn!(|(k, v): (
            (String, String),
            (i32, f32, f32, f32, f32, f32)
        )| {
            (
                k,
                (
                    v.0,
                    v.1,
                    v.2,
                    v.3,
                    (v.0 as f32) / v.5,
                    v.1 / v.5,
                    v.4 / v.5,
                    v.5 as i32,
                ),
            )
        }))
        .sort_by_key(true, NUM_PARTS);

    let _res = table.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q2_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_partsupp = PathBuf::from("/opt/data/ct_tpch_1g_partsupp");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");
    let dir_region = PathBuf::from("/opt/data/ct_tpch_1g_region");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| x.4.ends_with("BRASS") && x.5 == 15));
    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| x));
    let table_partsupp = sc
        .read_source(
            LocalFsReaderConfig::new(dir_partsupp).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TPartSupp| x));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TNation| x));
    let table_region = sc
        .read_source(
            LocalFsReaderConfig::new(dir_region).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .map(Fn!(|x: TRegion| x));

    let proj_region = table_region
        .filter(Fn!(|x: &TRegion| x.1.as_str() == "EUROPE"))
        .map(Fn!(|x: TRegion| (x.0, ())));
    let nkey_supplier = table_supplier.key_by(Fn!(|x: &TSupplier| x.3));
    let skey_partsupp = table_partsupp.key_by(Fn!(|x: &TPartSupp| x.1));

    let join_n_r = table_nation
        .key_by(Fn!(|x: &TNation| x.2))
        .join(proj_region, NUM_PARTS);
    let main_query = join_n_r
        .map(Fn!(|(_, (x, _)): (i32, (TNation, ()))| (x.0, x.1)))
        .join(nkey_supplier.clone(), NUM_PARTS)
        .map(Fn!(|(_, (x, y)): (i32, (String, TSupplier))| (
            y.0,
            (y.1, y.2, x, y.4, y.5, y.6)
        )))
        .join(skey_partsupp.clone(), NUM_PARTS)
        .map(Fn!(|(_, (x, y)): (
            i32,
            ((String, String, String, String, f32, String), TPartSupp)
        )| {
            let (s_name, s_address, n_name, s_phone, s_acctbal, s_comment) = x;
            let (ps_partkey, _, _, ps_supplycost, _) = y;
            (
                ps_partkey,
                (
                    s_name,
                    s_address,
                    n_name,
                    s_phone,
                    s_acctbal,
                    s_comment,
                    ps_supplycost,
                ),
            )
        }))
        .join(
            table_part.map(Fn!(|x: TPart| {
                let (p_partkey, _, p_mfgr, _, _, _, _, _, _) = x;
                (p_partkey, p_mfgr)
            })),
            NUM_PARTS,
        )
        .map(Fn!(|(p_partkey, (x, p_mfgr)): (
            i32,
            ((String, String, String, String, f32, String, f32), String)
        )| {
            let (s_name, s_address, n_name, s_phone, s_acctbal, s_comment, ps_supplycost) = x;
            (
                (p_partkey, OrderedFloat(ps_supplycost)),
                (
                    s_name, s_address, n_name, s_phone, s_acctbal, s_comment, p_mfgr,
                ),
            )
        }));

    let sub_query = join_n_r
        .map(Fn!(|(_, (x, _)): (i32, (TNation, ()))| (x.0, ())))
        .join(nkey_supplier, NUM_PARTS)
        .map(Fn!(|(_, (_, y)): (i32, ((), TSupplier))| (y.0, ())))
        .join(skey_partsupp, NUM_PARTS)
        .map(Fn!(|(_, (_, y)): (i32, ((), TPartSupp))| {
            let (ps_partkey, _, _, ps_supplycost, _) = y;
            (ps_partkey, ps_supplycost)
        }))
        .join(
            table_part.map(Fn!(|x: TPart| {
                let (p_partkey, _, _, _, _, _, _, _, _) = x;
                (p_partkey, ())
            })),
            NUM_PARTS,
        )
        .map(Fn!(|(ps_partkey, (ps_supplycost, _)): (i32, (f32, ()))| (
            ps_partkey,
            OrderedFloat(ps_supplycost)
        )))
        .reduce_by_key(
            Fn!(|(x, y): (OrderedFloat<f32>, OrderedFloat<f32>)| x.min(y)),
            NUM_PARTS,
        )
        .map(Fn!(|t| (t, ())));
    let table_final = main_query
        .join(sub_query, NUM_PARTS)
        .map(Fn!(|((p_partkey, _), (x, _)): (
            (i32, OrderedFloat<f32>),
            ((String, String, String, String, f32, String, String), ())
        )| {
            let (s_name, s_address, n_name, s_phone, s_acctbal, s_comment, p_mfgr) = x;
            (
                (OrderedFloat(s_acctbal), n_name, s_name, p_partkey),
                (s_address, s_phone, s_comment, p_mfgr),
            )
        }))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.take(100).unwrap();
    
    
    

    Ok(())
}

pub fn q3_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TCustomer| x.6.as_str() == "BUILDING"))
        .map(Fn!(|x: TCustomer| (x.0, ())));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| x.4 < Date::new(1995, 3, 15).unwrap()))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, _, o_orderdate, _, _, o_shippriority, _) = x;
            (o_custkey, (o_orderkey, o_orderdate, o_shippriority))
        }));
    let join_c_o = table_customer.join(table_orders, NUM_PARTS).map(Fn!(|(
        _,
        (_, (o_orderkey, o_orderdate, o_shippriority)),
    ): (
        i32,
        ((), (i32, Date, i32))
    )| (
        o_orderkey,
        (o_orderdate, o_shippriority)
    )));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| x.3 > Date::new(1995, 3, 15).unwrap()))
        .map(Fn!(|x: TLineItem| {
            let l_orderkey = x.0 .0;
            let l_extendedprice = x.0 .5;
            let l_discount = x.0 .6;
            (l_orderkey, (l_extendedprice, l_discount))
        }));
    let table_final = join_c_o
        .join(table_lineitem, NUM_PARTS)
        .map(Fn!(|(
            l_orderkey,
            ((o_orderdate, o_shippriority), (l_extendedprice, l_discount)),
        ): (i32, ((Date, i32), (f32, f32)))| {
            (
                (l_orderkey, o_orderdate, o_shippriority),
                l_extendedprice * (1.0 - l_discount),
            )
        }))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS)
        .map(Fn!(|(
            (l_orderkey, o_orderdate, o_shippriority),
            revenue,
        ): ((i32, Date, i32), f32)| (
            (OrderedFloat(revenue), o_orderdate),
            (l_orderkey, o_shippriority)
        )))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.take(10).unwrap();
    
    
    

    Ok(())
}

pub fn q4_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| {
            let mut upper_date = Date::new(1993, 7, 1).unwrap();
            upper_date.add_months(3);
            x.4 >= Date::new(1993, 7, 1).unwrap() && x.4 < upper_date
        }))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, _, _, _, _, o_orderpriority, _, _, _) = x;
            (o_orderkey, o_orderpriority)
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| x.4 < x.5))
        .map(Fn!(|x: TLineItem| {
            let l_orderkey = x.0 .0;
            (l_orderkey, ())
        }))
        .reduce_by_key(Fn!(|(x, y): ((), ())| ()), NUM_PARTS);
    let table_final = table_orders
        .join(table_lineitem, NUM_PARTS)
        .map(Fn!(|(_, (v, _)): (i32, (String, ()))| (v, 1i32)))
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q5_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");
    let dir_region = PathBuf::from("/opt/data/ct_tpch_1g_region");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TCustomer| x.6.as_str() == "BUILDING"))
        .map(Fn!(|x: TCustomer| ((x.0, x.3), ())));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| {
            let mut upper_date = Date::new(1994, 1, 1).unwrap();
            upper_date.add_years(1);
            x.4 >= Date::new(1994, 1, 1).unwrap() && x.4 < upper_date
        }))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, _, _, _, _, _, _) = x;
            (o_orderkey, o_custkey)
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &TLineItem| x.0 .2));
    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &TSupplier| x.3));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(
            |(n_nationkey, n_name, n_regionkey, n_comment): TNation| (
                n_regionkey,
                (n_nationkey, n_name, n_comment)
            )
        ));
    let table_region = sc
        .read_source(
            LocalFsReaderConfig::new(dir_region).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .filter(Fn!(|x: &TRegion| x.1.as_str() == "ASIA"))
        .map(Fn!(|x: TRegion| (x.0, ())));

    let table_final = table_region
        .join(table_nation, NUM_PARTS)
        .map(Fn!(|(_, (_, (n_nationkey, n_name, _))): (
            i32,
            ((), (i32, String, String))
        )| (n_nationkey, n_name)))
        .join(table_supplier, NUM_PARTS)
        .map(Fn!(|(s_nationkey, (n_name, x)): (
            i32,
            (String, TSupplier)
        )| {
            let s_suppkey = x.0;
            (s_suppkey, (s_nationkey, n_name))
        }))
        .join(table_lineitem, NUM_PARTS)
        .map(Fn!(|(_, ((s_nationkey, n_name), l)): (
            i32,
            ((i32, String), TLineItem)
        )| {
            let ((l_orderkey, _, _, _, _, l_extendedprice, l_discount, _), _, _, _, _, _, _, _, _) =
                l;
            (
                l_orderkey,
                (s_nationkey, n_name, l_extendedprice, l_discount),
            )
        }))
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(
            _,
            ((s_nationkey, n_name, l_extendedprice, l_discount), o_custkey),
        ): (i32, ((i32, String, f32, f32), i32))| (
            (o_custkey, s_nationkey),
            (n_name, l_extendedprice, l_discount)
        )))
        .join(table_customer, NUM_PARTS)
        .map(Fn!(|(_, ((n_name, l_extendedprice, l_discount), _)): (
            (i32, i32),
            ((String, f32, f32), ())
        )| {
            (n_name, l_extendedprice * (1.0 - l_discount))
        }))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS)
        .map(Fn!(|(n_name, revenue): (String, f32)| (
            OrderedFloat(revenue),
            n_name
        )))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.take(10).unwrap();
    
    
    

    Ok(())
}

pub fn q6_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let table = sc
        .read_source(
            LocalFsReaderConfig::new(dir).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer),
        )
        .filter(Fn!(|x: &TLineItem| {
            let mut upper_date = Date::new(1994, 1, 1).unwrap();
            upper_date.add_years(1);
            x.3 <= upper_date
                && x.3 > Date::new(1994, 1, 1).unwrap()
                && x.0 .6 >= 0.05
                && x.0 .6 <= 0.07
                && x.0 .4 < 24
        }))
        .map(Fn!(|x: TLineItem| x.0 .5 * x.0 .6));

    let _res = table.reduce(Fn!(|x: f32, y: f32| x + y)).unwrap();
    
    


    Ok(())
}

pub fn q7_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TCustomer| (x.3, x.0)));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, _, _, _, _, _, _) = x;
            (o_custkey, o_orderkey)
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| {
            x.3 >= Date::new(1995, 1, 1).unwrap() && x.3 <= Date::new(1996, 12, 31).unwrap()
        }))
        .map(Fn!(|x: TLineItem| {
            let (
                (l_orderkey, _, l_suppkey, _, _, l_extendedprice, l_discount, _),
                _,
                _,
                l_shipdate,
                _,
                _,
                _,
                _,
                _,
            ) = x;
            (
                l_suppkey,
                (l_orderkey, l_extendedprice, l_discount, l_shipdate),
            )
        }));

    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.3, x.0)));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(
            |x: &TNation| x.1.as_str() == "FRANCE" || x.1.as_str() == "GERMARY"
        ))
        .map(Fn!(|(n_nationkey, n_name, _, _): TNation| (
            n_nationkey,
            n_name
        )));
    let table_final = table_nation
        .join(table_customer, NUM_PARTS)
        .map(Fn!(|(_, (n_name, c_custkey)): (i32, (String, i32))| (
            c_custkey, n_name
        )))
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(_, (cust_nation, o_orderkey)): (
            i32,
            (String, i32)
        )| (o_orderkey, cust_nation)))
        .join(
            table_nation
                .join(table_supplier, NUM_PARTS)
                .map(Fn!(|(_, (n_name, s_suppkey)): (i32, (String, i32))| (
                    s_suppkey, n_name
                )))
                .join(table_lineitem, NUM_PARTS)
                .map(Fn!(|(
                    _,
                    (supp_nation, (l_orderkey, l_extendedprice, l_discount, l_shipdate)),
                ): (
                    i32,
                    (String, (i32, f32, f32, Date))
                )| (
                    l_orderkey,
                    (supp_nation, l_extendedprice, l_discount, l_shipdate)
                ))),
            NUM_PARTS,
        )
        .filter(Fn!(|x: &(i32, (String, (String, f32, f32, Date)))| x
            .1
             .0
            .as_str()
            == "FRANCE"
            && x.1 .1 .0.as_str() == "GERMANY"
            || x.1 .0.as_str() == "GERMANY"
                && x.1 .1 .0.as_str() == "FRANCE"))
        .map(Fn!(|(
            _,
            (cust_nation, (supp_nation, l_extendedprice, l_discount, l_shipdate)),
        ): (
            i32,
            (String, (String, f32, f32, Date))
        )| (
            (supp_nation, cust_nation, l_shipdate.get_year()),
            l_extendedprice * (1.0 - l_discount)
        )))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS)
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q8_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");
    let dir_region = PathBuf::from("/opt/data/ct_tpch_1g_region");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| x.4.as_str() == "ECONOMY ANODIZED STEEL"))
        .map(Fn!(|x: TPart| (x.0, ())));
    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TCustomer| (x.3, x.0)));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| x.4 >= Date::new(1995, 1, 1).unwrap()
            && x.4 <= Date::new(1996, 12, 31).unwrap()))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, _, o_orderdate, _, _, _, _) = x;
            (o_custkey, (o_orderkey, o_orderdate))
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TLineItem| {
            let (l_orderkey, l_partkey, l_suppkey, _, _, l_extendedprice, l_discount, _) = x.0;
            (
                l_partkey,
                (l_suppkey, l_orderkey, l_extendedprice + (1.0 - l_discount)),
            )
        }));
    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.3, x.0)));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TNation| x));
    let table_region = sc
        .read_source(
            LocalFsReaderConfig::new(dir_region).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TRegion| x.1.as_str() == "AMERICA"))
        .map(Fn!(|(r_regionkey, _, _): TRegion| (r_regionkey, ())));

    let table_left = table_nation
        .map(Fn!(|(n_nationkey, _, n_regionkey, _): TNation| (
            n_regionkey,
            n_nationkey
        )))
        .join(table_region, NUM_PARTS)
        .map(Fn!(|(_, x): (i32, (i32, ()))| x))
        .join(table_customer, NUM_PARTS)
        .map(Fn!(|(_, (_, c_custkey)): (i32, ((), i32))| (c_custkey, ())))
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(_, (_, (o_orderkey, o_orderdate))): (
            i32,
            ((), (i32, Date))
        )| (o_orderkey, o_orderdate)));
    let table_right = table_lineitem
        .join(table_part, NUM_PARTS)
        .map(Fn!(|(_, (x, _)): (i32, ((i32, i32, f32), ()))| {
            let (l_suppkey, l_orderkey, volume) = x;
            (l_suppkey, (l_orderkey, volume))
        }))
        .join(
            table_nation
                .map(Fn!(|(n_nationkey, n_name, _, _): TNation| (
                    n_nationkey,
                    n_name,
                )))
                .join(table_supplier, NUM_PARTS)
                .map(Fn!(|(_, (n_name, s_suppkey)): (i32, (String, i32))| (
                    s_suppkey, n_name
                ))),
            NUM_PARTS,
        )
        .map(Fn!(|(_, ((l_orderkey, volume), n_name)): (
            i32,
            ((i32, f32), String)
        )| (l_orderkey, (volume, n_name))));
    let table_final = table_left
        .join(table_right, NUM_PARTS)
        .map(Fn!(|(_, x): (i32, (Date, (f32, String)))| {
            let (o_orderdate, (volume, n_name)) = x;
            let o_year = o_orderdate.get_year();
            let case_volume = if n_name.as_str() == "BRAZIL" {
                volume
            } else {
                0.0
            };
            (o_year, (volume, case_volume))
        }))
        .reduce_by_key(
            Fn!(|(x, y): ((f32, f32), (f32, f32))| (x.0 + y.0, x.1 + y.1)),
            NUM_PARTS,
        )
        .map(Fn!(|(k, v): (u16, (f32, f32))| (k, v.0 / v.1)))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q9_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_partsupp = PathBuf::from("/opt/data/ct_tpch_1g_partsupp");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| x.1.contains("green")))
        .map(Fn!(|x: TPart| (x.0, ())));
    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.3, x.0)));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TLineItem| {
            let (l_orderkey, l_partkey, l_suppkey, _, l_quantity, l_extendedprice, l_discount, _) =
                x.0;
            (
                l_partkey,
                (
                    l_suppkey,
                    l_orderkey,
                    l_quantity,
                    l_extendedprice,
                    l_discount,
                ),
            )
        }));
    let table_partsupp = sc
        .read_source(
            LocalFsReaderConfig::new(dir_partsupp).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TPartSupp| ((x.1, x.0), x.3)));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, _, _, _, o_orderdate, _, _, _, _) = x;
            (o_orderkey, o_orderdate.get_year())
        }));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|(n_nationkey, n_name, _, _): TNation| (
            n_nationkey,
            n_name
        )));
    let table_final = table_part
        .join(table_lineitem, NUM_PARTS)
        .map(Fn!(|(l_partkey, (_, x)): (
            i32,
            ((), (i32, i32, i32, f32, f32))
        )| {
            let (l_suppkey, l_orderkey, l_quantity, l_extendedprice, l_discount) = x;
            (
                l_suppkey,
                (
                    l_partkey,
                    l_orderkey,
                    l_quantity,
                    l_extendedprice,
                    l_discount,
                ),
            )
        }))
        .join(
            table_nation
                .join(table_supplier, NUM_PARTS)
                .map(Fn!(|(_, (n_name, s_suppkey)): (i32, (String, i32))| (
                    s_suppkey, n_name
                ))),
            NUM_PARTS,
        )
        .map(Fn!(|(
            l_suppkey,
            ((l_partkey, l_orderkey, l_quantity, l_extendedprice, l_discount), n_name),
        ): (
            i32,
            ((i32, i32, i32, f32, f32), String)
        )| (
            (l_suppkey, l_partkey),
            (l_orderkey, l_quantity, l_extendedprice, l_discount, n_name)
        )))
        .join(table_partsupp, NUM_PARTS)
        .map(Fn!(|(_, (x, ps_supplycost)): (
            (i32, i32),
            ((i32, i32, f32, f32, String), f32)
        )| {
            let (l_orderkey, l_quantity, l_extendedprice, l_discount, n_name) = x;
            let amount = l_extendedprice * (1.0 - l_discount) - ps_supplycost * l_quantity as f32;
            (l_orderkey, (n_name, amount))
        }))
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(_, ((n_name, amount), o_year)): (
            i32,
            ((String, f32), u16)
        )| ((n_name, o_year), amount)))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q10_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &TCustomer| x.0));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| x.4 >= Date::new(1993, 10, 1).unwrap()
            && x.4 <= Date::new(1994, 1, 1).unwrap()))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, _, _, _, _, _, _) = x;
            (o_custkey, o_orderkey)
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| x.1.as_str() == "R"))
        .map(Fn!(|x: TLineItem| {
            let (l_orderkey, _, _, _, _, l_extendedprice, l_discount, _) = x.0;
            (l_orderkey, (l_extendedprice, l_discount))
        }));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|(n_nationkey, n_name, _, _): TNation| (
            n_nationkey,
            n_name
        )));

    let table_final = table_orders
        .join(table_customer, NUM_PARTS)
        .map(Fn!(|(_, (o_orderkey, c)): (i32, (i32, TCustomer))| {
            let (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, _, c_comment) = c;
            (
                c_nationkey,
                (
                    o_orderkey, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment,
                ),
            )
        }))
        .join(table_nation, NUM_PARTS)
        .map(Fn!(|(_, (x, n_name)): (
            i32,
            ((i32, i32, String, String, String, f32, String), String),
        )| {
            let (o_orderkey, c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment) = x;
            (
                o_orderkey,
                (
                    c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name,
                ),
            )
        }))
        .join(table_lineitem, NUM_PARTS)
        .map(Fn!(|(_, (x, (l_extendedprice, l_discount))): (
            i32,
            (
                (i32, String, String, String, f32, String, String),
                (f32, f32)
            )
        )| {
            let (c_custkey, c_name, c_address, c_phone, c_acctbal, c_comment, n_name) = x;
            (
                (
                    c_custkey,
                    c_name,
                    OrderedFloat(c_acctbal),
                    c_phone,
                    n_name,
                    c_address,
                    c_comment,
                ),
                l_extendedprice * (1.0 - l_discount),
            )
        }))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS)
        .map(Fn!(|(x, y): (
            (
                i32,
                String,
                OrderedFloat<f32>,
                String,
                String,
                String,
                String
            ),
            f32
        )| (OrderedFloat(y), x)))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q11_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_partsupp = PathBuf::from("/opt/data/ct_tpch_1g_partsupp");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");

    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.3, x.0)));
    let table_partsupp = sc
        .read_source(
            LocalFsReaderConfig::new(dir_partsupp).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .key_by(Fn!(|x: &TPartSupp| x.1));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TNation| x.1.as_str() == "GERMANY"))
        .map(Fn!(|(n_nationkey, _, _, _): TNation| (n_nationkey, ())));

    let join_three = table_nation
        .join(table_supplier, NUM_PARTS)
        .map(Fn!(|(_, (_, s_suppkey)): (i32, ((), i32))| (s_suppkey, ())))
        .join(table_partsupp, NUM_PARTS);

    let table_final = join_three
        .map(Fn!(|(_, (_, x)): (i32, ((), TPartSupp))| {
            let (ps_partkey, _, ps_availqty, ps_supplycost, _) = x;
            (ps_partkey, ps_supplycost * ps_availqty as f32)
        }))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS)
        .map(Fn!(|x: (i32, f32)| ((), x)))
        .join(
            join_three
                .map(Fn!(|(_, (_, x)): (i32, ((), TPartSupp))| {
                    let (ps_partkey, _, ps_availqty, ps_supplycost, _) = x;
                    ((), 0.0001 * ps_supplycost * ps_availqty as f32)
                }))
                .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS),
            NUM_PARTS,
        )
        .filter(Fn!(move |x: &((), ((i32, f32), f32))| x.1 .0 .1 > x.1 .1))
        .map(Fn!(|(_, ((k, v), _)): ((), ((i32, f32), f32))| (
            OrderedFloat(v),
            k
        )))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q12_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, _, _, _, _, o_orderpriority, _, _, _) = x;
            (o_orderkey, o_orderpriority)
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| {
            let mut upper_date = Date::new(1994, 1, 1).unwrap();
            upper_date.add_years(1);
            (x.7.as_str() == "MAIL" || x.7.as_str() == "SHIP")
                && x.4 < x.5
                && x.3 < x.4
                && x.5 >= Date::new(1994, 1, 1).unwrap()
                && x.5 < upper_date
        }))
        .map(Fn!(|x: TLineItem| {
            let l_orderkey = x.0 .0;
            let l_shipmode = x.7;
            (l_orderkey, l_shipmode)
        }));
    let table_final = table_lineitem
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(_, (l_shipmode, o_orderpriority)): (
            i32,
            (String, String)
        )| {
            let high_line_count =
                if o_orderpriority.as_str() == "1-URGENT" || o_orderpriority.as_str() == "2-HIGH" {
                    1
                } else {
                    0
                };
            let low_line_count =
                if o_orderpriority.as_str() != "1-URGENT" && o_orderpriority.as_str() != "2-HIGH" {
                    1
                } else {
                    0
                };
            (l_shipmode, (high_line_count, low_line_count))
        }))
        .reduce_by_key(
            Fn!(|(x, y): ((i32, i32), (i32, i32))| (x.0 + y.0, x.1 + y.1)),
            NUM_PARTS,
        )
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q13_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TCustomer| (x.0, ())));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| {
            let mut iter = x.8.split("special").into_iter();
            iter.next();
            let mut r = false;
            for ch in iter {
                r = r || ch.contains("requests");
            }
            !r
        }))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, _, _, _, _, _, _) = x;
            (o_custkey, o_orderkey)
        }));

    //currently we do not support outer join
    let table_final = table_customer
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(o_custkey, (_, o_orderkey)): (i32, ((), i32))| (
            o_custkey, 1
        )))
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .map(Fn!(|(o_custkey, c_count): (i32, i32)| (c_count, 1i32)))
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .map(Fn!(|(c_count, custdist): (i32, i32)| (
            (custdist, c_count),
            ()
        )))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q14_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| x.1.contains("green")))
        .map(Fn!(|x: TPart| (x.0, x.4)));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| {
            let mut upper_date = Date::new(1995, 9, 1).unwrap();
            upper_date.add_months(1);
            x.3 >= Date::new(1995, 9, 1).unwrap() && x.3 < upper_date
        }))
        .map(Fn!(|x: TLineItem| (x.0 .1, (x.0 .5, x.0 .6))));
    let table_final =
        table_part
            .join(table_lineitem, NUM_PARTS)
            .map(Fn!(|x: (i32, (String, (f32, f32)))| {
                let (_, (p_type, (l_extendedprice, l_discount))) = x;
                let value = l_extendedprice * (1.0 - l_discount);
                (
                    100.0
                        * if p_type.starts_with("PROMO") {
                            value
                        } else {
                            0.0
                        },
                    value,
                )
            }));

    let _res = table_final
        .reduce(Fn!(|x: (f32, f32), y: (f32, f32)| (x.0 + y.0, x.1 + y.1)))
        .unwrap();
    
    


    Ok(())
}

pub fn q15_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.0, (x.1, x.2, x.4))));
    let table_revenue0 = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| {
            let mut upper_date = Date::new(1996, 1, 1).unwrap();
            upper_date.add_months(3);
            x.3 >= Date::new(1996, 1, 1).unwrap() && x.3 < upper_date
        }))
        .map(Fn!(|x: TLineItem| (x.0 .2, x.0 .5 * (1.0 - x.0 .6))))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS);
    let max_revenue = table_revenue0
        .map(Fn!(|x: (i32, f32)| ((), OrderedFloat(x.1))))
        .reduce_by_key(
            Fn!(|(x, y): (OrderedFloat<f32>, OrderedFloat<f32>)| { std::cmp::max(x, y) }),
            NUM_PARTS,
        )
        .map(Fn!(|(k, v): ((), OrderedFloat<f32>)| (v, k)));

    let table_final = table_revenue0
        .map(Fn!(|(x, y): (i32, f32)| (OrderedFloat(y), x)))
        .join(max_revenue, NUM_PARTS)
        .map(Fn!(|(total, (s_suppkey, _)): (
            OrderedFloat<f32>,
            (i32, ())
        )| (s_suppkey, total)))
        .join(table_supplier, NUM_PARTS)
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q16_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_partsupp = PathBuf::from("/opt/data/ct_tpch_1g_partsupp");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| {
            x.3.as_str() != "Brand#45"
                && !x.4.starts_with("MEDIUM POLISHED")
                && vec![3, 9, 14, 19, 23, 36, 45, 49]
                    .binary_search(&x.5)
                    .is_ok()
        }))
        .map(Fn!(|(
            p_partkey,
            _,
            _,
            p_brand,
            p_type,
            p_size,
            _,
            _,
            _,
        ): TPart| (
            p_partkey,
            (p_brand, p_type, p_size)
        )));
    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TSupplier| {
            let mut iter = x.6.split("Customer").into_iter();
            iter.next();
            let mut r = false;
            for ch in iter {
                r = r || ch.contains("Complaints");
            }
            !r
        }))
        .map(Fn!(|x: TSupplier| (x.0, ())));
    let table_partsupp = sc
        .read_source(
            LocalFsReaderConfig::new(dir_partsupp).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TPartSupp| (x.1, x.0)));
    let table_final = table_supplier
        .join(table_partsupp, NUM_PARTS)
        .map(Fn!(|(ps_supplykey, (_, ps_partkey)): (i32, ((), i32))| (
            ps_partkey,
            ps_supplykey
        )))
        .join(table_part, NUM_PARTS)
        .map(Fn!(|(_, (ps_supplykey, t)): (
            i32,
            (i32, (String, String, i32))
        )| (t, ps_supplykey)))
        .distinct()
        .map(Fn!(|(t, _): ((String, String, i32), i32)| (t, 1)))
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .map(Fn!(|((p_brand, p_type, p_size), supplier_cnt): (
            (String, String, i32),
            i32
        )| (
            (supplier_cnt, p_brand, p_type, p_size),
            ()
        )))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q17_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(
            |x: &TPart| x.3.as_str() == "Brand#23" && x.6.as_str() == "MED BOX"
        ))
        .map(Fn!(|x: TPart| (x.0, ())));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TLineItem| (x.0 .1, (x.0 .4, x.0 .5))));
    let table_final = table_part
        .join(table_lineitem.clone(), NUM_PARTS)
        .map(Fn!(|x: (i32, ((), (i32, f32)))| {
            let (l_partkey, (_, (l_quantity, l_extendedprice))) = x;
            (l_partkey, (l_quantity, 1))
        }))
        .reduce_by_key(
            Fn!(|(x, y): ((i32, i32), (i32, i32))| (x.0 + y.0, x.1 + y.1)),
            NUM_PARTS,
        )
        .map(Fn!(|(l_partkey, (sum_quantity, count)): (
            i32,
            (i32, i32)
        )| {
            (l_partkey, 0.2 * (sum_quantity as f32) / (count as f32))
        }))
        .join(
            table_part
                .join(table_lineitem, NUM_PARTS)
                .map(Fn!(|x: (i32, ((), (i32, f32)))| {
                    let (l_partkey, (_, (l_quantity, l_extendedprice))) = x;
                    (l_partkey, (l_quantity, l_extendedprice))
                })),
            NUM_PARTS,
        )
        .filter(Fn!(|(_, (avg_quantity, (l_quantity, _))): &(
            i32,
            (f32, (i32, f32))
        )| (*l_quantity as f32) < *avg_quantity))
        .map(Fn!(|x: (i32, (f32, (i32, f32)))| x.1 .1 .1));

    let _res = table_final
        .reduce(Fn!(|x: f32, y: f32| x + y))
        .unwrap();
    
    


    Ok(())
}

pub fn q18_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TCustomer| (x.0, x.1)));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| x.4 >= Date::new(1993, 10, 1).unwrap()
            && x.4 <= Date::new(1994, 1, 1).unwrap()))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, o_custkey, _, o_totalprice, o_orderdate, _, _, _, _) = x;
            (o_orderkey, (o_custkey, o_totalprice, o_orderdate))
        }));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TLineItem| {
            let (l_orderkey, _, _, _, l_quantity, _, _, _) = x.0;
            (l_orderkey, l_quantity)
        }));
    let table_final = table_lineitem
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .filter(Fn!(|x: &(i32, i32)| x.1 > 300))
        .map(Fn!(|x: (i32, i32)| (x.0, ())))
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(k, (_, v)): (i32, ((), (i32, f32, Date)))| (k, v)))
        .join(table_lineitem, NUM_PARTS)
        .map(Fn!(|(
            o_orderkey,
            ((o_custkey, o_totalprice, o_orderdate), l_quantity),
        ): (i32, ((i32, f32, Date), i32))| (
            o_custkey,
            (o_orderkey, o_totalprice, o_orderdate, l_quantity)
        )))
        .join(table_customer, NUM_PARTS)
        .map(Fn!(|(
            c_custkey,
            ((o_orderkey, o_totalprice, o_orderdate, l_quantity), c_name),
        ): (i32, ((i32, f32, Date, i32), String))| (
            (
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                OrderedFloat(o_totalprice)
            ),
            l_quantity
        )))
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .map(Fn!(|(
            (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice),
            sum_quantity,
        ): (
            (String, i32, i32, Date, OrderedFloat<f32>),
            i32
        )| (
            (o_totalprice, o_orderdate),
            (c_name, c_custkey, o_orderkey, sum_quantity)
        )));

    let _res = table_final.take(100).unwrap();
    
    
    

    Ok(())
}

pub fn q19_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| x.5 >= 1))
        .map(Fn!(|x: TPart| (x.0, (x.3, x.5, x.6))));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| (x.7.as_str() == "AIR"
            || x.7.as_str() == "AIR REG")
            && x.6.as_str() == "DELIVER IN PERSON"))
        .map(Fn!(|x: TLineItem| {
            let (l_orderkey, _, _, _, l_quantity, l_extendedprice, l_discount, _) = x.0;
            (l_orderkey, (l_quantity, l_extendedprice, l_discount))
        }));
    let table_final = table_part
        .join(table_lineitem, NUM_PARTS)
        .filter(Fn!(|x: &(
            i32,
            ((String, i32, String), (i32, f32, f32))
        )| {
            let (
                l_partkey,
                ((p_brand, p_size, p_container), (l_quantity, l_extendedprice, l_discount)),
            ) = x;
            let p_container_set0 = vec!["SM CASE", "SM BOX", "SM PACK", "SM PKG"];
            let p_container_set1 = vec!["MED BAG", "MED BOX", "MED PKG", "MED PACK"];
            let p_container_set2 = vec!["LG CASE", "LG BOX", "LG PACK", "LG PKG"];
            (p_brand.as_str() == "Brand#12"
                && p_container_set0.contains(&p_container.as_str())
                && *l_quantity >= 1
                && *l_quantity <= 11
                && *p_size <= 5)
                || (p_brand.as_str() == "Brand#23"
                    && p_container_set1.contains(&p_container.as_str())
                    && *l_quantity >= 10
                    && *l_quantity <= 20
                    && *p_size <= 10)
                || (p_brand.as_str() == "Brand#34"
                    && p_container_set2.contains(&p_container.as_str())
                    && *l_quantity >= 20
                    && *l_quantity <= 30
                    && *p_size <= 15)
        }))
        .map(Fn!(|x: (i32, ((String, i32, String), (i32, f32, f32)))| {
            x.1 .1 .1 * (1.0 - x.1 .1 .2)
        }));

    let _res = table_final
        .reduce(Fn!(|x: f32, y: f32| x + y))
        .unwrap();
    
    


    Ok(())
}

pub fn q20_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_part = PathBuf::from("/opt/data/ct_tpch_1g_part");
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_partsupp = PathBuf::from("/opt/data/ct_tpch_1g_partsupp");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");

    let table_part = sc
        .read_source(
            LocalFsReaderConfig::new(dir_part).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TPart| x.1.starts_with("forest")))
        .map(Fn!(|x: TPart| (x.0, ())))
        .reduce_by_key(Fn!(|(x, y): ((), ())| ()), NUM_PARTS);
    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.3, (x.0, x.1, x.2))));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TLineItem| {
            let mut upper_date = Date::new(1994, 1, 1).unwrap();
            upper_date.add_years(1);
            x.3 <= upper_date && x.3 > Date::new(1994, 1, 1).unwrap()
        }))
        .map(Fn!(|x: TLineItem| {
            let (_, l_partkey, l_suppkey, _, l_quantity, _, _, _) = x.0;
            ((l_partkey, l_suppkey), 0.5 * l_quantity as f32)
        }))
        .reduce_by_key(Fn!(|(x, y): (f32, f32)| x + y), NUM_PARTS);
    let table_partsupp = sc
        .read_source(
            LocalFsReaderConfig::new(dir_partsupp).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TPartSupp| (x.0, (x.1, x.2))));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TNation| x.1.as_str() == "CANADA"))
        .map(Fn!(|(n_nationkey, _, _, _): TNation| (n_nationkey, ())));
    let table_final = table_part
        .join(table_partsupp, NUM_PARTS)
        .map(Fn!(|x: (i32, ((), (i32, i32)))| {
            let (ps_partkey, (_, (ps_supplykey, ps_availqty))) = x;
            ((ps_partkey, ps_supplykey), ps_availqty)
        }))
        .join(table_lineitem, NUM_PARTS)
        .filter(Fn!(|x: &((i32, i32), (i32, f32))| x.1 .0 as f32 > x.1 .1))
        .map(Fn!(|((_, _), (ps_supplykey, _)): (
            (i32, i32),
            (i32, f32)
        )| (ps_supplykey, ())))
        .reduce_by_key(Fn!(|(_, _)| ()), NUM_PARTS)
        .join(
            table_supplier
                .join(table_nation, NUM_PARTS)
                .map(Fn!(|(_, (x, _)): (i32, ((i32, String, String), ()))| {
                    let (s_suppkey, s_name, s_address) = x;
                    (s_suppkey, (s_name, s_address))
                })),
            NUM_PARTS,
        )
        .map(Fn!(|(_, (_, (s_name, s_address))): (
            i32,
            ((), (String, String))
        )| (s_name, s_address)))
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}

pub fn q21_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_supplier = PathBuf::from("/opt/data/ct_tpch_1g_supplier");
    let dir_lineitem = PathBuf::from("/opt/data/ct_tpch_1g_lineitem");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");
    let dir_nation = PathBuf::from("/opt/data/ct_tpch_1g_nation");

    let table_supplier = sc
        .read_source(
            LocalFsReaderConfig::new(dir_supplier).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TSupplier| (x.3, (x.0, x.1))));
    let table_lineitem = sc
        .read_source(
            LocalFsReaderConfig::new(dir_lineitem).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TLineItem| x));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TOrders| x.2.as_str() == "F"))
        .map(Fn!(|x: TOrders| {
            let (o_orderkey, _, _, _, _, _, _, _, _) = x;
            (o_orderkey, ())
        }));
    let table_nation = sc
        .read_source(
            LocalFsReaderConfig::new(dir_nation).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .filter(Fn!(|x: &TNation| x.1.as_str() == "SAUDI ARABIA"))
        .map(Fn!(|(n_nationkey, _, _, _): TNation| (n_nationkey, ())));

    let table_final = table_nation
        .join(table_supplier, NUM_PARTS)
        .map(Fn!(|(_, (_, (s_suppkey, s_name))): (
            i32,
            ((), (i32, String))
        )| (s_suppkey, s_name)))
        .join(
            table_lineitem
                .filter(Fn!(|x: &TLineItem| x.4 < x.5))
                .map(Fn!(|x: TLineItem| (x.0 .2, x.0 .0))),
            NUM_PARTS,
        )
        .map(Fn!(|(l_suppkey, (s_name, l_orderkey)): (
            i32,
            (String, i32)
        )| (l_orderkey, (l_suppkey, s_name))))
        .join(table_orders, NUM_PARTS)
        .map(Fn!(|(l_orderkey, ((l_suppkey, s_name), _)): (
            i32,
            ((i32, String), ())
        )| (l_orderkey, (l_suppkey, s_name))))
        .join(
            table_lineitem
                .map(Fn!(|x: TLineItem| (x.0 .0, x.0 .2)))
                .distinct()
                .map(Fn!(|(x, _): (i32, i32)| (x, 1i32)))
                .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
                .filter(Fn!(|x: &(i32, i32)| x.1 > 1))
                .map(Fn!(|(key, _): (i32, i32)| (key, ()))),
            NUM_PARTS,
        )
        .map(Fn!(|(l_orderkey, ((l_suppkey, s_name), ())): (
            i32,
            ((i32, String), ())
        )| ((l_suppkey, l_orderkey), s_name)))
        .join(
            table_lineitem
                .filter(Fn!(|x: &TLineItem| x.4 < x.5))
                .map(Fn!(|x: TLineItem| ((x.0 .0, x.0 .2), x.0 .2)))
                .reduce_by_key(Fn!(|(x, y): (i32, i32)| std::cmp::max(x, y)), NUM_PARTS)
                .map(Fn!(|((l_orderkey, l_suppkey), max_suppkey): (
                    (i32, i32),
                    i32
                )| (l_orderkey, (max_suppkey, 1i32))))
                .reduce_by_key(
                    Fn!(|(x, y): ((i32, i32), (i32, i32))| (std::cmp::max(x.0, y.0), x.1 + y.1)),
                    NUM_PARTS,
                )
                .filter(Fn!(|x: &(i32, (i32, i32))| x.1 .1 == 1))
                .map(Fn!(|(key, (suppkey_max, _)): (i32, (i32, i32))| ((
                    (suppkey_max, key),
                    ()
                )))),
            NUM_PARTS,
        )
        .map(Fn!(|(_, (s_name, _)): ((i32, i32), (String, ()))| (
            s_name, 1
        )))
        .reduce_by_key(Fn!(|(x, y): (i32, i32)| x + y), NUM_PARTS)
        .map(Fn!(|(s_name, numwait): (String, i32)| (numwait, s_name)));

    let _res = table_final.take(100).unwrap();
    
    
    

    Ok(())
}

pub fn q22_sec() -> Result<()> {
    let sc = Context::new()?;

    let deserializer = Box::new(Fn!(|file: Vec<u8>| {
        bincode::deserialize::<Vec<Vec<u8>>>(&file).unwrap() //ItemE = Vec<u8>
    }));

    
    let dir_customer = PathBuf::from("/opt/data/ct_tpch_1g_customer");
    let dir_orders = PathBuf::from("/opt/data/ct_tpch_1g_orders");

    let table_customer = sc
        .read_source(
            LocalFsReaderConfig::new(dir_customer).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TCustomer| x));
    let table_orders = sc
        .read_source(
            LocalFsReaderConfig::new(dir_orders).num_partitions_per_executor(NUM_PARTS_LOCAL),
            None,
            Some(deserializer.clone()),
        )
        .map(Fn!(|x: TOrders| {
            let (_, o_custkey, _, _, _, _, _, _, _) = x;
            (o_custkey, ())
        }))
        .reduce_by_key(Fn!(|(_, _)| ()), NUM_PARTS);

    //currently we do not support outer join
    let table_final = table_orders
        .join(
            table_customer
                .filter(Fn!(|x: &TCustomer| {
                    let s = vec!["13", "31", "23", "29", "30", "18", "17"];
                    s.contains(&&x.4[1..3])
                }))
                .map(Fn!(
                    |(c_custkey, _, _, _, c_phone, c_acctbal, _, _): TCustomer| {
                        (c_custkey, (c_phone[1..3].to_string(), c_acctbal))
                    }
                )),
            NUM_PARTS,
        )
        .map(Fn!(|(_, (_, (cntrycode, c_acctbal))): (
            i32,
            ((), (String, f32))
        )| ((), (cntrycode, c_acctbal))))
        .join(
            table_customer
                .filter(Fn!(|x: &TCustomer| {
                    let s = vec!["13", "31", "23", "29", "30", "18", "17"];
                    s.contains(&&x.4[1..3]) && x.5 > 0.0
                }))
                .map(Fn!(|x: TCustomer| ((), (x.5, 1))))
                .reduce_by_key(
                    Fn!(|(x, y): ((f32, i32), (f32, i32))| (x.0 + y.0, x.1 + y.1)),
                    NUM_PARTS,
                )
                .map(Fn!(|(k, v): ((), (f32, i32))| (k, v.0 / v.1 as f32))),
            NUM_PARTS,
        )
        .filter(Fn!(|x: &((), ((String, f32), f32))| x.1 .0 .1 > x.1 .1))
        .map(Fn!(|(_, ((cntrycode, c_acctbal), _)): (
            (),
            ((String, f32), f32)
        )| (cntrycode, (1, c_acctbal))))
        .reduce_by_key(
            Fn!(|(x, y): ((i32, f32), (i32, f32))| (x.0 + y.0, x.1 + y.1)),
            NUM_PARTS,
        )
        .sort_by_key(true, NUM_PARTS);

    let _res = table_final.collect().unwrap();
    
    
    

    Ok(())
}