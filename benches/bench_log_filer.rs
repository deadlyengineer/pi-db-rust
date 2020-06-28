#![feature(test)]

extern crate test;
use test::Bencher;

use std::sync::Arc;


use atom::Atom;
use bon::WriteBuffer;
use guid::GuidGen;
use pi_db::db::{TabKV, TabMeta};
use pi_db::log_file_db::LogFileDB;
use pi_db::mgr::{DatabaseWare, Mgr};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use sinfo;

#[bench]
fn bench_log_file_write(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let _ = rt.spawn(rt.alloc(), async move { log_file_write(1).await });
	});
}

#[bench]
fn bench_log_file_read(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let _ = rt.spawn(rt.alloc(), async move { log_file_read().await });
    });
}

async fn log_file_read() {
	let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(
        Atom::from("./testlogfile"),
        1024 * 1024 * 1024,
    ));
    let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
    let mut tr = mgr.transaction(true).await;

    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    tr.alter(
        &Atom::from("logfile"),
        &Atom::from("./testlogfile/hello"),
        Some(Arc::new(meta)),
    )
    .await;
    tr.prepare().await;
    tr.commit().await;

    let mut wb = WriteBuffer::new();
    wb.write_bin(b"hello", 0..5);

    let item1 = TabKV {
        ware: Atom::from("logfile"),
        tab: Atom::from("./testlogfile/hello"),
        key: Arc::new(wb.bytes.clone()),
        value: None,
        index: 0,
    };

    let mut tr2 = mgr.transaction(true).await;

    let r = tr2.modify(vec![item1], None, false).await;
    let p = tr2.prepare().await;
    tr2.commit().await;
}

async fn log_file_write(count: usize) {
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(
        Atom::from("./testlogfile"),
        1024 * 1024 * 1024,
    ));
    let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
    let mut tr = mgr.transaction(true).await;

    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    tr.alter(
        &Atom::from("logfile"),
        &Atom::from("./testlogfile/hello"),
        Some(Arc::new(meta)),
    )
    .await;
    tr.prepare().await;
    tr.commit().await;

	let mut items = vec![];
	
	for i in 0..count {
		let key = format!("hello world{:?}", i);
		let mut wb = WriteBuffer::new();
		wb.write_bin(key.as_bytes(), 0..key.len());

		items.push(TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(wb.bytes.clone()),
			value: Some(Arc::new(wb.bytes)),
			index: i,
		})
	}

    let mut tr2 = mgr.transaction(true).await;

    let r = tr2.modify(items, None, false).await;
    let p = tr2.prepare().await;
	tr2.commit().await;
}
