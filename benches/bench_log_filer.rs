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
use r#async::rt::{AsyncRuntime, AsyncValue};
use sinfo;

#[bench]
fn bench_log_file_write(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
		let _ = rt.spawn(rt.alloc(), async move { log_file_write().await });
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

#[bench]
fn bench_file_db_concurrent_write(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
		let rt_clone = rt.clone();
		let _ = rt.spawn(rt.alloc(), async move {
			test_log_file_db_concurrent_write(rt_clone).await
		});
	});
}

#[bench]
fn bench_file_db_concurrent_read(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
		let rt_clone = rt.clone();
		let _ = rt.spawn(rt.alloc(), async move {
			test_log_file_db_concurrent_read(rt_clone).await
		});
	});
}

async fn test_log_file_db_concurrent_read(rt: MultiTaskRuntime<()>) {
	let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(
        Atom::from("./testlogfile"),
        1024 * 1024 * 1024,
    ).await);
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

	let mgr2 = mgr.clone();
	let mgr3 = mgr.clone();
	let mgr5 = mgr.clone();
	let mgr4 = mgr.clone();
	let mgr6 = mgr.clone();


	let mut map = rt.map();

	async move {
		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world1";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr2.transaction(true).await;

			let r = tr2.query(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world2";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr3.transaction(true).await;

			let r = tr2.query(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world3";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr4.transaction(true).await;

			let r = tr2.query(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world4";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr5.transaction(true).await;

			let r = tr2.query(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world5";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr6.transaction(true).await;

			let r = tr2.query(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});
		map.map(AsyncRuntime::Multi(rt.clone()), false).await;
	}.await
}

async fn test_log_file_db_concurrent_write(rt: MultiTaskRuntime<()>) {
	let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(
        Atom::from("./testlogfile"),
        1024 * 1024 * 1024,
    ).await);
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

	let mgr2 = mgr.clone();
	let mgr3 = mgr.clone();
	let mgr5 = mgr.clone();
	let mgr4 = mgr.clone();
	let mgr6 = mgr.clone();


	let mut map = rt.map();

	async move {
		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world1";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr2.transaction(true).await;

			let r = tr2.modify(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world2";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr3.transaction(true).await;

			let r = tr2.modify(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world3";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr4.transaction(true).await;

			let r = tr2.modify(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world4";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr5.transaction(true).await;

			let r = tr2.modify(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let mut items = vec![];
			let mut wb = WriteBuffer::new();
			let key = b"hello world5";
			wb.write_bin(key, 0..key.len());

			items.push(TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			});

			let mut tr2 = mgr6.transaction(true).await;

			let r = tr2.modify(items, None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});
		map.map(AsyncRuntime::Multi(rt.clone()), false).await;
	}.await
}

async fn log_file_read() {
	let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(
        Atom::from("./testlogfile"),
        1024 * 1024 * 1024,
    ).await);
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

async fn log_file_write() {
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(
        Atom::from("./testlogfile"),
        1024 * 1024 * 1024,
    ).await);
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
	
	let mut wb = WriteBuffer::new();
	let key = b"hello world";
	wb.write_bin(key, 0..key.len());

	items.push(TabKV {
		ware: Atom::from("logfile"),
		tab: Atom::from("./testlogfile/hello"),
		key: Arc::new(wb.bytes.clone()),
		value: Some(Arc::new(wb.bytes)),
		index: 0,
	});

    let mut tr2 = mgr.transaction(true).await;

    let r = tr2.modify(items, None, false).await;
    let p = tr2.prepare().await;
	tr2.commit().await;
}
