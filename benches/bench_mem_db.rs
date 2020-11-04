#![feature(test)]
extern crate test;
use test::Bencher;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::RwLock;

use atom::Atom;
use bon::WriteBuffer;
use guid::GuidGen;
use pi_db::db::{TabKV, TabMeta};
use pi_db::mgr::{DatabaseWare, Mgr};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::single_thread::{SingleTaskRunner};
use r#async::rt::AsyncRuntime;
use pi_db::memery_db::MemDB;
use sinfo;


#[bench]
fn bench_mem_db_write_single_tab(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
		let _ = rt.spawn(rt.alloc(), async move {
			test_mem_db_write().await;
		});
	});
}

#[bench]
fn bench_mem_db_read_single_tab(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let _ = rt.spawn(rt.alloc(), async move { test_mem_db_read().await });
    });
}

#[bench]
fn bench_mem_db_concurrent_write(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
		let rt_clone = rt.clone();
		let _ = rt.spawn(rt.alloc(), async move {
			test_mem_db_concurrent_write(rt_clone).await
		});
	});
}

#[bench]
fn bench_mem_db_concurrent_read(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
		let rt_clone = rt.clone();
		let _ = rt.spawn(rt.alloc(), async move {
			test_mem_db_concurrent_read(rt_clone).await
		});
	});
}

#[bench]
fn bench_mem_db_iter(b: &mut Bencher) {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 8, 1024 * 1024, 10, Some(10));
	let rtm: MultiTaskRuntime<()> = pool.startup(false);

	let runner  = SingleTaskRunner::new();
	let rt = runner.startup().unwrap();

	let flag = Arc::new(AtomicBool::new(false));
	let flag_clone = flag.clone();
	let mgr11 = Arc::new(RwLock::new(None));
	let mgr11_clone = mgr11.clone();

	let _ = rtm.spawn(rtm.alloc(), async move {
		let mgr = setup_data().await;
		*mgr11_clone.write().unwrap() = Some(mgr);
		flag_clone.store(true, Ordering::Relaxed);
	});

	loop {
		if flag.load(Ordering::Relaxed) {
			break;
		}
	}

	let mgr = mgr11.write().unwrap().take().unwrap();

	b.iter(|| {
		let mgr_clone = mgr.clone();
		let _ = rt.spawn(rt.alloc(), async move {
			test_mem_db_iter(mgr_clone).await;
		});
		runner.run_once();
	});
}

async fn setup_data() -> Mgr {
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let ware = DatabaseWare::new_mem_ware(MemDB::new());
	let _ = mgr.register(Atom::from("memory"), Arc::new(ware)).await;
	let mut tr = mgr.transaction(true).await;
	let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
	tr.alter(&Atom::from("memory"), &Atom::from("hello"), Some(Arc::new(meta))).await;

	tr.prepare().await;
	tr.commit().await;

	let mut tr2 = mgr.transaction(true).await;
	
	let mut items = vec![];

	for i in 0..20000 {
		let key = format!("hello world {:?}", i);
		let mut wb = WriteBuffer::new();
		wb.write_bin(key.as_bytes(), 0..key.len());

		items.push(TabKV {
			ware: Atom::from("memory"),
			tab: Atom::from("hello"),
			key: Arc::new(wb.bytes.clone()),
			value: Some(Arc::new(wb.bytes)),
			index: 0,
		});
	}

	tr2.modify(items.clone(), None, false).await;
	tr2.prepare().await;
	tr2.commit().await;

	mgr
}

async fn test_mem_db_iter(mgr: Mgr) {
	let mut tr3 = mgr.transaction(false).await;
	
	let mut iter = tr3.iter(&Atom::from("memory"), &Atom::from("hello"), None, false, None).await.unwrap();

	while let Some(Ok(Some(elem))) = iter.next() {
		// println!("elem = {:?}", elem);
	}

	tr3.prepare().await;
	tr3.commit().await;
}

async fn test_mem_db_concurrent_read(rt: MultiTaskRuntime<()>) {
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let ware = DatabaseWare::new_mem_ware(MemDB::new());
	let _ = mgr.register(Atom::from("memory"), Arc::new(ware)).await;
	let mut tr = mgr.transaction(true).await;
	let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

	tr.alter(&Atom::from("memory"), &Atom::from("hello"), Some(Arc::new(meta)));

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
			let tr2 =  mgr2.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello1", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr2.transaction(true).await;
	
			let r = tr2.query(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr3.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello2", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr3.transaction(true).await;
	
			let r = tr2.query(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr4.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello3", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr4.transaction(true).await;
	
			let r = tr2.query(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr5.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello4", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr5.transaction(true).await;
	
			let r = tr2.query(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr6.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello5", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr6.transaction(true).await;
	
			let r = tr2.query(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});
		map.map(AsyncRuntime::Multi(rt.clone()), false).await;
	}.await
}

async fn test_mem_db_concurrent_write(rt: MultiTaskRuntime<()>) {
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let ware = DatabaseWare::new_mem_ware(MemDB::new());
	let _ = mgr.register(Atom::from("memory"), Arc::new(ware)).await;
	let mut tr = mgr.transaction(true).await;
	let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

	tr.alter(&Atom::from("memory"), &Atom::from("hello"), Some(Arc::new(meta))).await;

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
			let tr2 =  mgr2.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello1", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr2.transaction(true).await;
	
			let r = tr2.modify(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr3.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello2", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr3.transaction(true).await;
	
			let r = tr2.modify(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr4.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello3", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr4.transaction(true).await;
	
			let r = tr2.modify(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr5.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello4", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr5.transaction(true).await;
	
			let r = tr2.modify(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});

		map.join(AsyncRuntime::Multi(rt.clone()), async move {
			let tr2 =  mgr6.transaction(true).await;
			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello5", 0..6);
	
			let mut item1 = TabKV {
				ware: Atom::from("memory"),
				tab: Atom::from("hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0,
			};
	
			let mut tr2 = mgr6.transaction(true).await;
	
			let r = tr2.modify(vec![item1], None, false).await;
			let p = tr2.prepare().await;
			tr2.commit().await;
			Ok(())
		});
		map.map(AsyncRuntime::Multi(rt.clone()), false).await;
	}.await
}

async fn test_mem_db_write() {
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let ware = DatabaseWare::new_mem_ware(MemDB::new());
	let _ = mgr.register(Atom::from("memory"), Arc::new(ware)).await;
	let mut tr = mgr.transaction(true).await;

	let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

	tr.alter(
		&Atom::from("memory"),
		&Atom::from("hello"),
		Some(Arc::new(meta)),
	)
	.await;
	let p = tr.prepare().await;
	tr.commit().await;

	let info = tr
		.tab_info(&Atom::from("memory"), &Atom::from("hello"))
		.await;

	let mut wb = WriteBuffer::new();
	wb.write_bin(b"hello", 0..5);

	// println!("wb = {:?}", wb.bytes);

	let mut item1 = TabKV {
		ware: Atom::from("memory"),
		tab: Atom::from("hello"),
		key: Arc::new(wb.bytes.clone()),
		value: Some(Arc::new(wb.bytes)),
		index: 0,
	};

	let mut tr2 = mgr.transaction(true).await;

	let r = tr2.modify(vec![item1], None, false).await;
	let p = tr2.prepare().await;
	tr2.commit().await;
}

async fn test_mem_db_read() {
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let ware = DatabaseWare::new_mem_ware(MemDB::new());
	let _ = mgr.register(Atom::from("memory"), Arc::new(ware)).await;
	let mut tr = mgr.transaction(true).await;

	let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
	let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

	tr.alter(
		&Atom::from("memory"),
		&Atom::from("hello"),
		Some(Arc::new(meta)),
	)
	.await;
	let p = tr.prepare().await;
	tr.commit().await;

	let info = tr
		.tab_info(&Atom::from("memory"), &Atom::from("hello"))
		.await;

	let mut wb = WriteBuffer::new();
	wb.write_bin(b"hello", 0..5);

	let mut item1 = TabKV {
		ware: Atom::from("memory"),
		tab: Atom::from("hello"),
		key: Arc::new(wb.bytes.clone()),
		value: Some(Arc::new(wb.bytes)),
		index: 0,
	};

	let mut tr2 = mgr.transaction(true).await;

	let r = tr2.query(vec![item1], None, false).await;
	let p = tr2.prepare().await;
	tr2.commit().await;
}