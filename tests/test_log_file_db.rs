use std::sync::Arc;

use pi_db::mgr::{ DatabaseWare, Mgr };
use pi_db::log_file_db::LogFileDB;
use atom::Atom;
use sinfo;
use guid::GuidGen;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use pi_db::db::{TabKV, TabMeta};
use bon::WriteBuffer;

#[test]
fn test_log_file_db() {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()>  = pool.startup(true);

	let _ = rt.spawn(rt.alloc(), async move {
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024));
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
		let mut tr = mgr.transaction(true).await;

		let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
		let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), Some(Arc::new(meta))).await;
		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/world"), Some(Arc::new(meta1))).await;
		let p = tr.prepare().await;
		println!("tr prepare ---- {:?}", p);
		tr.commit().await;

		let info = tr.tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/hello")).await;
		println!("info ---- {:?} ", info);

		let mut wb = WriteBuffer::new();
		wb.write_bin(b"hello", 0..5);

		println!("wb = {:?}", wb.bytes);

		let mut item1 = TabKV {
			ware: Atom::from("logfile"),
			tab: Atom::from("./testlogfile/hello"),
			key: Arc::new(wb.bytes.clone()),
			value: Some(Arc::new(wb.bytes)),
			index: 0
		};

		let mut tr2 = mgr.transaction(true).await;

		let r = tr2.modify(vec![item1.clone()], None, false).await;
		println!("logfile result = {:?}", r);
		let p = tr2.prepare().await;
		tr2.commit().await;

		let mut tr3 = mgr.transaction(false).await;
		item1.value = None;

		let q = tr3.query(vec![item1], None, false).await;
		println!("query item = {:?}", q);
		tr3.prepare().await;
		tr3.commit().await;

		let mut tr4 = mgr.transaction(false).await;
		let size = tr4.tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/hello")).await;
		println!("tab size = {:?}", size);
		{
			let iter = tr4.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), None, false, None).await;

			if let Ok(mut it) = iter {
				loop {
					let item = it.next();
					println!("iter item = {:?}", item);
					match item {
						Some(Ok(None)) | Some(Err(_)) => break,
						_ => {}
					}
				}
			}
		}

		let tabs = tr4.list(&Atom::from("logfile")).await;
		println!("tabs = {:?}", tabs);

		tr4.prepare().await;
		tr4.commit().await;
	});

	std::thread::sleep(std::time::Duration::from_secs(2));
}