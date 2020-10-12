use std::{collections::HashMap, sync::Arc};
use std::thread;
use std::time::Duration;
use std::sync::Mutex;
use atom::Atom;
use bon::{Encode, Decode, WriteBuffer, ReadBuffer, ReadBonErr};
use pi_db::mgr::{ DatabaseWare, Mgr };
use pi_db::log_file_db::LogFileDB;
use sinfo;
use guid::GuidGen;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use pi_db::db::{TabKV, TabMeta};
use pi_db::fork::TableMetaInfo;

#[test]
fn test_fork() {
	let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
	let rt: MultiTaskRuntime<()>  = pool.startup(true);

	let _ = rt.spawn(rt.alloc(), async move {
		let mgr = Mgr::new(GuidGen::new(0, 0));
		let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await);
		let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;

		let mut tr = mgr.transaction(true).await;
		let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Bin);
		let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

		// 创建一个用于存储元信息的表
		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/tabs_meta"), Some(Arc::new(meta))).await;
		tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), Some(Arc::new(meta1))).await;
		let p = tr.prepare().await;
		tr.commit().await;

		let mut wb = WriteBuffer::new();
		wb.write_bin(b"hello", 0..5);

		// let tm = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
		// let tmi = TableMetaInfo::new(Atom::from("hello"), tm);
		// let mut wb1 = WriteBuffer::new();
		// tmi.encode(&mut wb1);

		// let item = TabKV {
		// 	ware: Atom::from("logfile"),
		// 	tab: Atom::from("./testlogfile/tabs_meta"),
		// 	key: Arc::new(wb.bytes.clone()),
		// 	value: Some(Arc::new(wb1.bytes)),
		// 	index: 0
		// };

		// let mut tr2 = mgr.transaction(true).await;
		// tr2.modify(vec![item], None, false).await;
		// tr2.prepare().await;
		// tr2.commit().await;


		// 需要开一个新的事务来执行分叉操作？？
		let mut tr3 = mgr.transaction(true).await;
		let tabs = tr3.list(&Atom::from("logfile")).await;
		println!("tabs === {:?}", tabs);
		let tm = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
		tr3.fork_tab(Atom::from("./testlogfile/hello"), Atom::from("./testlogfile/hello_fork"), tm).await;
		let p = tr3.prepare().await;
		println!("prepare ==== {:?}", p);
		let c = tr3.commit().await;
		println!("commit=== {:?}", c);

	});

	thread::sleep(Duration::from_secs(3));
}