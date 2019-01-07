// extern crate pi_lib;
// extern crate pi_db;
// extern crate fnv;

// use pi_db::memery_db::{MTab, MemeryTxn};
// use pi_db::db::{Tab, TabTxn, IterResult, NextResult, Bin};

// use pi_lib::atom::{Atom};
// use pi_lib::guid::{GuidGen};
// use pi_lib::time::now_nanos;
// use pi_lib::bon::WriteBuffer;

// use std::sync::{Arc};

// #[test]
// fn test_memery_db() {
// 	//打开表
// 	let tab = MTab::new(&Atom::from("test"));
// 	let guid_gen = GuidGen::new(1, 11111);
// 	let guid = guid_gen.gen(1);

// 	let tab2 = &tab;

// 	//创建事务
// 	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
// 	let mut txn = txn.borrow_mut();
// 	let mut key1 = WriteBuffer::new();
// 	key1.write_utf8("1");
// 	let key1 = Arc::new(key1.unwrap());
// 	assert_eq!(txn.upsert(key1.clone(), Arc::new(b"v1".to_vec())), Ok(()));
// 	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
// 	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
// 	assert_eq!(txn.prepare1(), Ok(()));
// 	match txn.commit1() {
// 		Ok(_) => (),
// 		_ => panic!("commit1 fail"),
// 	}

// 	//创建事务(添加key:2 并回滚)
// 	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
// 	let mut key2 = WriteBuffer::new();
// 	key2.write_utf8("2");
// 	let key2 = Arc::new(key2.unwrap());
// 	assert_eq!(txn.borrow_mut().upsert(key2.clone(), Arc::new(b"v2".to_vec())), Ok(()));
// 	assert_eq!(txn.borrow_mut().get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
// 	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
// 	assert_eq!(txn.borrow_mut().rollback1(), Ok(()));

// 	//创建事务
// 	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
// 	let mut txn = txn.borrow_mut();
// 	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
// 	assert_eq!(txn.get(key2.clone()), None);
// 	assert_eq!(txn.prepare1(), Ok(()));
// 	match txn.commit1() {
// 		Ok(_) => (),
// 		_ => panic!("commit1 fail"),
// 	}

// 	//创建事务
// 	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
// 	let mut txn = txn.borrow_mut();
// 	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
// 	assert_eq!(txn.get(key2.clone()), None);
// 	assert_eq!(txn.prepare1(), Ok(()));
// 	match txn.commit1() {
// 		Ok(_) => (),
// 		_ => panic!("commit1 fail"),
// 	}
// }

// #[test]
// fn test_memery_db_p() {
// 	//打开表
// 	let tab = MTab::new(&Atom::from("test2"));
// 	let guid_gen = GuidGen::new(2, 22222);
// 	let guid = guid_gen.gen(2);

// 	let tab2 = &tab;

// 	let start = now_nanos();
// 	//创建事务
// 	let txn = MemeryTxn::new(tab2.clone(), &guid, true);

// 	for n in 0..100 {
// 		let mut key = WriteBuffer::new();
// 		key.write_utf8(&n.to_string());
// 		let key = Arc::new(key.unwrap());
// 		let v = Vec::from("vvvvvvvvvvvvvvvvvvvv");
// 		assert_eq!(txn.borrow_mut().upsert(key.clone(), Arc::new(v)), Ok(()));
// 	}
// 	let mut key = WriteBuffer::new();
// 	key.write_utf8(&99.to_string());
// 	let key = Arc::new(key.unwrap());
// 	assert_eq!(txn.borrow_mut().get(key.clone()), Some(Arc::new(Vec::from("vvvvvvvvvvvvvvvvvvvv"))));
// 	let mut it=txn.iter(None, false, None, Arc::new(|_i:IterResult|{})).unwrap().unwrap();

// 	let mut key = WriteBuffer::new();
// 	key.write_utf8(&0.to_string());
// 	let key = Arc::new(key.unwrap());
// 	assert_eq!(it.next(Arc::new(|_i:NextResult<(Bin, Bin)>|{})), Some(Ok(Some((key.clone(), Arc::new(Vec::from("vvvvvvvvvvvvvvvvvvvv")))))));
// 	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
// 	let mut txn = txn.borrow_mut();
// 	match txn.commit1() {
// 		Ok(_) => (),
// 		_ => panic!("commit1 fail"),
// 	}

// 	let end = now_nanos();//获取结束时间
//     println!("done!start : {:?},end :{:?},duration:{:?}",start,end,end-start);
// }

// #[test]
// fn test_memery_db_p2() {
// 	println!("test p2!!!!!!!");
// 	//打开表
// 	let tab = MTab::new(&Atom::from("test3"));
// 	let guid_gen = GuidGen::new(3, 3333);

// 	let tab2 = &tab;

// 	// let start = now_nanos();

// 	for n in 0..10 {
// 		//创建事务
// 		let txn = MemeryTxn::new(tab2.clone(), &guid_gen.gen(3), true);
// 		let mut txn = txn.borrow_mut();
// 		let mut key = WriteBuffer::new();
// 		key.write_utf8(&n.to_string());
// 		let v = Vec::from("vvvvvvvvvvvvvvvvvvvv");

// 		assert_eq!(txn.upsert(Arc::new(key.unwrap()), Arc::new(v)), Ok(()));
// 		assert_eq!(txn.prepare1(), Ok(()));
// 		match txn.commit1() {
// 			Ok(_) => (),
// 			_ => panic!("commit1 fail"),
// 		}
// 	};

// 	// let end = now_nanos();//获取结束时间
//     // println!("done!start : {:?},end :{:?},duration:{:?}",start,end,end-start);
// }

extern crate pi_db;

extern crate atom;
extern crate bon;
extern crate guid;
extern crate sinfo;

use std::sync::Arc;
use std::thread;
use std::time;

use atom::Atom;
use bon::WriteBuffer;
use guid::Guid;
use sinfo::{EnumType, StructInfo};

use pi_db::db::{Bin, TabKV, TabMeta, Ware};
use pi_db::memery_db::DB;

#[test]
fn test_memorydb_multi_thread() {
	let db = DB::new();
	let snapshot = db.snapshot();

	let sinfo = Arc::new(TabMeta::new(
		EnumType::Str,
		EnumType::Struct(Arc::new(StructInfo::new(Atom::from("test_table_2"), 8888))),
	));
	snapshot.alter(&Atom::from("test_table_2"), Some(sinfo.clone()));

	let meta_txn = snapshot.meta_txn(&Guid(0));

	assert!(meta_txn
		.alter(
			&Atom::from("test_table_2"),
			Some(sinfo.clone()),
			Arc::new(move |alter| {
				assert!(alter.is_ok());
			}),
		)
		.is_some());

	assert!(meta_txn
		.prepare(
			1000,
			Arc::new(move |p| {
				assert!(p.is_ok());
			}),
		)
		.is_some());

	thread::sleep(time::Duration::from_millis(1000));

	// commit success
	assert!(meta_txn
		.commit(Arc::new(move |c| {
			assert!(c.is_ok());
		}))
		.is_some());

	thread::sleep(time::Duration::from_millis(1000));

	assert!(snapshot.tab_info(&Atom::from("test_table_2")).is_some());

	let db_clone = Arc::into_raw(Arc::new(db.clone())) as usize;
	// concurrently insert 3000 items with 3 threads
	let h1 = thread::spawn(move || {
		let db1 = unsafe { Arc::from_raw(db_clone as *const pi_db::memery_db::DB) };
		let snapshot = db1.snapshot();

		assert!(snapshot.tab_info(&Atom::from("test_table_2")).is_some());

		let txn1 = snapshot
			.tab_txn(
				&Atom::from("test_table_2"),
				&Guid(0),
				true,
				Box::new(|_r| {}),
			)
			.unwrap()
			.unwrap();

		for i in 0..1000 {
			let k = build_db_key(&format!("test_key{:?}", i));
			let v = build_db_val(&format!("test_value{:?}", i));
			let item = create_tabkv(
				Atom::from("testdb"),
				Atom::from("test_table_2"),
				k.clone(),
				0,
				Some(v.clone()),
			);

			let m = txn1.modify(
				Arc::new(vec![item]),
				None,
				false,
				Arc::new(move |m| {
					assert!(m.is_ok());
				}),
			);
			assert!(m.is_some());
		}

		let p = txn1.prepare(
			1000,
			Arc::new(move |p| {
				assert!(p.is_ok());
			}),
		);
		assert!(p.is_some());

		let c = txn1.commit(Arc::new(move |c| {
			assert!(c.is_ok());
		}));
		assert!(c.is_some());
	})
	.join();

	thread::sleep(time::Duration::from_millis(2000));
}

fn create_tabkv(ware: Atom, tab: Atom, key: Bin, index: usize, value: Option<Bin>) -> TabKV {
	TabKV {
		ware,
		tab,
		key,
		index,
		value,
	}
}

fn build_db_key(key: &str) -> Arc<Vec<u8>> {
	let mut wb = WriteBuffer::new();
	wb.write_utf8(key);
	Arc::new(wb.get_byte().to_vec())
}

fn build_db_val(val: &str) -> Arc<Vec<u8>> {
	let mut wb = WriteBuffer::new();
	wb.write_utf8(val);
	Arc::new(wb.get_byte().to_vec())
}
