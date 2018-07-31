extern crate pi_lib;
extern crate pi_db;
extern crate fnv;

use pi_db::memery_db::{MTab, MemeryTxn};
use pi_db::db::{Tab, TabTxn, IterResult, NextResult, Bin};


use pi_lib::atom::{Atom};
use pi_lib::guid::{GuidGen};
use pi_lib::time::now_nanos;
use pi_lib::bon::WriteBuffer;

use std::sync::{Arc};

#[test]
fn test_memery_db() {
	//打开表
	let tab = MTab::new(&Atom::from("test"));
	let guid_gen = GuidGen::new(1, 11111);
	let guid = guid_gen.gen(1);

	let tab2 = &tab;

	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
	let mut txn = txn.borrow_mut();
	let mut key1 = WriteBuffer::new();
	key1.write_utf8("1");
	let key1 = Arc::new(key1.unwrap());
	assert_eq!(txn.upsert(key1.clone(), Arc::new(b"v1".to_vec())), Ok(()));
	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.prepare1(), Ok(()));
	match txn.commit1() {
		Ok(_) => (),
		_ => panic!("commit1 fail"),
	}

	//创建事务(添加key:2 并回滚)
	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
	let mut key2 = WriteBuffer::new();
	key2.write_utf8("2");
	let key2 = Arc::new(key2.unwrap());
	assert_eq!(txn.borrow_mut().upsert(key2.clone(), Arc::new(b"v2".to_vec())), Ok(()));
	assert_eq!(txn.borrow_mut().get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	assert_eq!(txn.borrow_mut().rollback1(), Ok(()));

	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
	let mut txn = txn.borrow_mut();
	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.get(key2.clone()), None);
	assert_eq!(txn.prepare1(), Ok(()));
	match txn.commit1() {
		Ok(_) => (),
		_ => panic!("commit1 fail"),
	}

	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
	let mut txn = txn.borrow_mut();
	assert_eq!(txn.get(key1.clone()), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.get(key2.clone()), None);
	assert_eq!(txn.prepare1(), Ok(()));
	match txn.commit1() {
		Ok(_) => (),
		_ => panic!("commit1 fail"),
	}
}

#[test]
fn test_memery_db_p() {
	//打开表
	let tab = MTab::new(&Atom::from("test2"));
	let guid_gen = GuidGen::new(2, 22222);
	let guid = guid_gen.gen(2);

	let tab2 = &tab;

	let start = now_nanos();
	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid, true);
	
	for n in 0..100 {
		let mut key = WriteBuffer::new();
		key.write_utf8(&n.to_string());
		let key = Arc::new(key.unwrap());
		let v = Vec::from("vvvvvvvvvvvvvvvvvvvv");
		assert_eq!(txn.borrow_mut().upsert(key.clone(), Arc::new(v)), Ok(()));
	}
	let mut key = WriteBuffer::new();
	key.write_utf8(&99.to_string());
	let key = Arc::new(key.unwrap());
	assert_eq!(txn.borrow_mut().get(key.clone()), Some(Arc::new(Vec::from("vvvvvvvvvvvvvvvvvvvv"))));
	let mut it=txn.iter(None, false, None, Arc::new(|_i:IterResult|{})).unwrap().unwrap();

	let mut key = WriteBuffer::new();
	key.write_utf8(&0.to_string());
	let key = Arc::new(key.unwrap());
	assert_eq!(it.next(Arc::new(|_i:NextResult<(Bin, Bin)>|{})), Some(Ok(Some((key.clone(), Arc::new(Vec::from("vvvvvvvvvvvvvvvvvvvv")))))));
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	let mut txn = txn.borrow_mut();
	match txn.commit1() {
		Ok(_) => (),
		_ => panic!("commit1 fail"),
	}

	let end = now_nanos();//获取结束时间
    println!("done!start : {:?},end :{:?},duration:{:?}",start,end,end-start);
}

#[test]
fn test_memery_db_p2() {
	println!("test p2!!!!!!!");
	//打开表
	let tab = MTab::new(&Atom::from("test3"));
	let guid_gen = GuidGen::new(3, 3333);

	let tab2 = &tab;

	// let start = now_nanos();
	
	for n in 0..10 {
		//创建事务
		let txn = MemeryTxn::new(tab2.clone(), &guid_gen.gen(3), true);
		let mut txn = txn.borrow_mut();
		let mut key = WriteBuffer::new();
		key.write_utf8(&n.to_string());
		let v = Vec::from("vvvvvvvvvvvvvvvvvvvv");

		assert_eq!(txn.upsert(Arc::new(key.unwrap()), Arc::new(v)), Ok(()));
		assert_eq!(txn.prepare1(), Ok(()));
		match txn.commit1() {
			Ok(_) => (),
			_ => panic!("commit1 fail"),
		}
	};
	
	// let end = now_nanos();//获取结束时间
    // println!("done!start : {:?},end :{:?},duration:{:?}",start,end,end-start);
}