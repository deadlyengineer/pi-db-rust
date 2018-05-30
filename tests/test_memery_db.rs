extern crate pi_lib;
extern crate pi_db;
extern crate fnv;

use pi_db::memery_db::{ArcMutexTab, MemeryTxn, MemeryKV, MemeryTab};

use pi_db::db::{Txn, TabTxn, TabKV, TxIterCallback, TxQueryCallback, SResult, MetaTxn, Tab, Ware, TxCallback, TxState, Cursor, DBResult};

use pi_lib::atom::{Atom};
use pi_lib::ordmap::{OrdMap, Entry};
use pi_lib::sbtree::{Tree, new};
use pi_lib::guid::{Guid, GuidGen};
use pi_lib::sinfo::StructInfo;
use pi_lib::time::now_nanos;

use std::sync::{Arc, Mutex};
use fnv::FnvHashMap;
use std::cell::RefCell;

#[test]
fn test_memery_db() {
	//打开表
	let tree:MemeryKV = None;
	let mut root= OrdMap::new(tree);
	let tab = MemeryTab {
		prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		root: root,
		tab: Atom::from("test"),
	};
	let tab: ArcMutexTab = Arc::new(Mutex::new(tab));
	let guidGen = GuidGen::new(1, 1111111);
	let guid = guidGen.gen(1);

	let tab2 = &tab;

	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid);
	assert_eq!(txn.borrow_mut().upsert(Arc::new(b"1".to_vec()), Arc::new(b"v1".to_vec())), Ok(()));
	assert_eq!(txn.borrow_mut().get(Arc::new(b"1".to_vec())), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.borrow_mut().get(Arc::new(b"1".to_vec())), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	assert_eq!(txn.borrow_mut().commit1(), Ok(()));

	//创建事务(添加key:2 并回滚)
	let txn = MemeryTxn::new(tab2.clone(), &guid);
	assert_eq!(txn.borrow_mut().upsert(Arc::new(b"2".to_vec()), Arc::new(b"v2".to_vec())), Ok(()));
	assert_eq!(txn.borrow_mut().get(Arc::new(b"1".to_vec())), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	assert_eq!(txn.borrow_mut().rollback1(), Ok(()));

	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid);
	assert_eq!(txn.borrow_mut().get(Arc::new(b"1".to_vec())), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.borrow_mut().get(Arc::new(b"2".to_vec())), None);
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	assert_eq!(txn.borrow_mut().commit1(), Ok(()));

	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid);
	assert_eq!(txn.borrow_mut().get(Arc::new(b"1".to_vec())), Some(Arc::new(b"v1".to_vec())));
	assert_eq!(txn.borrow_mut().get(Arc::new(b"2".to_vec())), None);
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	assert_eq!(txn.borrow_mut().commit1(), Ok(()));
}

#[test]
fn test_memery_db_p() {
	//打开表
	let tree:MemeryKV = None;
	let mut root= OrdMap::new(tree);
	let tab = MemeryTab {
		prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		root: root,
		tab: Atom::from("test2"),
	};
	let tab: ArcMutexTab = Arc::new(Mutex::new(tab));
	let guidGen = GuidGen::new(2, 22222);
	let guid = guidGen.gen(2);

	let tab2 = &tab;

	let start = now_nanos();
	//创建事务
	let txn = MemeryTxn::new(tab2.clone(), &guid);
	
	for n in 0..1000000 {
		let key = [n];
		let v = Vec::from("vvvvvvvvvvvvvvvvvvvv");
		assert_eq!(txn.borrow_mut().upsert(Arc::new(key.to_vec()), Arc::new(v)), Ok(()));
	}
	assert_eq!(txn.borrow_mut().get(Arc::new([999999].to_vec())), Some(Arc::new(Vec::from("vvvvvvvvvvvvvvvvvvvv"))));
	assert_eq!(txn.borrow_mut().prepare1(), Ok(()));
	assert_eq!(txn.borrow_mut().commit1(), Ok(()));

	let end = now_nanos();//获取结束时间
    println!("done!start : {:?},end :{:?},duration:{:?}",start,end,end-start);
}

#[test]
fn test_memery_db_p2() {
	println!("test p2!!!!!!!");
	//打开表
	let tree:MemeryKV = None;
	let mut root= OrdMap::new(tree);
	let tab = MemeryTab {
		prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		root: root,
		tab: Atom::from("test3"),
	};
	let tab: ArcMutexTab = Arc::new(Mutex::new(tab));
	let guidGen = GuidGen::new(3, 3333);
	let guid = guidGen.gen(3);

	let tab2 = &tab;

	// let start = now_nanos();
	
	for n in (0..10) {
		//创建事务
		let txn = MemeryTxn::new(tab2.clone(), &guidGen.gen(3));
		let mut txn = txn.borrow_mut();
		let key = [n];
		let v = Vec::from("vvvvvvvvvvvvvvvvvvvv");

		assert_eq!(txn.upsert(Arc::new(key.to_vec()), Arc::new(v)), Ok(()));
		assert_eq!(txn.prepare1(), Ok(()));
		assert_eq!(txn.commit1(), Ok(()));
	};
	
	// let end = now_nanos();//获取结束时间
    // println!("done!start : {:?},end :{:?},duration:{:?}",start,end,end-start);
}