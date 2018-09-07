extern crate pi_lib;
extern crate pi_base;
extern crate pi_db;

use std::thread;
use std::sync::Arc;
use pi_lib::atom::Atom;
use pi_lib::guid::GuidGen;
use pi_lib::sinfo::EnumType;
use pi_lib::bon::{Encode, ReadBuffer, WriteBuffer};
use pi_base::task::TaskType;
use pi_base::pi_base_impl::{STORE_TASK_POOL};
use pi_base::worker_pool::WorkerPool;
use pi_db::mgr::Mgr;
use pi_db::memery_db::DB;
use pi_db::db::{TabMeta, TabKV};
use pi_db::util::{dump, restore};

#[test]
fn test_dump_restore() {
	let worker_pool = Box::new(WorkerPool::new(10, 1024 * 1024, 10000));
    worker_pool.run(STORE_TASK_POOL.clone());

	//创建库表
	let ware_name = Atom::from("db");
	let tab_name = Atom::from("tmp");
	let file = String::from("./") + ware_name.as_str() + "/" + tab_name.as_str();
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let db = DB::new();
	mgr.register(ware_name.clone(), Arc::new(db));

	//注册表元信息
	let tr = mgr.transaction(true);
	tr.alter(&ware_name, &tab_name, Some(Arc::new(TabMeta::new(EnumType::Str, EnumType::Str))), Arc::new(|r|{}));

	//插入数据
	let mut kvs = Vec::new();
	let mut v1 = WriteBuffer::new();
	v1.write_utf8("1");
	let v1 = Arc::new(v1.unwrap());
	let mut item1 = TabKV::new(ware_name.clone(), tab_name.clone(), v1.clone());
	item1.value = Some(v1.clone());
	kvs.push(item1);
	tr.modify(kvs, None, false, Arc::new(|r|{}));
	tr.prepare(Arc::new(|r| {}));
	tr.commit(Arc::new(|r| {}));

	//dump
	dump(&mgr, ware_name.clone(), tab_name.clone(), file.clone(), Arc::new(|r|{
		println!("dump-----------------{:?}", r);
		assert!(r.is_ok());
	}));
	thread::sleep_ms(3000);

	//restore
	let callback = Box::new(move |result: Result<(), String>| {
		println!("!!!!!!result: {:?}", result);
		assert!(result.is_ok());
	});
	restore(&mgr, ware_name.clone(), tab_name.clone(), Atom::from(file), callback);
	thread::sleep_ms(10000);
}