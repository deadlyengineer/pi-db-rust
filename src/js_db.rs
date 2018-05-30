/**
 * 封装db的query和modify方法提供给js， 简化了js调用时其参数和返回值的复杂度
 */

use db::{TxCallback, TabKV, DBResult, SResult};
use mgr::{Tr, Mgr};
use pi_lib::atom::Atom;
use pi_lib::sinfo::StructInfo;
use std::sync::Arc;
use std::ops::Deref;
use memery_db::MemeryDB;

// 查询
pub fn query(tr: &Tr, arr: Vec<(String, String, Vec<u8>)>, lock_time: Option<usize>, readonly: bool, cb: Arc<Fn(SResult<Vec<(String, String, Vec<u8>, Vec<u8>)>>)> )->Option<SResult<Vec<(String, String, Vec<u8>, Vec<u8>)>>>{
	let mut v = Vec::new();
	for elem in arr.into_iter(){
		let tk = TabKV::new(Atom::from(elem.0), Atom::from(elem.1), Arc::new(Vec::from(elem.2)));
		v.push(tk);
	}
	let back = move |r: SResult<Vec<TabKV>>|{
		cb(parse_query(r));
	};
	let r = tr.query(v, lock_time, readonly, Arc::new(back));
	match r {
		Some(v) => {Some(parse_query(v))},
		None => {None},
	}
}

// 修改，插入、删除及更新
pub fn modify(tr: &Tr, arr: Vec<(String, String, Vec<u8>, Vec<u8>)>, lock_time: Option<usize>, readonly: bool, cb: TxCallback) -> DBResult {
	let mut v = Vec::new();
	for elem in arr.into_iter(){
		let mut tk = TabKV::new(Atom::from(elem.0), Atom::from(elem.1), Arc::new(Vec::from(elem.2)));
		tk.value = Some(Arc::new(elem.3));
		v.push(tk);
	}
	tr.modify(v, lock_time, readonly, cb)
}

// 注册内存数据库
pub fn register_memery_db(mgr: &Mgr, prefix: String, ware: MemeryDB) -> bool {
	mgr.register(Atom::from(prefix), Arc::new(ware))
}

pub fn create_sinfo(name: String, hash: u32) -> Arc<StructInfo>{
	Arc::new(StructInfo::new(Atom::from(name), hash))
}

// 取消注册数据库
// pub fn unregister_memery_db(mgr: &mut Mgr, prefix: &Atom) -> Option<Arc<MemeryDB>> {
// 	mgr.unregister_builder(prefix)
// }

fn parse_query(v: SResult<Vec<TabKV>>) -> SResult<Vec<(String, String, Vec<u8>, Vec<u8>)>>{
	match v {
		Ok(r) => {
			let mut r1 = Vec::new();
			for elem in r.into_iter(){
				r1.push( (String::from(elem.ware.deref().as_str()), String::from(elem.tab.deref().as_str()), elem.key.deref().clone(), elem.value.unwrap().deref().clone()) );
			}
			Ok(r1)
		},
		Err(v) => {
			Err(v)
		},
	}
}