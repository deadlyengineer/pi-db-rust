#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]

/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::{Arc, Weak, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use pi_lib::ordmap::ImOrdMap;
use pi_lib::asbtree::{TreeMap, new};


use db::{TxnInfo, Txn, DB, DBBuilder, DBResult, CBResult, TxState, TabKV, Cursor, TxCallback, TxQueryCallback, TxIterCallback};


pub type TxHandler = Box<FnMut(&mut ArcTx)>;


pub struct Mgr {
	// 构建器
	builders: TreeMap<String, Box<DBBuilder>>,
	//数据表
	tabs: TreeMap<String, Box<DB>>,
	// 定时轮
	// 管理用的弱引用事务
	map: TreeMap<u128, Weak<Tx>>,
}


impl Mgr {
	// 注册构建器
	fn register_builder(&mut self, clazz: String, builder: Box<DBBuilder>) -> bool {
		return false;
	}
	// 注册数据表
	fn register_tab(&mut self, name: String, db: Box<DB>) -> bool {
		return false;
	}
	// 取消注册数据库
	fn unregister(&mut self, prefix: String) {

	}
	// 读事务，无限尝试直到超时，默认10秒
	fn read(&mut self, tx: TxHandler, timeout:usize, cb: TxCallback) {

	}
	// 写事务，无限尝试直到超时，默认10秒
	fn write(&mut self, tx: TxHandler, timeout:usize, cb: TxCallback) {

	}
	fn transaction(&mut self, writable: bool, timeout:usize) -> ArcTx {
		Arc::new(Mutex::new(Tx{
			writable: writable,
			timeout: timeout,
			builders: self.builders.clone(),
			tabs: self.tabs.clone(),
			start_time: 0,
			state: TxState::Ok,
			timer_ref: 0,
			txns: HashMap::new(),
		}))
	}

}

pub struct Tx {
	writable: bool,
	timeout: usize,
	builders: TreeMap<String, Box<DBBuilder>>,
	tabs: TreeMap<String, Box<DB>>,
	start_time: u64, // us
	state: TxState,
	timer_ref: usize,
	txns: HashMap<String, Box<Txn>>,
	// result: CBResult,
}

impl Tx {

	// 预提交事务
	fn prepare(&mut self, atx: ArcTx, cb: TxCallback) -> CBResult {
		self.state = TxState::Preparing;
		let count = Arc::new(AtomicUsize::new(self.txns.len()));
		let c = count.clone();
		let f = move |r: DBResult<()>| {
			match r {
				Ok(_) => {
					if c.fetch_sub(1, Ordering::SeqCst) == 1 {
						let mut t = atx.lock().unwrap();
						match t.state {
							TxState::Preparing => {
								t.state = TxState::PreparOk;
								(*cb)(r)
								//let ptr = Arc::into_raw(cb.clone());
								//((*ptr) as FnMut(Result<(), String>))(r);
								//unsafe { Arc::from_raw(ptr) };
							},
							_ => ()
						}
					}
				},
				_ => {
					let mut t = atx.lock().unwrap();
					match t.state {
						TxState::Preparing => {
							t.state = TxState::PreparFail;
							(*cb)(r)
						},
						_ => ()
					}
					
				}
			}
		};
		let bf = Arc::new(f);
		for (_, val) in self.txns.iter_mut() {
			match (*val).prepare(bf.clone()) {
				Some(r) => match r {
					Ok(_) => {
						if count.fetch_sub(1, Ordering::SeqCst) == 1 {
							self.state = TxState::PreparOk;
							return Some(r)
						}
					},
					_ => {
						self.state = TxState::PreparFail;
						return Some(r)
					}
				},
				_ => (),
			}
		}
		return None
	}
	// 提交同步事务
	fn commit(&mut self) -> CBResult {
		// for (_, val) in self.txns.iter_mut() {
		// 	match (*val).commit(bf.clone()) {
		// 		Some(r) => {
		// 			if count.fetch_sub(1, Ordering::SeqCst) == 1{
		// 				return Some(r)
		// 			}
		// 		},
		// 		_ => (),
		// 	}
		// }
		return None
	}
	// 查询
	fn query(&mut self, atx: ArcTx, arr:Vec<TabKV>, lock_time:Option<usize>, cb: TxQueryCallback) -> Option<DBResult<Vec<TabKV>>> {
		self.state = TxState::Preparing;
		let count = Arc::new(AtomicUsize::new(arr.len()));
		let c = count.clone();
		let f = move |r: DBResult<Vec<TabKV>>| {
			match r {
				Ok(_) => {
					if c.fetch_sub(1, Ordering::SeqCst) == 1 {
						let mut t = atx.lock().unwrap();
						match t.state {
							TxState::Preparing => {
								t.state = TxState::PreparOk;
								let ptr = Arc::into_raw(cb.clone());
								// ((*ptr) as FnMut(Result<(), String>))(r);
								unsafe { Arc::from_raw(ptr) };
							},
							_ => ()
						}
					}
				},
				_ => {
					let mut t = atx.lock().unwrap();
					match t.state {
						TxState::Preparing => {
							t.state = TxState::PreparFail;
							()// (*cb)(r),
						},
						_ => ()
					}
					
				}
			}
		};
		// let bf = Arc::new(f);
		// for tk in arr.iter_mut() {
		// 	tk.index = 1;
		// 	match (*val).prepare(bf.clone()) {
		// 		Some(r) => match r {
		// 			Ok(_) => {
		// 				if count.fetch_sub(1, Ordering::SeqCst) == 1 {
		// 					self.state = TxState::PreparOk;
		// 					return Some(r)
		// 				}
		// 			},
		// 			_ => {
		// 				self.state = TxState::PreparFail;
		// 				return Some(r)
		// 			}
		// 		},
		// 		_ => (),
		// 	}
		// }
		return None
	}

}

pub type ArcTx = Arc<Mutex<Tx>>;

impl TxnInfo for ArcTx {

	// 判断事务是否可写
	fn is_writable(&self) -> bool {
		self.lock().unwrap().writable
	}
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize {
		(*self).lock().unwrap().timeout
	}
}
impl Txn for ArcTx {

	// 获得事务的状态
	fn get_state(&self) -> TxState {
		(*self).lock().unwrap().state.clone()
	}
	// 预提交一个事务
	fn prepare(&mut self, cb: TxCallback) -> CBResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.prepare(self.clone(), cb),
			_ => return Some(Err(String::from("InvalidState")))
		}
	}
	// 提交一个事务, TODO 单表事务容许预提交和提交合并？
	fn commit(&mut self, cb: TxCallback) -> CBResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => {
				return t.prepare(self.clone(), cb)
			},
			TxState::PreparOk => return t.prepare(self.clone(), cb),
			_ => return Some(Err(String::from("InvalidState")))
		}
	}
	// 回滚一个事务
	fn rollback(&mut self, cb: TxCallback) -> CBResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.prepare(self.clone(), cb),
			TxState::PreparOk => return t.prepare(self.clone(), cb),
			_ => return Some(Err(String::from("InvalidState")))
		}
	}
	// 锁
	fn lock1(&mut self, arr:Vec<TabKV>, lock_time:usize, cb: TxCallback) -> CBResult {
		return None
	}
	// 查询
	fn query(&mut self, arr:Vec<TabKV>, lock_time:Option<usize>, cb: TxQueryCallback) -> Option<DBResult<Vec<TabKV>>> {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.query(self.clone(), arr, lock_time, cb),
			_ => return Some(Err(String::from("InvalidState")))
		}
	}
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKV>, lock_time:Option<usize>, cb: TxCallback) -> CBResult {
		return None
	}
	// 删除
	fn delete(&mut self, arr:Vec<TabKV>, lock_time:Option<usize>, cb: TxCallback) -> CBResult {
		return None
	}
	// 迭代
	fn iter(&mut self, tab_key:TabKV, descending: bool, key_only:bool, filter:String, cb: TxIterCallback) -> Option<DBResult<Box<Cursor>>> {
		return None
	}
	// 索引迭代
	fn index(&mut self, tab_key:TabKV, descending: bool, key_only:bool, filter:String, cb: TxIterCallback) -> Option<DBResult<Box<Cursor>>> {
		return None
	}
	// 新增 修改 删除 表
	fn alter(&mut self, tab: String, clazz: String, metaJson:String, cb: TxCallback) -> CBResult {
		return None
	}
}

// // 创建每表的键参数表
// fn tab_map(arr:Vec<TabKV>) -> HashMap<String, Vec<TabKV>> {
// 	let s = RandomState::new(); // 应该为静态常量
// 	let mut map = HashMap::with_capacity_and_hasher(arr.len(), s);
// 	let mut i= 1;
// 	for tk in arr.iter_mut() {
// 		tk.index = i;
// 		i = i + 1;
// 		match map.get_mut(&tk.tab) {
// 			Some(ref r) => r.
// 			_ => map.insert(tk.tab.clone(), vec![tk])
// 		}
		
// 	}
// 	// map.insert(1, 2);
// 	return map
// }