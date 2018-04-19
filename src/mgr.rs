#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]

/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::mem;

use fnv::FnvHashMap;

use pi_lib::ordmap::ImOrdMap;
use pi_lib::asbtree::{new, TreeMap};
use pi_lib::sinfo::StructInfo;

use db::{UsizeResult, Cursor, TabBuilder, DBResult, TabKV, TxCallback, TxIterCallback, TxQueryCallback, TxState, Txn, TxnInfo, Tab};

pub type TxHandler = Box<FnMut(&mut ArcTx)>;

pub struct Mgr {
	// 构建器
	builders: TreeMap<Arc<String>, Arc<TabBuilder>>,
	//数据表
	tabs: TreeMap<Arc<String>, Arc<Tab>>,
	// 定时轮
	// 管理用的弱引用事务
	map: TreeMap<u128, Weak<Tx>>,
}

impl Mgr {
	// 注册构建器
	fn register_builder(&mut self, class: Arc<String>, builder: Box<TabBuilder>) -> bool {
		return false;
	}
	// 注册数据表
	fn register_tab(&mut self, name: Arc<String>, db: Box<Tab>) -> bool {
		return false;
	}
	// 取消注册数据库
	fn unregister_builder(&mut self, class: Arc<String>) {}


	// 读事务，无限尝试直到超时，默认10秒
	fn read(&mut self, tx: TxHandler, timeout: usize, cb: TxCallback) {}
	// 写事务，无限尝试直到超时，默认10秒
	fn write(&mut self, tx: TxHandler, timeout: usize, cb: TxCallback) {}
	fn transaction(&mut self, writable: bool, timeout: usize) -> ArcTx {
		Arc::new(Mutex::new(Tx {
			id: 0,
			writable: writable,
			timeout: timeout,
			builders: self.builders.clone(),
			tabs: self.tabs.clone(),
			old_tabs: self.tabs.clone(),
			start_time: 0,
			state: TxState::Ok,
			timer_ref: 0,
			txns: HashMap::new(),
		}))
	}
}

pub struct Tx {
	id: u128,
	writable: bool,
	timeout: usize, // 子事务的预提交的超时时间
	builders: TreeMap<Arc<String>, Arc<TabBuilder>>,
	tabs: TreeMap<Arc<String>, Arc<Tab>>,
	old_tabs: TreeMap<Arc<String>, Arc<Tab>>,
	start_time: u64, // us
	state: TxState,
	timer_ref: usize,
	txns: HashMap<Arc<String>, Option<Box<Txn>>>,
}

impl Tx {
	// 预提交事务
	fn prepare(&mut self, atx: ArcTx, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Preparing;
		let len = self.txns.len();
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let f = move |r: DBResult<usize> | match r {
			Ok(_) => {
				if c.fetch_sub(1, Ordering::SeqCst) == 1 {
					if atx.set_state(TxState::Preparing, TxState::PreparOk) {
						(*cb)(Ok(len))
					}
				}
			}
			_ => {
				if atx.set_state(TxState::Preparing, TxState::PreparFail) {
					(*cb)(r)
				}
			}
		};
		let bf = Arc::new(f);
		for (_, val) in self.txns.iter_mut() {
			match val {
				&mut Some(ref mut v) => match v.prepare(bf.clone()) {
					Some(r) => match r {
						Ok(_) => {
							if count.fetch_sub(1, Ordering::SeqCst) == 1 {
								self.state = TxState::PreparOk;
								return Some(Ok(len));
							}
						}
						_ => {
							self.state = TxState::PreparFail;
							return Some(r);
						}
					},
					_ => (),
				},
				_ => (),
			}
		}
		return None;
	}
	// 提交同步事务
	fn commit(&mut self) -> UsizeResult {
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
		return None;
	}
	// 修改，插入、删除及更新
	fn klock(&mut self, atx: ArcTx, arr: Vec<TabKV>, lock_time: usize, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		let len = arr.len();
		let count = Arc::new(AtomicUsize::new(self.txns.len()));
		let c = count.clone();
		let f = move |r: DBResult<usize> | match r {
			Ok(rc) => {
				if c.fetch_sub(rc, Ordering::SeqCst) == 1 {
					if atx.set_state(TxState::Doing, TxState::Ok) {
						(*cb)(Ok(len))
					}
				}
			}
			_ => {
				if atx.set_state(TxState::Doing, TxState::Fail) {
					(*cb)(r)
				}
			}
		};
		let bf = Arc::new(f);
		let map = tab_map(arr);
		let tabs = &self.tabs;
		let id = self.id;
		let writable = self.writable;
		let timeout = self.timeout;
		for (key, val) in map.into_iter() {
			match self.txns.entry(key.clone()).or_insert_with(move || {
				// 创建新的子事务
				match tabs.get(&key) {
					Some(ref tab) => {
						Some(tab.transaction(id, writable, timeout))
					},
					_ => None
				}
			}) {
				&mut Some(ref mut txn) => {
					// 调用每个子事务的修改
					match txn.klock(val, lock_time, bf.clone()) {
						Some(r) => match r {
							Ok(rc) => {
								if count.fetch_sub(rc, Ordering::SeqCst) == 1 {
									self.state = TxState::Ok;
									return Some(Ok(len));
								}
							}
							_ => {
								self.state = TxState::Fail;
								return Some(r);
							}
						},
						_ => ()
					}
				},
				_ => {
					self.state = TxState::Fail;
					return Some(Err(String::from("TabNotFound")))
				}
			}
		}
		return None;
	}
	// 查询
	fn query(
		&mut self,
		atx: ArcTx,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		self.state = TxState::Doing;
		let len = arr.len();
		// 创建指定长度的结果集，接收结果
		let mut vec = Vec::with_capacity(len);
		vec.resize(len, Default::default());
		let rvec = Arc::new(Mutex::new((len, vec)));
		let c = rvec.clone();
		let f = move |r: DBResult<Vec<TabKV>>| match r {
			Ok(vec) => {
				match merge_result(&c, vec) {
					None => (),
					Some(rr) => {
						if atx.set_state(TxState::Doing, TxState::Ok) {
							(*cb)(rr)
						}
					}
				}
			}
			_ => {
				if atx.set_state(TxState::Doing, TxState::Fail) {
					(*cb)(r)
				}
			}
		};
		let bf = Arc::new(f);
		let map = tab_map(arr);
		let tabs = &self.tabs;
		let id = self.id;
		let writable = self.writable;
		let timeout = self.timeout;
		for (key, val) in map.into_iter() {
			match self.txns.entry(key.clone()).or_insert_with(move || {
				// 创建新的子事务
				match tabs.get(&key) {
					Some(ref tab) => {
						Some(tab.transaction(id, writable, timeout))
					},
					_ => None
				}
			}) {
				&mut Some(ref mut txn) => {
					// 调用每个子事务查询
					match txn.query(val, lock_time, bf.clone()) {
						Some(r) => match r {
							Ok(vec) => {
								match merge_result(&rvec, vec) {
									None => (),
									rr => {
										self.state = TxState::Ok;
										return rr
									}
								}
							}
							_ => {
								self.state = TxState::Fail;
								return Some(r)
							}
						},
						_ => ()
					}
				},
				_ => {
					self.state = TxState::Fail;
					return Some(Err(String::from("TabNotFound")))
				}
			}
		}
		return None;
	}
	// 修改，插入、删除及更新
	fn modify(&mut self, atx: ArcTx, arr: Vec<TabKV>, lock_time: Option<usize>, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		let len = arr.len();
		let count = Arc::new(AtomicUsize::new(self.txns.len()));
		let c = count.clone();
		let f = move |r: DBResult<usize> | match r {
			Ok(rc) => {
				if c.fetch_sub(rc, Ordering::SeqCst) == 1 {
					if atx.set_state(TxState::Doing, TxState::Ok) {
						(*cb)(Ok(len))
					}
				}
			}
			_ => {
				if atx.set_state(TxState::Doing, TxState::Fail) {
					(*cb)(r)
				}
			}
		};
		let bf = Arc::new(f);
		let map = tab_map(arr);
		let tabs = &self.tabs;
		let id = self.id;
		let writable = self.writable;
		let timeout = self.timeout;
		for (key, val) in map.into_iter() {
			match self.txns.entry(key.clone()).or_insert_with(move || {
				// 创建新的子事务
				match tabs.get(&key) {
					Some(ref tab) => {
						Some(tab.transaction(id, writable, timeout))
					},
					_ => None
				}
			}) {
				&mut Some(ref mut txn) => {
					// 调用每个子事务的修改
					match txn.modify(val, lock_time, bf.clone()) {
						Some(r) => match r {
							Ok(rc) => {
								if count.fetch_sub(rc, Ordering::SeqCst) == 1 {
									self.state = TxState::Ok;
									return Some(Ok(len));
								}
							}
							_ => {
								self.state = TxState::Fail;
								return Some(r);
							}
						},
						_ => ()
					}
				},
				_ => {
					self.state = TxState::Fail;
					return Some(Err(String::from("TabNotFound")))
				}
			}
		}
		return None;
	}
	// 修改，插入、删除及更新
	fn alter(&mut self, atx: ArcTx, tab: Arc<String>, meta: Option<StructInfo>, cb: TxCallback) -> UsizeResult {

		return None;
	}
}

pub type ArcTx = Arc<Mutex<Tx>>;
trait ArcTxFn {
	fn set_state(&self, old: TxState, new: TxState) -> bool;
}

impl ArcTxFn for ArcTx {
	fn set_state(&self, old: TxState, new: TxState) -> bool {
		let mut t = self.lock().unwrap();
		if t.state.clone() as usize == old as usize {
			t.state = new;
			return true;
		}
		return false;
	}
}

impl TxnInfo for ArcTx {
	// 判断事务是否可写
	fn is_writable(&self) -> bool {
		self.lock().unwrap().writable
	}
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize {
		self.lock().unwrap().timeout
	}
}
impl Txn for ArcTx {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		self.lock().unwrap().state.clone()
	}
	// 预提交一个事务
	fn prepare(&mut self, cb: TxCallback) -> UsizeResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.prepare(self.clone(), cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
	// 提交一个事务, TODO 单表事务容许预提交和提交合并？
	fn commit(&mut self, cb: TxCallback) -> UsizeResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.prepare(self.clone(), cb),
			TxState::PreparOk => return t.prepare(self.clone(), cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
	// 回滚一个事务
	fn rollback(&mut self, cb: TxCallback) -> UsizeResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.prepare(self.clone(), cb),
			TxState::PreparOk => return t.prepare(self.clone(), cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
	// 锁
	fn klock(&mut self, arr: Vec<TabKV>, lock_time: usize, cb: TxCallback) -> UsizeResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.klock(self.clone(), arr, lock_time, cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
	// 查询
	fn query(
		&mut self,
		mut arr: Vec<TabKV>,
		lock_time: Option<usize>,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.query(self.clone(), arr, lock_time, cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
	// 修改，插入、删除及更新
	fn modify(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, cb: TxCallback) -> UsizeResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.modify(self.clone(), arr, lock_time, cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
	// 迭代
	fn iter(
		&mut self,
		tab_key: TabKV,
		descending: bool,
		key_only: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		return None;
	}
	// 索引迭代
	fn index(
		&mut self,
		tab_key: TabKV,
		descending: bool,
		key_only: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		return None;
	}
	// 新增 修改 删除 表
	fn alter(&mut self, tab: Arc<String>, meta: Option<StructInfo>, cb: TxCallback) -> UsizeResult {
		let mut t = self.lock().unwrap();
		match t.state {
			TxState::Ok => return t.alter(self.clone(), tab, meta, cb),
			_ => return Some(Err(String::from("InvalidState"))),
		}
	}
}

// 创建每表的键参数表，不负责键的去重
fn tab_map(mut arr: Vec<TabKV>) -> FnvHashMap<Arc<String>, Vec<TabKV>> {
	let mut len = arr.len();
	let mut map = FnvHashMap::with_capacity_and_hasher(len, Default::default());
	while len > 0 {
		let mut tk = arr.pop().unwrap();
		tk.index = len;
		len -= 1;
		let r = map.entry(tk.tab.clone()).or_insert(Vec::new());
		r.push(tk);
	}
	return map;
}

// 合并结果集
fn merge_result(rvec: &Arc<Mutex<(usize, Vec<TabKV>)>>, vec: Vec<TabKV>) -> Option<DBResult<Vec<TabKV>>> {
	let mut t = rvec.lock().unwrap();
	t.0 -= vec.len();
	for r in vec.into_iter() {
		let i = (&r).index - 1;
		t.1[i] = r;
	}
	if t.0 == 0 {
		// 将结果集向量转移出来，没有拷贝
		return Some(Ok(mem::replace(&mut t.1, Vec::new())));
	}
	return None
}
