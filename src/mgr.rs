#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]

/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::Arc;
use std::sync::Weak;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;

use pi_lib::ordmap::ImOrdMap;
use pi_lib::asbtree::{TreeMap, new};


use db::{Txn, AsyncDB, SyncDB, DBResult, DBResultDefault, TxState, TabKey, TabKeyValue, AsyncTxn, SyncTxn, TxCallback, TxQueryCallback, TxIterCallback};


pub type TxHandler = Box<FnMut(&mut ArcTx)>;


pub struct Mgr {
	async: TreeMap<String, Box<AsyncDB<AsyncTxn>>>,
	sync: TreeMap<String, Box<SyncDB<SyncTxn>>>,
	// 定时轮
	// 管理用的弱引用事务
	map: TreeMap<u128, Weak<RwLock<Tx>>>,
}


impl Mgr {
	// 注册数据库
	fn register_async(&mut self, name: String, db: Box<AsyncDB<AsyncTxn>>) -> bool {
		return false;
	}
	fn register_sync(&mut self, name: String, db: Box<AsyncDB<AsyncTxn>>) -> bool {
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
		Arc::new(Tx{
			writable: writable,
			timeout: timeout,
			async_db: self.async.clone(),
			sync_db: self.sync.clone(),
			start_time: 0,
			state: RwLock::new(TxState::Ok),
			timer_ref: 0,
			async: HashMap::new(),
			sync: HashMap::new(),
			lock: RwLock::new(1),
		}))
	}

}

pub struct Tx {
	writable: bool,
	timeout: usize,
	async_db: TreeMap<String, Box<AsyncDB<AsyncTxn>>>,
	sync_db: TreeMap<String, Box<SyncDB<SyncTxn>>>,
	start_time: u64, // us
	state: RwLock<TxState>,
	timer_ref: usize,
	async: HashMap<String, Box<AsyncTxn>>,
	sync: HashMap<String, Box<SyncTxn>>,
	result: Option<DBResultDefault>,
	lock: RwLock<usize>,
}

impl Tx {
	// 预提交异步事务
	fn prepare(&mut self, tx: ArcTx, mut cb: TxCallback) => Option<DBResultDefault> {
		self.state = TxState::Prepared;
		let count = Arc::new(AtomicUsize::new(self.async.len()));
		let f = move |r: DBResultDefault| {
			match r {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						
						cb(Ok(()))
					}
				},
				err => cb(err),
			}
		};
		let mut bf = Arc::new(f);
		for (_, val) in self.async.iter_mut() {
			match (*val).prepare(bf.clone()) {
				Some(r) -> {
					if count.fetch_sub(1, Ordering::SeqCst) == 1
						return Some(r)
				},
				_ -> (),
			}
		}
		return None
	}
	// 预提交同步事务
	fn prepare_sync(&mut self) -> DBResultDefault {
		for (_, val) in self.sync.iter_mut() {
			match (*val).prepare() {
				Ok(_) => continue,
				err => return err,
			}
		}
		return Ok(())
	}
	// 提交同步事务
	fn commit_sync(&mut self) -> DBResultDefault {
		let mut r = Ok(());
		for (_, val) in self.sync.iter_mut() {
			match (*val).commit() {
				Ok(_) => continue,
				err => r = err,
			}
		}
		return r
	}
	// 回滚事务
	fn rollback_sync(&mut self) -> DBResultDefault {
		let mut r = Ok(());
		for (_, val) in self.sync.iter_mut() {
			match (*val).rollback() {
				Ok(_) => continue,
				err => r = err,
			}
		}
		return r
	}
}

pub type ArcTx = Arc<Tx>;

impl Txn for ArcTx {

	// 判断事务是否可写
	fn is_writable(&self) -> bool {
		self.writable
	}
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize {
		self.timeout
	}
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		self.state.read().unwrap().clone()
	}
	// 预提交一个事务
	fn prepare(&mut self, mut cb: TxCallback) {
		let mut r = Ok(());
		{
			let mut t = self.state.write().unwrap();
			match t {
				TxState::Ok => *t = TxState::Preparing,
				_ => r = Err(String::from("InvalidState")),
			}
		}
		if is_err(r)
			(*cb)(r)
		match Tx.prepare_sync(self) {
			Ok(_) => r = t.prepare_async(self.clone(), cb),
			err => (*cb)(err),
		},
	}
	// 提交一个事务
	fn commit(&mut self, mut cb: TxCallback) {
		let mut t = self.write().unwrap();
		match t.state {
		   TxState::Prepared => match t.prepare_sync() {
			   Ok(_) => t.prepare_async(cb),
			   err => cb(err),
		   },
		   _ =>
				return (*cb)(Err(String::from("InvalidState")))
		}
	}
	// 回滚一个事务
	fn rollback(&mut self, mut cb: TxCallback) {
		match self.read().unwrap().state {
		   TxState::Ok => (),
		   TxState::Prepared => (),
		   _ =>
				return (*cb)(Err(String::from("InvalidState")))
		}
	}
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, mut cb: TxCallback) {
		
	}
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, mut cb: TxQueryCallback) {
		
	}
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, mut cb: TxCallback) {
		
	}
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, mut cb: TxCallback) {
		
	}
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, mut cb: TxIterCallback) {

	}
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, mut cb: TxIterCallback) {

	}
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, mut cb: TxCallback) {

	}


}