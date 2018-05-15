/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::mem;

use fnv::FnvHashMap;

use pi_lib::ordmap::OrdMap;
use pi_lib::asbtree::{Tree, new};
use pi_lib::atom::Atom;
use pi_lib::sinfo::StructInfo;
use pi_lib::guid::{Guid, GuidGen};

use tabs::TabLog;
use db::{UsizeResult, Cursor, DBResult, TabKV, TxCallback, TxIterCallback, TxQueryCallback, TxState, MetaTxn, TabTxn, TabBuilder};

#[cfg(test)]
use memery_db;
#[cfg(test)]
use pi_lib::bon::{BonBuffer, Encode, Decode};
#[cfg(test)]
use std::collections::HashMap;

pub type TxHandler = Box<FnMut(&mut Tr)>;

// 表、事务管理器
#[derive(Clone)]
pub struct Mgr(Arc<(Mutex<Manager>, Mutex<NameMap>, GuidGen)>);

impl Mgr {
	// 注册管理器
	pub fn new(gen: GuidGen) -> Self {
		Mgr(Arc::new((Mutex::new(Manager::new()), Mutex::new(NameMap::new()), gen)))
	}
	// 注册构建器
	pub fn register(&self, prefix: Atom, builder: Arc<TabBuilder>) -> bool {
		(self.0).1.lock().unwrap().register(prefix, builder)
	}
	// 取消注册数据库
	pub fn unregister(&mut self, prefix: &Atom) -> Option<Arc<TabBuilder>> {
		(self.0).1.lock().unwrap().unregister(prefix)
	}
	// 表的元信息
	pub fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		match self.find(tab_name) {
			Some((_, builder)) => builder.tab_info(tab_name),
			_ => None
		}
	}
	// 创建事务
	pub fn transaction(&self, writable: bool, timeout: usize) -> Tr {
		let id = (self.0).2.gen(0);
		(self.0).0.lock().unwrap().transaction(self.clone(), writable, timeout, id)
	}
	// 寻找和指定表名能前缀匹配的表构建器
	fn find(&self, tab_name: &Atom) -> Option<(Atom, Arc<TabBuilder>)> {
		let tree = {
			(self.0).1.lock().unwrap().0.clone()
		};
		// TODO 前缀树查找
		match tree.get(tab_name) {
			Some(builder) => return Some((tab_name.clone(), builder.clone())),
			_ => None
		}
	}
}

#[derive(Clone)]
pub struct Tr(Arc<Mutex<Tx>>);

impl Tr {
	// 判断事务是否可写
	pub fn is_writable(&self) -> bool {
		self.0.lock().unwrap().writable
	}
	// 获得事务的超时时间
	pub fn get_timeout(&self) -> usize {
		self.0.lock().unwrap().timeout
	}
	// 获得事务的状态
	pub fn get_state(&self) -> TxState {
		self.0.lock().unwrap().state.clone()
	}
	// 预提交一个事务
	pub fn prepare(&self, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.prepare(self, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 提交一个事务
	pub fn commit(&self, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::PreparOk => t.commit(self, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 回滚一个事务
	pub fn rollback(&self, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Committing|TxState::Commited|TxState::CommitFail|TxState::Rollbacking|TxState::Rollbacked|TxState::RollbackFail =>
				return Some(Err(String::from("InvalidState"))),
			_ => t.rollback(self, cb)
		}
	}
	// 锁
	pub fn key_lock(&self, arr: Vec<TabKV>, lock_time: usize, read_lock: bool, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.key_lock(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 查询
	pub fn query(
		&self,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		read_lock: bool,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.query(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 修改，插入、删除及更新
	pub fn modify(&self, arr: Vec<TabKV>, lock_time: Option<usize>, read_lock: bool, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.modify(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 范围查询
	pub fn range(
		&self,
		_tab: &Atom,
		_min_key:Vec<u8>,
		_max_key:Vec<u8>,
		_key_only: bool,
		_cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		None
	}
	// 迭代
	pub fn iter(
		&self,
		_tab: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_key_only: bool,
		_filter: String,
		_cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 索引迭代
	pub fn index(
		&self,
		_tab: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_filter: String,
		_cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 表的元信息
	pub fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		match self.0.lock().unwrap().find(tab_name) {
			Some((_, _, builder)) => builder.tab_info(tab_name),
			_ => None
		}
	}
	// 表的大小
	pub fn tab_size(&self, tab: &Atom, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.tab_size(self, tab, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 创建、修改或删除表
	pub fn alter(&self, tab: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.alter(self, tab, meta, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 表改名
	pub fn rename(&self, tab: &Atom, new_name: Atom, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.rename(self, tab, new_name, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}

	// 比较并设置状态
	fn cs_state(&self, old: TxState, new: TxState) -> bool {
		let mut t = self.0.lock().unwrap();
		if t.state.clone() as usize == old as usize {
			t.state = new;
			return true;
		}
		return false;
	}
}

//================================ 内部结构和方法
// 表、事务管理器
struct Manager {
	// 已有的事务数量
	tr_acount: usize,
	// 结束的事务数量
	tr_ends: usize,
	// 定时轮
	// 管理用的弱引用事务
	weak_map: FnvHashMap<Guid, Weak<Mutex<Tx>>>,
}
impl Manager {
	// 注册管理器
	fn new() -> Self {
		Manager {
			tr_acount: 0,
			tr_ends: 0,
			weak_map: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}
	// 创建事务
	fn transaction(&mut self, mgr: Mgr, writable: bool, timeout: usize, id: Guid) -> Tr {
		let tr = Tr(Arc::new(Mutex::new(Tx {
			mgr: mgr,
			writable: writable,
			timeout: timeout,
			id: id.clone(),
			//name_map: (mgr.0).1.lock().unwrap().clone(),
			tab_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			state: TxState::Ok,
			timer_ref: 0,
			tab_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			meta_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		})));
		// TODO 遍历mgr.name_map, 将每个TabBuilder的快照TabLog记录下来
		self.tr_acount += 1;
		self.weak_map.insert(id, Arc::downgrade(&(tr.0)));
		tr
	}

}

// 表名对应构建器的表
// TODO 改成cow的前缀树
#[derive(Clone)]
struct NameMap(OrdMap<Tree<Atom, Arc<TabBuilder>>>);

impl NameMap {
	fn new() -> Self {
		NameMap(OrdMap::new(new()))
	}
	// 注册构建器
	fn register(&mut self, prefix: Atom, builder: Arc<TabBuilder>) -> bool {
		self.0.insert(prefix, builder)
		// TODO 更新前缀树
	}
	// 取消注册数据库
	fn unregister(&mut self, prefix: &Atom) -> Option<Arc<TabBuilder>> {
		match self.0.delete(prefix, true) {
			Some(r) => r,
			_ => None,
		}
		// TODO 更新前缀树
	}
	// 寻找和指定表名能前缀匹配的表构建器
	fn find(&self, tab_name: &Atom) -> Option<(Atom, Arc<TabBuilder>)> {
		// 遍历寻找和指定表名能前缀匹配的表构建器, TODO 用前缀树匹配的字符串
		match self.0.get(tab_name) {
			Some(b) => Some((tab_name.clone(), b.clone())),
			_ => None
		}
	}
}

// 子事务
struct Tx {
	mgr: Mgr, // TODO 下面6个可以放到锁的外部，减少锁
	writable: bool,
	timeout: usize, // 子事务的预提交的超时时间
	id: Guid,
	tab_logs: FnvHashMap<Atom, (TabLog, Arc<TabBuilder>)>, // 前缀树，包括了所有表的快照
	state: TxState,
	timer_ref: usize,
	tab_txns: FnvHashMap<Atom, Arc<TabTxn>>, //表事务表
	meta_txns: FnvHashMap<Atom, Arc<MetaTxn>>, //元信息事务表
}

impl Tx {
	// 预提交事务
	fn prepare(&mut self, tr: &Tr, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Preparing;
		// 先检查mgr上的meta alter的预提交
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for prefix in self.meta_txns.keys() {
				let (log, builder) = self.tab_logs.get_mut(prefix).unwrap();
				builder.prepare(&self.id, log);
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r: DBResult<usize> | match r {
			Ok(_) => if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				if tr1.cs_state(TxState::Preparing, TxState::PreparOk) {
					(*cb)(Ok(len))
				}
			}
			_ => if tr1.cs_state(TxState::Preparing, TxState::PreparFail) {
				(*cb)(r)
			}
		});

		//处理每个表的预提交
		for val in self.tab_txns.values_mut() {
			match val.prepare(bf.clone()) {
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
				}
				_ => ()
			}
		}
		//处理tab alter的预提交
		for val in self.meta_txns.values_mut() {
			match val.prepare(bf.clone()) {
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
				}
				_ => ()
			}
		}
		None
	}
	// 提交事务
	fn commit(&mut self, tr: &Tr, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Committing;
		// 先提交mgr上的事务
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for prefix in self.meta_txns.keys() {
				self.tab_logs.get(prefix).unwrap().1.commit(&self.id);
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r: DBResult<usize> | {
			match r {
				Ok(_) => tr1.cs_state(TxState::Committing, TxState::Commited),
				_ => tr1.cs_state(TxState::Committing, TxState::CommitFail)
			};
			if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				(*cb)(Ok(len))
			}
		});

		//处理每个表的预提交
		for val in self.tab_txns.values_mut() {
			match val.commit(bf.clone()) {
				Some(r) => {
					match r {
						Ok(_) => (),
						_ => self.state = TxState::CommitFail
					};
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						return Some(Ok(len))
					}
				}
				_ => ()
			}
		}
		//处理tab alter的预提交
		for val in self.meta_txns.values_mut() {
			match val.commit(bf.clone()) {
				Some(r) => {
					match r {
						Ok(_) => (),
						_ => self.state = TxState::CommitFail
					};
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						return Some(Ok(len))
					}
				}
				_ => ()
			}
		}
		None
	}
	// 回滚事务
	fn rollback(&mut self, tr: &Tr, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Rollbacking;
		// 先回滚mgr上的事务
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for prefix in self.meta_txns.keys() {
				self.tab_logs.get(prefix).unwrap().1.rollback(&self.id);
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r: DBResult<usize> | {
			match r {
				Ok(_) => tr1.cs_state(TxState::Rollbacking, TxState::Rollbacked),
				_ => tr1.cs_state(TxState::Rollbacking, TxState::RollbackFail)
			};
			if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				(*cb)(Ok(len))
			}
		});

		//处理每个表的预提交
		for val in self.tab_txns.values_mut() {
			match val.rollback(bf.clone()) {
				Some(r) => {
					match r {
						Ok(_) => (),
						_ => self.state = TxState::RollbackFail
					};
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						return Some(Ok(len))
					}
				}
				_ => ()
			}
		}
		//处理tab alter的预提交
		for val in self.meta_txns.values_mut() {
			match val.rollback(bf.clone()) {
				Some(r) => {
					match r {
						Ok(_) => (),
						_ => self.state = TxState::RollbackFail
					};
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						return Some(Ok(len))
					}
				}
				_ => ()
			}
		}
		None
	}
	// 修改，插入、删除及更新
	fn key_lock(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: usize, read_lock: bool, cb: TxCallback) -> UsizeResult {
		let len = arr.len();
		if len == 0 {
			return Some(Ok(0))
		}
		self.state = TxState::Doing;
		let count = Arc::new(AtomicUsize::new(len));
		let c1 = count.clone();
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			handle_result(r, &tr1, len, &c1, &cb1)
		});
		let map = tab_map(arr);
		for (key, val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = count.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(tr, &key, Box::new(move |r| {
				match r {
					Ok(t) => match t.key_lock(tkv1.clone(), lock_time, read_lock, bf1.clone()) {
						Some(r) => handle_result(r, &tr2, len, &c2, &cb2),
						_ => ()
					},
					Err(s) => single_result(Err(s), &tr2, &cb2)
				}
			})) {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, len, t.key_lock(tkv, lock_time, read_lock, bf.clone())) {
						None => (),
						rr => return rr
					}
					Err(s) => return Some(Err(s))
				},
				_ => ()
			}
		}
		None
	}
	// 查询
	fn query(
		&mut self,
		tr: &Tr,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		read_lock: bool,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		let len = arr.len();
		if len == 0 {
			return Some(Ok(Vec::new()))
		}
		self.state = TxState::Doing;
		// 创建指定长度的结果集，接收结果
		let mut vec = Vec::with_capacity(len);
		vec.resize(len, Default::default());
		let rvec = Arc::new(Mutex::new((len, vec)));
		let c1 = rvec.clone();
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			query_result(r, &tr1, &c1, &cb1)
		});
		let map = tab_map(arr);
		for (key, val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = rvec.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(tr, &key, Box::new(move |r| match r {
				Ok(t) => match t.query(tkv1.clone(), lock_time, read_lock, bf1.clone()) {
					Some(r) => query_result(r, &tr2, &c2, &cb2),
					_ => ()
				},
				Err(s) => {
					if tr2.cs_state(TxState::Doing, TxState::Fail) {
						(*cb2)(Err(s))
					}
				}
			})) {
				Some(r) => match r {
					Ok(t) => match t.query(tkv, lock_time, read_lock, bf.clone()) {
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
					},
					Err(s) => return Some(Err(s))
				},
				_ => ()
			}
		}
		None
	}
	// 修改，插入、删除及更新
	fn modify(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: Option<usize>, read_lock: bool, cb: TxCallback) -> UsizeResult {
		let len = arr.len();
		if len == 0 {
			return Some(Ok(0))
		}
		self.state = TxState::Doing;
		let count = Arc::new(AtomicUsize::new(len));
		let c1 = count.clone();
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			handle_result(r, &tr1, len, &c1, &cb1)
		});
		let map = tab_map(arr);
		for (key, val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = count.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(tr, &key, Box::new(move |r| {
				match r {
					Ok(t) => match t.modify(tkv1.clone(), lock_time, read_lock, bf1.clone()) {
						Some(r) => handle_result(r, &tr2, len, &c2, &cb2),
						_ => ()
					},
					Err(s) => single_result(Err(s), &tr2, &cb2)
				}
			})) {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, len, t.modify(tkv, lock_time, read_lock, bf.clone())) {
						None => (),
						rr => return rr
					}
					Err(s) => return Some(Err(s))
				},
				_ => ()
			}
		}
		None
	}
	// 表的大小
	fn tab_size(&mut self, tr: &Tr, tab: &Atom, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			single_result(r, &tr1, &cb1)
		});
		let bf1 = bf.clone();
		let tr2 = tr.clone();
		match self.build(tr, tab, Box::new(move |r| {
			match r {
				Ok(t) => match t.tab_size(bf1.clone()) {
					Some(r) => single_result(r, &tr2, &cb),
					_ => ()
				},
				Err(s) => single_result(Err(s), &tr2, &cb)
			}
		})) {
			Some(r) => match r {
				Ok(t) => match self.single_result(t.tab_size(bf)) {
					None => (),
					rr => return rr
				}
				Err(s) => return Some(Err(s))
			},
			_ => ()
		}
		None
	}
	// 新增 修改 删除 表
	fn alter(&mut self, tr: &Tr, tab: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		let (prefix, builder) = match self.find(tab) {
			Some((prefix, log, builder)) => match builder.check(&tab, &meta) { // 检查
				Ok(_) =>{
					log.alter(tab, meta.clone());
					(prefix, builder)
				},
				Err(s) => return self.single_result(Some(Err(s)))
			},
			_ => return self.single_result(Some(Err(String::from("builder not found"))))
		};
		let id = &self.id;
		let timeout = self.timeout;
		let txn = self.meta_txns.entry(prefix.clone()).or_insert_with(|| {
			builder.meta_txn(id, timeout)
		}).clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			single_result(r, &tr1, &cb)
		});
		self.single_result(txn.alter(tab, meta, bf))
	}
	// 表改名
	fn rename(&mut self, tr: &Tr, tab: &Atom, new_name: Atom, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		// TODO
		None
	}

	// 寻找和指定表名能前缀匹配的表构建器
	fn find(&mut self, tab_name: &Atom) -> Option<(Atom, &mut TabLog, Arc<TabBuilder>)> {
		// 遍历寻找和指定表名能前缀匹配的表构建器, TODO 用前缀树匹配的字符串
		match self.tab_logs.get_mut(tab_name) {
			Some((log, b)) => Some((tab_name.clone(), log, b.clone())),
			_ => None
		}
	}
	// 创建表
	fn build(&mut self, tr: &Tr, tab_name: &Atom, cb: Box<Fn(DBResult<Arc<TabTxn>>)>) -> Option<DBResult<Arc<TabTxn>>> {
		let txn = match self.tab_txns.get(tab_name) {
			Some(r) => return Some(Ok(r.clone())),
			_ => match self.find(tab_name) {
				Some((_, _, builder)) => match builder.tab_txn(tab_name, &self.id, self.writable, self.timeout, cb) {
					Some(r) => match r {
						Ok(txn) => txn,
						err => {
							self.state = TxState::Fail;
							return Some(err)
						}
					},
					_ => return None
				},
				_ => return Some(Err(String::from("TabBuilderNotFound")))
			}
		};
		self.tab_txns.insert(tab_name.clone(), txn.clone());
		Some(Ok(txn))
	}
	// 处理同步返回的数量结果
	fn handle_result(&mut self, count: &Arc<AtomicUsize>, len: usize, result: Option<DBResult<usize>>) -> UsizeResult {
		match result {
			Some(r) => match r {
				Ok(rc) => {
					if count.fetch_sub(rc, Ordering::SeqCst) == 1 {
						self.state = TxState::Ok;
						Some(Ok(len))
					}else{
						None
					}
				}
				_ => {
					self.state = TxState::Fail;
					Some(r)
				}
			},
			_ => None
		}
	}
	// 处理同步返回的单个结果
	fn single_result(&mut self, result: Option<DBResult<usize>>) -> UsizeResult {
		match result {
			Some(r) => match r {
				Ok(_) => {
					self.state = TxState::Ok;
					Some(r)
				}
				_ => {
					self.state = TxState::Fail;
					Some(r)
				}
			},
			_ => None
		}
	}
}

//================================ 内部静态方法
// 创建每表的键参数表，不负责键的去重
fn tab_map(mut arr: Vec<TabKV>) -> FnvHashMap<Atom, Vec<TabKV>> {
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
// 处理异步返回的查询结果
fn query_result(r: DBResult<Vec<TabKV>>, tr: &Tr, rvec: &Arc<Mutex<(usize, Vec<TabKV>)>>, cb: &TxQueryCallback) {
	match r {
		Ok(vec) => {
			match merge_result(rvec, vec) {
				Some(rr) => if tr.cs_state(TxState::Doing, TxState::Ok) {
					(*cb)(rr)
				}
				_ => (),
			}
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Fail) {
			(*cb)(r)
		}
	}
}
// 处理异步返回的数量结果
fn handle_result(r: DBResult<usize>, tr: &Tr, len: usize, count: &Arc<AtomicUsize>, cb: &TxCallback) {
	match r {
		Ok(rc) => if count.fetch_sub(rc, Ordering::SeqCst) == 1 && tr.cs_state(TxState::Doing, TxState::Ok) {
			(*cb)(Ok(len))
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Fail) {
			(*cb)(r)
		}
	}
}
// 处理异步返回的单个结果
fn single_result(r: DBResult<usize>, tr: &Tr, cb: &TxCallback) {
	match r {
		Ok(_) => if tr.cs_state(TxState::Doing, TxState::Ok) {
			(*cb)(r)
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Fail) {
			(*cb)(r)
		}
	}
}

#[cfg(test)]
#[derive(Debug)]
struct Player{
	name: String,
	id: u32,
}

#[cfg(test)]
impl Encode for Player{
	fn encode(&self, bb: &mut BonBuffer){
		self.name.encode(bb);
		self.id.encode(bb);
	}
}

#[cfg(test)]
impl Decode for Player{
	fn decode(bb: &mut BonBuffer) -> Self{
		Player{
			name: String::decode(bb),
			id: u32::decode(bb),
		}
	}
}

#[test]
fn test_memery_db_mgr(){

	let mgr = Mgr::new(GuidGen::new(1,1));
	mgr.register_builder(&Atom::from("memery"), Arc::new(memery_db::MemeryDB::new(Atom::from("memery"))));
	let mgr = Arc::new(mgr);

	let tr = mgr.transaction(true, 1000);
	let tr1 = tr.clone();
	let mut sinfo = StructInfo::new(Atom::from("Player"), 555555555555);
	let mut m = HashMap::new();
	m.insert(Atom::from("class"), Atom::from("memery"));
	sinfo.notes = Some(m);
	let alter_back = Arc::new(move|r: DBResult<usize>|{
		println!("alter: {:?}", r);
		
		match tr1.prepare(Arc::new(|r|{println!("prepare_alter:{:?}", r)})){
			Some(r) => println!("prepare_alter:{:?}", r),
			_ => println!("prepare_alter:fail"),
		};
		match tr1.commit(Arc::new(|r|{println!("commit_alter:{:?}", r)})){
			Some(r) => println!("commit_alter:{:?}", r),
			_ => println!("commit_alter:fail"),
		};
		println!("alter_succsess");
		let mgr1 = mgr.clone();
		let write = move||{
			let mgr2 = mgr1.clone();
			let read = move||{
				let tr = mgr2.transaction(false, 1000);
				let tr1 = tr.clone();
				let mut arr = Vec::new();
				let t1 = TabKV{
					tab: Atom::from("Player"),
					key: vec![5u8],
					index: 0,
					value: None,
				};
				arr.push(t1);

				let read_back = Arc::new(move|r: DBResult<Vec<TabKV>>|{
					match r {
						Ok(mut v) => {
							println!("read:ok");
							for elem in v.iter_mut(){
								match elem.value {
									Some(ref mut v) => {
										let mut buf = BonBuffer::with_bytes(Arc::make_mut(v).clone(), None, None);
										let p = Player::decode(&mut buf);
										println!("{:?}", p);
									},
									None => (),
								}
							}
						},
						Err(v) => println!("read:fail, {}", v),
					}
					//println!("read: {:?}", r);
					match tr1.prepare(Arc::new(|r|{println!("prepare_read:{:?}", r)})){
						Some(r) => println!("prepare_read:{:?}", r),
						_ => println!("prepare_read:fail"),
					};
					match tr1.commit(Arc::new(|r|{println!("commit_read:{:?}", r)})){
						Some(r) => println!("commit_read:{:?}", r),
						_ => println!("commit_read:fail"),
					};
					//println!("succsess:{}", arr.len());
				});

				let r = tr.query(arr, Some(100), true, read_back.clone());
				if r.is_some(){
					read_back(r.unwrap());
				}
			};

			let tr = mgr1.transaction(true, 1000);
			let tr1 = tr.clone();
			let p = Player{
				name: String::from("chuanyan"),
				id:5
			};
			let mut bonbuf = BonBuffer::new();
			let bon = p.encode(&mut bonbuf);
			let buf = bonbuf.unwrap();

			let mut arr = Vec::new();
			let t1 = TabKV{
				tab: Atom::from("Player"),
				key: vec![5u8],
				index: 0,
				value: Some(Arc::new(buf)),
			};
			arr.push(t1);

			let write_back = Arc::new(move|r|{
				println!("write: {:?}", r);
				match tr1.prepare(Arc::new(|r|{println!("prepare_write:{:?}", r)})){
					Some(r) => println!("prepare_write:{:?}", r),
					_ => println!("prepare_write:fail"),
				};
				match tr1.commit(Arc::new(|r|{println!("commit_write:{:?}", r)})){
					Some(r) => println!("commit_write:{:?}", r),
					_ => println!("commit_write:fail"),
				};
				&read();
			});
			let r = tr.modify(arr, Some(100), false, write_back.clone());
			if r.is_some(){
				write_back(r.unwrap());
			}
		};
		write();
	});
	let r = tr.alter(&Atom::from("Player"), Some(Arc::new(sinfo)), alter_back.clone());
	if r.is_some(){
		alter_back(r.unwrap());
	}
}

// #[test]
// fn test_file_db_mgr(){

// 	let mgr = Mgr::new(GuidGen::new(1,1));
// 	mgr.register_builder(&Atom::from("file"), Arc::new(memery_db::MemeryDB::new(Atom::from("file"))));
// 	let mgr = Arc::new(mgr);

// 	let tr = mgr.transaction(true, 1000);
// 	let tr1 = tr.clone();
// 	let mut sinfo = StructInfo::new(Atom::from("Player"), 555555555555);
// 	let mut m = HashMap::new();
// 	m.insert(Atom::from("class"), Atom::from("memery"));
// 	sinfo.notes = Some(m);
// 	let alter_back = Arc::new(move|r: DBResult<usize>|{
// 		println!("alter: {:?}", r);
		
// 		match tr1.prepare(Arc::new(|r|{println!("prepare_alter:{:?}", r)})){
// 			Some(r) => println!("prepare_alter:{:?}", r),
// 			_ => println!("prepare_alter:fail"),
// 		};
// 		match tr1.commit(Arc::new(|r|{println!("commit_alter:{:?}", r)})){
// 			Some(r) => println!("commit_alter:{:?}", r),
// 			_ => println!("commit_alter:fail"),
// 		};
// 		println!("alter_succsess");
// 		let mgr1 = mgr.clone();
// 		let write = move||{
// 			let mgr2 = mgr1.clone();
// 			let read = move||{
// 				let tr = mgr2.transaction(false, 1000);
// 				let tr1 = tr.clone();
// 				let mut arr = Vec::new();
// 				let t1 = TabKV{
// 					tab: Atom::from("Player"),
// 					key: vec![5u8],
// 					index: 0,
// 					value: None,
// 				};
// 				arr.push(t1);

// 				let read_back = Arc::new(move|r: DBResult<Vec<TabKV>>|{
// 					match r {
// 						Ok(mut v) => {
// 							println!("read:ok");
// 							for elem in v.iter_mut(){
// 								match elem.value {
// 									Some(ref mut v) => {
// 										let mut buf = BonBuffer::with_bytes(Arc::make_mut(v).clone(), None, None);
// 										let p = Player::decode(&mut buf);
// 										println!("{:?}", p);
// 									},
// 									None => (),
// 								}
// 							}
// 						},
// 						Err(v) => println!("read:fail, {}", v),
// 					}
// 					//println!("read: {:?}", r);
// 					match tr1.prepare(Arc::new(|r|{println!("prepare_read:{:?}", r)})){
// 						Some(r) => println!("prepare_read:{:?}", r),
// 						_ => println!("prepare_read:fail"),
// 					};
// 					match tr1.commit(Arc::new(|r|{println!("commit_read:{:?}", r)})){
// 						Some(r) => println!("commit_read:{:?}", r),
// 						_ => println!("commit_read:fail"),
// 					};
// 					//println!("succsess:{}", arr.len());
// 				});

// 				let r = tr.query(arr, Some(100), true, read_back.clone());
// 				if r.is_some(){
// 					read_back(r.unwrap());
// 				}
// 			};

// 			let tr = mgr1.transaction(true, 1000);
// 			let tr1 = tr.clone();
// 			let p = Player{
// 				name: String::from("chuanyan"),
// 				id:5
// 			};
// 			let mut bonbuf = BonBuffer::new();
// 			let bon = p.encode(&mut bonbuf);
// 			let buf = bonbuf.unwrap();

// 			let mut arr = Vec::new();
// 			let t1 = TabKV{
// 				tab: Atom::from("Player"),
// 				key: vec![5u8],
// 				index: 0,
// 				value: Some(Arc::new(buf)),
// 			};
// 			arr.push(t1);

// 			let write_back = Arc::new(move|r|{
// 				println!("write: {:?}", r);
// 				match tr1.prepare(Arc::new(|r|{println!("prepare_write:{:?}", r)})){
// 					Some(r) => println!("prepare_write:{:?}", r),
// 					_ => println!("prepare_write:fail"),
// 				};
// 				match tr1.commit(Arc::new(|r|{println!("commit_write:{:?}", r)})){
// 					Some(r) => println!("commit_write:{:?}", r),
// 					_ => println!("commit_write:fail"),
// 				};
// 				&read();
// 			});
// 			let r = tr.modify(arr, Some(100), false, write_back.clone());
// 			if r.is_some(){
// 				write_back(r.unwrap());
// 			}
// 		};
// 		write();
// 	});
// 	let r = tr.alter(&Atom::from("Player"), Some(Arc::new(sinfo)), alter_back.clone());
// 	if r.is_some(){
// 		alter_back(r.unwrap());
// 	}
// }

