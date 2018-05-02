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

use db::{UsizeResult, Cursor, DBResult, TabKV, TxCallback, TxIterCallback, TxQueryCallback, TxState, MetaTxn, Tab, TabTxn, TabBuilder};

pub type TxHandler = Box<FnMut(&mut Tr)>;


// 表、事务管理器
pub struct Mgr {
	// 构建器
	builders: OrdMap<Tree<Atom, Arc<TabBuilder>>>,
	//数据表
	tabs: OrdMap<Tree<Atom, TabInfo>>,
	// 事务ID生成器
	gen: GuidGen,
	// 当前的事务数量
	tr_count: AtomicUsize,
	// 定时轮
	// 管理用的弱引用事务
	weak_map: Mutex<FnvHashMap<Guid, Weak<Mutex<Tx>>>>,
	// 预提交的表
	prepare: Mutex<FnvHashMap<Guid, Tr>>,
}

impl Mgr {
	// 注册管理器
	pub fn new(gen: GuidGen) -> Self {
		Mgr {
			builders : OrdMap::new(new()),
			tabs : OrdMap::new(new()),
			gen: gen,
			tr_count: AtomicUsize::new(0),
			weak_map: Mutex::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			prepare: Mutex::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
		}
	}
	// 注册构建器
	pub fn register_builder(&mut self, builder: Arc<TabBuilder>) -> bool {
		let b = self.builders.insert(builder.get_class().clone(), builder.clone());
		if !b {
			return b;
		}
		// 加载全部的表
		for (name, meta) in builder.list() {
			self.tabs.insert(name.clone(), TabInfo::new(builder.clone(), meta));
		};
		return true;
	}
	// 取消注册数据库
	pub fn unregister_builder(&mut self, class: &Atom) -> Option<Arc<TabBuilder>> {
		match self.builders.delete(class, true) {
			Some(r) => r,
			_ => None,
		}
	}
	// 表的元信息
	pub fn tab_info(&self, tab: &Atom) -> Option<Arc<StructInfo>> {
		match self.tabs.get(tab) {
			Some(ref info) => Some(info.meta.clone()),
			_ => None,
		}
	}

	// 读事务，无限尝试直到超时，默认10秒
	pub fn read(&mut self, tx: TxHandler, timeout: usize, cb: TxCallback) {}
	// 写事务，无限尝试直到超时，默认10秒
	pub fn write(&mut self, tx: TxHandler, timeout: usize, cb: TxCallback) {}
	// 创建事务
	pub fn transaction(&mut self, writable: bool, timeout: usize) -> Tr {
		let id = self.gen.gen(0);
		let tr = Tr(Arc::new(Mutex::new(Tx {
			id: id.clone(),
			writable: writable,
			timeout: timeout,
			builders: self.builders.clone(),
			tabs: self.tabs.clone(),
			old_tabs: self.tabs.clone(),
			state: TxState::Ok,
			timer_ref: 0,
			tab_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			meta_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			alter_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			rename_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		})));
		self.tr_count.fetch_add(1, Ordering::SeqCst);
		let mut weak_map = self.weak_map.lock().unwrap();
		weak_map.insert(id, Arc::downgrade(&(tr.0)));
		tr
	}

	// 注册数据表
	fn register_tab(&mut self, name: &Atom, info: TabInfo) -> bool {
		self.tabs.insert(name.clone(), info)
	}
	// 表的预提交
	fn parpare(&mut self, name: &Atom) -> bool {
		false
	}
	// 表的提交
	fn commit(&mut self, name: &Atom) -> bool {
		false
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
	pub fn prepare(&mut self, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.prepare(self, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 提交一个事务
	pub fn commit(&mut self, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.prepare(self, cb),
			TxState::PreparOk => t.prepare(self, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 回滚一个事务
	pub fn rollback(&mut self, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.prepare(self, cb),
			TxState::PreparOk => t.prepare(self, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 锁
	pub fn key_lock(&mut self, arr: Vec<TabKV>, lock_time: usize, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.key_lock(self, arr, lock_time, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 查询
	pub fn query(
		&mut self,
		mut arr: Vec<TabKV>,
		lock_time: Option<usize>,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.query(self, arr, lock_time, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 修改，插入、删除及更新
	pub fn modify(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.modify(self, arr, lock_time, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 范围查询
	pub fn range(
		&mut self,
		tab: &Atom,
		min_key:Vec<u8>,
		max_key:Vec<u8>,
		key_only: bool,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		None
	}
	// 迭代
	pub fn iter(
		&mut self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		key_only: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 索引迭代
	pub fn index(
		&mut self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 表的元信息
	pub fn tab_info(&self, tab: &Atom) -> Option<Arc<StructInfo>> {
		let t = self.0.lock().unwrap();
		match t.tabs.get(tab) {
			Some(ref info) => Some(info.meta.clone()),
			_ => None,
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
	pub fn alter(&mut self, tab: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.alter(self, tab, meta, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 表改名
	pub fn rename(&mut self, tab: &Atom, new_name: Atom, cb: TxCallback) -> UsizeResult {
		let mut t = self.0.lock().unwrap();
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
// 表信息
#[derive(Clone)]
struct TabInfo {
	builder: Arc<TabBuilder>,
	meta: Arc<StructInfo>,
	var: Arc<Mutex<TabVar>>,
}
struct TabVar {
	tab: DBResult<Arc<Tab>>,
	wait: Option<Vec<Box<Fn(DBResult<Arc<Tab>>)>>>, // 为None表示表示tab已经加载
}
impl TabInfo {
	fn new(builder: Arc<TabBuilder>, meta: Arc<StructInfo>) -> Self {
		TabInfo{
			builder: builder,
			meta: meta,
			var: Arc::new(Mutex::new(TabVar {
				tab: Err(String::from("")),
				wait:Some(Vec::new()),
			})),
		}
	}
}

struct Tx {
	id: Guid,
	writable: bool,
	timeout: usize, // 子事务的预提交的超时时间
	builders: OrdMap<Tree<Atom, Arc<TabBuilder>>>,
	tabs: OrdMap<Tree<Atom, TabInfo>>,
	old_tabs: OrdMap<Tree<Atom, TabInfo>>, // 用于判断mgr中tabs是否修改过
	state: TxState,
	timer_ref: usize,
	tab_txns: FnvHashMap<Atom, Arc<TabTxn>>, //表事务表
	meta_txns: FnvHashMap<Atom, Arc<MetaTxn>>, //元信息事务表
	alter_logs: FnvHashMap<(Atom, usize), Option<Arc<StructInfo>>>, // 记录每个被改过元信息的表
	rename_logs: FnvHashMap<Atom, (Atom, usize)>, // 新名字->(源名字, 版本号)
}

impl Tx {
	// 预提交事务
	fn prepare(&mut self, tr: &Tr, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Preparing;
		// TODO 处理tab alter的预提交
		let len = self.tab_txns.len();
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let f = move |r: DBResult<usize> | match r {
			Ok(_) => if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				if tr1.cs_state(TxState::Preparing, TxState::PreparOk) {
					(*cb)(Ok(len))
				}
			}
			_ => if tr1.cs_state(TxState::Preparing, TxState::PreparFail) {
				(*cb)(r)
			}
		};
		let bf = Arc::new(f);
		for (_, val) in self.tab_txns.iter_mut() {
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
				},
				_ => (),
			}
		}
		None
	}
	// 提交同步事务
	fn commit(&mut self) -> UsizeResult {
		// for (_, val) in self.tab_txns.iter_mut() {
		// 	match (*val).commit(bf.clone()) {
		// 		Some(r) => {
		// 			if count.fetch_sub(1, Ordering::SeqCst) == 1{
		// 				return Some(r)
		// 			}
		// 		},
		// 		_ => (),
		// 	}
		// }
		None
	}
	// 修改，插入、删除及更新
	fn key_lock(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: usize, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		let len = arr.len();
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
					Ok(t) => match t.key_lock(tkv1.clone(), lock_time, bf1.clone()) {
						Some(r) => handle_result(r, &tr2, len, &c2, &cb2),
						_ => ()
					},
					Err(s) => (*cb2)(Err(s))
				}
			})) {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, len, t.key_lock(tkv, lock_time, bf.clone())) {
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
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		self.state = TxState::Doing;
		let len = arr.len();
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
				Ok(t) => match t.query(tkv1.clone(), lock_time, bf1.clone()) {
					Some(r) => query_result(r, &tr2, &c2, &cb2),
					_ => ()
				},
				Err(s) => (*cb2)(Err(s))
			})) {
				Some(r) => match r {
					Ok(t) => match t.query(tkv, lock_time, bf.clone()) {
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
	fn modify(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: Option<usize>, cb: TxCallback) -> UsizeResult {
		self.state = TxState::Doing;
		let len = arr.len();
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
					Ok(t) => match t.modify(tkv1.clone(), lock_time, bf1.clone()) {
						Some(r) => handle_result(r, &tr2, len, &c2, &cb2),
						_ => ()
					},
					Err(s) => (*cb2)(Err(s))
				}
			})) {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, len, t.modify(tkv, lock_time, bf.clone())) {
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
				Err(s) => (*cb)(Err(s))
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
		let builder = match &meta {
			&Some(ref sinfo) => match self.tabs.get(&tab) { // 修改
				Some(ref tabinfo) =>{
					match tabinfo.builder.check(&tab, sinfo) { // 检查
						Ok(_) => &tabinfo.builder,
						Err(s) => return Some(Err(s))
					}
				},
				_ => match sinfo.get_note(&Atom::from("class")) { // 新增, TODO 以后优化成静态字符串，使用完美哈希
					Some(class) => match self.builders.get(class) {
						Some(b) => {
							match b.check(&tab, sinfo) { // 检查
								Ok(_) => b,
								Err(s) => return Some(Err(s))
							}
						},
						_ => return self.single_result(Some(Err(String::from("builder not found"))))
					},
					_ => return self.single_result(Some(Err(String::from("class not found in meta"))))
				}
			},
			_ => match self.tabs.get(&tab) { // 删除
				Some(ref tabinfo) => &tabinfo.builder,
				_ => return Some(Ok(0))
			}
		};
		// 先查找rename_logs，获取该表的源名字及版本，然后修改alter_logs
		let tab_ver = match self.rename_logs.get(&tab) {
			Some(v) => v.clone(),
			_ => (tab.clone(), 0),
		};
		self.alter_logs.entry(tab_ver.clone()).or_insert(meta.clone());
		let id = &self.id;
		let timeout = self.timeout;
		let txn = self.meta_txns.entry(tab.clone()).or_insert_with(|| {
			builder.transaction(id, timeout)
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

	// 创建表
	fn build(&mut self, tr: &Tr, tab_name: &Atom, cb: Box<Fn(DBResult<Arc<TabTxn>>)>) -> Option<DBResult<Arc<TabTxn>>> {
		let txn = match self.tab_txns.get(tab_name) {
			Some(r) => return Some(Ok(r.clone())),
			_ => match self.tabs.get(tab_name) {
				Some(ref info) => {
					let tab = {
						let mut var = info.var.lock().unwrap();
						match var.wait {
							Some(ref mut vec) => {// 表尚未build
								if vec.len() == 0 {// 第一次调用
									let var1 = info.var.clone();
									match info.builder.build(tab_name, info.meta.clone(), Box::new(move |tab| {
										// 异步返回，解锁后设置结果，返回等待函数数组
										let vec:Vec<Box<Fn(DBResult<Arc<Tab>>)>> = {
											let mut var = var1.lock().unwrap();
											let vec = mem::replace(var.wait.as_mut().unwrap(), Vec::new());
											var.tab = tab.clone();
											var.wait = None;
											vec
										};
										// 通知所有的等待函数数组
										for f in vec.into_iter() {
											(*f)(tab.clone())
										}
									})) {
										Some(r) => {// 同步返回，设置结果
											var.tab = r;
											var.wait = None;
											var.tab.clone()
										},
										_ => { //异步的第1次调用，直接返回
											vec.push(handle_fn(tr.clone(), tab_name.clone(), self.id.clone(), self.writable, self.timeout, cb));
											return None
										}
									}
								}else { // 异步的第n次调用，直接返回
									vec.push(handle_fn(tr.clone(), tab_name.clone(), self.id.clone(), self.writable, self.timeout, cb));
									return None
								}
							},
							_ => var.tab.clone()
						}
					};
					// 根据结果创建事务或返回错误
					match tab {
						Ok(tab) => tab.transaction(&self.id, self.writable, self.timeout),
						Err(s) => return Some(Err(s))
					}
				},
				_ => {
					self.state = TxState::Fail;
					return Some(Err(String::from("TabNotFound")))
				}
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

// 表构建函数的回调函数
fn handle_fn(tr: Tr, tab_name: Atom, id: Guid, writable: bool, timeout: usize, cb: Box<Fn(DBResult<Arc<TabTxn>>)>) -> Box<Fn(DBResult<Arc<Tab>>)> {
	let name = tab_name.clone();
	let id = id.clone();
	Box::new(move |r| {
		match r {
			Ok(tab) => {
				// 创建事务，并解锁tr，放入到事务表中
				let txn = tab.transaction(&id, writable, timeout);
				tr.0.lock().unwrap().tab_txns.insert(name.clone(), txn.clone());
				(*cb)(Ok(txn))
			},
			Err(s) => (*cb)(Err(s))
		}
	})
}
