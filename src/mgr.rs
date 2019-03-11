/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::mem;

use fnv::FnvHashMap;

use ordmap::ordmap::{OrdMap, Entry, ImOrdMap, Keys};
use ordmap::asbtree::{Tree, new};
use atom::Atom;
use sinfo::EnumType;
use guid::{Guid, GuidGen};

use db::{SResult, DBResult, IterResult, KeyIterResult, Filter, TabKV, TxCallback, TxQueryCallback, TxState, MetaTxn, TabTxn, Ware, WareSnapshot, Bin, RwLog, TabMeta};

// 表库及事务管理器
#[derive(Clone)]
pub struct Mgr(Arc<Mutex<Manager>>, Arc<Mutex<WareMap>>, Arc<GuidGen>, Statistics);

impl Mgr {
	// 注册管理器
	pub fn new(gen: GuidGen) -> Self {
		Mgr(Arc::new(Mutex::new(Manager::new())), Arc::new(Mutex::new(WareMap::new())), Arc::new(gen), Statistics::new())
	}
	// 浅拷贝，库表不同，共用同一个统计信息和GuidGen
	pub fn shallow_clone(&self) -> Self {
		// TODO 拷库表
		Mgr(Arc::new(Mutex::new(Manager::new())), Arc::new(Mutex::new(self.1.lock().unwrap().wares_clone())), self.2.clone(), self.3.clone())
	}
	// 深拷贝，库表及统计信息不同
	pub fn deep_clone(&self, clone_guid_gen: bool) -> Self {
		let gen = if clone_guid_gen {
			self.2.clone()
		}else{
			Arc::new(GuidGen::new(self.2.node_time(), self.2.node_id()))
		};
		Mgr(Arc::new(Mutex::new(Manager::new())), Arc::new(Mutex::new(WareMap::new())), gen, self.3.clone())
	}
	// 注册库
	pub fn register(&self, ware_name: Atom, ware: Arc<Ware>) -> bool {
		self.1.lock().unwrap().register(ware_name, ware)
	}
	// 取消注册数据库
	pub fn unregister(&mut self, ware_name: &Atom) -> bool {
		self.1.lock().unwrap().unregister(ware_name)
	}
	// 表的元信息
	pub fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.find(ware_name) {
			Some(b) => b.tab_info(tab_name),
			_ => None
		}
	}
	// 创建事务
	pub fn transaction(&self, writable: bool) -> Tr {		
		let id = self.2.gen(0);
		self.3.acount.fetch_add(1, Ordering::SeqCst);
		let map = {
			self.1.lock().unwrap().clone()
		};
		self.0.lock().unwrap().transaction(map, writable, id, self.clone())
	}

	pub fn listen(&self, monitor: Arc<Monitor>){
		self.0.lock().unwrap().register_monitor(monitor);
	}

	pub fn ware_name_list(&self) -> Vec<String> {
		let mut arr = Vec::new();
		let lock = self.1.lock().unwrap();
		let mut iter = lock.keys(None, false);
		loop {
			match iter.next() {
				Some(e) => arr.push(e.as_str().to_string()),
				None => break,
			}
		}
		arr
	}

	// 寻找指定的库
	fn find(&self, ware_name: &Atom) -> Option<Arc<Ware>> {
		let map = {
			self.1.lock().unwrap().clone()
		};
		map.find(ware_name)
	}
}

pub struct Event {
	pub ware: Atom,
	pub tab: Atom,
	pub other: EventType
}
pub enum EventType{
	Meta(Option<EnumType>),
	Tab{key: Bin, value: Option<Bin>},
}

pub trait Monitor {
	fn notify(&self, event: Event, mgr: Mgr);
}


// 事务统计
#[derive(Clone)]
pub struct Statistics {
	acount: Arc<AtomicUsize>,
	ok_count: Arc<AtomicUsize>,
	err_count: Arc<AtomicUsize>,
	fail_count: Arc<AtomicUsize>,
	//conflict: Arc<Mutex<Vec<String>>>,
}
impl Statistics {
	fn new() -> Self {
		Statistics {
			acount: Arc::new(AtomicUsize::new(0)),
			ok_count: Arc::new(AtomicUsize::new(0)),
			err_count: Arc::new(AtomicUsize::new(0)),
			fail_count: Arc::new(AtomicUsize::new(0)),
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
	pub fn prepare(&self, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.prepare(self, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 提交一个事务
	pub fn commit(&self, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::PreparOk => t.commit(self, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::PreparOk, found:") + t.state.to_string().as_str())),
		}
	}
	// 回滚一个事务
	pub fn rollback(&self, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Committing|TxState::Commited|TxState::CommitFail|TxState::Rollbacking|TxState::Rollbacked|TxState::RollbackFail =>
				return Some(Err(String::from("InvalidState, expect:TxState::Committing | TxState::Commited| TxState::CommitFail| TxState::Rollbacking| TxState::Rollbacked| TxState::RollbackFail, found:") + t.state.to_string().as_str())),
			_ => t.rollback(self, cb)
		}
	}
	// 锁
	pub fn key_lock(&self, arr: Vec<TabKV>, lock_time: usize, read_lock: bool, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.key_lock(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 查询
	pub fn query(
		&self,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		read_lock: bool,
		cb: TxQueryCallback,
	) -> Option<SResult<Vec<TabKV>>> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.query(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 修改，插入、删除及更新
	pub fn modify(&self, arr: Vec<TabKV>, lock_time: Option<usize>, read_lock: bool, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.modify(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 范围查询
	pub fn range(
		&self,
		_ware: &Atom,
		_tab: &Atom,
		_min_key:Vec<u8>,
		_max_key:Vec<u8>,
		_key_only: bool,
		_cb: TxQueryCallback,
	) -> Option<SResult<Vec<TabKV>>> {
		None
	}
	// 迭代
	pub fn iter(
		&self,
		ware: &Atom,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(IterResult)>,
	) -> Option<IterResult> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.iter(self, ware, tab, key, descending, filter, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 键迭代
	pub fn key_iter(
		&self,
		ware: &Atom,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(KeyIterResult)>,
	) -> Option<KeyIterResult> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.key_iter(self, ware, tab, key, descending, filter, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 索引迭代
	pub fn index(
		&self,
		_ware: &Atom,
		_tab: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_filter: String,
		_cb: Arc<Fn(IterResult)>,
	) -> Option<IterResult> {
		None
	}
	// 列出指定库的所有表
	pub fn list(&self, ware_name: &Atom) -> Option<Vec<Atom>> {
		match self.0.lock().unwrap().ware_log_map.get(ware_name) {
			Some(ware) => {
				let mut arr = Vec::new();
				for e in ware.list(){
					arr.push(e.clone())
				}
				Some(arr)
			},
			_ => None
		}
	}
	// 表的元信息
	pub fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.0.lock().unwrap().ware_log_map.get(ware_name) {
			Some(ware) => ware.tab_info(tab_name),
			_ => None
		}
	}
	// 表的大小
	pub fn tab_size(&self, ware_name:&Atom, tab_name: &Atom, cb: Arc<Fn(SResult<usize>)>) -> Option<SResult<usize>> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.tab_size(self, ware_name, tab_name, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 创建、修改或删除表
	pub fn alter(&self, ware_name:&Atom, tab_name: &Atom, meta: Option<Arc<TabMeta>>, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.alter(self, ware_name, tab_name, meta, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
		}
	}
	// 表改名
	pub fn rename(&self, ware_name:&Atom, old_name: &Atom, new_name: Atom, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.rename(self, ware_name, old_name, new_name, cb),
			_ => Some(Err(String::from("InvalidState, expect:TxState::Ok, found:") + t.state.to_string().as_str())),
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
const TIMEOUT: usize = 100;

// 事务管理器
struct Manager {
	// 定时轮
	// 管理用的弱引用事务
	//weak_map: FnvHashMap<Guid, Weak<Mutex<Tx>>>,
	monitors: OrdMap<Tree<usize, Arc<Monitor>>>,//监听器列表
}
impl Manager {
	// 注册管理器
	fn new() -> Self {
		Manager {
			//weak_map: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			monitors: OrdMap::new(Tree::new())
		}
	}
	// 创建事务
	fn transaction(&mut self, ware_map: WareMap, writable: bool, id: Guid, mgr: Mgr) -> Tr {
		// 遍历ware_map, 将每个Ware的快照TabLog记录下来
		let mut map = FnvHashMap::with_capacity_and_hasher(ware_map.0.size() * 3 / 2, Default::default());
		for Entry(k, v) in ware_map.0.iter(None, false){
			map.insert(k.clone(), v.snapshot());
		}
		let tr = Tr(Arc::new(Mutex::new(Tx {
			writable: writable,
			timeout: TIMEOUT,
			id: id.clone(),
			ware_log_map: map,
			state: TxState::Ok,
			_timer_ref: 0,
			tab_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			meta_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			monitors: self.monitors.clone(),
			mgr:mgr,
		})));
		//self.weak_map.insert(id, Arc::downgrade(&(tr.0)));
		tr
	}

	fn register_monitor(&mut self, monitor: Arc<Monitor>){
		self.monitors.insert(self.monitors.size(), monitor);
	}
}

// 库表
#[derive(Clone)]
struct WareMap(OrdMap<Tree<Atom, Arc<Ware>>>);

impl WareMap {
	fn new() -> Self {
		WareMap(OrdMap::new(new()))
	}

	fn wares_clone(&self) -> Self{
		let mut wares = Vec::new();
		for ware in self.0.iter(None, false){
			wares.push(Entry(ware.0.clone(), ware.1.tabs_clone()));
		}
		WareMap(OrdMap::new(Tree::from_order(wares)))
	}
	// 注册库
	fn register(&mut self, ware_name: Atom, ware: Arc<Ware>) -> bool {
		self.0.insert(ware_name, ware)
	}
	// 取消注册的库
	fn unregister(&mut self, ware_name: &Atom) -> bool {
		match self.0.delete(ware_name, false) {
			Some(_) => true,
			_ => false,
		}
	}
	// 寻找和指定表名能前缀匹配的表库
	fn find(&self, ware_name: &Atom) -> Option<Arc<Ware>> {
		match self.0.get(&ware_name) {
			Some(b) => Some(b.clone()),
			_ => None
		}
	}

	fn keys(&self, key: Option<&Atom>, descending: bool) -> Keys<Tree<Atom, Arc<Ware>>> {
		self.0.keys(key, descending)
	}
}


// 子事务
struct Tx {
	// TODO 下面几个可以放到锁的外部，减少锁
	writable: bool,
	timeout: usize, // 子事务的预提交的超时时间, TODO 取提交的库的最大超时时间
	id: Guid,
	ware_log_map: FnvHashMap<Atom, Arc<WareSnapshot>>,// 库名对应库快照
	state: TxState,
	_timer_ref: usize,
	tab_txns: FnvHashMap<(Atom, Atom), Arc<TabTxn>>, //表事务表
	meta_txns: FnvHashMap<Atom, Arc<MetaTxn>>, //元信息事务表
	monitors: OrdMap<Tree<usize, Arc<Monitor>>>, //监听器列表
	mgr: Mgr,
}

impl Tx {
	// 预提交事务
	fn prepare(&mut self, tr: &Tr, cb: TxCallback) -> DBResult {
		//如果预提交内容为空，直接返回预提交成功
		if self.meta_txns.len() == 0 && self.tab_txns.len() == 0 {
			self.state = TxState::PreparOk;
			return Some(Ok(()));
		}
		self.state = TxState::Preparing;
		// 先检查mgr上的meta alter的预提交
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				match self.ware_log_map.get_mut(ware).unwrap().prepare(&self.id) {
					Err(s) =>{
						self.state = TxState::PreparFail;
						return Some(Err(s))
					},
					_ => ()
				}
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r: SResult<()> | match r {
			Ok(_) => if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				if tr1.cs_state(TxState::Preparing, TxState::PreparOk) {
					cb(Ok(()))
				}
			}
			_ => if tr1.cs_state(TxState::Preparing, TxState::PreparFail) {
				cb(r)
			}
		});

		//处理每个表的预提交
		for val in self.tab_txns.values_mut() {
			match val.prepare(self.timeout, bf.clone()) {
				Some(r) => match r {
					Ok(_) => {
						if count.fetch_sub(1, Ordering::SeqCst) == 1 {
							self.state = TxState::PreparOk;
							return Some(Ok(()));
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
			match val.prepare(self.timeout, bf.clone()) {
				Some(r) => match r {
					Ok(_) => {
						if count.fetch_sub(1, Ordering::SeqCst) == 1 {
							self.state = TxState::PreparOk;
							return Some(Ok(()));
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
	fn commit(&mut self, tr: &Tr, cb: TxCallback) -> DBResult {
		self.state = TxState::Committing;
		// 先提交mgr上的事务
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				self.ware_log_map.get(ware).unwrap().commit(&self.id);
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r: SResult<()> | {
			match r {
				Ok(_) => tr1.cs_state(TxState::Committing, TxState::Commited),
				_ => tr1.cs_state(TxState::Committing, TxState::CommitFail)
			};
			if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				cb(Ok(()))
			}
		});

		//处理每个表的提交
		for (txn_name, val) in self.tab_txns.iter_mut() {
			match val.commit(bf.clone()) {
				Some(r) => {
					match r {
						Ok(logs) => {
							for (k, v) in logs.into_iter(){ //将表的提交日志添加到事件列表中
								match v {
									RwLog::Write(value) => {
										for Entry(_, monitor) in self.monitors.iter(None, false){
											monitor.notify(Event{ware: txn_name.0.clone(), tab: txn_name.1.clone(), other: EventType::Tab{key:k.clone(), value: value.clone()}}, self.mgr.clone())
										}
									},
									_ => (),
								}
							}
						}
						_ => self.state = TxState::CommitFail
					};
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						return Some(Ok(()))
					}
				}
				_ => ()
			}
		}
		//处理tab alter的提交
		for val in self.meta_txns.values_mut() {
			match val.commit(bf.clone()) {
				Some(r) => {
					match r {
						Ok(_) => (),
						_ => self.state = TxState::CommitFail
					};
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						return Some(Ok(()))
					}
				}
				_ => ()
			}
		}
		None
	}
	// 回滚事务
	fn rollback(&mut self, tr: &Tr, cb: TxCallback) -> DBResult {
		self.state = TxState::Rollbacking;
		// 先回滚mgr上的事务
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				self.ware_log_map.get(ware).unwrap().rollback(&self.id);
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		let c = count.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r: SResult<()> | {
			match r {
				Ok(_) => tr1.cs_state(TxState::Rollbacking, TxState::Rollbacked),
				_ => tr1.cs_state(TxState::Rollbacking, TxState::RollbackFail)
			};
			if c.fetch_sub(1, Ordering::SeqCst) == 1 {
				cb(Ok(()))
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
						return Some(Ok(()))
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
						return Some(Ok(()))
					}
				}
				_ => ()
			}
		}
		None
	}
	// 修改，插入、删除及更新
	fn key_lock(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: usize, read_lock: bool, cb: TxCallback) -> DBResult {
		if arr.len() == 0 {
			return Some(Ok(()))
		}
		let map = tab_map(arr);
		self.state = TxState::Doing;
		let count = Arc::new(AtomicUsize::new(map.len()));
		let c1 = count.clone();
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			handle_result(r, &tr1, &c1, &cb1)
		});
		for ((ware_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = count.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(&ware_name, &tab_name, Box::new(move |r| {
				match r {
					Ok(t) => match t.key_lock(tkv1.clone(), lock_time, read_lock, bf1.clone()) {
						Some(r) => handle_result(r, &tr2, &c2, &cb2),
						_ => ()
					},
					Err(s) => single_result_err(Err(s), &tr2, &cb2)
				}
			})) {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, t.key_lock(tkv, lock_time, read_lock, bf.clone())) {
						None => (),
						rr => return rr
					}
					Err(s) => return self.single_result_err(Err(s))
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
	) -> Option<SResult<Vec<TabKV>>> {
		let len = arr.len();
		if arr.len() == 0 {
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
		for ((ware_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = rvec.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(&ware_name, &tab_name, Box::new(move |r| match r {
				Ok(t) => match t.query(tkv1.clone(), lock_time, read_lock, bf1.clone()) {
					Some(r) => query_result(r, &tr2, &c2, &cb2),
					_ => ()
				},
				Err(s) => {
					if tr2.cs_state(TxState::Doing, TxState::Err) {
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
								self.state = TxState::Err;
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
	fn modify(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: Option<usize>, read_lock: bool, cb: TxCallback) -> DBResult {
		if arr.len() == 0 {
			return Some(Ok(()))
		}
		let map = tab_map(arr);
		self.state = TxState::Doing;
		let count = Arc::new(AtomicUsize::new(map.len()));
		let c1 = count.clone();
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			handle_result(r, &tr1, &c1, &cb1)
		});
		for ((ware_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = count.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(&ware_name, &tab_name, Box::new(move |r| {
				match r {
					Ok(t) => match t.modify(tkv1.clone(), lock_time, read_lock, bf1.clone()) {
						Some(r) => handle_result(r, &tr2, &c2, &cb2),
						_ => ()
					},
					Err(s) => single_result_err(Err(s), &tr2, &cb2)
				}
			})) {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, t.modify(tkv, lock_time, read_lock, bf.clone())) {
						None => (),
						rr => return rr
					}
					Err(s) => return self.single_result_err(Err(s))
				},
				_ => ()
			}
		}
		None
	}
	// 迭代
	fn iter(&mut self, tr: &Tr, ware: &Atom, tab: &Atom, key: Option<Bin>, descending: bool, filter: Filter, cb: Arc<Fn(IterResult)>) -> Option<IterResult> {
		let tr1 = tr.clone();
		let tr2 = tr.clone();
		let key1 = key.clone();
		let filter1 = filter.clone();
		let cb1 = cb.clone();
		let bf = Arc::new(move |r| {
			iter_result(r, &tr1, &cb1)
		});
		let bf1 = bf.clone();
		self.state = TxState::Doing;
		let tab_clone1 = tab.clone();
		let tab_clone2 = tab.clone();
		match self.build(&ware, &tab, Box::new(move |r| {
			match r {
				Ok(t) => match t.iter(&tab_clone1, key1.clone(), descending, filter1.clone(), bf1.clone()) {
					Some(r) => iter_result(r, &tr2, &cb),
					_ => ()
				},
				Err(s) => if tr2.cs_state(TxState::Doing, TxState::Err) {
					cb(Err(s))
				}
			}
		})) {
			Some(r) => match r {
				Ok(t) => self.iter_result(t.iter(&tab_clone2, key, descending, filter, bf)),
				Err(s) => {
					self.state = TxState::Err;
					Some(Err(s))
				}
			},
			_ => None
		}
	}
	// 迭代
	fn key_iter(&mut self, tr: &Tr, ware: &Atom, tab: &Atom, key: Option<Bin>, descending: bool, filter: Filter, cb: Arc<Fn(KeyIterResult)>) -> Option<KeyIterResult> {
		let tr1 = tr.clone();
		let tr2 = tr.clone();
		let key1 = key.clone();
		let filter1 = filter.clone();
		let cb1 = cb.clone();
		let bf = Arc::new(move |r| {
			iter_result(r, &tr1, &cb1)
		});
		let bf1 = bf.clone();
		self.state = TxState::Doing;
		match self.build(&ware, &tab, Box::new(move |r| {
			match r {
				Ok(t) => match t.key_iter(key1.clone(), descending, filter1.clone(), bf1.clone()) {
					Some(r) => iter_result(r, &tr2, &cb),
					_ => ()
				},
				Err(s) => if tr2.cs_state(TxState::Doing, TxState::Err) {
					cb(Err(s))
				}
			}
		})) {
			Some(r) => match r {
				Ok(t) => self.iter_result(t.key_iter(key, descending, filter, bf)),
				Err(s) => {
					self.state = TxState::Err;
					Some(Err(s))
				}
			},
			_ => None
		}
	}
	// 表的大小
	fn tab_size(&mut self, tr: &Tr, ware_name: &Atom, tab_name: &Atom, cb: Arc<Fn(SResult<usize>)>) -> Option<SResult<usize>> {
		self.state = TxState::Doing;
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			single_result(r, &tr1, &cb1)
		});
		let bf1 = bf.clone();
		let tr2 = tr.clone();
		match self.build(ware_name, tab_name, Box::new(move |r| {
			match r {
				Ok(t) => match t.tab_size(bf1.clone()) {
					Some(r) => single_result(r, &tr2, &cb),
					_ => ()
				},
				Err(s) => single_result_err(Err(s), &tr2, &cb)
			}
		})) {
			Some(r) => match r {
				Ok(t) => match self.single_result(t.tab_size(bf)) {
					None => (),
					rr => return rr
				}
				Err(s) => return self.single_result_err(Err(s))
			},
			_ => ()
		}
		None
	}
	// 新增 修改 删除 表
	fn alter(&mut self, tr: &Tr, ware_name: &Atom, tab_name: &Atom, meta: Option<Arc<TabMeta>>, cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		let ware = match self.ware_log_map.get(ware_name) {
			Some(w) => match w.check(tab_name, &meta) { // 检查
				Ok(_) =>{
					w.alter(tab_name, meta.clone());
					w
				},
				Err(s) => return self.single_result_err(Err(s))
			},
			_ => return self.single_result_err(Err(format!("ware not found:{}", ware_name.as_str()) ))
		};
		let id = &self.id;
		let txn = self.meta_txns.entry(ware_name.clone()).or_insert_with(|| {
			ware.meta_txn(&id)
		}).clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			single_result(r, &tr1, &cb)
		});
		self.single_result(txn.alter(tab_name, meta, bf))
	}
	// 表改名
	fn rename(&mut self, _tr: &Tr, _ware_name: &Atom, _old_name: &Atom, _new_name: Atom, _cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		// TODO
		None
	}
	// 创建表
	fn build(&mut self, ware_name: &Atom, tab_name: &Atom, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>> {
		//let txn_key = Atom::from(String::from((*ware_name).as_str()) + "##" + tab_name.as_str());
		let txn_key = (ware_name.clone(), tab_name.clone());
		let txn = match self.tab_txns.get(&txn_key) {
			Some(r) => return Some(Ok(r.clone())),
			_ => match self.ware_log_map.get(ware_name) {
				Some(ware) => match ware.tab_txn(tab_name, &self.id, self.writable, cb) {
					Some(r) => match r {
						Ok(txn) => txn,
						err => {
							self.state = TxState::Err;
							return Some(err)
						}
					},
					_ => return None
				},
				_ => return Some(Err(String::from("WareNotFound")))
			}
		};
		self.tab_txns.insert(txn_key, txn.clone());
		Some(Ok(txn))
	}
	// 处理同步返回的数量结果
	#[inline]
	fn handle_result(&mut self, count: &Arc<AtomicUsize>, result: DBResult) -> DBResult {
		match &result {
			&Some(ref r) => match r {
				Ok(_) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Ok;
						result
					}else{
						None
					}
				}
				_ => {
					self.state = TxState::Err;
					result
				}
			},
			_ => None
		}
	}
	#[inline]
	fn iter_result<T>(&mut self, result: Option<SResult<T>>) -> Option<SResult<T>> {
		match &result {
			&Some(ref r) => match r {
				Ok(_) => {
					self.state = TxState::Ok;
					result
				}
				_ => {
					self.state = TxState::Err;
					result
				}
			},
			_ => None
		}
	}
	// 处理同步返回的单个结果
	#[inline]
	fn single_result<T>(&mut self, result: Option<SResult<T>>) -> Option<SResult<T>> {
		match result {
			Some(r) => match r {
				Ok(_) => {
					self.state = TxState::Ok;
					Some(r)
				}
				_ => {
					self.state = TxState::Err;
					Some(r)
				}
			},
			_ => None
		}
	}
	#[inline]
	// 处理同步返回的错误
	fn single_result_err<T>(&mut self, r: SResult<T>) -> Option<SResult<T>> {
		self.state = TxState::Err;
		Some(r)
	}
}

//================================ 内部静态方法
// 创建每表的键参数表，不负责键的去重
fn tab_map(mut arr: Vec<TabKV>) -> FnvHashMap<(Atom, Atom), Vec<TabKV>> {
	let mut len = arr.len();
	let mut map = FnvHashMap::with_capacity_and_hasher(len * 3 / 2, Default::default());
	while len > 0 {
		let mut tk = arr.pop().unwrap();
		tk.index = len;
		len -= 1;
		let r = map.entry((tk.ware.clone(), tk.tab.clone())).or_insert(Vec::new());
		r.push(tk);
	}
	return map;
}

// 合并结果集
#[inline]
fn merge_result(rvec: &Arc<Mutex<(usize, Vec<TabKV>)>>, vec: Vec<TabKV>) -> Option<SResult<Vec<TabKV>>> {
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
#[inline]
fn query_result(r: SResult<Vec<TabKV>>, tr: &Tr, rvec: &Arc<Mutex<(usize, Vec<TabKV>)>>, cb: &TxQueryCallback) {
	match r {
		Ok(vec) => {
			match merge_result(rvec, vec) {
				Some(rr) => if tr.cs_state(TxState::Doing, TxState::Ok) {
					cb(rr)
				}
				_ => (),
			}
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			cb(r)
		}
	}
}
// 处理异步返回的数量结果
#[inline]
fn handle_result(r: SResult<()>, tr: &Tr, count: &Arc<AtomicUsize>, cb: &TxCallback) {
	match r {
		Ok(_) => if count.fetch_sub(1, Ordering::SeqCst) == 1 && tr.cs_state(TxState::Doing, TxState::Ok) {
			cb(Ok(()))
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			cb(r)
		}
	}
}
#[inline]
fn iter_result<T>(r: SResult<T>, tr: &Tr, cb: &Arc<Fn(SResult<T>)>) {
	match r {
		Ok(_) => if tr.cs_state(TxState::Doing, TxState::Ok) {
			cb(r)
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			cb(r)
		}
	}
}
// 处理异步返回的单个结果
#[inline]
fn single_result<T>(r: SResult<T>, tr: &Tr, cb: &Arc<Fn(SResult<T>)>) {
	match r {
		Ok(_) => if tr.cs_state(TxState::Doing, TxState::Ok) {
			cb(r)
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			cb(r)
		}
	}
}
// 处理异步返回的错误
#[inline]
fn single_result_err<T>(r: SResult<T>, tr: &Tr, cb: &Arc<Fn(SResult<T>)>) {
	if tr.cs_state(TxState::Doing, TxState::Err) {
		cb(r)
	}
}

#[cfg(test)]
use memery_db;
#[cfg(test)]
use bon::{WriteBuffer, ReadBuffer, Encode, Decode, ReadBonErr};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use sinfo::StructInfo;

#[cfg(test)]
#[derive(Debug)]
struct Player{
	name: String,
	id: u32,
}

#[cfg(test)]
impl Encode for Player{
	fn encode(&self, bb: &mut WriteBuffer){
		self.name.encode(bb);
		self.id.encode(bb);
	}
}

#[cfg(test)]
impl Decode for Player{
	fn decode(bb: &mut ReadBuffer) -> Result<Self, ReadBonErr>{
		Ok(Player{
			name: String::decode(bb)?,
			id: u32::decode(bb)?,
		})
	}
}

#[test]
fn test_memery_db_mgr(){

	let mgr = Mgr::new(GuidGen::new(1,1));
	let db = memery_db::DB::new();
	mgr.register(Atom::from("memery"), Arc::new(db));
	let mgr = Arc::new(mgr);

	let tr = mgr.transaction(true);
	let tr1 = tr.clone();
	let sinfo = EnumType::Struct(Arc::new(StructInfo::new(Atom::from("Player"), 55555555)));
	let mut m = HashMap::new();
	m.insert(Atom::from("class"), Atom::from("memery"));
	let alter_back = Arc::new(move|r: SResult<()>|{
		println!("alter: {:?}", r);
		assert!(r.is_ok());
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
				let tr = mgr2.transaction(false);
				let tr1 = tr.clone();
				let mut arr = Vec::new();
				let mut key = WriteBuffer::new();
				key.write_u8(5);
				let t1 = TabKV{
					ware: Atom::from("memery"),
					tab: Atom::from("Player"),
					key: Arc::new(key.unwrap()),
					index: 0,
					value: None,
				};
				arr.push(t1);

				let read_back = Arc::new(move|r: SResult<Vec<TabKV>>|{
					assert!(r.is_ok());
					match r {
						Ok(mut v) => {
							println!("read:ok");
							for elem in v.iter_mut(){
								match elem.value {
									Some(ref mut v) => {
										println!("kkkkkkkkkkkkk{:?}", v.len());
										let mut buf = ReadBuffer::new(Arc::make_mut(v),0);
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

			let tr = mgr1.transaction(true);
			let tr1 = tr.clone();
			let p = Player{
				name: String::from("chuanyan"),
				id:5
			};
			let mut bonbuf = WriteBuffer::new();
			p.encode(&mut bonbuf);
			let v = bonbuf.unwrap();
			println!("vvvvvvvvvvvvvvvvvvvvvvvvvvvvv{}", v.len());

			let mut arr = Vec::new();
			let mut key = WriteBuffer::new();
			key.write_u8(5);
			let t1 = TabKV{
				ware: Atom::from("memery"),
				tab: Atom::from("Player"),
				key: Arc::new(key.unwrap()),
				index: 0,
				value: Some(Arc::new(v)),
			};
			arr.push(t1);

			let write_back = Arc::new(move|r: SResult<()>|{
				println!("write: {:?}", r);
				assert!(r.is_ok());
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
	let r = tr.alter(&Atom::from("memery"), &Atom::from("Player"), Some(Arc::new(TabMeta{
		k:EnumType::U8,
		v:sinfo
	})), alter_back.clone());
	if r.is_some(){
		alter_back(r.unwrap());
	}
}