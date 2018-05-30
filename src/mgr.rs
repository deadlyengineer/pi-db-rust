/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::mem;

use fnv::FnvHashMap;

use pi_lib::ordmap::{OrdMap, Entry};
use pi_lib::asbtree::{Tree, new};
use pi_lib::atom::Atom;
use pi_lib::sinfo::StructInfo;
use pi_lib::guid::{Guid, GuidGen};

use tabs::TabLog;
use db::{SResult, DBResult, Cursor, TabKV, TxCallback, TxIterCallback, TxQueryCallback, TxState, MetaTxn, TabTxn, Ware};

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
		Mgr(Arc::new(Mutex::new(Manager::new())), Arc::new(Mutex::new(WareMap::new())), self.2.clone(), self.3.clone())
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
	pub fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<StructInfo>> {
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
		self.0.lock().unwrap().transaction(map, writable, id)
	}
	// 寻找指定的库
	fn find(&self, ware_name: &Atom) -> Option<Arc<Ware>> {
		let map = {
			self.1.lock().unwrap().clone()
		};
		map.find(ware_name)
	}
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
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 提交一个事务
	pub fn commit(&self, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::PreparOk => t.commit(self, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 回滚一个事务
	pub fn rollback(&self, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Committing|TxState::Commited|TxState::CommitFail|TxState::Rollbacking|TxState::Rollbacked|TxState::RollbackFail =>
				return Some(Err(String::from("InvalidState"))),
			_ => t.rollback(self, cb)
		}
	}
	// 锁
	pub fn key_lock(&self, arr: Vec<TabKV>, lock_time: usize, read_lock: bool, cb: TxCallback) -> DBResult {
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
	) -> Option<SResult<Vec<TabKV>>> {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.query(self, arr, lock_time, read_lock, cb),
			_ => Some(Err(String::from("InvalidState"))),
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
	) -> Option<SResult<Vec<TabKV>>> {
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
	) -> Option<SResult<Box<Cursor>>> {
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
	) -> Option<SResult<Box<Cursor>>> {
		None
	}
	// 列出指定库的所有表
	pub fn list(&self, _ware: &Atom) -> Option<Vec<Atom>> {
		None
	}
	// 表的元信息
	pub fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		match self.0.lock().unwrap().ware_log_map.get(ware_name) {
			Some((ware, _)) => ware.tab_info(tab_name),
			_ => None
		}
	}
	// 表的大小
	pub fn tab_size(&self, ware_name:&Atom, tab_name: &Atom, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		match t.state {
			TxState::Ok => t.tab_size(self, ware_name, tab_name, cb),
			_ => Some(Err(String::from("InvalidState"))),
		}
	}
	// 创建、修改或删除表
	pub fn alter(&self, ware_name:&Atom, tab_name: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> DBResult {
		let mut t = self.0.lock().unwrap();
		if !t.writable {
			return Some(Err(String::from("Readonly")))
		}
		match t.state {
			TxState::Ok => t.alter(self, ware_name, tab_name, meta, cb),
			_ => Some(Err(String::from("InvalidState"))),
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
const TIMEOUT: usize = 100;

// 事务管理器
struct Manager {
	// 定时轮
	// 管理用的弱引用事务
	weak_map: FnvHashMap<Guid, Weak<Mutex<Tx>>>,
}
impl Manager {
	// 注册管理器
	fn new() -> Self {
		Manager {
			weak_map: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}
	// 创建事务
	fn transaction(&mut self, ware_map: WareMap, writable: bool, id: Guid) -> Tr {
		// 遍历ware_map, 将每个Ware的快照TabLog记录下来
		let mut map = FnvHashMap::with_capacity_and_hasher(ware_map.0.size() * 3 / 2, Default::default());
		let mut f = |e: &Entry<Atom, Arc<Ware>>| {
			map.insert(e.key().clone(), (e.value().clone(), e.value().snapshot()));
		};
		ware_map.0.select(None, false, &mut f);
		let tr = Tr(Arc::new(Mutex::new(Tx {
			writable: writable,
			timeout: TIMEOUT,
			id: id.clone(),
			ware_log_map: map,
			state: TxState::Ok,
			_timer_ref: 0,
			tab_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			meta_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		})));
		self.weak_map.insert(id, Arc::downgrade(&(tr.0)));
		tr
	}

}

// 库表
#[derive(Clone)]
struct WareMap(OrdMap<Tree<Atom, Arc<Ware>>>);

impl WareMap {
	fn new() -> Self {
		WareMap(OrdMap::new(new()))
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
}


// 子事务
struct Tx {
	// TODO 下面几个可以放到锁的外部，减少锁
	writable: bool,
	timeout: usize, // 子事务的预提交的超时时间, TODO 取提交的库的最大超时时间
	id: Guid,
	ware_log_map: FnvHashMap<Atom, (Arc<Ware>, TabLog)>,// 库名对应库及其所有表的快照
	state: TxState,
	_timer_ref: usize,
	tab_txns: FnvHashMap<Atom, Arc<TabTxn>>, //表事务表
	meta_txns: FnvHashMap<Atom, Arc<MetaTxn>>, //元信息事务表
}

impl Tx {
	// 预提交事务
	fn prepare(&mut self, tr: &Tr, cb: TxCallback) -> DBResult {
		self.state = TxState::Preparing;
		// 先检查mgr上的meta alter的预提交
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				let (ware, log) = self.ware_log_map.get_mut(ware).unwrap();
				match ware.prepare(&self.id, log) {
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
					(*cb)(Ok(()))
				}
			}
			_ => if tr1.cs_state(TxState::Preparing, TxState::PreparFail) {
				(*cb)(r)
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
				self.ware_log_map.get(ware).unwrap().0.commit(&self.id);
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
				(*cb)(Ok(()))
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
						return Some(Ok(()))
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
				self.ware_log_map.get(ware).unwrap().0.rollback(&self.id);
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
				(*cb)(Ok(()))
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
		for ((war_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = count.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(&war_name, &tab_name, Box::new(move |r| {
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
		for ((war_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = rvec.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(&war_name, &tab_name, Box::new(move |r| match r {
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
		for ((war_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let bf1 = bf.clone();
			let c2 = count.clone();
			let cb2 = cb.clone();
			let tr2 = tr.clone();
			match self.build(&war_name, &tab_name, Box::new(move |r| {
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
	// 表的大小
	fn tab_size(&mut self, tr: &Tr, war_name: &Atom, tab_name: &Atom, cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		let cb1 = cb.clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			single_result(r, &tr1, &cb1)
		});
		let bf1 = bf.clone();
		let tr2 = tr.clone();
		match self.build(war_name, tab_name, Box::new(move |r| {
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
	fn alter(&mut self, tr: &Tr, war_name: &Atom, tab_name: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		let ware = match self.ware_log_map.get_mut(war_name) {
			Some((ware, log)) => match ware.check(tab_name, &meta) { // 检查
				Ok(_) =>{
					log.alter(tab_name, meta.clone());
					ware
				},
				Err(s) => return self.single_result_err(Err(s))
			},
			_ => return self.single_result_err(Err(String::from("ware not found")))
		};
		let id = &self.id;
		let txn = self.meta_txns.entry(war_name.clone()).or_insert_with(|| {
			ware.meta_txn(id)
		}).clone();
		let tr1 = tr.clone();
		let bf = Arc::new(move |r| {
			single_result(r, &tr1, &cb)
		});
		self.single_result(txn.alter(tab_name, meta, bf))
	}
	// 表改名
	fn rename(&mut self, _tr: &Tr, _war_name: &Atom, _old_name: &Atom, _new_name: Atom, _cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		// TODO
		None
	}
	// 创建表
	fn build(&mut self, war_name: &Atom, tab_name: &Atom, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>> {
		let txn = match self.tab_txns.get(tab_name) {
			Some(r) => return Some(Ok(r.clone())),
			_ => match self.ware_log_map.get_mut(war_name) {
				Some((ware, _)) => match ware.tab_txn(tab_name, &self.id, self.writable, cb) {
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
		self.tab_txns.insert(tab_name.clone(), txn.clone());
		Some(Ok(txn))
	}
	// 处理同步返回的数量结果
	#[inline]
	fn handle_result(&mut self, count: &Arc<AtomicUsize>, result: Option<SResult<()>>) -> DBResult {
		match result {
			Some(r) => match r {
				Ok(()) => {
					if count.fetch_sub(1, Ordering::SeqCst) == 1 {
						self.state = TxState::Ok;
						Some(Ok(()))
					}else{
						None
					}
				}
				_ => {
					self.state = TxState::Err;
					Some(r)
				}
			},
			_ => None
		}
	}
	// 处理同步返回的单个结果
	#[inline]
	fn single_result(&mut self, result: Option<SResult<()>>) -> DBResult {
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
	fn single_result_err(&mut self, r: SResult<()>) -> DBResult {
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
					(*cb)(rr)
				}
				_ => (),
			}
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			(*cb)(r)
		}
	}
}
// 处理异步返回的数量结果
#[inline]
fn handle_result(r: SResult<()>, tr: &Tr, count: &Arc<AtomicUsize>, cb: &TxCallback) {
	match r {
		Ok(_) => if count.fetch_sub(1, Ordering::SeqCst) == 1 && tr.cs_state(TxState::Doing, TxState::Ok) {
			(*cb)(Ok(()))
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			(*cb)(r)
		}
	}
}
// 处理异步返回的单个结果
#[inline]
fn single_result(r: SResult<()>, tr: &Tr, cb: &TxCallback) {
	match r {
		Ok(_) => if tr.cs_state(TxState::Doing, TxState::Ok) {
			(*cb)(r)
		},
		_ => if tr.cs_state(TxState::Doing, TxState::Err) {
			(*cb)(r)
		}
	}
}
// 处理异步返回的错误
#[inline]
fn single_result_err(r: SResult<()>, tr: &Tr, cb: &TxCallback) {
	if tr.cs_state(TxState::Doing, TxState::Err) {
		(*cb)(r)
	}
}

#[cfg(test)]
use memery_db;
#[cfg(test)]
use pi_lib::bon::{WriteBuffer, ReadBuffer, Encode, Decode};
#[cfg(test)]
use std::collections::HashMap;

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
	fn decode(bb: &mut ReadBuffer) -> Self{
		Player{
			name: String::decode(bb),
			id: u32::decode(bb),
		}
	}
}

#[test]
fn test_memery_db_mgr(){

	let mgr = Mgr::new(GuidGen::new(1,1));
	mgr.register(Atom::from("memery"), Arc::new(memery_db::MemeryDB::new()));
	let mgr = Arc::new(mgr);

	let tr = mgr.transaction(true);
	let tr1 = tr.clone();
	let mut sinfo = StructInfo::new(Atom::from("Player"), 55555555);
	let mut m = HashMap::new();
	m.insert(Atom::from("class"), Atom::from("memery"));
	sinfo.notes = Some(m);
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
				let t1 = TabKV{
					ware: Atom::from("memery"),
					tab: Atom::from("Player"),
					key: Arc::new(vec![5u8]),
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

			let mut arr = Vec::new();
			let t1 = TabKV{
				ware: Atom::from("memery"),
				tab: Atom::from("Player"),
				key: Arc::new(vec![5u8]),
				index: 0,
				value: Some(Arc::new(bonbuf.unwrap())),
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
	let r = tr.alter(&Atom::from("memery"), &Atom::from("Player"), Some(Arc::new(sinfo)), alter_back.clone());
	if r.is_some(){
		alter_back(r.unwrap());
	}
}

// #[test]
// fn test_file_db_mgr(){

// 	let mgr = Mgr::new(GuidGen::new(1,1));
// 	mgr.register(&Atom::from("file"), Arc::new(memery_db::MemeryDB::new(Atom::from("file"))));
// 	let mgr = Arc::new(mgr);

// 	let tr = mgr.transaction(true, 1000);
// 	let tr1 = tr.clone();
// 	let mut sinfo = StructInfo::new(Atom::from("Player"), 55555555);
// 	let mut m = HashMap::new();
// 	m.insert(Atom::from("class"), Atom::from("memery"));
// 	sinfo.notes = Some(m);
// 	let alter_back = Arc::new(move|r: SResult<()>|{
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

// 				let read_back = Arc::new(move|r: SResult<Vec<TabKV>>|{
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

