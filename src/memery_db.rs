
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::cell::RefCell;
use std::mem;

use fnv::FnvHashMap;

use pi_lib::ordmap::{OrdMap, Entry};
use pi_lib::asbtree::{Tree, new};
use pi_lib::atom::{Atom};
use pi_lib::guid::Guid;
use pi_lib::sinfo::StructInfo;

use db::{Txn, TabTxn, TabKV, TxIterCallback, TxQueryCallback, MetaTxn, Tab, Ware, TxCallback, TxState, Cursor, SResult, DBResult};
use tabs::{TabLog, Tabs};

pub type Rwlog = HashMap<Arc<Vec<u8>>, RwLog>;
pub type MemeryKV = Tree<Arc<Vec<u8>>, Arc<Vec<u8>>>;
pub type Root = OrdMap<MemeryKV>;

// 内存库
pub struct MemeryDB(RwLock<Tabs>);

impl MemeryDB {
	pub fn new() -> Self {
		MemeryDB(RwLock::new(Tabs::new()))
	}
}
impl Ware for MemeryDB {
	// 拷贝全部的表
	// fn tabs_clone(&self) -> Self {
	// 	self.clone()
	// }
	// 列出全部的表
	fn list(&self) -> Vec<Atom> {
		return vec![]
	}
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	fn timeout(&self) -> usize {
		TIMEOUT
	}
	// 表的元信息
	fn tab_info(&self, _tab_name: &Atom) -> Option<Arc<StructInfo>> {
		None
	}
	// 打开指定的表，表必须有meta
	fn open(
		&self,
		tab: &Atom,
		_cb: Box<Fn(SResult<Arc<Tab>>)>,
	) -> Option<SResult<Arc<Tab>>> {
		let tree:MemeryKV = None;
		let root= OrdMap::new(tree);
		let tab = MemeryTab {
			prepare: HashMap::new(),
			root: root,
			tab: tab.clone(),
		};
		let tab: ArcMutexTab = Arc::new(Mutex::new(tab));
		Some(Ok(Arc::new(tab)))
	}
	// 获取当前表结构快照
	fn snapshot(&self) -> TabLog {
		self.0.read().unwrap().snapshot()
	}
	// 创建指定表的表事务
	fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>> {
		self.0.read().unwrap().build(self, tab_name, id, writable, cb)
	}
	// 检查该表是否可以创建
	fn check(
		&self,
		tab: &Atom,
		meta: &Option<Arc<StructInfo>>,
	) -> SResult<()> {
		Ok(())
	}

	// 创建一个meta事务
	fn meta_txn(&self, id: &Guid) -> Arc<MetaTxn> {
		Arc::new(MemeryMetaTxn())
	}
	// 元信息的预提交
	fn prepare(&self, id: &Guid, log: &mut TabLog) -> SResult<()>{
		self.0.write().unwrap().prepare(id, log)
	}
	// 元信息的提交
	fn commit(&self, id: &Guid){
		self.0.write().unwrap().commit(id)
	}
	// 回滚
	fn rollback(&self, id: &Guid){
		self.0.write().unwrap().rollback(id)
	}
}

// 内存表
pub struct MemeryTab {
	pub prepare: HashMap<Guid, Rwlog>,
	pub root: Root,
	pub tab: Atom,
}
pub type ArcMutexTab = Arc<Mutex<MemeryTab>>;
impl Tab for ArcMutexTab {
	fn transaction(&self, id: &Guid, _writable: bool) -> Arc<TabTxn> {
		let txn = MemeryTxn::new(self.clone(), id);
		return Arc::new(txn)
	}

}

// 内存事务
pub struct MemeryTxn {
	id: Guid,
	tab: ArcMutexTab,
	root: Root,
	old: Root,
	rwlog: Rwlog,
	state: TxState,
}

pub type RefMemeryTxn = RefCell<MemeryTxn>;

impl MemeryTxn {
	//开始事务
	pub fn new(tab: ArcMutexTab, id: &Guid) -> RefMemeryTxn {
		let root = tab.lock().unwrap().root.clone();
		let txn = MemeryTxn {
			id: id.clone(),
			tab: tab,
			root: root.clone(),
			old: root,
			rwlog: HashMap::new(),
			state: TxState::Ok,
		};
		return RefCell::new(txn)
	}
	//获取数据
	pub fn get(&mut self, key: Arc<Vec<u8>>) -> Option<Arc<Vec<u8>>> {
		match self.root.get(&key) {
			Some(v) => {
				match self.rwlog.get(&key) {
					Some(_) => (),
					None => {
						&mut self.rwlog.insert(key, RwLog::Read);
						()
					}
				}
				return Some(v.clone())
			},
			None => return None
		}
	}
	//插入/修改数据
	pub fn upsert(&mut self, key: Arc<Vec<u8>>, value: Arc<Vec<u8>>) -> SResult<()> {
		self.root.upsert(key.clone(), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));
		Ok(())
	}
	//删除
	pub fn delete(&mut self, key: Arc<Vec<u8>>) -> SResult<()> {
		self.root.delete(&key, false);
		self.rwlog.insert(key.clone(), RwLog::Write(None));
		Ok(())
	}
	//迭代
	pub fn select<F>(&self, key: Option<&Arc<Vec<u8>>>, descending: bool, func: &mut F) where F: FnMut(&Entry<Arc<Vec<u8>>, Arc<Vec<u8>>>) {
		self.root.select(key, descending, func)
	}
	//预提交
	pub fn prepare1(&mut self) -> SResult<()> {
		let mut tab = self.tab.lock().unwrap();
		//遍历事务中的读写日志
		for (key, rw_v) in self.rwlog.iter() {
			//检查预提交是否冲突
			for o_rwlog in tab.prepare.values() {
				match o_rwlog.get(key) {
					Some(RwLog::Read) => match rw_v {
						RwLog::Read => (),
						_ => return Err(String::from("parpare conflicted rw"))
					},
					None => (),
					Some(_e) => {
						return Err(String::from("parpare conflicted rw2"))
					},
				}
			}
			//检查Tab根节点是否改变
			if tab.root.ptr_eq(&self.old) == false {
				match tab.root.get(&key) {
					Some(r1) => match self.old.get(&key) {
						Some(r2) if (r1 as *const Arc<Vec<u8>>) == (r2 as *const Arc<Vec<u8>>) => (),
						_ => return Err(String::from("parpare conflicted value diff"))
					},
					_ => match self.old.get(&key) {
						None => (),
						_ => return Err(String::from("parpare conflicted old not None"))
					}
				}
			}
		}
		let rwlog = mem::replace(&mut self.rwlog, HashMap::with_capacity_and_hasher(0, Default::default()));
		//写入预提交
		tab.prepare.insert(self.id.clone(), rwlog);
		return Ok(())
	}
	//提交
	pub fn commit1(&mut self) -> SResult<()> {
		let mut tab = self.tab.lock().unwrap();
		let mut write = Vec::new();
		let root_if_eq = tab.root.ptr_eq(&self.old);
		//判断根节点是否相等
		if root_if_eq == false {
			match tab.prepare.get(&self.id) {
				Some(rwlog) => {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							RwLog::Read => (),
							_ => {
								write.push((k.clone(), rw_v.clone()));
								()
							},
						}
					}
				},
				None => {
					return Err(String::from("error prepare null"))
					},
			}
			for (k, rw_v) in write {
				match rw_v {
					RwLog::Write(None) => {
						tab.root.delete(&k, false);
						()
					},
					RwLog::Write(Some(v)) => {
						tab.root.upsert(k, v.clone(), false);
						()
					},
					_ => (),
				}
			}
			//删除预提交
			tab.prepare.remove(&self.id);
		} else {
			tab.root = self.root.clone();
			//删除预提交
			tab.prepare.remove(&self.id);
		}
		Ok(())
	}
	//回滚
	pub fn rollback1(&mut self) -> SResult<()> {
		self.tab.lock().unwrap().prepare.remove(&self.id);
		Ok(())
	}
}

impl Txn for RefMemeryTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		self.borrow().state.clone()
	}
	// 预提交一个事务
	fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Preparing;
		match txn.prepare1() {
			Ok(()) => {
				txn.state = TxState::PreparOk;
				return Some(Ok(()))
			},
			Err(e) => {
				txn.state = TxState::PreparFail;
				return Some(Err(e.to_string()))
			},
		}
	}
	// 提交一个事务
	fn commit(&self, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Committing;
		match txn.commit1() {
			Ok(()) => {
				txn.state = TxState::Commited;
				return Some(Ok(()))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Rollbacking;
		match txn.rollback1() {
			Ok(()) => {
				txn.state = TxState::Rollbacked;
				return Some(Ok(()))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
}

impl TabTxn for RefMemeryTxn {
	// 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
	fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool, _cb: TxCallback) -> DBResult {
		None
	}
	// 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool,
		_cb: TxQueryCallback,
	) -> Option<SResult<Vec<TabKV>>> {
		let mut txn = self.borrow_mut();
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let mut value = None;
			match txn.get(tabkv.key.clone()) {
				Some(v) =>
					{
						value = Some(v);
						()
					},
				_ =>
					{
						return Some(Err(String::from("null")))
					},
			}
			value_arr.push(
				TabKV{
				ware: tabkv.ware.clone(),
				tab: tabkv.tab.clone(),
				key: tabkv.key.clone(),
				index: tabkv.index.clone(),
				value: value,
				}
			)
		}
		Some(Ok(value_arr))
	}
	// 修改，插入、删除及更新
	fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
		for tabkv in arr.iter() {
			if tabkv.value == None {
				match txn.delete(tabkv.key.clone()) {
				Ok(_) => (),
				Err(e) => 
					{
						return Some(Err(e.to_string()))
					},
				};
			} else {
				match txn.upsert(tabkv.key.clone(), tabkv.value.clone().unwrap()) {
				Ok(_) => (),
				Err(e) =>
					{
						return Some(Err(e.to_string()))
					},
				};
			}
		}
		Some(Ok(()))
	}
	// 迭代
	fn iter(
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
	fn index(
		&self,
		_tab: &Atom,
		_index_key: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_filter: String,
		_cb: TxIterCallback,
	) -> Option<SResult<Box<Cursor>>> {
		None
	}
	// 表的大小
	fn tab_size(&self, _cb: TxCallback) -> DBResult {
		None
	}
}





//================================ 内部结构和方法
const TIMEOUT: usize = 100;

#[derive(Clone, Debug)]
pub enum RwLog {
	Read,
	Write(Option<Arc<Vec<u8>>>),
}

pub struct MemeryMetaTxn();

impl MetaTxn for MemeryMetaTxn {
	// 创建表、修改指定表的元数据
	fn alter(
		&self,
		_tab: &Atom,
		_meta: Option<Arc<StructInfo>>,
		_cb: TxCallback,
	) -> DBResult {
		Some(Ok(()))
	}
	// 修改指定表的名字
	fn rename(
		&self,
		_tab: &Atom,
		_new_name: &Atom,
		_cb: TxCallback,
	) -> DBResult {
		Some(Ok(()))
	}
}
impl Txn for MemeryMetaTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		TxState::Ok
	}
	// 预提交一个事务
	fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
	// 提交一个事务
	fn commit(&self, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
}
//================================ 内部静态方法
