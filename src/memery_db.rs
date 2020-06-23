
use std::sync::{Arc};
use std::cell::RefCell;
use std::mem;

use fnv::FnvHashMap;

use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::{Tree};
use atom::{Atom};
use guid::Guid;
use apm::counter::{GLOBAL_PREF_COLLECT, PrefCounter};

use crate::db::{Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult, NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn, Tab, OpenTab, Event, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult, RwLog, Bon, TabMeta};
use crate::tabs::{TabLog, Tabs, Prepare};
use crate::db::BuildDbType;
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;

//内存库前缀
const MEMORY_WARE_PREFIX: &'static str = "mem_ware_";
//内存表前缀
const MEMORY_TABLE_PREFIX: &'static str = "mem_table_";
//内存表事务创建数量后缀
const MEMORY_TABLE_TRANS_COUNT_SUFFIX: &'static str = "_trans_count";
//内存表事务预提交数量后缀
const MEMORY_TABLE_PREPARE_COUNT_SUFFIX: &'static str = "_prepare_count";
//内存表事务提交数量后缀
const MEMORY_TABLE_COMMIT_COUNT_SUFFIX: &'static str = "_commit_count";
//内存表事务回滚数量后缀
const MEMORY_TABLE_ROLLBACK_COUNT_SUFFIX: &'static str = "_rollback_count";
//内存表读记录数量后缀
const MEMORY_TABLE_READ_COUNT_SUFFIX: &'static str = "_read_count";
//内存表读记录字节数量后缀
const MEMORY_TABLE_READ_BYTE_COUNT_SUFFIX: &'static str = "_read_byte_count";
//内存表写记录数量后缀
const MEMORY_TABLE_WRITE_COUNT_SUFFIX: &'static str = "_write_count";
//内存表写记录字节数量后缀
const MEMORY_TABLE_WRITE_BYTE_COUNT_SUFFIX: &'static str = "_write_byte_count";
//内存表删除记录数量后缀
const MEMORY_TABLE_REMOVE_COUNT_SUFFIX: &'static str = "_remove_count";
//内存表删除记录字节数量后缀
const MEMORY_TABLE_REMOVE_BYTE_COUNT_SUFFIX: &'static str = "_remove_byte_count";
//内存表关键字迭代数量后缀
const MEMORY_TABLE_KEY_ITER_COUNT_SUFFIX: &'static str = "_key_iter_count";
//内存表关键字迭代字节数量后缀
const MEMORY_TABLE_KEY_ITER_BYTE_COUNT_SUFFIX: &'static str = "_key_iter_byte_count";
//内存表迭代数量后缀
const MEMORY_TABLE_ITER_COUNT_SUFFIX: &'static str = "_iter_count";
//内存表关键字迭代字节数量后缀
const MEMORY_TABLE_ITER_BYTE_COUNT_SUFFIX: &'static str = "_iter_byte_count";

lazy_static! {
	//内存库创建数量
	static ref MEMORY_WARE_CREATE_COUNT: PrefCounter = GLOBAL_PREF_COLLECT.new_static_counter(Atom::from("mem_ware_create_count"), 0).unwrap();
	//内存表创建数量
	static ref MEMORY_TABLE_CREATE_COUNT: PrefCounter = GLOBAL_PREF_COLLECT.new_static_counter(Atom::from("mem_table_create_count"), 0).unwrap();
}

#[derive(Clone)]
pub struct MTab(Arc<Mutex<MemeryTab>>);

impl MTab {
	pub fn new(tab: &Atom) -> Self {
		MEMORY_WARE_CREATE_COUNT.sum(1);

		let tab = MemeryTab {
			prepare: Prepare::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::new(None),
			tab: tab.clone(),
			trans_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_TRANS_COUNT_SUFFIX), 0).unwrap(),
			prepare_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_PREPARE_COUNT_SUFFIX), 0).unwrap(),
			commit_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_COMMIT_COUNT_SUFFIX), 0).unwrap(),
			rollback_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_ROLLBACK_COUNT_SUFFIX), 0).unwrap(),
			read_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_READ_COUNT_SUFFIX), 0).unwrap(),
			read_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_READ_BYTE_COUNT_SUFFIX), 0).unwrap(),
			write_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_WRITE_COUNT_SUFFIX), 0).unwrap(),
			write_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_WRITE_BYTE_COUNT_SUFFIX), 0).unwrap(),
			remove_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_REMOVE_COUNT_SUFFIX), 0).unwrap(),
			remove_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_REMOVE_BYTE_COUNT_SUFFIX), 0).unwrap(),
		};
		MTab(Arc::new(Mutex::new(tab)))
	}
	pub async fn transaction(&self, id: &Guid, writable: bool) -> Arc<RefMemeryTxn> {
		self.0.lock().await.trans_count.sum(1);

		let txn = MemeryTxn::new(self.clone(), id, writable).await;
		return Arc::new(txn)
	}
}

/**
* 内存库
*/
#[derive(Clone)]
pub struct DB(Arc<RwLock<Tabs>>);

impl DB {
	/**
	* 构建内存库
	* @returns 返回内存库
	*/
	pub fn new() -> Self {
		MEMORY_WARE_CREATE_COUNT.sum(1);

		DB(Arc::new(RwLock::new(Tabs::new())))
	}

	// TODO
	// 打开指定的表，表必须有meta
	// fn open<'a, T: Tab>(&self, tab: &Atom, _cb: Box<dyn Fn(SResult<T>) + 'a>) -> Option<SResult<T>> {
	// 	Some(Ok(T::new(tab)))
	// }

	pub async fn open(tab: &Atom) -> Option<SResult<MTab>> {
		Some(Ok(MTab::new(tab)))
	}

	// 拷贝全部的表
	pub async fn tabs_clone(&self) -> Arc<Self> {
		Arc::new(DB(Arc::new(RwLock::new(self.0.read().await.clone_map()))))
	}
	// 列出全部的表
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		Box::new(self.0.read().await.list())
	}
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	pub fn timeout(&self) -> usize {
		TIMEOUT
	}
	// 表的元信息
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.0.read().await.get(tab_name)
	}
	// 获取当前表结构快照
	pub async fn snapshot(&self) -> Arc<DBSnapshot> {
		Arc::new(DBSnapshot(self.clone(), Mutex::new(self.0.read().await.snapshot())))
	}
}

// 内存库快照
pub struct DBSnapshot(DB, Mutex<TabLog>);

impl DBSnapshot {
	// 列出全部的表
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		Box::new(self.1.lock().await.list())
	}
	// 表的元信息
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.1.lock().await.get(tab_name)
	}
	// 检查该表是否可以创建
	pub fn check(&self, _tab: &Atom, _meta: &Option<Arc<TabMeta>>) -> SResult<()> {
		Ok(())
	}
	// 新增 修改 删除 表
	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		self.1.lock().await.alter(tab_name, meta)
	}
	// 创建指定表的表事务
	pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> Option<SResult<Arc<RefMemeryTxn>>> {
		self.1.lock().await.build(BuildDbType::MemoryDB, tab_name, id, writable).await
	}
	// 创建一个meta事务
	pub fn meta_txn(&self, _id: &Guid) -> Arc<MemeryMetaTxn> {
		Arc::new(MemeryMetaTxn)
	}
	// 元信息的预提交
	pub async fn prepare(&self, id: &Guid) -> SResult<()>{
		(self.0).0.write().await.prepare(id, &mut *self.1.lock().await)
	}
	// 元信息的提交
	pub async fn commit(&self, id: &Guid){
		(self.0).0.write().await.commit(id)
	}
	// 回滚
	pub async fn rollback(&self, id: &Guid){
		(self.0).0.write().await.rollback(id)
	}
	// 库修改通知
	pub fn notify(&self, _event: Event) {}
}

// 内存事务
pub struct MemeryTxn {
	id: Guid,
	writable: bool,
	tab: MTab,
	root: BinMap,
	old: BinMap,
	rwlog: FnvHashMap<Bin, RwLog>,
	state: TxState,
}

pub struct RefMemeryTxn(RefCell<MemeryTxn>);

impl MemeryTxn {
	//开始事务
	pub async fn new(tab: MTab, id: &Guid, writable: bool) -> RefMemeryTxn {
		let root = tab.0.lock().await.root.clone();
		let txn = MemeryTxn {
			id: id.clone(),
			writable: writable,
			root: root.clone(),
			tab: tab,
			old: root,
			rwlog: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			state: TxState::Ok,
		};
		return RefMemeryTxn(RefCell::new(txn))
	}
	//获取数据
	pub async fn get(&mut self, key: Bin) -> Option<Bin> {
		self.tab.0.lock().await.read_count.sum(1);

		match self.root.get(&Bon::new(key.clone())) {
			Some(v) => {
				if self.writable {
					match self.rwlog.get(&key) {
						Some(_) => (),
						None => {
							&mut self.rwlog.insert(key, RwLog::Read);
							()
						}
					}
				}

				self.tab.0.lock().await.read_byte.sum(v.len());

				return Some(v.clone())
			},
			None => return None
		}
	}
	//插入/修改数据
	pub async fn upsert(&mut self, key: Bin, value: Bin) -> SResult<()> {
		self.root.upsert(Bon::new(key.clone()), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));

		{
			let tab = self.tab.0.lock().await;
			tab.write_byte.sum(value.len());
			tab.write_count.sum(1);
		}

		Ok(())
	}
	//删除
	pub async fn delete(&mut self, key: Bin) -> SResult<()> {
		if let Some(Some(value)) = self.root.delete(&Bon::new(key.clone()), false) {
			{
				let tab = self.tab.0.lock().await;
				tab.remove_byte.sum(key.len() + value.len());
				tab.remove_count.sum(1);
			}
		}
		self.rwlog.insert(key, RwLog::Write(None));

		Ok(())
	}

	//预提交
	pub async fn prepare1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().await;
		//遍历事务中的读写日志
		for (key, rw_v) in self.rwlog.iter() {
			//检查预提交是否冲突
			match tab.prepare.try_prepare(key, rw_v) {
				Ok(_) => (),
				Err(s) => return Err(s),
			};
			//检查Tab根节点是否改变
			if tab.root.ptr_eq(&self.old) == false {
				let key = Bon::new(key.clone());
				match tab.root.get(&key) {
					Some(r1) => match self.old.get(&key) {
						Some(r2) if (r1 as *const Bin) == (r2 as *const Bin) => (),
						_ => return Err(String::from("parpare conflicted value diff"))
					},
					_ => match self.old.get(&key) {
						None => (),
						_ => return Err(String::from("parpare conflicted old not None"))
					}
				}
			}
		}
		let rwlog = mem::replace(&mut self.rwlog, FnvHashMap::with_capacity_and_hasher(0, Default::default()));
		//写入预提交
		tab.prepare.insert(self.id.clone(), rwlog);

		tab.prepare_count.sum(1);

		return Ok(())
	}
	//提交
	pub async fn commit1(&mut self) -> SResult<FnvHashMap<Bin, RwLog>> {
		let mut tab = self.tab.0.lock().await;
		let log = match tab.prepare.remove(&self.id) {
			Some(rwlog) => {
				let root_if_eq = tab.root.ptr_eq(&self.old);
				//判断根节点是否相等
				if root_if_eq == false {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							RwLog::Read => (),
							_ => {
								let k = Bon::new(k.clone());
								match rw_v {
									RwLog::Write(None) => {
										tab.root.delete(&k, false);
										()
									},
									RwLog::Write(Some(v)) => {
										tab.root.upsert(k.clone(), v.clone(), false);
										()
									},
									_ => (),
								}
								()
							},
						}
					}
				} else {
					tab.root = self.root.clone();
				}
				rwlog
			},
			None => return Err(String::from("error prepare null"))
		};

		tab.commit_count.sum(1);

		Ok(log)
	}
	//回滚
	pub async fn rollback1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().await;
		tab.prepare.remove(&self.id);

		tab.rollback_count.sum(1);

		Ok(())
	}
}

impl RefMemeryTxn {
	// 获得事务的状态
	pub fn get_state(&self) -> TxState {
		self.0.borrow().state.clone()
	}
	// 预提交一个事务
	pub async fn prepare(&self, _timeout: usize) -> DBResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Preparing;
		match txn.prepare1().await {
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
	pub async fn commit(&self) -> CommitResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Committing;
		match txn.commit1().await {
			Ok(log) => {
				txn.state = TxState::Commited;
				return Some(Ok(log))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
	// 回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Rollbacking;
		match txn.rollback1().await {
			Ok(()) => {
				txn.state = TxState::Rollbacked;
				return Some(Ok(()))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}

	// 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
	pub async fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool) -> DBResult {
		None
	}
	// 查询
	pub async fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool
	) -> Option<SResult<Vec<TabKV>>> {
		let mut txn = self.0.borrow_mut();
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let value = match txn.get(tabkv.key.clone()).await {
				Some(v) => Some(v),
				_ => None
			};

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
	pub async fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool) -> DBResult {
		let mut txn = self.0.borrow_mut();
		for tabkv in arr.iter() {
			if tabkv.value == None {
				match txn.delete(tabkv.key.clone()).await {
				Ok(_) => (),
				Err(e) => 
					{
						return Some(Err(e.to_string()))
					},
				};
			} else {
				match txn.upsert(tabkv.key.clone(), tabkv.value.clone().unwrap()).await {
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
	pub async fn iter(
		&self,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> Option<IterResult> {
		let b = self.0.borrow_mut();
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};

		Some(Ok(Box::new(MemIter::new(tab, b.root.clone(), b.root.iter( key, descending), filter))))
	}
	// 迭代
	pub async fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> Option<KeyIterResult> {
		let b = self.0.borrow_mut();
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};
		let tab = b.tab.0.lock().await.tab.clone();
		Some(Ok(Box::new(MemKeyIter::new(&tab, b.root.clone(), b.root.keys(key, descending), filter))))
	}
	// 索引迭代
	pub fn index(
		&self,
		_tab: &Atom,
		_index_key: &Atom,
		_key: Option<Bin>,
		_descending: bool,
		_filter: Filter,
	) -> Option<IterResult> {
		None
	}
	// 表的大小
	pub async fn tab_size(&self) -> Option<SResult<usize>> {
		let txn = self.0.borrow();
		Some(Ok(txn.root.size()))
	}
}

//================================ 内部结构和方法
const TIMEOUT: usize = 100;


type BinMap = OrdMap<Tree<Bon, Bin>>;

// 内存表
struct MemeryTab {
	pub prepare: Prepare,
	pub root: BinMap,
	pub tab: Atom,
	trans_count:	PrefCounter,	//事务计数
	prepare_count:	PrefCounter,	//预提交计数
	commit_count:	PrefCounter,	//提交计数
	rollback_count:	PrefCounter,	//回滚计数
	read_count:		PrefCounter,	//读计数
	read_byte:		PrefCounter,	//读字节
	write_count:	PrefCounter,	//写计数
	write_byte:		PrefCounter,	//写字节
	remove_count:	PrefCounter,	//删除计数
	remove_byte:	PrefCounter,	//删除字节
}

pub struct MemIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
	iter_count:		PrefCounter,	//迭代计数
	iter_byte:		PrefCounter,	//迭代字节
}

impl Drop for MemIter{
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
    }
}

impl MemIter{
	pub fn new<'a>(tab: &Atom, root: BinMap, it: <Tree<Bon, Bin> as OIter<'a>>::IterType, filter: Filter) -> MemIter{
		MemIter{
			_root: root,
			_filter: filter,
			point: Box::into_raw(Box::new(it)) as usize,
			iter_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_ITER_COUNT_SUFFIX), 0).unwrap(),
			iter_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_ITER_BYTE_COUNT_SUFFIX), 0).unwrap(),
		}
	}
}

impl Iter for MemIter{
	type Item = (Bin, Bin);
	fn next(&mut self, _cb: Arc<dyn Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>>{
		self.iter_count.sum(1);

		let mut it = unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
		// println!("MemIter next----------------------------------------------------------------");
		let r = Some(Ok(match it.next() {
			Some(&Entry(ref k, ref v)) => {
				self.iter_byte.sum(k.len() + v.len());

				Some((k.clone(), v.clone()))
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

pub struct MemKeyIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
	iter_count:		PrefCounter,	//迭代计数
	iter_byte:		PrefCounter,	//迭代字节
}

impl Drop for MemKeyIter{
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
    }
}

impl MemKeyIter{
	pub fn new(tab: &Atom, root: BinMap, keys: Keys<'_, Tree<Bon, Bin>>, filter: Filter) -> MemKeyIter{
		MemKeyIter{
			_root: root,
			_filter: filter,
			point: Box::into_raw(Box::new(keys)) as usize,
			iter_count: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_KEY_ITER_COUNT_SUFFIX), 0).unwrap(),
			iter_byte: GLOBAL_PREF_COLLECT.
				new_dynamic_counter(
					Atom::from(MEMORY_TABLE_PREFIX.to_string() + tab + MEMORY_TABLE_KEY_ITER_BYTE_COUNT_SUFFIX), 0).unwrap(),
		}
	}
}

impl Iter for MemKeyIter{
	type Item = Bin;
	fn next(&mut self, _cb: Arc<dyn Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>>{
		self.iter_count.sum(1);

		let it = unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
		let r = Some(Ok(match unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)}.next() {
			Some(k) => {
				self.iter_byte.sum(k.len());

				Some(k.clone())
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

#[derive(Clone)]
pub struct MemeryMetaTxn;

impl MemeryMetaTxn {
	// 创建表、修改指定表的元数据
	pub async fn alter(&self, _tab: &Atom, _meta: Option<Arc<TabMeta>>) -> DBResult {
		Some(Ok(()))
	}

	// 快照拷贝表
	pub async fn snapshot(&self, _tab: &Atom, _from: &Atom) -> DBResult{
		Some(Ok(()))
	}
	// 修改指定表的名字
	pub async fn rename(&self, _tab: &Atom, _new_name: &Atom) -> DBResult {
		Some(Ok(()))
	} 

	// 获得事务的状态
	pub fn get_state(&self) -> TxState {
		TxState::Ok
	}
	// 预提交一个事务
	pub async fn prepare(&self, _timeout: usize) -> DBResult {
		Some(Ok(()))
	}
	// 提交一个事务
	pub async fn commit(&self) -> CommitResult {
		Some(Ok(FnvHashMap::with_capacity_and_hasher(0, Default::default())))
	}
	// 回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		Some(Ok(()))
	}
}

mod tests {
	use crate::mgr::{ DatabaseWare, Mgr };
	use atom::Atom;
	use sinfo;
	use super::*;
	use guid::{Guid, GuidGen};
	use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
	use crate::db::TabMeta;

	#[test]
	fn it_works() {
		let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
		let rt: MultiTaskRuntime<()>  = pool.startup(true);

		let _ = rt.spawn(rt.alloc(), async move {
			let mgr = Mgr::new(GuidGen::new(0, 0));
			let ware = DatabaseWare::new_memware(DB::new());
			let _ = mgr.register(Atom::from("memory"), Arc::new(ware));
			let tr = mgr.transaction(true).await;
			// let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
			// tr.alter(&Atom::from("memory"), &Atom::from("hello"), Some(Arc::new(meta))).await;
			// tr.prepare().await;
			// tr.commit().await;
		});
	}
}
