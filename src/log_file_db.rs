use std::sync::Arc;
use std::mem;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::Instant;
use std::collections::BTreeMap;
use std::env;
use std::io::Result;

use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::Tree;
use atom::Atom;
use guid::Guid;
use hash::{XHashMap, XHashSet};
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;
use pi_store::log_store::log_file::{PairLoader, LogMethod, LogFile};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::{AsyncRuntime, AsyncValue};
use r#async::lock::spin_lock::SpinLock;
use num_cpus;

use crate::db::{Bin, TabKV, SResult, IterResult, KeyIterResult, NextResult, Event, Filter, TxState, Iter, RwLog, Bon, TabMeta, CommitResult, DBResult};
use crate::tabs::{TabLog, Tabs, Prepare};
use crate::db::BuildDbType;
use crate::tabs::TxnType;
use crate::fork::{ALL_TABLES, TableMetaInfo, build_fork_chain};
use bon::{Decode, Encode, ReadBuffer, WriteBuffer};

lazy_static! {
	pub static ref STORE_RUNTIME: Arc<RwLock<Option<MultiTaskRuntime<()>>>> = Arc::new(RwLock::new(None));
}

pub const DB_META_TAB_NAME: &'static str = "tabs_meta";

/**
* 基于file log的数据库
*/
#[derive(Clone)]
pub struct LogFileDB(Arc<Tabs>);

impl LogFileDB {
	/**
	* 构建基于file log的数据库
	* @param db_path 数据库路径
	* @param db_size 数据库文件最大大小
	* @returns 返回基于file log的数据库
	*/
	pub async fn new(db_path: Atom, db_size: usize) -> Self {
		if !Path::new(&db_path.to_string()).exists() {
            let _ = fs::create_dir(db_path.to_string());
		}

		// 从元信息表加载所有表元信息
		let db_path = env::var("DB_PATH").unwrap_or("./".to_string());
		let mut path = PathBuf::new();
		path.push(db_path);
		path.push(DB_META_TAB_NAME);

		let file = match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024, None).await {
			Err(e) => {
				panic!("!!!!!!open table = {:?} failed, e: {:?}", "tabs_meta", e);
			},
			Ok(store) => store
		};

		let mut store = AsyncLogFileStore {
			removed: Arc::new(SpinLock::new(XHashMap::default())),
			map: Arc::new(SpinLock::new(BTreeMap::new())),
			log_file: file.clone(),
		};

		file.load(&mut store, None, false).await;

		let mut tabs = Tabs::new();

		let map = store.map.lock();
		for (k, v) in map.iter() {
			let tab_name = Atom::decode(&mut ReadBuffer::new(k, 0)).unwrap();
			let meta = TableMetaInfo::decode(&mut ReadBuffer::new(v.clone().to_vec().as_ref(), 0)).unwrap();
			tabs.set_tab_meta(tab_name.clone(), Arc::new(meta.meta.clone())).await;
			ALL_TABLES.lock().await.insert(tab_name, meta);
		}

		LogFileDB(Arc::new(tabs))
	}

	pub async fn open(tab: &Atom) -> SResult<LogFileTab> {
		let chains = build_fork_chain(tab.clone()).await;
		// println!("tab = {:?}, fork chains == {:?}", tab, chains);
		Ok(LogFileTab::new(tab, &chains).await)
	}

	// 拷贝全部的表
	pub async fn tabs_clone(&self) -> Arc<Self> {
		Arc::new(LogFileDB(Arc::new(self.0.clone_map())))
	}
	// 列出全部的表
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		Box::new(self.0.list().await)
	}
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	pub fn timeout(&self) -> usize {
		TIMEOUT
	}
	// 表的元信息
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.0.get(tab_name).await
	}
	// 获取当前表结构快照
	pub async fn snapshot(&self) -> Arc<LogFileDBSnapshot> {
		Arc::new(LogFileDBSnapshot(self.clone(), Mutex::new(self.0.snapshot().await)))
	}
}

// 内存库快照
pub struct LogFileDBSnapshot(LogFileDB, Mutex<TabLog>);

impl LogFileDBSnapshot {
	// 列出全部的表
	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		Box::new(self.1.lock().await.list())
	}
	// 表的元信息
	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.1.lock().await.get(tab_name)
	}
	// 检查该表是否可以创建
	pub fn check(&self, _tab: &Atom, _meta: &Option<Arc<TabMeta>>) -> DBResult {
		Ok(())
	}
	// 新增 修改 删除 表
	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		self.1.lock().await.alter(tab_name, meta)
	}
	// 创建指定表的表事务
	pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> SResult<TxnType> {
		self.1.lock().await.build(BuildDbType::LogFileDB, tab_name, id, writable).await
	}
	// 创建一个meta事务
	pub fn meta_txn(&self, _id: &Guid) -> Arc<LogFileMetaTxn> {
		Arc::new(LogFileMetaTxn {
			alters: Arc::new(Mutex::new(XHashMap::default())),
		})
	}
	// 元信息的预提交
	pub async fn prepare(&self, id: &Guid) -> DBResult{
		(self.0).0.prepare(id, &mut *self.1.lock().await).await
	}
	// 元信息的提交
	pub async fn commit(&self, id: &Guid){
		(self.0).0.commit(id).await
	}
	// 回滚
	pub async fn rollback(&self, id: &Guid){
		(self.0).0.rollback(id).await
	}
	// 库修改通知
	pub fn notify(&self, _event: Event) {}
}

// 内存事务
pub struct FileMemTxn {
	id: Guid,
	writable: bool,
	tab: LogFileTab,
	root: BinMap,
	old: BinMap,
	rwlog: XHashMap<Bin, RwLog>,
	state: TxState,
}

pub struct RefLogFileTxn(Mutex<FileMemTxn>);

unsafe impl Sync for RefLogFileTxn  {}

impl FileMemTxn {
	//开始事务
	pub async fn new(tab: LogFileTab, id: &Guid, writable: bool) -> RefLogFileTxn {
		let root = tab.0.lock().await.root.clone();
		let txn = FileMemTxn {
			id: id.clone(),
			writable,
			root: root.clone(),
			tab,
			old: root,
			rwlog: XHashMap::default(),
			state: TxState::Ok,
		};
		return RefLogFileTxn(Mutex::new(txn))
	}
	//获取数据
	pub async fn get(&mut self, key: Bin) -> Option<Bin> {
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

				return Some(v.clone())
			},
			None => return None
		}
	}
	//插入/修改数据
	pub async fn upsert(&mut self, key: Bin, value: Bin) -> DBResult {
		self.root.upsert(Bon::new(key.clone()), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));

		Ok(())
	}
	//删除
	pub async fn delete(&mut self, key: Bin) -> DBResult {
		self.root.delete(&Bon::new(key.clone()), false);
		self.rwlog.insert(key, RwLog::Write(None));

		Ok(())
	}

	//预提交
	pub async fn prepare_inner(&mut self) -> DBResult {
		//遍历事务中的读写日志
		for (key, rw_v) in self.rwlog.iter() {
			//检查预提交是否冲突
			match self.tab.0.lock().await.prepare.try_prepare(key, rw_v) {
				Ok(_) => (),
				Err(s) => return Err(s),
			};
			//检查Tab根节点是否改变
			if self.tab.0.lock().await.root.ptr_eq(&self.old) == false {
				let key = Bon::new(key.clone());
				match self.tab.0.lock().await.root.get(&key) {
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
		let rwlog = mem::replace(&mut self.rwlog, XHashMap::with_capacity_and_hasher(0, Default::default()));
		//写入预提交
		self.tab.0.lock().await.prepare.insert(self.id.clone(), rwlog);

		return Ok(())
	}

	// 内部提交方法
	pub async fn commit_inner(&mut self) -> CommitResult {
		let logs = self.tab.0.lock().await.prepare.remove(&self.id);
		let logs = match logs {
			Some(rwlog) => {
				let root_if_eq = self.tab.0.lock().await.root.ptr_eq(&self.old);
				//判断根节点是否相等
				if !root_if_eq {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							RwLog::Read => (),
							_ => {
								let k = Bon::new(k.clone());
								match rw_v {
									RwLog::Write(None) => {
										self.tab.0.lock().await.root.delete(&k, false);
									},
									RwLog::Write(Some(v)) => {
										self.tab.0.lock().await.root.upsert(k.clone(), v.clone(), false);
									},
									_ => (),
								}
							},
						}
					}
				} else {
					self.tab.0.lock().await.root = self.root.clone();
				}
				rwlog
			}
			None => return Err(String::from("error prepare null"))
		};

		let async_tab = self.tab.1.clone();

		let mut insert_pairs: Vec<(&[u8], &[u8])> = vec![];
		let mut delete_keys: Vec<&[u8]> = vec![];

		for (k, rw_v) in &logs {
			match rw_v {
				RwLog::Read => {},
				_ => {
					match rw_v {
						RwLog::Write(None) => {
							delete_keys.push(k);
						}
						RwLog::Write(Some(v)) => {
							insert_pairs.push((k, v));
						}
						_ => {}
					}
				}
			}
		}

		if insert_pairs.len() > 0 {
			async_tab.write_batch(&insert_pairs).await;
		}

		if delete_keys.len() > 0 {
			async_tab.remove_batch(&delete_keys).await;
		}

		Ok(logs)
	}
	//回滚
	pub async fn rollback_inner(&mut self) -> DBResult {
		let mut tab = self.tab.0.lock().await;
		tab.prepare.remove(&self.id);

		Ok(())
	}

	/// 强制产生分裂
	async fn force_fork_inner(&self) -> Result<usize> {
		self.tab.1.clone().force_fork().await
	}

	pub async fn fork_prepare_inner(&self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, meta: TabMeta) -> DBResult {
		// 检查元信息表中是否有重复的表名
		if let Some(_) = ALL_TABLES.lock().await.get(&fork_tab_name) {
			return Err("duplicate fork tab name in meta tab".to_string())
		}
		Ok(())
	}

	/// 执行真正的分裂
	pub async fn fork_commit_inner(&self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, meta: TabMeta) -> DBResult {
		let index = match self.force_fork_inner().await {
			Ok(idx) => idx,
			Err(e) => return Err(e.to_string())
		};
		println!("fork_index = {:?}", index);

		let mut tmi = TableMetaInfo::new(fork_tab_name.clone(), meta);
		tmi.parent = Some(tab_name.clone());

		tmi.parent_log_id = Some(index);
		tmi.parent = Some(tab_name.clone());

		let mut wb = WriteBuffer::new();
		tmi.encode(&mut wb);
		let mut wb1 = WriteBuffer::new();
		fork_tab_name.encode(&mut wb1);

		let db_path = env::var("DB_PATH").unwrap_or("./".to_string());

		ALL_TABLES.lock().await.insert(fork_tab_name, tmi);

		let mut path = PathBuf::new();
		path.push(db_path);
		path.push(DB_META_TAB_NAME);

		let file = match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024, None).await {
			Err(e) => {
				panic!("!!!!!!open table = {:?} failed, e: {:?}", "tabs_meta", e);
			},
			Ok(store) => store
		};

		let mut store = AsyncLogFileStore {
			removed: Arc::new(SpinLock::new(XHashMap::default())),
			map: Arc::new(SpinLock::new(BTreeMap::new())),
			log_file: file.clone(),
		};

		// 找到父表的元信息，将它的引用计数加一
		ALL_TABLES.lock().await.entry(tab_name.clone()).and_modify(|tab| {
			println!("add ref_count tab_name = {:?}", tab_name);
			tab.ref_count += 1;
			let mut b = WriteBuffer::new();
			tab_name.encode(&mut b);

			let mut b2 = WriteBuffer::new();
			tab.encode(&mut b2);
			store.write(b.bytes, b2.bytes);
		});

		// 新创建的分叉表信息写入元信息表中
		// TODO: 错误处理
		store.write(wb1.bytes, wb.bytes).await;

		Ok(())
	}

	pub async fn fork_rollback_inner(&self) -> DBResult {
		// 已经分裂则无法实现回滚
		Ok(())
	}
}

impl RefLogFileTxn {
	// 获得事务的状态
	pub async fn get_state(&self) -> TxState {
		self.0.lock().await.state.clone()
	}
	// 预提交一个事务
	pub async fn prepare(&self, _timeout: usize) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.state = TxState::Preparing;
		match txn.prepare_inner().await {
			Ok(()) => {
				txn.state = TxState::PreparOk;
				return Ok(())
			},
			Err(e) => {
				txn.state = TxState::PreparFail;
				return Err(e.to_string())
			},
		}
	}
	// 提交一个事务
	pub async fn commit(&self) -> CommitResult {
		let mut txn = self.0.lock().await;
		txn.state = TxState::Committing;
		match txn.commit_inner().await {
			Ok(log) => {
				txn.state = TxState::Commited;
				return Ok(log)
			},
			Err(e) => {
				txn.state = TxState::CommitFail;
				return Err(e.to_string())
			}
		}
	}
	// 回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.state = TxState::Rollbacking;
		match txn.rollback_inner().await {
			Ok(()) => {
				txn.state = TxState::Rollbacked;
				return Ok(())
			},
			Err(e) => {
				txn.state = TxState::RollbackFail;
				return Err(e.to_string())
			}
		}
	}

	/// fork 预提交
	pub async fn fork_prepare(&self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, meta: TabMeta) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.fork_prepare_inner(ware, tab_name, fork_tab_name, meta).await
	}

	/// fork 提交
	pub async fn fork_commit(&self, ware: Atom, tab_name: Atom, fork_tab_name: Atom, meta: TabMeta) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.fork_commit_inner(ware, tab_name, fork_tab_name, meta).await
	}

	/// fork 回滚
	pub async fn fork_rollback(&self) -> DBResult {
		let mut txn = self.0.lock().await;
		txn.fork_rollback_inner().await
	}

	/// 强制产生分裂
	pub async fn force_fork(&self) -> Result<usize> {
		self.0.lock().await.force_fork_inner().await
	}

	// 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
	pub async fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool) -> DBResult {
		Ok(())
	}
	// 查询
	pub async fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool
	) -> SResult<Vec<TabKV>> {
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let value = match self.0.lock().await.get(tabkv.key.clone()).await {
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
		Ok(value_arr)
	}
	// 修改，插入、删除及更新
	pub async fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool) -> DBResult {
		for tabkv in arr.iter() {
			if tabkv.value == None {
				match self.0.lock().await.delete(tabkv.key.clone()).await {
					Ok(_) => (),
					Err(e) => return Err(e.to_string())
				};
			} else {
				match self.0.lock().await.upsert(tabkv.key.clone(), tabkv.value.clone().unwrap()).await {
					Ok(_) => (),
					Err(e) => return Err(e.to_string())
				};
			}
		}
		Ok(())
	}
	// 迭代
	pub async fn iter(
		&self,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> IterResult {
		let b = self.0.lock().await;
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};

		Ok(Box::new(MemIter::new(tab, b.root.clone(), b.root.iter( key, descending), filter)))
	}
	// 迭代
	pub async fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> KeyIterResult {
		let b = self.0.lock().await;
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};
		let tab = b.tab.0.lock().await.tab.clone();
		Ok(Box::new(MemKeyIter::new(&tab, b.root.clone(), b.root.keys(key, descending), filter)))
	}
	// 索引迭代
	pub fn index(
		&self,
		_tab: &Atom,
		_index_key: &Atom,
		_key: Option<Bin>,
		_descending: bool,
		_filter: Filter,
	) -> IterResult {
		Err("not implemeted".to_string())
	}
	// 表的大小
	pub async fn tab_size(&self) -> SResult<usize> {
		let txn = self.0.lock().await;
		Ok(txn.root.size())
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
}

pub struct MemIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
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
		}
	}
}

impl Iter for MemIter{
	type Item = (Bin, Bin);
	fn next(&mut self) -> Option<NextResult<Self::Item>>{

		let mut it = unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
		let r = Some(Ok(match it.next() {
			Some(&Entry(ref k, ref v)) => {
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
		}
	}
}

impl Iter for MemKeyIter{
	type Item = Bin;
	fn next(&mut self) -> Option<NextResult<Self::Item>>{
		let it = unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
		let r = Some(Ok(match unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)}.next() {
			Some(k) => {
				Some(k.clone())
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

#[derive(Clone)]
pub struct LogFileMetaTxn {
	alters: Arc<Mutex<XHashMap<Atom, Option<Arc<TabMeta>>>>>,
}

impl LogFileMetaTxn {
	// 创建表、修改指定表的元数据
	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) -> DBResult {
		self.alters.lock().await.insert(tab_name.clone(), meta);
		Ok(())
	}
	// 快照拷贝表
	pub async fn snapshot(&self, _tab: &Atom, _from: &Atom) -> DBResult {
		Ok(())
	}
	// 修改指定表的名字
	pub async fn rename(&self, _tab: &Atom, _new_name: &Atom) -> DBResult {
		Ok(())
	}

	// 获得事务的状态
	pub async fn get_state(&self) -> TxState {
		TxState::Ok
	}
	// 预提交一个事务
	pub async fn prepare(&self, _timeout: usize) -> DBResult {
		Ok(())
	}
	// 提交一个事务
	pub async fn commit(&self) -> CommitResult {
		for (tab_name, meta) in self.alters.lock().await.iter() {
			if let Some(_) = ALL_TABLES.lock().await.get(tab_name) {
				return Err(format!("tab_name: {:?} exist", tab_name))
			}
			let mut kt = WriteBuffer::new();
			tab_name.clone().encode(&mut kt);
			let db_path = env::var("DB_PATH").unwrap_or("./".to_string());
			let mut path = PathBuf::new();
			path.push(db_path.clone());
			path.push(DB_META_TAB_NAME);

			let file = match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024, None).await {
				Err(e) => {
					panic!("!!!!!!open table = {:?} failed, e: {:?}", "tabs_meta", e);
				},
				Ok(store) => store
			};

			let mut store = AsyncLogFileStore {
				removed: Arc::new(SpinLock::new(XHashMap::default())),
				map: Arc::new(SpinLock::new(BTreeMap::new())),
				log_file: file.clone(),
			};

			match meta {
				Some(m) => {
					let mt = TabMeta::new(m.k.clone(), m.v.clone());
					let tmi = TableMetaInfo::new(tab_name.clone(), mt);
					let mut vt = WriteBuffer::new();
					tmi.encode(&mut vt);

					// 新创建的表加入ALL_TABLES的缓存
					let meta_name = Atom::from(db_path + &DB_META_TAB_NAME);
					ALL_TABLES.lock().await.insert(meta_name, tmi);

					// 新创建表的元信息写入元信息表中
					store.write(kt.bytes, vt.bytes).await;
				}
				None => {
					let mut parent = None;
					match ALL_TABLES.lock().await.get(&tab_name) {
						Some(tab) => {
							if tab.ref_count > 0 {
								return Err(format!("delete tab: {:?} failed, ref_count = {:?}", tab.tab_name, tab.ref_count))
							} else {
								store.remove(kt.bytes).await;
								parent = tab.parent.clone();
							}
						}
						None => {
							return Err(format!("delete tab: {:?} not found", tab_name))
						}
					}
					ALL_TABLES.lock().await.remove(&tab_name);
					// 找到他的父表，将父表的引用计数减一
					let mut wb = WriteBuffer::new();
					parent.clone().unwrap().encode(&mut wb);
					ALL_TABLES.lock().await.entry(parent.clone().unwrap()).and_modify(|t| {
						t.ref_count -= 1;
						let mut wb2 = WriteBuffer::new();
						t.encode(&mut wb2);
						store.write(wb.bytes, wb2.bytes);
					});
				}
			}
		}
		Ok(XHashMap::with_capacity_and_hasher(0, Default::default()))
	}
	// 回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		self.alters.lock().await.clear();
		Ok(())
	}
}

#[derive(Clone)]
struct AsyncLogFileStore {
	removed: Arc<SpinLock<XHashMap<Vec<u8>, ()>>>,
	map: Arc<SpinLock<BTreeMap<Vec<u8>, Arc<[u8]>>>>,
	log_file: LogFile
}

unsafe impl Send for AsyncLogFileStore {}
unsafe impl Sync for AsyncLogFileStore {}

impl PairLoader for AsyncLogFileStore {
    fn is_require(&self, log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
		!self.removed.lock().contains_key(key) && !self.map.lock().contains_key(key)
    }

    fn load(&mut self, log_file: Option<&PathBuf>, method: LogMethod, key: Vec<u8>, value: Option<Vec<u8>>) {
		if let Some(value) = value {
			self.map.lock().insert(key, value.into());
		} else {
			self.removed.lock().insert(key, ());
		}
    }
}

impl AsyncLogFileStore {
	async fn open<P: AsRef<Path> + std::fmt::Debug>(path: P, buf_len: usize, file_len: usize, log_file_index: Option<usize>) -> Result<LogFile> {
		// println!("AsyncLogFileStore open ====== {:?}, log_index = {:?}", path, log_file_index);
		match LogFile::open(STORE_RUNTIME.read().await.as_ref().unwrap().clone(), path, buf_len, file_len, log_file_index).await {
            Err(e) =>panic!("LogFile::open error {:?}", e),
            Ok(file) => Ok(file),
		}
	}

	async fn write_batch(&self, pairs: &[(&[u8], &[u8])]) -> Result<()> {
		let mut id = 0;
		for (key, value) in pairs {
			id = self.log_file.append(LogMethod::PlainAppend, key, value);
		}
		
		match self.log_file.delay_commit(id, false, 0).await {
			Ok(_) => {
				for (key, value) in pairs {
					self.map.lock().insert(key.to_vec(), value.clone().into());
				}
				Ok(())
			}
			Err(e) => {
				println!("write batch error");
				Err(e)
			}
		}
	}

	async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let id = self.log_file.append(LogMethod::PlainAppend, key.as_ref(), value.as_ref());
        if let Err(e) = self.log_file.delay_commit(id, false, 10).await {
            Err(e)
        } else {
            if let Some(value) = self.map.lock().insert(key, value.into()) {
                //更新指定key的存储数据，则返回更新前的存储数据
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
	}
	
	fn read(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        if let Some(value) = self.map.lock().get(key) {
            return Some(value.clone())
        }

        None
	}

	async fn remove_batch(&self, keys: &[&[u8]]) -> Result<()> {
		let mut id = 0;
		for key in keys {
			id = self.log_file.append(LogMethod::Remove, key, &[]);
		}

		match self.log_file.delay_commit(id, false, 10).await {
			Ok(_) => {
				for key in keys {
					self.map.lock().remove(key.clone());
				}
				Ok(())
			}
			Err(e) => Err(e)
		}
	}

	pub async fn remove(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let id = self.log_file.append(LogMethod::Remove, key.as_ref(), &[]);
        if let Err(e) = self.log_file.delay_commit(id, false, 10).await {
            Err(e)
        } else {
            if let Some(value) = self.map.lock().remove(&key) {
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    fn last_key(&self) -> Option<Vec<u8>> {
        self.map.lock().iter().last().map(|(k, _)| {
            k.clone()
        })
	}
	
	/// 强制产生分裂
	pub async fn force_fork(&self) -> Result<usize> {
		self.log_file.split().await
	}
}

#[derive(Clone)]
pub struct LogFileTab(Arc<Mutex<MemeryTab>>, AsyncLogFileStore);

unsafe impl Send for LogFileTab {}
unsafe impl Sync for LogFileTab {}

impl LogFileTab {
	async fn new(tab: &Atom, chains: &[TableMetaInfo]) -> Self {
		let mut file_mem_tab = MemeryTab {
			prepare: Prepare::new(XHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::<Tree<Bon, Bin>>::new(None),
			tab: tab.clone(),
		};

		let mut path = PathBuf::new();
		let db_path = env::var("DB_PATH").unwrap_or(".".to_string());
		path.push(db_path);
		let tab_name = tab.clone();
		let tab_name_clone = tab.clone();
		path.push(tab_name.clone().to_string());


		let mut log_file_id = None;
		// 首先加载叶子节点数据
		let log_file_index = if chains.len() > 0 {
			log_file_id = chains[0].parent_log_id;
			chains[0].parent_log_id
		} else {
			None
		};
		// println!("LogFileTab::new  log_file_index = {:?}, tab = {:?}, chains = {:?}", log_file_index, tab, chains);
		let file = match AsyncLogFileStore::open(path.clone(), 8000, 200 * 1024 * 1024, log_file_index).await {
			Err(e) => panic!("!!!!!!open table = {:?} failed, e: {:?}", tab_name, e),
			Ok(file) => file
		};

		let mut store = AsyncLogFileStore {
			removed: Arc::new(SpinLock::new(XHashMap::default())),
			map: Arc::new(SpinLock::new(BTreeMap::new())),
			log_file: file.clone()
		};

		file.load(&mut store, Some(path), false).await;

		let mut root= OrdMap::<Tree<Bon, Bin>>::new(None);
		let mut load_size = 0;
		let start_time = Instant::now();
		let map = store.map.lock();
		for (k, v) in map.iter() {
			load_size += k.len() + v.len();
			root.upsert(Bon::new(Arc::new(k.clone())), Arc::new(v.to_vec()), false);
		}
		debug!("====> load tab: {:?} size: {:?}byte time elapsed: {:?} <====", tab_name_clone, load_size, start_time.elapsed());

		// 再加载分叉路径中的表的数据
		for tm in chains.iter().skip(1) {
			let file = match AsyncLogFileStore::open(tm.tab_name.as_ref(), 8000, 200 * 1024 * 1024, tm.parent_log_id).await {
				Err(e) => panic!("!!!!!!open table = {:?} failed, e: {:?}", tm.parent, e),
				Ok(file) => file
			};
			let mut store = AsyncLogFileStore {
				removed: Arc::new(SpinLock::new(XHashMap::default())),
				map: Arc::new(SpinLock::new(BTreeMap::new())),
				log_file: file.clone()
			};
	
			let mut path = PathBuf::new();
			path.push(tm.tab_name.clone().as_ref());
			path.push(format!("{:0>width$}", log_file_id.unwrap()-1, width = 6));
			file.load(&mut store, Some(path), false).await;
	
			let mut load_size = 0;
			let start_time = Instant::now();
			let map = store.map.lock();
			for (k, v) in map.iter() {
				load_size += k.len() + v.len();
				root.upsert(Bon::new(Arc::new(k.clone())), Arc::new(v.to_vec()), false);
			}
			log_file_id = tm.parent_log_id;
			debug!("====> load tab: {:?} size: {:?}byte time elapsed: {:?} <====", tm.tab_name, load_size, start_time.elapsed());
		}

		file_mem_tab.root = root;

		return LogFileTab(Arc::new(Mutex::new(file_mem_tab)), store);
	}

	pub async fn transaction(&self, id: &Guid, writable: bool) -> RefLogFileTxn {
		FileMemTxn::new(self.clone(), id, writable).await
	}
}
