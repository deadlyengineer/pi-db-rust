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
use hash::XHashMap;
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

lazy_static! {
	pub static ref STORE_RUNTIME: MultiTaskRuntime<()> = {
        let pool = MultiTaskPool::new("File-Runtime".to_string(), num_cpus::get(), 1024 * 1024, 10, Some(10));
        pool.startup(true)
    };
}

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
	pub fn new(db_path: Atom, db_size: usize) -> Self {
		if !Path::new(&db_path.to_string()).exists() {
            let _ = fs::create_dir(db_path.to_string());
		}

		LogFileDB(Arc::new(Tabs::new()))
	}

	pub async fn open(tab: &Atom) -> SResult<LogFileTab> {
		// 确定这个表的分叉关系： 是否是从某个表分叉而来，如果是从其他表分叉而来，需要先加载其他表
		// 如何处理分叉之后元信息的变动？ 现在的是js层处理序列化与反序列化的问题
	
		// 这里可能出现会重复加载某些表的问题，比如说B，C表是从A表分叉而来，那么这里就会加载A表两次。
		// 所以是否还需要懒加载？ 懒加载的话就可能出现某个表加载多次的问题。
		// 解决方法： 有分叉关系的表不进行懒加载，没有分叉过的表进行懒加载。创建数据库的时候就要先加载分叉的表。
		Ok(LogFileTab::new(tab).await)
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
		Arc::new(LogFileMetaTxn)
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
			rwlog: XHashMap::with_capacity_and_hasher(0, Default::default()),
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

		for (k, rw_v) in &logs {
			match rw_v {
				RwLog::Read => {},
				_ => {
					match rw_v {
						RwLog::Write(None) => {
							debug!("delete key = {:?}", k);
							if let Err(_) =  async_tab.remove(k.to_vec()).await {
								return Err("remove error".to_string())
							}
						}
						RwLog::Write(Some(v)) => {
							debug!("insert k = {:?}, v = {:?}", k, v);
							if let Err(_) = async_tab.write(k.to_vec(), v.to_vec()).await {
								return Err("write error".to_string())
							}
						}
						_ => {}
					}
				}
			}
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
	pub async fn force_fork_inner(&self) -> DBResult {
		self.tab.1.clone().force_fork().await
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

	/// 强制产生分裂
	pub async fn force_fork(&self) -> DBResult {
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
pub struct LogFileMetaTxn;

impl LogFileMetaTxn {
	// 创建表、修改指定表的元数据
	pub async fn alter(&self, _tab: &Atom, _meta: Option<Arc<TabMeta>>) -> DBResult {
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
		Ok(XHashMap::with_capacity_and_hasher(0, Default::default()))
	}
	// 回滚一个事务
	pub async fn rollback(&self) -> DBResult {
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
	async fn open<P: AsRef<Path> + std::fmt::Debug>(path: P, buf_len: usize, file_len: usize, log_file_index: Option<usize>) -> Result<Self> {
		match LogFile::open(STORE_RUNTIME.clone(), path, buf_len, file_len, log_file_index).await {
            Err(e) => Err(e),
            Ok(file) => {
                //打开指定路径的日志存储成功
                let mut store = AsyncLogFileStore {
					removed: Arc::new(SpinLock::new(XHashMap::default())),
					map: Arc::new(SpinLock::new(BTreeMap::new())),
					log_file: file.clone()
				};

                if let Err(e) = file.load(&mut store, None, true).await {
                    Err(e)
                } else {
                    //初始化内存数据成功
                    Ok(store)
                }
            },
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
	pub async fn force_fork(&self) -> DBResult {
		let id = self.log_file.append(LogMethod::PlainAppend, &[], &[]);
		if let Err(e) = self.log_file.commit(id, true, true).await {
			Err(e.to_string())
		} else {
			Ok(())
		}
	}
}

#[derive(Clone)]
pub struct LogFileTab(Arc<Mutex<MemeryTab>>, AsyncLogFileStore);

unsafe impl Send for LogFileTab {}
unsafe impl Sync for LogFileTab {}

impl LogFileTab {
	async fn new(tab: &Atom) -> Self {
		let mut file_mem_tab = MemeryTab {
			prepare: Prepare::new(XHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::<Tree<Bon, Bin>>::new(None),
			tab: tab.clone(),
		};

		let db_path = env::var("DB_PATH").unwrap_or("./".to_string());
		let mut path = PathBuf::new();
		let tab_name = tab.clone();
		let tab_name_clone = tab.clone();
		path.push(db_path);
		path.push(tab_name.clone().to_string());

		let async_value = AsyncValue::new(AsyncRuntime::Multi(STORE_RUNTIME.clone()));
		let async_value_clone = async_value.clone();

		let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
			match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024, None).await {
				Err(e) => {
					error!("!!!!!!open table = {:?} failed, e: {:?}", tab_name, e);
				},
				Ok(store) => {
					async_value_clone.set(store);
				}
			}
		});

		let store = async_value.await;

		let mut root= OrdMap::<Tree<Bon, Bin>>::new(None);
		let mut load_size = 0;
		let start_time = Instant::now();
		let map = store.map.lock();
		for (k, v) in map.iter() {
			load_size += v.len();
			root.upsert(Bon::new(Arc::new(k.clone())), Arc::new(v.to_vec()), false);
		}
		file_mem_tab.root = root;
		debug!("====> load tab: {:?} size: {:?}byte time elapsed: {:?} <====", tab_name_clone, load_size, start_time.elapsed());

		return LogFileTab(Arc::new(Mutex::new(file_mem_tab)), store);

	}

	pub async fn transaction(&self, id: &Guid, writable: bool) -> RefLogFileTxn {
		FileMemTxn::new(self.clone(), id, writable).await
	}
}
