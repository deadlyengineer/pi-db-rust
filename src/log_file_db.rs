
use std::sync::{Arc};
use std::cell::RefCell;
use std::mem;
use std::path::{Path, PathBuf};
use std::fs::{self, read_to_string};
use std::time::Instant;
use fnv::FnvHashMap;
use std::collections::{ BTreeMap, HashMap };
use std::env;
use std::collections::HashSet;
use std::sync::atomic::{ Ordering, AtomicIsize };
use std::io::Result;

use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::{Tree};
use atom::{Atom};
use guid::Guid;
use hash::XHashMap;
use r#async::lock::mutex_lock::Mutex;
use r#async::lock::rw_lock::RwLock;

use crossbeam_channel::{unbounded, Receiver, Sender};

use crate::db::{EventType, Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult, NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn, Tab, OpenTab, Event, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult, RwLog, Bon, TabMeta, TxCbWrapper};
use crate::tabs::{TabLog, Tabs, Prepare};
use crate::db::BuildDbType;
use crate::tabs::TxnType;
use pi_store::log_store::log_file::{PairLoader, LogMethod, LogFile};

use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::lock::spin_lock::SpinLock;
use futures::channel::oneshot::channel;

use bon::{ ReadBuffer, WriteBuffer };
use num_cpus;

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
pub struct LogFileDB(Arc<RwLock<Tabs>>);

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

		LogFileDB(Arc::new(RwLock::new(Tabs::new())))
	}

	pub async fn open(tab: &Atom) -> Option<SResult<LogFileTab>> {
		Some(Ok(LogFileTab::new(tab).await))
	}

	// 拷贝全部的表
	pub async fn tabs_clone(&self) -> Arc<Self> {
		Arc::new(LogFileDB(Arc::new(RwLock::new(self.0.read().await.clone_map()))))
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
	pub async fn snapshot(&self) -> Arc<LogFileDBSnapshot> {
		Arc::new(LogFileDBSnapshot(self.clone(), Mutex::new(self.0.read().await.snapshot())))
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
	pub fn check(&self, _tab: &Atom, _meta: &Option<Arc<TabMeta>>) -> SResult<()> {
		Ok(())
	}
	// 新增 修改 删除 表
	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		self.1.lock().await.alter(tab_name, meta)
	}
	// 创建指定表的表事务
	pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> Option<SResult<TxnType>> {
		self.1.lock().await.build(BuildDbType::LogFileDB, tab_name, id, writable).await
	}
	// 创建一个meta事务
	pub fn meta_txn(&self, _id: &Guid) -> Arc<LogFileMetaTxn> {
		Arc::new(LogFileMetaTxn)
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
pub struct FileMemTxn {
	id: Guid,
	writable: bool,
	tab: LogFileTab,
	root: BinMap,
	old: BinMap,
	rwlog: FnvHashMap<Bin, RwLog>,
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
			rwlog: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
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
	pub async fn upsert(&mut self, key: Bin, value: Bin) -> SResult<()> {
		self.root.upsert(Bon::new(key.clone()), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));

		Ok(())
	}
	//删除
	pub async fn delete(&mut self, key: Bin) -> SResult<()> {
		self.root.delete(&Bon::new(key.clone()), false);
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

		return Ok(())
	}

	// 同时异步存储数据到log file 中
	//提交
	pub async fn commit1(&mut self) -> CommitResult {
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
									},
									RwLog::Write(Some(v)) => {
										tab.root.upsert(k.clone(), v.clone(), false);
									},
									_ => (),
								}
							},
						}
					}
				} else {
					tab.root = self.root.clone();
				}
				rwlog
			},
			None => return Some(Err(String::from("error prepare null")))
		};

		let async_tab = self.tab.1.clone();

		for (k, rw_v) in log {
			match rw_v {
				RwLog::Read => {},
				_ => {
					match rw_v {
						RwLog::Write(None) => {
							debug!("delete key = {:?}", k);
							if let Err(_) =  async_tab.remove(k.to_vec()).await {
								return Some(Err("remove error".to_string()))
							}
						}
						RwLog::Write(Some(v)) => {
							debug!("insert k = {:?}, v = {:?}", k, v);
							if let Err(_) = async_tab.write(k.to_vec(), v.to_vec()).await {
								return Some(Err("write error".to_string()))
							}
						}
						_ => {}
					}
				}
			}
		}
		None
	}
	//回滚
	pub async fn rollback1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().await;
		tab.prepare.remove(&self.id);

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
		let mut txn = self.0.lock().await;
		txn.state = TxState::Committing;
		match txn.commit1().await {
			Some(Ok(log)) => {
				txn.state = TxState::Commited;
				return Some(Ok(log))
			},
			Some(Err(e)) => return Some(Err(e.to_string())),
			None => None
		}
	}
	// 回滚一个事务
	pub async fn rollback(&self) -> DBResult {
		let mut txn = self.0.lock().await;
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
		let mut txn = self.0.lock().await;
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
		let mut txn = self.0.lock().await;
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
		let b = self.0.lock().await;
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
		let txn = self.0.lock().await;
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
	pub async fn alter(&self, _tab: &Atom, _meta: Option<Arc<TabMeta>>) -> DBResult{
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
	pub async fn get_state(&self) -> TxState {
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

#[derive(Clone)]
struct AsyncLogFileStore {
	removed: Arc<SpinLock<XHashMap<Vec<u8>, ()>>>,
	map: Arc<SpinLock<BTreeMap<Vec<u8>, Arc<[u8]>>>>,
	log_file: LogFile
}

unsafe impl Send for AsyncLogFileStore {}
unsafe impl Sync for AsyncLogFileStore {}

impl PairLoader for AsyncLogFileStore {
    fn is_require(&self, key: &Vec<u8>) -> bool {
		!self.removed.lock().contains_key(key) && !self.map.lock().contains_key(key)
    }

    fn load(&mut self, _method: LogMethod, key: Vec<u8>, value: Option<Vec<u8>>) {
		if let Some(value) = value {
			self.map.lock().insert(key, value.into());
		} else {
			self.removed.lock().insert(key, ());
		}
    }
}

impl AsyncLogFileStore {
	async fn open<P: AsRef<Path> + std::fmt::Debug>(path: P, buf_len: usize, file_len: usize) -> Result<Self> {
		match LogFile::open(STORE_RUNTIME.clone(), path, buf_len, file_len).await {
            Err(e) => Err(e),
            Ok(file) => {
                //打开指定路径的日志存储成功
                let mut store = AsyncLogFileStore {
					removed: Arc::new(SpinLock::new(XHashMap::default())),
					map: Arc::new(SpinLock::new(BTreeMap::new())),
					log_file: file.clone()
				};

                if let Err(e) = file.load(&mut store, true).await {
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
        if let Err(e) = self.log_file.delay_commit(id, 10).await {
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
        if let Err(e) = self.log_file.delay_commit(id, 10).await {
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
}

#[derive(Clone)]
pub struct LogFileTab(Arc<Mutex<MemeryTab>>, AsyncLogFileStore);

unsafe impl Send for LogFileTab {}
unsafe impl Sync for LogFileTab {}

impl LogFileTab {
	async fn new(tab: &Atom) -> Self {
		let mut file_mem_tab = MemeryTab {
			prepare: Prepare::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::<Tree<Bon, Bin>>::new(None),
			tab: tab.clone(),
		};

		let db_path = env::var("DB_PATH").unwrap_or("./".to_string());
		let mut path = PathBuf::new();
		let tab_name = tab.clone();
		let tab_name_clone = tab.clone();
		path.push(db_path);
		path.push(tab_name.clone().to_string());
		let (s, r) = channel();

		// 异步加载数据， 通过channel 同步
		let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
			match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024).await {
				Err(e) => {
					error!("!!!!!!open table = {:?} failed, e: {:?}", tab_name, e);
				},
				Ok(store) => {
					// 数据加载完毕，通过channel返回句柄
					let _ = s.send(store);
				}
			}
		});
		
		match r.await {
			Ok(store) => {
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
			Err(e) => {
				panic!("LogFileTab::new failed, error = {:?}", e);
			}
		}

	}
	pub async fn transaction(&self, id: &Guid, writable: bool) -> RefLogFileTxn {
		FileMemTxn::new(self.clone(), id, writable).await
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
	use bon::WriteBuffer;

	#[test]
	fn test_log_file_db() {
		let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
		let rt: MultiTaskRuntime<()>  = pool.startup(true);

		let _ = rt.spawn(rt.alloc(), async move {
			let mgr = Mgr::new(GuidGen::new(0, 0));
			let ware = DatabaseWare::new_log_file_ware(LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024));
			let _ = mgr.register(Atom::from("logfile"), Arc::new(ware)).await;
			let mut tr = mgr.transaction(true).await;

			let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
			let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

			tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), Some(Arc::new(meta))).await;
			tr.alter(&Atom::from("logfile"), &Atom::from("./testlogfile/world"), Some(Arc::new(meta1))).await;
			let p = tr.prepare().await;
			println!("tr prepare ---- {:?}", p);
			tr.commit().await;

			let info = tr.tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/hello")).await;
			println!("info ---- {:?} ", info);

			let mut wb = WriteBuffer::new();
			wb.write_bin(b"hello", 0..5);

			println!("wb = {:?}", wb.bytes);

			let mut item1 = TabKV {
				ware: Atom::from("logfile"),
				tab: Atom::from("./testlogfile/hello"),
				key: Arc::new(wb.bytes.clone()),
				value: Some(Arc::new(wb.bytes)),
				index: 0
			};

			let mut tr2 = mgr.transaction(true).await;

			let r = tr2.modify(vec![item1.clone()], None, false).await;
			println!("logfile result = {:?}", r);
			let p = tr2.prepare().await;
			tr2.commit().await;

			let mut tr3 = mgr.transaction(false).await;
			item1.value = None;

			let q = tr3.query(vec![item1], None, false).await;
			println!("query item = {:?}", q);
			tr3.prepare().await;
			tr3.commit().await;

			let mut tr4 = mgr.transaction(false).await;
			let size = tr4.tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/hello")).await;
			println!("tab size = {:?}", size);
			{
				let iter = tr4.iter(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"), None, false, None).await;

				if let Some(Ok(mut it)) = iter {
					loop {
						let item = it.next();
						println!("iter item = {:?}", item);
						match item {
							Some(Ok(None)) | Some(Err(_)) => break,
							_ => {}
						}
					}
				}
			}

			let tabs = tr4.list(&Atom::from("logfile")).await;
			println!("tabs = {:?}", tabs);

			tr4.prepare().await;
			tr4.commit().await;
		});

		std::thread::sleep(std::time::Duration::from_secs(2));
	}
}