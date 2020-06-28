/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */


use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TrySendError};
use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::mem;

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::AtomicU64;

use fnv::FnvHashMap;

use ordmap::ordmap::{OrdMap, Entry, ImOrdMap, Keys};
use ordmap::asbtree::{Tree, new};
use atom::Atom;
use sinfo::EnumType;
use guid::{Guid, GuidGen};

use crate::db::{SResult, DBResult, IterResult, KeyIterResult, Filter, TabKV, TxCallback, TxQueryCallback, TxState, MetaTxn, TabTxn, Event, EventType, Ware, WareSnapshot, Bin, RwLog, TabMeta, CommitResult};
use r#async::lock::mutex_lock::Mutex;
use crate::memery_db::{MemDBSnapshot, MemDB, RefMemeryTxn, MemeryMetaTxn};
use crate::tabs::TxnType;
use crate::log_file_db::{LogFileDBSnapshot, RefLogFileTxn, LogFileMetaTxn, LogFileDB};

pub struct CommitChan(pub Guid, pub Sender<Arc<Vec<TabKV>>>);

unsafe impl Send for CommitChan {}
unsafe impl Sync for CommitChan {}

lazy_static! {
	pub static ref COMMIT_CHAN: (Sender<CommitChan>, Receiver<CommitChan>) = unbounded();
	pub static ref SEQ_CHAN: (Sender<u64>, Receiver<u64>) = unbounded();
}

/**
* 表库及事务管理器
*/
#[derive(Clone)]
pub struct Mgr(Arc<Mutex<WareMap>>, Arc<GuidGen>, Statistics);

unsafe impl Send for Mgr {}
unsafe impl Sync for Mgr {}

impl Mgr {
	/**
	* 构建表库及事务管理器管理器
	* @param gen 全局唯一id生成器
	* @returns 返回表库及事务管理器管理器
	*/
	pub fn new(gen: GuidGen) -> Self {
		Mgr(Arc::new(Mutex::new(WareMap::new())), Arc::new(gen), Statistics::new())
	}
	// 浅拷贝，库表不同，共用同一个统计信息和GuidGen
	pub async fn shallow_clone(&self) -> Self {
		// TODO 拷库表
		Mgr(Arc::new(Mutex::new(self.0.lock().await.wares_clone().await)), self.1.clone(), self.2.clone())
	}
	// 深拷贝，库表及统计信息不同
	pub fn deep_clone(&self, clone_guid_gen: bool) -> Self {
		let gen = if clone_guid_gen {
			self.1.clone()
		}else{
			Arc::new(GuidGen::new(self.1.node_time(), self.1.node_id()))
		};
		Mgr(Arc::new(Mutex::new(WareMap::new())), gen, self.2.clone())
	}
	// 注册库
	pub async fn register(&self, ware_name: Atom, ware: Arc<DatabaseWare>) -> bool {
		self.0.lock().await.register(ware_name, ware)
	}
	// 取消注册数据库
	pub async fn unregister(&mut self, ware_name: &Atom) -> bool {
		self.0.lock().await.unregister(ware_name)
	}
	/**
	* 获取表的元信息
	* @param ware_name 库名
	* @param tab_name 表名
	* @returns 返回表的元信息
	*/
	pub async fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.find(ware_name).await {
			Some(b) => b.tab_info(tab_name).await,
			_ => None
		}
	}
	/**
	* 创建事务
	* @param writable 是否为写事务
	* @returns 返回事务
	*/
	pub async fn transaction(&self, writable: bool) -> Tr {		
		let id = self.1.gen(0);
		self.2.acount.fetch_add(1, Ordering::SeqCst);
		let ware_map = {
			self.0.lock().await.clone()
		};

		let mut map = FnvHashMap::with_capacity_and_hasher(ware_map.0.size() * 3 / 2, Default::default());
		let mut tmp = vec![];
		for Entry(k, v) in ware_map.0.iter(None, false) {
			tmp.push((k.clone(), v));
		}

		for (k, v) in tmp {
			map.insert(k, v.snapshot().await);
		}

		Tr {
			writable,
			timeout: TIMEOUT,
			id: id.clone(),
			ware_log_map: map,
			state: TxState::Ok,
			tab_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			meta_txns: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}

	pub async fn ware_name_list(&self) -> Vec<String> {
		let mut arr = Vec::new();
		let lock = self.0.lock().await;
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
	pub async fn find(&self, ware_name: &Atom) -> Option<Arc<DatabaseWare>> {
		let map = {
			self.0.lock().await.clone()
		};
		map.find(ware_name)
	}
}

pub trait Monitor {
	fn notify(&self, event: Event, mgr: Mgr);
}


// 事务统计
#[derive(Clone, Debug)]
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

//================================ 内部结构和方法
const TIMEOUT: usize = 100;

// 库表
#[derive(Clone)]
struct WareMap(OrdMap<Tree<Atom, Arc<DatabaseWare>>>);

impl fmt::Debug for WareMap {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "WareMap size: {:?}", self.0.size())
	}
}

enum DatabaseWareSnapshot {
	MemSnapshot(Arc<MemDBSnapshot>),
	LogFileSnapshot(Arc<LogFileDBSnapshot>)
}

impl DatabaseWareSnapshot {
	pub fn new_mem_ware_snapshot(snapshot: MemDBSnapshot) -> DatabaseWareSnapshot {
		DatabaseWareSnapshot::MemSnapshot(Arc::new(snapshot))
	}

	pub async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.list().await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.list().await
			}
		}
	}

	pub async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.tab_info(tab_name).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.tab_info(tab_name).await
			}
		}
	}

	pub fn check(&self, tab: &Atom, meta: &Option<Arc<TabMeta>>) -> SResult<()> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.check(tab, meta)
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.check(tab, meta)
			}
		}
	}

	pub async fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.alter(tab_name, meta).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.alter(tab_name, meta).await
			}
		}
	}

	pub async fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool) -> Option<SResult<Arc<DatabaseTabTxn>>> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				let txn = shot.tab_txn(tab_name, id, writable).await;
				match txn {
					Some(Ok(t)) => {
						match t {
							TxnType::MemTxn(t1) => Some(Ok(Arc::new(DatabaseTabTxn::MemTabTxn(t1)))),
							TxnType::LogFileTxn(t1) => Some(Ok(Arc::new(DatabaseTabTxn::LogFileTabTxn(t1))))
						}
					}
					_ => Some(Err("create mem tab txn failed".to_string()))
				}
				
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				let txn = shot.tab_txn(tab_name, id, writable).await;
				match txn {
					Some(Ok(t)) => {
						match t {
							TxnType::MemTxn(t1) => Some(Ok(Arc::new(DatabaseTabTxn::MemTabTxn(t1)))),
							TxnType::LogFileTxn(t1) => Some(Ok(Arc::new(DatabaseTabTxn::LogFileTabTxn(t1))))
						}
					}
					_ => Some(Err("create log file tab txn failed".to_string()))
				}
			}
		}
	}

	pub fn meta_txn(&self, id: &Guid) -> Arc<DatabaseMetaTxn> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				Arc::new(DatabaseMetaTxn::MemMetaTxn(shot.meta_txn(id)))
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				Arc::new(DatabaseMetaTxn::LogFileMetaTxn(shot.meta_txn(id)))
			}
		}
	}

	pub async fn prepare(&self, id: &Guid) -> SResult<()> {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.prepare(id).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.prepare(id).await
			}
		}
	}

	pub async fn commit(&self, id: &Guid) {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.commit(id).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.commit(id).await
			}
		}
	}

	pub async fn rollback(&self, id: &Guid) {
		match self {
			DatabaseWareSnapshot::MemSnapshot(shot) => {
				shot.rollback(id).await
			}
			DatabaseWareSnapshot::LogFileSnapshot(shot) => {
				shot.rollback(id).await
			}
		}
	}

	pub fn notify(&self, _event: Event) {}
}


pub enum DatabaseWare {
	MemWare(Arc<MemDB>),
	LogFileWare(Arc<LogFileDB>)
}

unsafe impl Send for DatabaseWare {}
unsafe impl Sync for DatabaseWare {}

impl DatabaseWare {
	pub fn new_memware(db: MemDB) -> DatabaseWare {
		DatabaseWare::MemWare(Arc::new(db))
	}

	pub fn new_log_file_ware(db: LogFileDB) -> DatabaseWare {
		DatabaseWare::LogFileWare(Arc::new(db))
	}

	async fn tabs_clone(&self) -> Arc<DatabaseWare> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				let cloned = memdb.tabs_clone().await;
				Arc::new(DatabaseWare::MemWare(cloned))
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				let cloned = logfiledb.tabs_clone().await;
				Arc::new(DatabaseWare::LogFileWare(cloned))
			}
		}
	}

	async fn snapshot(&self) -> Arc<DatabaseWareSnapshot> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				let shot = memdb.snapshot().await;
				Arc::new(DatabaseWareSnapshot::MemSnapshot(shot))
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				let shot = logfiledb.snapshot().await;
				Arc::new(DatabaseWareSnapshot::LogFileSnapshot(shot))
			}
		}
	}

	async fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				memdb.tab_info(tab_name).await
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				logfiledb.tab_info(tab_name).await
			}
		}
	}

	async fn list(&self) -> Box<dyn Iterator<Item=Atom>> {
		match self {
			DatabaseWare::MemWare(memdb) => {
				memdb.list().await
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				logfiledb.list().await
			}
		}
	}

	fn timeout(&self) -> usize {
		match self {
			DatabaseWare::MemWare(memdb) => {
				memdb.timeout()
			}
			DatabaseWare::LogFileWare(logfiledb) => {
				logfiledb.timeout()
			}
		}
	}
}

pub enum DatabaseTabTxn {
	MemTabTxn(Arc<RefMemeryTxn>),
	LogFileTabTxn(Arc<RefLogFileTxn>)
}

impl DatabaseTabTxn {
	fn new_mem_tab_txn(txn: RefMemeryTxn) -> DatabaseTabTxn {
		DatabaseTabTxn::MemTabTxn(Arc::new(txn))
	}

	async fn get_state(&self) -> TxState {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.get_state().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.get_state().await
			}
		}
	}

	pub async fn prepare(&self, timeout: usize) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.prepare(timeout).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.prepare(timeout).await
			}
		}
	}

	pub async fn commit(&self) -> CommitResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.commit().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.commit().await
			}
		}
	}

	pub async fn rollback(&self) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.rollback().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.rollback().await
			}
		}
	}

	pub async fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.key_lock(_arr, _lock_time, _readonly).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.key_lock(_arr, _lock_time, _readonly).await
			}
		}
	}

	pub async fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool
	) -> Option<SResult<Vec<TabKV>>> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.query(arr, _lock_time, _readonly).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.query(arr, _lock_time, _readonly).await
			}
		}
	}

	pub async fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool) -> DBResult {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.modify(arr, _lock_time, _readonly).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.modify(arr, _lock_time, _readonly).await
			}
		}
	}

	// 迭代
	pub async fn iter(
		&self,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> Option<IterResult> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.iter(tab, key, descending, filter).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.iter(tab, key, descending, filter).await
			}
		}
	}

	pub async fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter
	) -> Option<KeyIterResult> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.key_iter(key, descending, filter).await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.key_iter(key, descending, filter).await
			}
		}
	}

	pub fn index(
		&self,
		_tab: &Atom,
		_index_key: &Atom,
		_key: Option<Bin>,
		_descending: bool,
		_filter: Filter,
	) -> Option<IterResult> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.index(_tab, _index_key, _key, _descending, _filter)
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.index(_tab, _index_key, _key, _descending, _filter)
			}
		}
	}

	pub async fn tab_size(&self) -> Option<SResult<usize>> {
		match self {
			DatabaseTabTxn::MemTabTxn(txn) => {
				txn.tab_size().await
			}
			DatabaseTabTxn::LogFileTabTxn(txn) => {
				txn.tab_size().await
			}
		}
	}
}

enum DatabaseMetaTxn {
	MemMetaTxn(Arc<MemeryMetaTxn>),
	LogFileMetaTxn(Arc<LogFileMetaTxn>)
}

impl DatabaseMetaTxn {
	fn new_mem_meta_txn(txn: MemeryMetaTxn) -> DatabaseMetaTxn {
		DatabaseMetaTxn::MemMetaTxn(Arc::new(txn))
	}

	async fn alter(&self, _tab: &Atom, _meta: Option<Arc<TabMeta>>) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.alter(_tab, _meta).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.alter(_tab, _meta).await
			}
		}
	}

	async fn snapshot(&self, _tab: &Atom, _from: &Atom) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.snapshot(_tab, _from).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.snapshot(_tab, _from).await
			}
		}
	}

	async fn rename(&self, _tab: &Atom, _new_name: &Atom) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.rename(_tab, _new_name).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.rename(_tab, _new_name).await
			}
		}
	}

	fn get_state(&self) -> TxState {
		TxState::Ok
	}

	async fn prepare(&self, _timeout: usize) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.prepare(_timeout).await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.prepare(_timeout).await
			}
		}
	}

	async fn commit(&self) -> CommitResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.commit().await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.commit().await
			}
		}

	}

	async fn rollback(&self) -> DBResult {
		match self {
			DatabaseMetaTxn::MemMetaTxn(txn) => {
				txn.rollback().await
			}
			DatabaseMetaTxn::LogFileMetaTxn(txn) => {
				txn.rollback().await
			}
		}
	}
}

impl WareMap {
	fn new() -> Self {
		WareMap(OrdMap::new(new()))
	}

	async fn wares_clone(&self) -> Self{
		let mut wares = Vec::new();
		for ware in self.0.iter(None, false){
			wares.push(Entry(ware.0.clone(), ware.1.tabs_clone().await));
		}
		WareMap(OrdMap::new(Tree::from_order(wares)))
	}
	// 注册库
	fn register(&mut self, ware_name: Atom, ware: Arc<DatabaseWare>) -> bool {
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
	fn find(&self, ware_name: &Atom) -> Option<Arc<DatabaseWare>> {
		match self.0.get(&ware_name) {
			Some(b) => Some(b.clone()),
			_ => None
		}
	}

	fn keys(&self, key: Option<&Atom>, descending: bool) -> Keys<Tree<Atom, Arc<DatabaseWare>>> {
		self.0.keys(key, descending)
	}
}

// 子事务
pub struct Tr {
	// TODO 下面几个可以放到锁的外部，减少锁
	writable: bool,
	timeout: usize, // 子事务的预提交的超时时间, TODO 取提交的库的最大超时时间
	id: Guid,
	ware_log_map: FnvHashMap<Atom, Arc<DatabaseWareSnapshot>>,// 库名对应库快照
	state: TxState,
	tab_txns: FnvHashMap<(Atom, Atom), Arc<DatabaseTabTxn>>, //表事务表
	meta_txns: FnvHashMap<Atom, Arc<DatabaseMetaTxn>>, //元信息事务表
}

impl Tr {
	// 获得事务的状态
	pub fn get_state(&self) -> TxState {
		self.state.clone()
	}
	// 预提交事务
	pub async fn prepare(&mut self) -> DBResult {
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
				match self.ware_log_map.get_mut(ware).unwrap().prepare(&self.id).await {
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

		//处理每个表的预提交
		for val in self.tab_txns.values_mut() {
			match val.prepare(self.timeout).await {
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
			match val.prepare(self.timeout).await {
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
	pub async fn commit(&mut self) -> DBResult {
		self.state = TxState::Committing;
		// 先提交mgr上的事务
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				self.ware_log_map.get(ware).unwrap().commit(&self.id).await;
			}
		}
		let len = self.tab_txns.len() + alter_len;
		if len == 0 {
			return Some(Ok(()));
		}
		// println!(" ======== pi_db::mgr::commit txid: {:?}, alter_len: {:?}, tab_txn_len: {:?}", self.id.time(), alter_len, self.tab_txns.len());
		let count = Arc::new(AtomicUsize::new(len));

		//处理每个表的提交
		for (txn_name, val) in self.tab_txns.iter_mut() {
			match val.commit().await {
				Some(r) => {
					match r {
						Ok(logs) => {
							for (k, v) in logs.into_iter(){ //将表的提交日志添加到事件列表中
								match v {
									RwLog::Write(value) => {
										
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
			match val.commit().await {
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
	pub async fn rollback(&mut self) -> DBResult {
		self.state = TxState::Rollbacking;
		// 先回滚mgr上的事务
		let alter_len = self.meta_txns.len();
		if alter_len > 0 {
			for ware in self.meta_txns.keys() {
				self.ware_log_map.get(ware).unwrap().rollback(&self.id).await;
			}
		}
		let len = self.tab_txns.len() + alter_len;
		let count = Arc::new(AtomicUsize::new(len));
		
		//处理每个表的预提交
		for val in self.tab_txns.values_mut() {
			match val.rollback().await {
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
			match val.rollback().await {
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
	async fn key_lock(&mut self, tr: &Tr, arr: Vec<TabKV>, lock_time: usize, read_lock: bool) -> DBResult {
		None
	}
	// 查询
	pub async fn query(
		&mut self,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		read_lock: bool
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
		
		let map = tab_map(arr);
		for ((ware_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let c2 = rvec.clone();
			match self.build(&ware_name, &tab_name).await {
				Some(r) => match r {
					Ok(t) => match t.query(tkv, lock_time, read_lock).await {
						Some(r) => match r {
							Ok(vec) => {
								match merge_result(&rvec, vec).await {
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
	pub async fn modify(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, read_lock: bool) -> DBResult {
		if arr.len() == 0 {
			return Some(Ok(()))
		}

		// 保存每个txid的修改
		let data = arr.iter().cloned().collect::<Vec<TabKV>>();
		let map = tab_map(arr);
		self.state = TxState::Doing;
		let count = Arc::new(AtomicUsize::new(map.len()));

		for ((ware_name, tab_name), val) in map.into_iter() {
			let tkv = Arc::new(val);
			let tkv1 = tkv.clone();
			let c2 = count.clone();
			match self.build(&ware_name, &tab_name).await {
				Some(r) => match r {
					Ok(t) => match self.handle_result(&count, t.modify(tkv, lock_time, read_lock).await) {
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
	pub async fn iter(&mut self, ware: &Atom, tab: &Atom, key: Option<Bin>, descending: bool, filter: Filter) -> Option<IterResult> {
		let key1 = key.clone();
		let filter1 = filter.clone();

		self.state = TxState::Doing;
		let tab_clone1 = tab.clone();
		let tab_clone2 = tab.clone();
		match self.build(&ware, &tab).await {
			Some(r) => match r {
				Ok(t) => self.iter_result(t.iter(&tab_clone2, key, descending, filter).await),
				Err(s) => {
					self.state = TxState::Err;
					Some(Err(s))
				}
			},
			_ => None
		}
	}
	// 迭代
	pub async fn key_iter(&mut self, ware: &Atom, tab: &Atom, key: Option<Bin>, descending: bool, filter: Filter) -> Option<KeyIterResult> {
		self.state = TxState::Doing;
		match self.build(&ware, &tab).await {
			Some(r) => match r {
				Ok(t) => self.iter_result(t.key_iter(key, descending, filter).await),
				Err(s) => {
					self.state = TxState::Err;
					Some(Err(s))
				}
			},
			_ => None
		}
	}
	// 表的大小
	pub async fn tab_size(&mut self, ware_name: &Atom, tab_name: &Atom) -> Option<SResult<usize>> {
		self.state = TxState::Doing;
		match self.build(ware_name, tab_name).await {
			Some(r) => match r {
				Ok(t) => match self.single_result(t.tab_size().await) {
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
	pub async fn alter(&mut self, ware_name: &Atom, tab_name: &Atom, meta: Option<Arc<TabMeta>>) -> DBResult {
		self.state = TxState::Doing;
		let ware = match self.ware_log_map.get(ware_name) {
			Some(w) => match w.check(tab_name, &meta) { // 检查
				Ok(_) =>{
					w.alter(tab_name, meta.clone()).await;
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
		
		self.single_result(txn.alter(tab_name, meta).await)
	}
	// 表改名
	pub fn rename(&mut self, _ware_name: &Atom, _old_name: &Atom, _new_name: Atom, _cb: TxCallback) -> DBResult {
		self.state = TxState::Doing;
		// TODO
		None
	}

	// 表的元信息
	pub async fn tab_info(&self, ware_name:&Atom, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.ware_log_map.get(ware_name) {
			Some(ware) => ware.tab_info(tab_name).await,
			_ => None
		}
	}

	// 列出指定库的所有表
	pub async fn list(&self, ware_name: &Atom) -> Option<Vec<String>> {
		match self.ware_log_map.get(ware_name) {
			Some(ware) => {
				let mut arr = Vec::new();
				for e in ware.list().await {
						arr.push(e.to_string())
				}
				Some(arr)
			},
			_ => None
		}
	}
	
	// 创建表
	pub async fn build(&mut self, ware_name: &Atom, tab_name: &Atom) -> Option<SResult<Arc<DatabaseTabTxn>>> {
		//let txn_key = Atom::from(String::from((*ware_name).as_str()) + "##" + tab_name.as_str());
		let txn_key = (ware_name.clone(), tab_name.clone());
		let txn = match self.tab_txns.get(&txn_key) {
			Some(r) => return Some(Ok(r.clone())),
			_ => match self.ware_log_map.get(ware_name) {
				Some(ware) => match ware.tab_txn(tab_name, &self.id, self.writable).await {
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
async fn merge_result(rvec: &Arc<Mutex<(usize, Vec<TabKV>)>>, vec: Vec<TabKV>) -> Option<SResult<Vec<TabKV>>> {
	let mut t = rvec.lock().await;
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
