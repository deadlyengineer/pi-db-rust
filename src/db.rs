/**
 * DB的定义
 */

use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;

use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::sinfo::StructInfo;

use tabs::TabLog;

// 系统表的前缀
pub const PRIFIX: &str = "_$";

pub type SResult<T> = Result<T, String>;
pub type DBResult = Option<SResult<()>>;

pub type TxCallback = Arc<Fn(SResult<()>)>;
pub type TxQueryCallback = Arc<Fn(SResult<Vec<TabKV>>)>;
pub type TxIterCallback = Arc<Fn(SResult<Box<Cursor>>)>;

//事务
pub trait Txn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&self, timeout:usize, cb: TxCallback) -> DBResult;
	// 提交一个事务
	fn commit(&self, cb: TxCallback) -> DBResult;
	// 回滚一个事务
	fn rollback(&self, cb: TxCallback) -> DBResult;
}

// 每个表的事务
pub trait TabTxn : Txn{
	// 键锁，key可以不存在，根据lock_time的值，大于0是锁，0为解锁。 分为读写锁，读写互斥，读锁可以共享，写锁只能有1个
	fn key_lock(&self, arr: Arc<Vec<TabKV>>, lock_time: usize, read_lock: bool, cb: TxCallback) -> DBResult;
	// 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		lock_time: Option<usize>,
		read_lock: bool,
		cb: TxQueryCallback,
	) -> Option<SResult<Vec<TabKV>>>;
	// 修改，插入、删除及更新。
	fn modify(&self, arr: Arc<Vec<TabKV>>, lock_time: Option<usize>, read_lock: bool, cb: TxCallback) -> DBResult;
	// 迭代
	fn iter(
		&self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		key_only: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<SResult<Box<Cursor>>>;
	// 索引迭代
	fn index(
		&self,
		tab: &Atom,
		index_key: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<SResult<Box<Cursor>>>;
	// 表的大小
	fn tab_size(&self, cb: TxCallback) -> DBResult;
}

// 表
pub trait Tab {
	// 创建表事务
	fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn>;
}

// 每个Ware的元信息事务
pub trait MetaTxn : Txn {

	// 创建表、修改指定表的元数据
	fn alter(
		&self,
		tab: &Atom,
		meta: Option<Arc<StructInfo>>,
		cb: TxCallback,
	) -> DBResult;
	// 修改指定表的名字
	fn rename(
		&self,
		tab: &Atom,
		new_name: &Atom,
		cb: TxCallback,
	) -> DBResult;

}

// 库
pub trait Ware {
	// 拷贝全部的表
	//fn tabs_clone(&self) -> Self;
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	fn timeout(&self) -> usize;
	// 列出全部的表
	fn list(&self) -> Vec<Atom>;
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>>;
	// 打开指定的表，表必须有meta
	fn open(
		&self,
		tab: &Atom,
		cb: Box<Fn(SResult<Arc<Tab>>)>,
	) -> Option<SResult<Arc<Tab>>>;
	// 获取当前表结构快照
	fn snapshot(&self) -> TabLog;
	// 创建指定表的表事务
	fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>>;

	// 检查该表是否可以创建
	fn check(
		&self,
		tab: &Atom,
		meta: &Option<Arc<StructInfo>>,
	) -> SResult<()>;
	// 创建一个meta事务
	fn meta_txn(&self, id: &Guid) -> Arc<MetaTxn>;
	// 元信息的预提交
	fn prepare(&self, id: &Guid, log: &mut TabLog) -> SResult<()>;
	// 元信息的提交
	fn commit(&self, id: &Guid);
	// 回滚
	fn rollback(&self, id: &Guid);

}

#[derive(Clone)]
pub enum TxState {
	Ok = 1,
	Doing,
	Err,
	Preparing,
	PreparOk,
	PreparFail,
	Committing,
	Commited,
	CommitFail,
	Rollbacking,
	Rollbacked,
	RollbackFail,
}

/**
 * 表键值条目
 * @example
 */
#[derive(Default, Clone)]
pub struct TabKV {
	pub ware: Atom,
	pub tab: Atom,
	pub key: Arc<Vec<u8>>,
	pub index: usize,
	pub value: Option<Arc<Vec<u8>>>,
}
impl TabKV {
	pub fn new(ware: Atom, tab: Atom, key: Arc<Vec<u8>>) -> Self {
		TabKV{
			ware: ware,
			tab: tab,
			key: key,
			index: 0,
			value: None,
		}
	}
}
pub trait Cursor {
	fn state(&self) -> SResult<bool>;
	fn key(&self) -> Arc<Vec<u8>>;
	fn value(&self) -> Option<Arc<Vec<u8>>>;
	fn next(&mut self);
}
