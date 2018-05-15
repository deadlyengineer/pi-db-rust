/**
 * Tab的定义
 * 会话负责用户、权限管理
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

pub type DBResult<T> = Result<T, String>;
pub type UsizeResult = Option<DBResult<usize>>;

pub type TxCallback = Arc<Fn(DBResult<usize>)>;
pub type TxQueryCallback = Arc<Fn(DBResult<Vec<TabKV>>)>;
pub type TxIterCallback = Arc<Fn(DBResult<Box<Cursor>>)>;

//事务
pub trait Txn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&self, cb: TxCallback) -> UsizeResult;
	// 提交一个事务
	fn commit(&self, cb: TxCallback) -> UsizeResult;
	// 回滚一个事务
	fn rollback(&self, cb: TxCallback) -> UsizeResult;
}

// 每个表的事务
pub trait TabTxn : Txn{
	// 键锁，key可以不存在，根据lock_time的值，大于0是锁，0为解锁。 分为读写锁，读写互斥，读锁可以共享，写锁只能有1个
	fn key_lock(&self, arr: Arc<Vec<TabKV>>, lock_time: usize, read_lock: bool, cb: TxCallback) -> UsizeResult;
	// 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		lock_time: Option<usize>,
		read_lock: bool,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>>;
	// 修改，插入、删除及更新。
	fn modify(&self, arr: Arc<Vec<TabKV>>, lock_time: Option<usize>, read_lock: bool, cb: TxCallback) -> UsizeResult;
	// 迭代
	fn iter(
		&self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		key_only: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	// 索引迭代
	fn index(
		&self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	// 表的大小
	fn tab_size(&self, cb: TxCallback) -> UsizeResult;
}

// 表
pub trait Tab {
	// 创建表事务
	fn transaction(&self, id: &Guid, writable: bool, timeout: usize) -> Arc<TabTxn>;
}

// 每个TabBuilder的元信息事务
pub trait MetaTxn : Txn {

	// 创建表、修改指定表的元数据
	fn alter(
		&self,
		tab: &Atom,
		meta: Option<Arc<StructInfo>>,
		cb: TxCallback,
	) -> UsizeResult;
	// 修改指定表的名字
	fn rename(
		&self,
		tab: &Atom,
		new_name: &Atom,
		cb: TxCallback,
	) -> UsizeResult;

}

// 表构建器
pub trait TabBuilder {
	// 列出全部的表
	fn list(
		&self,
	) -> Vec<(Atom, Arc<StructInfo>)>;
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>>;
	// 打开指定的表，表必须有meta
	fn open(
		&self,
		tab: &Atom,
		cb: Box<Fn(DBResult<Arc<Tab>>)>,
	) -> Option<DBResult<Arc<Tab>>>;
	// 获取当前表结构快照
	fn snapshot(&self) -> TabLog;
	// 创建指定表的表事务
	fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool, timeout:usize, cb: Box<Fn(DBResult<Arc<TabTxn>>)>) -> Option<DBResult<Arc<TabTxn>>>;

	// 检查该表是否可以创建
	fn check(
		&self,
		tab: &Atom,
		meta: &Option<Arc<StructInfo>>,
	) -> DBResult<()>;
	// 创建一个meta事务
	fn meta_txn(&self, id: &Guid, timeout: usize) -> Arc<MetaTxn>;
	// 元信息的预提交
	fn prepare(&self, id: &Guid, log: &mut TabLog) -> DBResult<usize>;
	// 元信息的提交
	fn commit(&self, id: &Guid);
	// 回滚
	fn rollback(&self, id: &Guid);

}

#[derive(Clone)]
pub enum TxState {
	Ok = 1,
	Doing,
	Fail,
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
	pub tab: Atom,
	pub key: Arc<Vec<u8>>,
	pub index: usize,
	pub value: Option<Arc<Vec<u8>>>,
}
impl TabKV {
	pub fn new(tab: Atom, key: Arc<Vec<u8>>) -> Self {
		TabKV{
			tab: tab,
			key: key,
			index: 0,
			value: None,
		}
	}
}
pub trait Cursor {
	fn state(&self) -> DBResult<bool>;
	fn key(&self) -> Arc<Vec<u8>>;
	fn value(&self) -> Option<Arc<Vec<u8>>>;
	fn next(&mut self);
}
