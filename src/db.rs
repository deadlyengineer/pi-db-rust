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
	fn get_state(&self) -> TxState {
		TxState::Fail
	}
	// 预提交一个事务
	fn prepare(&self, cb: TxCallback) -> UsizeResult {
		None
	}
	// 提交一个事务
	fn commit(&self, cb: TxCallback) -> UsizeResult {
		None
	}
	// 回滚一个事务
	fn rollback(&self, cb: TxCallback) -> UsizeResult {
		None
	}
}

// 每个表的事务
pub trait TabTxn : Txn{
	// 键锁，key可以不存在，根据lock_time的值，大于0是锁，0为解锁。 分为读写锁，读写互斥，读锁可以共享，写锁只能有1个
	fn key_lock(&self, arr: Arc<Vec<TabKV>>, lock_time: usize, readonly: bool, cb: TxCallback) -> UsizeResult {
		None
	}
	// 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		lock_time: Option<usize>,
		readonly: bool,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
		None
	}
	// 修改，插入、删除及更新。
	fn modify(&self, arr: Arc<Vec<TabKV>>, lock_time: Option<usize>, readonly: bool, cb: TxCallback) -> UsizeResult {
		None
	}
	// 迭代
	fn iter(
		&self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		key_only: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 索引迭代
	fn index(
		&self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
		None
	}
	// 表的大小
	fn tab_size(&self, cb: TxCallback) -> UsizeResult {
		None
	}
}

//
pub trait Tab {
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
	// 打开指定的表，表必须有meta
	fn open(
		&self,
		tab: &Atom,
		cb: Box<Fn(DBResult<Arc<Tab>>)>,
	) -> Option<DBResult<Arc<Tab>>>;
	// 检查该表是否可以创建
	fn check(
		&self,
		tab: &Atom,
		meta: &Arc<StructInfo>,
	) -> DBResult<()>;
	// 创建一个meta事务
	fn transaction(&self, id: &Guid, timeout: usize) -> Arc<MetaTxn>;

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
	pub key: Vec<u8>,
	pub index: usize,
	pub value: Option<Arc<Vec<u8>>>,
}
impl TabKV {
	pub fn new(tab: Atom, key: Vec<u8>) -> Self {
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
