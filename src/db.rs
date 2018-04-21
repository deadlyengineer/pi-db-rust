/**
 * Tab的定义
 * 会话负责用户、权限管理
 */

use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::u128;

use pi_lib::atom::Atom;
use pi_lib::sinfo::StructInfo;

// 系统表的前缀
pub const PRIFIX: &str = "_$";

pub type DBResult<T> = Result<T, String>;
pub type UsizeResult = Option<Result<usize, String>>;

pub type TxCallback = Arc<Fn(Result<usize, String>)>;
pub type TxQueryCallback = Arc<Fn(DBResult<Vec<TabKV>>)>;
pub type TxIterCallback = Arc<Fn(DBResult<Box<Cursor>>)>;


pub trait Txn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&mut self, cb: TxCallback) -> UsizeResult;
	// 提交一个事务
	fn commit(&mut self, cb: TxCallback) -> UsizeResult;
	// 回滚一个事务
	fn rollback(&mut self, cb: TxCallback) -> UsizeResult;
	// 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
	fn klock(&mut self, arr: Vec<TabKV>, lock_time: usize, cb: TxCallback) -> UsizeResult;
	// 查询
	fn query(
		&mut self,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>>;
	// 修改，插入、删除及更新
	fn modify(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, TxCallback) -> UsizeResult;
	// 迭代
	fn iter(
		&mut self,
		tab: Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		key_only: bool,
		filter: String,
		TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	// 索引迭代
	fn index(
		&mut self,
		tab: Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	// 表的大小
	fn tab_size(&mut self, tab: Atom, cb: TxCallback) -> UsizeResult;
}

//
pub trait Tab {
	fn transaction(&self, id: u128, writable: bool, timeout: usize) -> Box<Txn>;
}

//
pub trait TabBuilder {
	fn iter(
		&self,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	fn build(
		&mut self,
		tab: Atom,
		meta: Arc<Vec<u8>>,
		cb: TxCallback,
	) -> Option<Result<Arc<Tab>, String>>;
	fn alter(
		&mut self,
		meta: Arc<Vec<u8>>,
		cb: TxCallback,
	) -> Option<Result<Arc<Tab>, String>>;
	fn delete(&mut self, tab: Atom);
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
	Rollbacking,
	Rollbacked,
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
	pub fn new(tab: String, key: Vec<u8>) -> Self {
		TabKV{
			tab: Atom::from(tab),
			key: key,
			index: 0,
			value: None,
		}
	}
}
pub trait Cursor {
	fn state(&self) -> DBResult<bool>;
	fn key(&self) -> &[u8];
	fn value(&self) -> Option<Arc<Vec<u8>>>;
	fn next(&mut self);
}
