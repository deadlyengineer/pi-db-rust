/**
 * Tab的定义
 * 会话负责用户、权限管理
 */

use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::u128;

// 系统表的前缀
pub const PRIFIX: &str = "_$";

pub type DBResult<T> = Result<T, String>;
pub type DefaultResult = Option<Result<(), String>>;

pub type TxCallback = Arc<Fn(Result<(), String>)>;
pub type TxQueryCallback = Arc<Fn(DBResult<Vec<TabKV>>)>;
pub type TxIterCallback = Arc<FnMut(DBResult<Box<Cursor>>)>;

pub trait TxnInfo {
	fn is_writable(&self) -> bool;
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize;
	// 获得事务的状态
}

pub trait Txn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&mut self, TxCallback) -> DefaultResult;
	// 提交一个事务
	fn commit(&mut self, TxCallback) -> DefaultResult;
	// 回滚一个事务
	fn rollback(&mut self, TxCallback) -> DefaultResult;
	// 锁
	fn lock1(&mut self, arr: Vec<TabKV>, lock_time: usize, TxCallback) -> DefaultResult;
	// 查询
	fn query(
		&mut self,
		arr: Vec<TabKV>,
		lock_time: Option<usize>,
		TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>>;
	// 插入或更新
	fn upsert(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, TxCallback) -> DefaultResult;
	// 删除
	fn delete(&mut self, arr: Vec<TabKV>, lock_time: Option<usize>, TxCallback) -> DefaultResult;
	// 迭代
	fn iter(
		&mut self,
		tab_key: TabKV,
		descending: bool,
		key_only: bool,
		filter: String,
		TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	// 索引迭代
	fn index(
		&mut self,
		tab_key: TabKV,
		descending: bool,
		key_only: bool,
		filter: String,
		TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>>;
	// 新增 修改 删除 表
	fn alter(&mut self, tab: Arc<String>, class: Arc<String>, metaJson: String, TxCallback) -> DefaultResult;
}

//
pub trait Tab {
	// fn is_async(&self) -> bool;
	fn transaction(&self, id: u128, writable: bool, timeout: usize) -> Box<Txn>;
}

//
pub trait TabBuilder {
	fn build(
		&mut self,
		tab: String,
		clazz: String,
		metaJson: String,
		TxCallback,
	) -> Option<Result<Arc<Tab>, String>>;
}

#[derive(Clone)]
pub enum TxState {
	Ok = 1,
	Doing,
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
	pub tab: Arc<String>,
	pub key: Vec<u8>,
	pub index: usize,
	pub value: Option<Arc<Vec<u8>>>,
}
impl TabKV {
	pub fn new(tab: String, key: Vec<u8>) -> Self {
		TabKV{
			tab: Arc::new(tab),
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
