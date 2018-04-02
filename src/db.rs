#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]

/**
 * db的定义
 * 会话负责用户、权限管理
 */

use std::result::Result;
use std::vec::Vec;

/// 系统表的前缀
const PRIFIX = "_$";

/// 
pub trait AsyncDB<T: AsyncTxn> {
    fn transaction(&mut self, id: u128, writable: bool, timeout:usize) -> T;
}
pub trait SyncDB<T: SyncTxn> {
    fn transaction(&mut self, id: u128, writable: bool, timeout:usize) -> T;
}
pub enum DBError {
	None,
	Timeout,
	InvalidArgs,
	InvalidState,
	InvalidTab,
	ReadonlyTab,
	WriteonlyTab,
}

pub enum TxState {
	Ok,
	Doing,
	Preparing,
	Prepared,
	Committing,
	Commited,
	Rollbacking,
	Rollbacked,
}

pub type DBResult<T> = Result<T, DBError>;
pub type DBResultDefault = Result<(), DBError>;

pub type TxCallback = FnMut(DBResultDefault);
pub type TxQueryCallback = FnMut(DBResult<Vec<TabKeyValue>>);
pub type TxIterCallback = FnMut(DBResult<Cursor>);


/**
 * 表键条目
 * @example
 */
pub struct TabKey {
	tab: String,
	key: Vec<u8>,
}
/**
 * 表键值条目
 * @example
 */
pub struct TabKeyValue {
	tab: String,
	key: Vec<u8>,
	index: usize,
	value: Option<Arc<Vec<u8>>>,
}
pub trait Cursor {
    fn state(&self) -> DBResult<bool>;
    fn key(&self) -> &Vec<u8>;
    fn value(&self) -> Option<Arc<Vec<u8>>>;
    fn next(&mut self);
}
pub trait AsyncTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&mut self, TxCallback);
	// 提交一个事务
	fn commit(&mut self, TxCallback);
	// 回滚一个事务
	fn rollback(&mut self, TxCallback);
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, TxCallback);
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, TxQueryCallback);
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, TxCallback);
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, TxCallback);
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, TxIterCallback);
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, TxIterCallback);
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, TxCallback);

}
pub trait SyncTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&mut self) -> DBResultDefault;
	// 提交一个事务
	fn commit(&mut self) -> DBResultDefault;
	// 回滚一个事务
	fn rollback(&mut self) -> DBResultDefault;
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize) -> DBResultDefault;
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>) -> DBResult<Vec<TabKeyValue>>;
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>) -> DBResultDefault;
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>) -> DBResultDefault;
	// 迭代表
	fn iterTab(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String) -> DBResult<Cursor>;
	// 迭代索引
	fn iterIndex(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String) -> DBResult<Cursor>;
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String) -> DBResultDefault;

}

