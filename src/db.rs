/**
 * db的定义
 * 会话负责用户、权限管理
 */


use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::u128;

// 系统表的前缀
const PRIFIX:&str = "_$";

pub trait Txn {
	fn is_writable(&self) -> bool;
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize;
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&mut self, mut cb: TxCallback);
	// 提交一个事务
	fn commit(&mut self, mut cb: TxCallback);
	// 回滚一个事务
	fn rollback(&mut self, mut cb: TxCallback);
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, mut cb: TxCallback);
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, mut cb: TxQueryCallback);
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, mut cb: TxCallback);
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, mut cb: TxCallback);
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, mut cb: TxIterCallback);
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, mut cb: TxIterCallback);
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, mut cb: TxCallback);

}

//
pub trait AsyncDB<T: AsyncTxn> {
    fn transaction(&mut self, id: u128, writable: bool, timeout:usize) -> T;
}
pub trait SyncDB<T: SyncTxn> {
    fn transaction(&mut self, id: u128, writable: bool, timeout:usize) -> T;
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

pub type DBResult<T> = Result<T, String>;
pub type DBResultDefault = Result<(), String>;

pub type TxCallback = Box<FnMut(DBResultDefault)>;
pub type ArcTxCallback = Arc<FnMut(DBResultDefault)>;
pub type TxQueryCallback = Box<FnMut(DBResult<Vec<TabKeyValue>>)>;
pub type TxIterCallback = Box<FnMut(DBResult<Box<Cursor>>)>;

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
	fn prepare(&mut self, ArcTxCallback) -> Option<DBResultDefault>;
	// 提交一个事务
	fn commit(&mut self, ArcTxCallback) -> Option<DBResultDefault>;
	// 回滚一个事务
	fn rollback(&mut self, ArcTxCallback) -> Option<DBResultDefault>;
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, ArcTxCallback) -> Option<DBResultDefault>;
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, TxQueryCallback) -> DBResult<Vec<TabKeyValue>>;
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, ArcTxCallback) -> Option<DBResultDefault>;
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, ArcTxCallback) -> Option<DBResultDefault>;
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, TxIterCallback) -> Option<DBResult<Box<Cursor>>>;
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, TxIterCallback) -> Option<DBResult<Box<Cursor>>>;
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, TxCallback) -> Option<DBResultDefault>;

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
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String) -> DBResult<Box<Cursor>>;
	// 迭代索引
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String) -> DBResult<Box<Cursor>>;
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String) -> DBResultDefault;

}

