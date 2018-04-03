/**
 * db的定义
 * 会话负责用户、权限管理
 */


use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::u128;

// 系统表的前缀
const PRIFIX:&'static str = "_$";

//
pub trait AsyncDB<T: AsyncTxn> {
    fn transaction(&mut self, id: u128, writable: bool, timeout:usize) -> T;
}
pub trait SyncDB<T: SyncTxn> {
    fn transaction(&mut self, id: u128, writable: bool, timeout:usize) -> T;
}

#[derive(Clone)]
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

pub type DBResult<T> = Result<T, String>;
pub type DBResultDefault = Result<(), String>;

pub type TxCallback = FnMut(DBResultDefault);
pub type TxQueryCallback = FnMut(DBResult<Vec<TabKeyValue>>);
pub type TxIterCallback = FnMut(DBResult<Box<Cursor>>);


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
	fn prepare(&mut self, Box<TxCallback>);
	// 提交一个事务
	fn commit(&mut self, Box<TxCallback>);
	// 回滚一个事务
	fn rollback(&mut self, Box<TxCallback>);
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, Box<TxCallback>);
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, Box<TxQueryCallback>);
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, Box<TxCallback>);
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, Box<TxCallback>);
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, Box<TxIterCallback>);
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, Box<TxIterCallback>);
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, Box<TxCallback>);

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

