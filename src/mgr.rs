#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]

/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::Arc;
use std::sync::Weak;
use std::sync::RwLock;

use pi_lib::ordmap::ImOrdMap;
use pi_lib::asbtree::{TreeMap, new};


use db::{AsyncDB, SyncDB, DBResult, TxState, TabKey, TabKeyValue, AsyncTxn, SyncTxn, TxCallback, TxQueryCallback, TxIterCallback};


pub type TxHandler = FnMut(&mut ArcTx);
pub type TxIter = FnMut(DBResult<TabKeyValue>);

pub trait Txn {
	fn is_writable(&self) -> bool;
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize;
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&mut self, mut cb: Box<TxCallback>);
	// 提交一个事务
	fn commit(&mut self, mut cb: Box<TxCallback>);
	// 回滚一个事务
	fn rollback(&mut self, mut cb: Box<TxCallback>);
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, cb: Box<TxCallback>);
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: Box<TxQueryCallback>);
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: Box<TxCallback>);
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: Box<TxCallback>);
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, cb: Box<TxIter>);
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, cb: Box<TxIter>);
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, cb: Box<TxCallback>);

}

pub struct Mgr {
    async: TreeMap<String, Box<AsyncDB<AsyncTxn>>>,
    sync: TreeMap<String, Box<SyncDB<SyncTxn>>>,
    // 定时轮
    // 管理用的弱引用事务
    map: TreeMap<u128, Weak<RwLock<Tx>>>,
}


impl Mgr {
    // 注册数据库
	fn register_async(&mut self, prefix: String, db: Box<AsyncDB<AsyncTxn>>) -> bool {
        return false;
    }
	fn register_sync(&mut self, prefix: String, db: Box<AsyncDB<AsyncTxn>>) -> bool {
        return false;
    }
    // 取消注册数据库
	fn unregister(&mut self, prefix: String) {

    }
    // 读事务，无限尝试直到超时，默认10秒
	fn read(&mut self, tx: Box<TxHandler>, timeout:usize, cb: Box<TxCallback>) {

    }
	// 写事务，无限尝试直到超时，默认10秒
	fn write(&mut self, tx: Box<TxHandler>, timeout:usize, cb: Box<TxCallback>) {

    }
    fn transaction(&mut self, writable: bool, timeout:usize) -> ArcTx {
        Arc::new(RwLock::new(Tx{
            writable: writable,
            timeout: timeout,
            mgr: self as *mut Self,
            start_time: 0,
            state: TxState::Ok,
            timer_ref: 0,
            async: new(),
            sync: new(),
        }))
    }

}

pub struct Tx {
    writable: bool,
    timeout: usize,
    mgr: *mut Mgr,
    start_time: u64, // us
    state: TxState,
    timer_ref: usize,
    async: TreeMap<String, Arc<AsyncTxn>>,
    sync: TreeMap<String, Arc<SyncTxn>>,
}

pub type ArcTx = Arc<RwLock<Tx>>;

impl Txn for ArcTx {

	// 判断事务是否可写
	fn is_writable(&self) -> bool {
        self.read().unwrap().writable
    }
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize {
        self.read().unwrap().timeout
    }
	// 获得事务的状态
	fn get_state(&self) -> TxState {
        self.read().unwrap().state.clone()
    }
	// 预提交一个事务
	fn prepare(&mut self, mut cb: Box<TxCallback>) {
        match self.read().unwrap().state {
           TxState::Ok => (),
           _ =>
                return (*cb)(Err(String::from("InvalidState")))
        }
    }
	// 提交一个事务
	fn commit(&mut self, mut cb: Box<TxCallback>) {
        match self.read().unwrap().state {
           TxState::Prepared => (),
           _ =>
                return (*cb)(Err(String::from("InvalidState")))
        }
    }
	// 回滚一个事务
	fn rollback(&mut self, mut cb: Box<TxCallback>) {
        match self.read().unwrap().state {
           TxState::Ok => (),
           TxState::Prepared => (),
           _ =>
                return (*cb)(Err(String::from("InvalidState")))
        }
    }
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, cb: Box<TxCallback>) {
        
    }
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: Box<TxQueryCallback>) {
        
    }
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: Box<TxCallback>) {
        
    }
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: Box<TxCallback>) {
        
    }
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, cb: Box<TxIter>) {

    }
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, cb: Box<TxIter>) {

    }
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, cb: Box<TxCallback>) {

    }


}