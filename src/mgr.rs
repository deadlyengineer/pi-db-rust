#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]

/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */

use pi_lib::ordmap::Entry;
use pi_lib::ordmap::OrdMap;
use pi_lib::ordmap::ImOrdMap;
use pi_lib::asbtree::Tree;


use db::{DB, DBError, TxState, TabKey, Transaction, TxCallback};


pub type TxHandler = FnMut(&mut Transaction);

pub struct Mgr<T1, T2> {
    async: OrdMap<String, T1, asbtree<String, T1>>,
    sync: OrdMap<String, T2, asbtree<String, T2>>,
    // 定时轮
}


impl<T1: AsyncDB, T2: SyncDB> Mgr<T1, T2> {
    // 注册数据库
	fn register(prefix: String, db: T) -> bool {

    }
    // 取消注册数据库
	fn unregister(prefix: String) {

    }
    // 读事务，无限尝试直到超时，默认10秒
	fn read(tx:TxHandler, timeout:usize, TxCallback) {

    }
	// 写事务，无限尝试直到超时，默认10秒
	fn write(tx:TxHandler, timeout:usize, TxCallback) {

    }
    fn transaction(&mut self, writable: bool, timeout:usize) -> Txn {
        let Txn {

        }
    }

}

pub struct Txn<T1, T2> {
    writable: bool,
    timeout: usize,
    state: TxState,
    timer_ref: usize,
    async: OrdMap<String, T1, asbtree<String, T1>>,
    sync: OrdMap<String, T2, asbtree<String, T2>>,
}

impl<T1: AsyncTxn, T2: SyncTxn> Txn<T1, T2> {

	// 判断事务是否可写
	fn is_writable(&self) -> bool {
        self.writable
    }
	// 获得事务的超时时间
	fn get_timeout(&self) -> usize {
        self.timeout
    }
	// 获得事务的状态
	fn get_state(&self) -> TxState {
        self.state
    }
	// 预提交一个事务
	fn prepare(&mut self, TxCallback) {
        match self.state {
           TxState::Ok -> (),
           _ ->
                return TxCallback(DBError::Invalid_state)
        }
    }
	// 提交一个事务
	fn commit(&mut self, TxCallback) {
        match self.state {
           TxState::Prepared -> (),
           _ ->
                return TxCallback(DBError::Invalid_state)
        }
    }
	// 回滚一个事务
	fn rollback(&mut self, cb: TxCallback) {

    }
	// 锁
	fn lock(&mut self, arr:Vec<TabKey>, lock_time:usize, cb: TxCallback) {
        
    }
	// 查询
	fn query(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: TxQueryCallback) {
        
    }
	// 插入或更新
	fn upsert(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: TxCallback) {
        
    }
	// 删除
	fn delete(&mut self, arr:Vec<TabKey>, lock_time:Option<usize>, cb: TxCallback) {
        
    }
	// 迭代
	fn iter(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, cb: TxIterCallback) {

    }
	// 索引迭代
	fn index(&mut self, tab_key:TabKey, descending: bool, key_only:bool, filter:String, cb: TxIterCallback) {

    }
	// 新增 修改 删除 表
	fn alter(&mut self, tab:String, metaJson:String, cb: TxCallback) {

    }


}