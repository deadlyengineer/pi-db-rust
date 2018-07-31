/**
 * DB的定义
 */

use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::ops::{DerefMut, Deref};
use std::cmp::{Ord, Eq, PartialOrd, PartialEq, Ordering};

use fnv::FnvHashMap;

use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::sinfo::StructInfo;
use pi_lib::bon::ReadBuffer;


// 系统表的前缀
pub const PRIFIX: &str = "_$";

pub type Bin = Arc<Vec<u8>>;

pub type SResult<T> = Result<T, String>;
pub type DBResult = Option<SResult<()>>;
pub type CommitResult = Option<SResult<FnvHashMap<Bin, RwLog>>>;
pub type IterResult = SResult<Box<Iter<Item = (Bin, Bin)>>>;
pub type KeyIterResult = SResult<Box<Iter<Item = Bin>>>;
pub type NextResult<T> = SResult<Option<T>>;

pub type TxCallback = Arc<Fn(SResult<()>)>;
pub type TxQueryCallback = Arc<Fn(SResult<Vec<TabKV>>)>;

pub type Filter = Option<Arc<Fn(Bin)-> Option<Bin>>>;

//事务
pub trait Txn {
	// 获得事务的状态
	fn get_state(&self) -> TxState;
	// 预提交一个事务
	fn prepare(&self, timeout:usize, cb: TxCallback) -> DBResult;
	// 提交一个事务
	fn commit(&self, cb: TxCallback) -> CommitResult;
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
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(IterResult)>,
	) -> Option<IterResult>;
	// 迭代
	fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(KeyIterResult)>,
	) -> Option<KeyIterResult>;
	// 索引迭代
	fn index(
		&self,
		tab: &Atom,
		index_key: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(IterResult)>,
	) -> Option<IterResult>;
	// 表的大小
	fn tab_size(&self, cb: TxCallback) -> DBResult;
}

// 每个Ware的元信息事务
pub trait MetaTxn : Txn {
	// 创建表、修改指定表的元数据
	fn alter(&self, tab: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> DBResult;
	// 快照拷贝表
	fn snapshot(&self, tab: &Atom, from: &Atom, cb: TxCallback) -> DBResult;
	// 修改指定表的名字
	fn rename(&self, tab: &Atom, new_name: &Atom, cb: TxCallback) -> DBResult;
}

// 表定义
pub trait Tab {
	fn new(tab: &Atom) -> Self;
	// 创建表事务
	fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn>;
	
	//fn get_prepare() -> (Atom, Bin, Option<Bin>); //取到预提交信息， （tab_name, key, value）
}

// 打开表的接口定义
pub trait OpenTab {
	// 打开指定的表，表必须有meta
	fn open<'a, T: Tab>(&self, tab: &Atom, cb: Box<Fn(SResult<T>) + 'a>) -> Option<SResult<T>>;
}
// 库
pub trait Ware {
	// 拷贝全部的表
	fn tabs_clone(&self) -> Arc<Ware>;
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	fn timeout(&self) -> usize;
	// 列出全部的表
	fn list(&self) -> Box<Iterator<Item=Atom>>;
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>>;
	// 创建当前表结构快照
	fn snapshot(&self) -> Arc<WareSnapshot>;
}
// 库快照
pub trait WareSnapshot {
	// 列出全部的表
	fn list(&self) -> Box<Iterator<Item=Atom>>;
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>>;
	// 检查该表是否可以创建
	fn check(&self, tab: &Atom, meta: &Option<Arc<StructInfo>>) -> SResult<()>;
	// 新增 修改 删除 表
	fn alter(&self, tab_name: &Atom, meta: Option<Arc<StructInfo>>);
	// 创建指定表的表事务
	fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>>;
	// 创建一个meta事务
	fn meta_txn(&self, id: &Guid) -> Arc<MetaTxn>;
	// 元信息的预提交
	fn prepare(&self, id: &Guid) -> SResult<()>;
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
	pub key: Bin,
	pub index: usize,
	pub value: Option<Bin>,
}
impl TabKV {
	pub fn new(ware: Atom, tab: Atom, key: Bin) -> Self {
		TabKV{
			ware: ware,
			tab: tab,
			key: key,
			index: 0,
			value: None,
		}
	}
}

pub trait Iter {
	type Item;
	fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>>;
}

#[derive(Clone, Debug)]
pub enum RwLog {
	Read,
	Write(Option<Bin>),
	Meta(Option<Bin>),
}

pub struct Prepare(FnvHashMap<Guid, FnvHashMap<Bin, RwLog>>);

impl Prepare{
	pub fn new(map: FnvHashMap<Guid, FnvHashMap<Bin, RwLog>>) -> Prepare{
		Prepare(map)
	}

    //检查预提交是否冲突（如果预提交表中存在该条目，且其类型为write， 同时，本次预提交类型也为write， 即预提交冲突）
	pub fn try_prepare (&self, key: &Bin, log_type: &RwLog) -> Result<(), String> {
		for o_rwlog in self.0.values() {
			match o_rwlog.get(key) {
				Some(RwLog::Read) => match log_type {
					RwLog::Read => return Ok(()),
					_ => return Err(String::from("parpare conflicted rw"))
				},
				None => return Ok(()),
				Some(_e) => {
					return Err(String::from("parpare conflicted rw2"))
				},
			}
		}

		Ok(())
	}
}

impl Deref for Prepare {
    type Target = FnvHashMap<Guid, FnvHashMap<Bin, RwLog>>;

    fn deref(&self) -> &FnvHashMap<Guid, FnvHashMap<Bin, RwLog>> {
        &self.0
    }
}

impl DerefMut for Prepare {
    fn deref_mut(&mut self) -> &mut FnvHashMap<Guid, FnvHashMap<Bin, RwLog>> {
        &mut self.0
    }
}

#[derive(Default, Clone, Hash)]
pub struct Bon(Arc<Vec<u8>>);

impl Bon{
	pub fn new(inner: Arc<Vec<u8>>) -> Bon{
		Bon(inner)
	}
}

impl Deref for Bon{
	type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialOrd for Bon {
	fn partial_cmp(&self, other: &Bon) -> Option<Ordering> {
		ReadBuffer::new(self.0.as_slice(), 0).partial_cmp(&ReadBuffer::new(other.0.as_slice(), 0))
	}
}

impl PartialEq for Bon{
	 fn eq(&self, other: &Bon) -> bool {
        match self.partial_cmp(other){
			Some(Ordering::Equal) => return true,
			_ => return false
		};
    }
}

impl Eq for Bon{}

impl Ord for Bon{
	fn cmp(&self, other: &Bon) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}