/**
 * DB的定义
 */

use std::result::Result;
use std::sync::Arc;
use std::vec::Vec;
use std::ops::{Deref};
use std::cmp::{Ord, Eq, PartialOrd, PartialEq, Ordering};

use fnv::FnvHashMap;

use atom::Atom;
use guid::Guid;
use sinfo::EnumType;
use bon::{ReadBuffer, Decode, Encode, WriteBuffer, ReadBonErr};


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

#[derive(Debug)]
pub struct TabMeta {
	pub k: EnumType,
	pub v: EnumType
}

impl TabMeta{
	pub fn new(k: EnumType, v: EnumType) -> TabMeta{
		TabMeta{k, v}
	}
}

impl Decode for TabMeta{
	fn decode(bb: &mut ReadBuffer) -> Result<Self, ReadBonErr>{
		Ok(TabMeta{k: EnumType::decode(bb)?, v: EnumType::decode(bb)?})
	}
}

impl Encode for TabMeta{
	fn encode(&self, bb: &mut WriteBuffer){
		self.k.encode(bb);
		self.v.encode(bb);
	}
}

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
		tab: &Atom,
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
	fn tab_size(&self, cb: Arc<Fn(SResult<usize>)>) -> Option<SResult<usize>>;
}

// 每个Ware的元信息事务
pub trait MetaTxn : Txn {
	// 创建表、修改指定表的元数据
	fn alter(&self, tab: &Atom, meta: Option<Arc<TabMeta>>, cb: TxCallback) -> DBResult;
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

pub struct Event {
	pub ware: Atom,
	pub tab: Atom,
	pub other: EventType
}

pub enum EventType{
	Meta(Option<EnumType>),
	Tab{key: Bin, value: Option<Bin>},
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
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>>;
	// 创建当前表结构快照
	fn snapshot(&self) -> Arc<WareSnapshot>;
}
// 库快照
pub trait WareSnapshot {
	// 列出全部的表
	fn list(&self) -> Box<Iterator<Item=Atom>>;
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>>;
	// 检查该表是否可以创建
	fn check(&self, tab: &Atom, meta: &Option<Arc<TabMeta>>) -> SResult<()>;
	// 新增 修改 删除 表
	fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>);
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
	// 库修改通知
	fn notify(&self, event: Event);
}

#[derive(Clone, Debug)]
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

impl ToString for TxState{
	fn to_string(&self) -> String{
		match self {
			TxState::Ok => String::from("TxState::Ok"),
			TxState::Doing => String::from("TxState::Doing"),
			TxState::Err => String::from("TxState::Err"),
			TxState::Preparing => String::from("TxState::Preparing"),
			TxState::PreparOk => String::from("TxState::PreparOk"),
			TxState::PreparFail => String::from("TxState::PreparFail"),
			TxState::Committing => String::from("TxState::Committing"),
			TxState::Commited => String::from("TxState::Commited"),
			TxState::CommitFail => String::from("TxState::CommitFail"),
			TxState::Rollbacking => String::from("TxState::Rollbacking"),
			TxState::Rollbacked => String::from("TxState::Rollbacked"),
			TxState::RollbackFail => String::from("TxState::RollbackFail"),
		}
	}
}

impl TxState {
	//将整数转换为事务状态
	pub fn to_state(n: usize) -> Self {
		match n {
            1 => TxState::Ok,
            2 => TxState::Doing,
            3 => TxState::Err,
            4 => TxState::Preparing,
            5 => TxState::PreparOk,
            6 => TxState::PreparFail,
            7 => TxState::Committing,
            8 => TxState::Commited,
            9 => TxState::CommitFail,
            10 => TxState::Rollbacking,
            11 => TxState::Rollbacked,
            _ => TxState::RollbackFail,
        }
	}
}

/**
 * 表键值条目
 * @example
 */
#[derive(Default, Clone, Debug)]
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

//为了按照Bon协议比较字节数组， 定义了类型Bon
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