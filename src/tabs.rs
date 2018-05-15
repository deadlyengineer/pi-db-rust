/**
 * 基于2pc的db管理器，每个db实现需要将自己注册到管理器上
 */
use std::sync::{Arc, Mutex};
use std::mem;

use fnv::{FnvHashMap, FnvHashSet};

use pi_lib::ordmap::OrdMap;
use pi_lib::asbtree::{Tree, new};
use pi_lib::atom::Atom;
use pi_lib::sinfo::StructInfo;
use pi_lib::guid::Guid;

use db::{DBResult, Tab, TabTxn, TabBuilder};

// 表结构及修改日志
pub struct TabLog {
	map: OrdMap<Tree<Atom, TabInfo>>,
	old_map: OrdMap<Tree<Atom, TabInfo>>, // 用于判断mgr中tabs是否修改过
	meta_names: FnvHashSet<Atom>, //元信息表的名字
	alter_logs: FnvHashMap<(Atom, usize), Option<Arc<StructInfo>>>, // 记录每个被改过元信息的表
	rename_logs: FnvHashMap<Atom, (Atom, usize)>, // 新名字->(源名字, 版本号)
}
impl TabLog {
	pub fn new(map: &OrdMap<Tree<Atom, TabInfo>>) -> Self {
		TabLog {
			map: map.clone(),
			old_map: map.clone(),
			meta_names: FnvHashSet::with_capacity_and_hasher(0, Default::default()),
			alter_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			rename_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}
	// 表的元信息
	fn replace(&mut self) -> Self {
		TabLog {
			map: self.map.clone(),
			old_map: self.old_map.clone(),
			meta_names: mem::replace(&mut self.meta_names, FnvHashSet::with_capacity_and_hasher(0, Default::default())),
			alter_logs: mem::replace(&mut self.alter_logs, FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			rename_logs: mem::replace(&mut self.rename_logs, FnvHashMap::with_capacity_and_hasher(0, Default::default())),
		}
	}
	// 表的元信息
	pub fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		match self.map.get(tab_name) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}
	// 新增 修改 删除 表
	pub fn alter(&mut self, tab_name: &Atom, meta: Option<Arc<StructInfo>>) {
		// 先查找rename_logs，获取该表的源名字及版本，然后修改alter_logs
		let tab_ver = match self.rename_logs.get(tab_name) {
			Some(v) => v.clone(),
			_ => (tab_name.clone(), 0),
		};
		self.alter_logs.entry(tab_ver.clone()).or_insert(meta.clone());
		self.meta_names.insert(tab_name.clone());
	}
}

// 表管理器
pub struct Tabs {
	//全部的表结构
	map: OrdMap<Tree<Atom, TabInfo>>,
	// 预提交的元信息事务表
	prepare: Mutex<FnvHashMap<Guid, TabLog>>,
}

impl Tabs {
	pub fn new() -> Self {
		Tabs {
			map : OrdMap::new(new()),
			prepare: Mutex::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
		}
	}
	// 获取当前表结构快照
	pub fn snapshot(&self) -> TabLog {
		TabLog::new(&self.map)
	}
	// 获取表的元信息
	pub fn get_tab_meta(&self, tab: &Atom) -> Option<Arc<StructInfo>> {
		match self.map.get(tab) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}
	// 设置表的元信息
	pub fn set_tab_meta(&mut self, tab: Atom, meta: Arc<StructInfo>) -> bool {
		self.map.insert(tab, TabInfo::new(meta))
	}

	// 元信息的预提交
	pub fn parpare(&mut self, id: &Guid, log: &mut TabLog) -> DBResult<usize> {
		// 先检查预提交的交易是否有冲突
		let mut prepare = self.prepare.lock().unwrap();
		for val in prepare.values() {
			if val.meta_names.is_disjoint(&log.meta_names) {
				return Err(String::from("meta parpare conflicting"))
			}
		}
		// 然后检查数据表是否被修改
		if !self.map.ptr_eq(&log.old_map) {
			// 如果被修改，则检查是否有冲突
			// TODO 暂时没有考虑重命名的情况
			for name in log.meta_names.iter() {
				match self.map.get(name) {
					Some(r1) => match log.old_map.get(name) {
						Some(r2) if (r1 as *const TabInfo) == (r2 as *const TabInfo) => (),
						_ => return Err(String::from("meta parpare conflicted"))
					}
					_ => match log.old_map.get(name) {
						None => (),
						_ => return Err(String::from("meta parpare conflicted"))
					}
				}
			}
		}
		prepare.insert(id.clone(), log.replace());
		Ok(1)
	}
	// 元信息的提交
	pub fn commit(&mut self, id: &Guid) {
		match self.prepare.lock().unwrap().remove(id) {
			Some(log) => if self.map.ptr_eq(&log.old_map) {
				// 检查数据表是否被修改， 如果没有修改，则可以直接替换根节点
				self.map = log.map;
			}else{
				// 否则，重新执行一遍修改， TODO
			}
			_ => ()
		}
	}
	// 回滚
	pub fn rollback(&mut self, id: &Guid) {
		self.prepare.lock().unwrap().remove(id);
	}
	// 创建表事务
	pub fn build(&mut self, builder: &Arc<TabBuilder>, tab_name: &Atom, id: &Guid, writable: bool, timeout:usize, cb: Box<Fn(DBResult<Arc<TabTxn>>)>) -> Option<DBResult<Arc<TabTxn>>> {
		match self.map.get(tab_name) {
			Some(ref info) => {
				let tab = {
					let mut var = info.init.lock().unwrap();
					match var.wait {
						Some(ref mut vec) => {// 表尚未build
							if vec.len() == 0 {// 第一次调用
								let var1 = info.init.clone();
								match builder.open(tab_name, Box::new(move |tab| {
									// 异步返回，解锁后设置结果，返回等待函数数组
									let vec:Vec<Box<Fn(DBResult<Arc<Tab>>)>> = {
										let mut var = var1.lock().unwrap();
										let vec = mem::replace(var.wait.as_mut().unwrap(), Vec::new());
										var.tab = tab.clone();
										var.wait = None;
										vec
									};
									// 通知所有的等待函数数组
									for f in vec.into_iter() {
										(*f)(tab.clone())
									}
								})) {
									Some(r) => {// 同步返回，设置结果
										var.tab = r;
										var.wait = None;
										var.tab.clone()
									},
									_ => { //异步的第1次调用，直接返回
										vec.push(handle_fn(id.clone(), writable, timeout, cb));
										return None
									}
								}
							}else { // 异步的第n次调用，直接返回
								vec.push(handle_fn(id.clone(), writable, timeout, cb));
								return None
							}
						},
						_ => var.tab.clone()
					}
				};
				// 根据结果创建事务或返回错误
				match tab {
					Ok(tab) => Some(Ok(tab.transaction(&id, writable, timeout))),
					Err(s) => Some(Err(s))
				}
			},
			_ => Some(Err(String::from("TabNotFound")))
		}
	}
}
//================================ 内部结构和方法
// 表信息
#[derive(Clone)]
pub struct TabInfo {
	meta: Arc<StructInfo>,
	init: Arc<Mutex<TabInit>>,
}
impl TabInfo {
	fn new(meta: Arc<StructInfo>) -> Self {
		TabInfo{
			meta: meta,
			init: Arc::new(Mutex::new(TabInit {
				tab: Err(String::from("")),
				wait:Some(Vec::new()),
			})),
		}
	}
}
// 表初始化
pub struct TabInit {
	tab: DBResult<Arc<Tab>>,
	wait: Option<Vec<Box<Fn(DBResult<Arc<Tab>>)>>>, // 为None表示表示tab已经加载
}
//================================ 内部静态方法
// 表构建函数的回调函数
fn handle_fn(id: Guid, writable: bool, timeout: usize, cb: Box<Fn(DBResult<Arc<TabTxn>>)>) -> Box<Fn(DBResult<Arc<Tab>>)> {
	Box::new(move |r| {
		match r {
			Ok(tab) => {
				// 创建事务
				(*cb)(Ok(tab.transaction(&id, writable, timeout)))
			},
			Err(s) => (*cb)(Err(s))
		}
	})
}
