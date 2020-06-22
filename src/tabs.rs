/**
 * 表管理器，给每个具体的Ware用
 */
use std::sync::{Arc};
use std::mem;
use std::ops::{DerefMut, Deref};

use fnv::{FnvHashMap, FnvHashSet};

use ordmap::ordmap::{OrdMap, ActionResult, Keys};
use ordmap::asbtree::{Tree, new};
use atom::Atom;
use guid::Guid;

use crate::db::{SResult, Tab, TabTxn, Bin, RwLog, TabMeta, BuildDbType};
use crate::memery_db::DB as MemoryDB;
use crate::memery_db::{ MTab, RefMemeryTxn };
use r#async::lock::mutex_lock::Mutex;

// 表结构及修改日志
pub struct TabLog {
	map: OrdMap<Tree<Atom, TabInfo>>,
	old_map: OrdMap<Tree<Atom, TabInfo>>, // 用于判断mgr中tabs是否修改过
	meta_names: FnvHashSet<Atom>, //元信息表的名字
	alter_logs: FnvHashMap<(Atom, usize), Option<Arc<TabMeta>>>, // 记录每个被改过元信息的表
	rename_logs: FnvHashMap<Atom, (Atom, usize)>, // 新名字->(源名字, 版本号)
}
impl TabLog {
	// 列出全部的表
	pub fn list(&self) -> TabIter {
		TabIter::new(self.map.clone(), self.map.keys(None, false))
	}
	// 获取指定的表结构
	pub fn get(&self, tab: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.get(tab) {
			Some(t) => Some(t.meta.clone()),
			_ => None
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
	pub fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.get(tab_name) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}
	// 新增 修改 删除 表
	pub fn alter(&mut self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
		// 先查找rename_logs，获取该表的源名字及版本，然后修改alter_logs
		let (src_name, ver) = match self.rename_logs.get(tab_name) {
			Some(v) => v.clone(),
			_ => (tab_name.clone(), 0),
		};
		let mut f = |v: Option<&TabInfo>| {
			match v {
				Some(ti) => match &meta {
					Some(si) => ActionResult::Upsert(TabInfo {
						meta: si.clone(),
						init: ti.init.clone(),
					}),
					_ => ActionResult::Delete
				},
				_ => match &meta {
					Some(si) => ActionResult::Upsert(TabInfo::new(si.clone())),
					_ => ActionResult::Ignore
				}
			}
		};
		self.map.action(&src_name, &mut f);
		self.alter_logs.entry((src_name, ver)).or_insert(meta.clone());
		self.meta_names.insert(tab_name.clone());
	}
	// 创建表事务
	pub async fn build(&self, ware: BuildDbType, tab_name: &Atom, id: &Guid, writable: bool) -> Option<SResult<Arc<RefMemeryTxn>>> {
		match self.map.get(tab_name) {
			Some(ref info) => {
				let tab = {
					let mut var = info.init.lock().await;
					match var.wait {
						Some(ref mut vec) => {// 表尚未build
							if *vec {// 第一次调用
								match ware {
									BuildDbType::MemoryDB => {
										match MemoryDB::open(tab_name).await {
											Some(r) => {// 同步返回，设置结果
												if let Ok(t) = r {
													var.mem_tab = Some(t);
													var.wait = None;
													var.mem_tab.clone()
												} else {
													return Some(Err(String::from("memdb open error")))
												}
												
											},
											_ => { //异步的第1次调用，直接返回
												return None
											}
										}
									}
								}
							} else { // 异步的第n次调用，直接返回
								return None
							}
						},
						_ => {
							match ware {
								BuildDbType::MemoryDB => {
									var.mem_tab.clone()
								}
							}
						}
					}
				};
				// 根据结果创建事务或返回错误
				match tab {
					Some(tab) => Some(Ok(tab.transaction(&id, writable).await)),
					None => Some(Err(String::from("create tx error")))
				}
			},
			_ => {Some(Err(String::from("TabNotFound: ") + (*tab_name).as_str()))}
		}
	}
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

pub struct TabIter{
	_root: OrdMap<Tree<Atom, TabInfo>>,
	point: usize,
}

impl Drop for TabIter {
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut Keys<Tree<Atom, TabInfo>>)};
    }
}

impl<'a> TabIter {
	pub fn new(root: OrdMap<Tree<Atom, TabInfo>>, it: Keys<'a, Tree<Atom, TabInfo>>) -> TabIter{
		TabIter{
			_root: root.clone(),
			point: Box::into_raw(Box::new(it)) as usize
		}
	}
}

impl Iterator for TabIter {
	type Item = Atom;
	fn next(&mut self) -> Option<Self::Item>{
		let mut it = unsafe{Box::from_raw(self.point as *mut Keys<Tree<Atom, TabInfo>>)};
		let r = match it.next() {
			Some(k) => Some(k.clone()),
			None => None,
		};
		mem::forget(it);
		r
	}
}


// 表管理器
pub struct Tabs {
	//全部的表结构
	map: OrdMap<Tree<Atom, TabInfo>>,
	// 预提交的元信息事务表
	prepare: FnvHashMap<Guid, TabLog>,
}

impl Tabs {
	pub fn new() -> Self {
		Tabs {
			map : OrdMap::new(new()),
			prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}

	pub fn clone_map(&self) -> Self{
		Tabs {
			map : self.map.clone(),
			prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}

	// 列出全部的表
	pub fn list(&self) -> TabIter {
		TabIter::new(self.map.clone(), self.map.keys(None, false))
	}
	// 获取指定的表结构
	pub fn get(&self, tab: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.get(tab) {
			Some(t) => Some(t.meta.clone()),
			_ => None
		}
	}
	// 获取当前表结构快照
	pub fn snapshot(&self) -> TabLog {
		TabLog {
			map: self.map.clone(),
			old_map: self.map.clone(),
			meta_names: FnvHashSet::with_capacity_and_hasher(0, Default::default()),
			alter_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			rename_logs: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
		}
	}
	// 获取表的元信息
	pub fn get_tab_meta(&self, tab: &Atom) -> Option<Arc<TabMeta>> {
		match self.map.get(tab) {
			Some(info) => Some(info.meta.clone()),
			_ => None,
		}
	}
	// 设置表的元信息
	pub fn set_tab_meta(&mut self, tab: Atom, meta: Arc<TabMeta>) -> bool {
		let r = self.map.insert(tab, TabInfo::new(meta));
		r
	}

	// 预提交
	pub fn prepare(&mut self, id: &Guid, log: &mut TabLog) -> SResult<()> {
		// 先检查预提交的交易是否有冲突
		for val in self.prepare.values() {
			if !val.meta_names.is_disjoint(&log.meta_names) {
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
		self.prepare.insert(id.clone(), log.replace());
		Ok(())
	}
	// 元信息的提交
	pub fn commit(&mut self, id: &Guid) {
		match self.prepare.remove(id) {
			Some(log) => if self.map.ptr_eq(&log.old_map) {
				// 检查数据表是否被修改， 如果没有修改，则可以直接替换根节点
				self.map = log.map;
			}else{
				// 否则，重新执行一遍修改
				for r in log.alter_logs {
					if r.1.is_none() {
						self.map.delete(&Atom::from((r.0).0.as_ref()), false);
					} else {
						let tab_info = TabInfo::new(r.1.unwrap());
						self.map.upsert(Atom::from((r.0).0.as_ref()), tab_info, false);
					}
				}
			}
			_ => ()
		}
	}
	// 回滚
	pub fn rollback(&mut self, id: &Guid) {
		self.prepare.remove(id);
	}

}
//================================ 内部结构和方法
// 表信息
#[derive(Clone)]
pub struct TabInfo {
	meta:  Arc<TabMeta>,
	init: Arc<Mutex<TabInit>>,
}
impl TabInfo {
	fn new(meta: Arc<TabMeta>) -> Self {
		TabInfo{
			meta: meta,
			init: Arc::new(Mutex::new(TabInit {
				mem_tab: None,
				file_mem_tab: None,
				log_file_tab: None,
				wait:Some(true),
			})),
		}
	}
}
// 表初始化
struct TabInit {
	mem_tab: Option<MTab>,
	file_mem_tab: Option<MTab>,
	log_file_tab: Option<MTab>,
	wait: Option<bool>, // 为None表示tab已经加载
}
