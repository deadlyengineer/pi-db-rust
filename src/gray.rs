/**
 * mgr的灰度发布器
 */
use std::sync::{Arc, RwLock};

use fnv::FnvHashMap;

use mgr::Mgr;

pub struct Gray(Mgr, Arc<RwLock<FnvHashMap<usize, Mgr>>>);

impl Gray {
	pub fn new(mgr: Mgr) -> Self {
		Gray(mgr, Arc::new(RwLock::new(FnvHashMap::with_capacity_and_hasher(0, Default::default()))))
	}
	// 获得指定键的管理器
	pub fn get(&self, key: usize) -> Option<Mgr> {
		match self.1.read().unwrap().get(&key) {
			Some(mgr) => Some(mgr.clone()),
			_ => None
		}
	}
	// 设置指定键的管理器
	pub fn set(&self, key: usize, mgr: Mgr) {
		self.1.write().unwrap().insert(key, mgr);
	}
	// 设置指定键的管理器
	pub fn remove(&self, key: usize) {
		self.1.write().unwrap().remove(&key);
	}
	// 获得缺省管理器
	pub fn get_default(&self) -> Mgr {
		self.0.clone()
	}
	// 设置缺省管理器
	pub fn set_default(&mut self, key: usize) -> bool {
		match self.1.write().unwrap().remove(&key) {
			Some(mgr) => {
				self.0 = mgr;
				true
			},
			_ => false
		}
	}

}