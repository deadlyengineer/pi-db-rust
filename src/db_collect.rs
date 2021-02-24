use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc};
use std::fs;
use std::str::FromStr;

use atom::Atom;
use bon::{Decode, ReadBuffer};
use hash::XHashMap;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::{AsyncRuntime, AsyncValue};
use r#async::lock::spin_lock::SpinLock;
use chrono::prelude::*;
use cron::Schedule;

use crate::log_file_db::{AsyncLogFileStore, DB_META_TAB_NAME, LogFileDB, LogFileTab};

pub fn collect_db(rt: MultiTaskRuntime<()>, start_at: usize) {
	let local: DateTime<Local> = Local::now();
	let expression = format!("0  0  {}  *  *  *  *", start_at);
	let schedule = Schedule::from_str(&expression).unwrap();

	let date = schedule.upcoming(Local).take(1).into_iter().nth(0).unwrap();
	let trigger_after = date.signed_duration_since(local).num_milliseconds();
	info!("start db collect task after {} ms", trigger_after);

	let _ = rt.clone().spawn_timing(rt.clone().alloc(), async move {
		let start = Local::now();
		LogFileDB::force_split().await; // 强制分裂
		let meta = LogFileDB::open(&Atom::from(DB_META_TAB_NAME)).await.unwrap();
		let map = meta.1.map.lock();
		for (key, _) in map.iter() {
			let tab_name = Atom::decode(&mut ReadBuffer::new(key, 0)).unwrap();
			let mut file = LogFileDB::open(&tab_name).await.unwrap();
			info!("collect tab {:?} ", tab_name);
			file.1.log_file.collect(1024 * 1024, 32 * 1024, false).await;
		}
		info!("db collect done, start time =  {}, end time = {}", start, Local::now());
		collect_db(rt.clone(), start_at);
	}, trigger_after as usize);
}