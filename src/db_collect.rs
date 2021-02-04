use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc};
use std::fs;

use atom::Atom;
use bon::{Decode, ReadBuffer};
use hash::XHashMap;
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::{AsyncRuntime, AsyncValue};
use r#async::lock::spin_lock::SpinLock;
use chrono::prelude::*;

use crate::log_file_db::{AsyncLogFileStore, DB_META_TAB_NAME, LogFileDB, LogFileTab};

pub fn collect_db(rt: MultiTaskRuntime<()>, start_at: usize) {
	let local: DateTime<Local> = Local::now();
	let h = local.hour();
	let m = local.minute() as isize;
	let time = (start_at as isize) - (h as isize);
	let after = if time > 0 {
		time
	} else {
		time + 24
	};

	let trigger_after = ((after * 60 - m) as usize * 60) * 1000;

	info!("start db collect task after {} ms", trigger_after);

	let _ = rt.clone().spawn_timing(rt.clone().alloc(), async move {
		if let Err(e) = LogFileDB::collect().await {
			error!("db collect error: {:?}", e);
		}
		collect_db(rt.clone(), start_at);
	}, trigger_after);
}