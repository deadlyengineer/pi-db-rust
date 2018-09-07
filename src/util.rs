use std::boxed::FnBox;
use std::sync::Arc;
use std::io::Result as IoResult;
use std::cell::RefCell;

use crc::crc32::{Digest, IEEE};
use crc::Hasher32;

use mgr::Mgr;
use pi_lib::atom::Atom;
use pi_lib::bon::{WriteBuffer, Encode};
use pi_base::file::{AsyncFile, SharedFile, WriteOptions, AsynFileOptions, Shared};
use db::{Iter, Bin};


pub fn restore(mgr: &Mgr, ware: Atom, tab: Atom, file: Atom, callback: Box<FnBox(Result<(), String>)>) {

}

//dump数据库表数据
pub fn dump(mgr: &Mgr, ware: Atom, tab: Atom, file: String, callback: Arc<Fn(Result<(), String>)>) {
	let file_temp = file.clone()+ ".temp";
	let tr = mgr.transaction(false);
	//先将数据写入临时表中
	AsyncFile::open(file_temp.clone(), AsynFileOptions::OnlyAppend(1), Box::new(move |f: IoResult<AsyncFile>|{
		//写入数据回调，r：Result<(写入条目数量, 写入条目的crc值), String>
		let callback1 = callback.clone();
		let file_temp = file_temp.clone();
		let file = file.clone();
		//数据库数据写入完成后回调， 根据写入的条目数量和crc校验值改写文件头信息， 并删除原文件， 从命名temp文件
		let iter_cb = Arc::new(move |r: Result<(u64, u32), String>|{
			match r {
				Ok(v) => {
					let mut arr = Vec::with_capacity(12);
					arr.extend_from_slice(&v.0.to_bytes());
					arr.extend_from_slice(&v.1.to_bytes());
					let callback2 = callback1.clone();
					let file = file.clone();
					let file_temp1 = file_temp.clone();
					AsyncFile::open(file_temp.clone(), AsynFileOptions::OnlyWrite(1), Box::new(move |f: IoResult<AsyncFile>|{
						let f = match f {
							Ok(v) => SharedFile::new(v),
							Err(s) => {
								callback2(Err(s.to_string()));
								return;
							}
						};
						let file = file.clone();
						let file_temp = file_temp1.clone();
						let callback2 = callback2.clone();
						f.pwrite(WriteOptions::None, 12, arr, Box::new(move |_f: SharedFile, _r: IoResult<usize>|{
							let file1 = file.clone();
							let file_temp = file_temp.clone();
							let callback2 = callback2.clone();
							AsyncFile::remove(file, Box::new(move |_r|{
								{ let _drop = _f; }//释放SharedFile所有权
								let callback2 = callback2.clone();
								AsyncFile::rename(file_temp.clone(), file1, Box::new(move |_r1: String, _r2: String, r3: IoResult<()>|{
									match r3 {
										Ok(_v) => {
											callback2(Ok(()))
										},
										Err(s) => callback2(Err(s.to_string())),
									};
								}))
							}));
						}));
					}));
				},
				Err(s) => callback1(Err(s)),
			}
			
		});

		{
			let f = match f {
				Ok(v) => SharedFile::new(v),
				Err(s) => {
					callback(Err(s.to_string()));
					return;
				}
			};

			let mut arr = Vec::with_capacity(24);
			arr.extend_from_slice(&(0 as u32).to_bytes());//版本
			arr.extend_from_slice(&(0 as u64).to_bytes());////时间
			arr.extend_from_slice(&(0 as u64).to_bytes());//记录条目数量
			arr.extend_from_slice(&(0 as u32).to_bytes());//crc
			f.clone().pwrite(WriteOptions::None, 0, arr, Box::new(|_f: SharedFile, _r: IoResult<usize>|{}));

			let ware = ware.clone();
			let tab = tab.clone();
			let meta_bytes = match tr.tab_info(&ware, &tab) {
				Some(v) => {
					let mut bb = WriteBuffer::new();
					v.encode(&mut bb);
					bb.unwrap()
				},
				None => {
					callback(Err(String::from("meta is not exist, ware:") + ware.as_str() + ", tab:" + ware.as_str()));
					return;
				}
			};
			let meta_bytes_len = {
				let mut bb = WriteBuffer::new();
				bb.write_lengthen(meta_bytes.len() as u32);
				bb.unwrap()
			};
			f.clone().pwrite(WriteOptions::None, 0, meta_bytes_len, Box::new(|_f: SharedFile, _r: IoResult<usize>|{}));//meta 长度
			f.clone().pwrite(WriteOptions::None, 0, meta_bytes, Box::new(|_f: SharedFile, _r: IoResult<usize>|{}));//meta

			let crc = Arc::new(RefCell::new(Digest::new(IEEE)));
			let f1 = f.clone();
			let iter_cb1 = iter_cb.clone();
			let crc2 = crc.clone();
			let it = tr.iter(&ware, &tab, None, false, None, Arc::new(move |r|{
				match r {
					Ok(i) => {
						iter_ware_write(Arc::new(RefCell::new(i)), f1.clone(), crc2.clone(), 0, iter_cb1.clone());
					},
					Err(s) => iter_cb1(Err(s)),
				}
			}));

			match it {
				Some(v) => {
					match v {
						Ok(i) => {
							iter_ware_write(Arc::new(RefCell::new(i)), f.clone(), crc.clone(), 0, iter_cb.clone());
						},
						Err(s) => {
							iter_cb(Err(s))
						},
					}
				},
				None => (),
			};
		}
	}));
}

//遍历数据库并将数据写入文件， 同时累计计算crc的值
fn iter_ware_write(it: Arc<RefCell<Box<Iter<Item = (Bin, Bin)>>>>, f: SharedFile, crc: Arc<RefCell<Digest>>, count: u64, call_back: Arc<Fn(Result<(u64, u32), String>)>){
	let it1 = it.clone();
	let it2 = it.clone();
	let f1 = f.clone();
	let call_back1 = call_back.clone();
	let crc1 = crc.clone();
	let r = it.borrow_mut().next(Arc::new(move |r|{
		iter_ware_write1(r, it1.clone(), f.clone(), crc1.clone(), count, call_back.clone());
	}));
	match r {
		Some(v) => {
			iter_ware_write1(v, it2.clone(), f1.clone(), crc.clone(), count, call_back1.clone());
		},
		None => {
			()
		},
	}
}

//将一条数据写入文件， 同时累计计算crc的值
fn iter_ware_write1(r: Result<Option<(Bin, Bin)>, String>, it: Arc<RefCell<Box<Iter<Item = (Bin, Bin)>>>>, f: SharedFile, crc: Arc<RefCell<Digest>>, mut count: u64, call_back: Arc<Fn(Result<(u64, u32), String>)>){
	match r {
			Ok(v) => {
				match v {
					Some(r) => {
						count += 1;
						let l = r.0.len() + r.1.len();
						let mut arr = {
							let mut bb = WriteBuffer::with_capacity(4 + l);
							bb.write_lengthen(l as u32);
							bb.unwrap()
						};
						arr.extend_from_slice(r.0.as_slice());
						arr.extend_from_slice(r.1.as_slice());
						crc.borrow_mut().write(r.0.as_slice());
						crc.borrow_mut().write(r.1.as_slice());
						f.clone().pwrite(WriteOptions::None, 0, arr, Box::new(move |_f: SharedFile, _r: IoResult<usize>|{
							iter_ware_write(it.clone(), f.clone(), crc.clone(), count, call_back.clone()); //结果或通过回调给出
						}));//写len,key,value
					},
					None =>call_back(Ok((count, crc.borrow().sum32()))),
				}
			},
			Err(s) => call_back(Err(s)),
		}
}

#[cfg(test)]
use pi_lib::guid::GuidGen;
#[cfg(test)]
use pi_lib::sinfo::EnumType;
#[cfg(test)]
use memery_db::DB;
#[cfg(test)]
use db::{TabMeta, TabKV};
#[cfg(test)]
use pi_base::pi_base_impl::{STORE_TASK_POOL};
#[cfg(test)]
use pi_base::worker_pool::WorkerPool;

#[test]
fn test_dump(){
	let worker_pool1 = Box::new(WorkerPool::new(3, 1024 * 1024, 1000));
    worker_pool1.run(STORE_TASK_POOL.clone());


	let ware_name = Atom::from("test_dump");
	let tab_name = Atom::from("tab1");
	let file = String::from("./") + ware_name.as_str() + "/" + tab_name.as_str();
	let mgr = Mgr::new(GuidGen::new(0, 0));
	let db = DB::new();
	mgr.register(ware_name.clone(), Arc::new(db));
	let tr = mgr.transaction(true);

	tr.alter(&ware_name, &tab_name, Some(Arc::new(TabMeta::new(EnumType::Str, EnumType::Str))), Arc::new(|r|{}));

	let mut v1 = WriteBuffer::new();
	v1.write_utf8("1");
	let v1 = Arc::new(v1.unwrap());

	let mut v2 = WriteBuffer::new();
	v2.write_utf8("2");
	let v2 = Arc::new(v2.unwrap());

	let mut item1 = TabKV::new(ware_name.clone(), tab_name.clone(), v1.clone());
	item1.value = Some(v1.clone());
	let mut item2 = TabKV::new(ware_name.clone(), tab_name.clone(), v2.clone());
	item2.value = Some(v2.clone());

	let arr = vec![item1, item2];
	tr.modify(arr, None, false, Arc::new(|r|{}));
	
	tr.prepare(Arc::new(|r| {}));
	tr.commit(Arc::new(|r| {}));

	dump(&mgr, ware_name.clone(), tab_name.clone(), file, Arc::new(|r|{
		println!("dump-----------------{:?}", r);
		assert!(r.is_ok());
	}));
}