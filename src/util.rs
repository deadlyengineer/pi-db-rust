use std::sync::Arc;
use std::io::Result as IOResult;
use std::boxed::FnBox;
use std::cell::RefCell;

use crc::crc64::{self, Digest, Hasher64};

use bon::{Encode, ReadBuffer, WriteBuffer};
use atom::Atom;
use file::file::{AsyncFileOptions, WriteOptions, AsyncFile, Shared, SharedFile};

use mgr::Mgr;
use db::{SResult, TabKV, Iter, Bin};

//批量恢复限制
const BATCH_RESTORE_LIMIT: u64 = 1000;

/*
* 还原指定表
*/
pub fn restore(mgr: &Mgr, ware: Atom, tab: Atom, file: Atom, callback: Box<FnBox(Result<(), String>)>) {
	let mgr_copy = mgr.clone();
	let file_copy = file.clone();
	let open = Box::new(move |result: IOResult<AsyncFile>| {
		match result {
			Err(e) => callback_error(format!("restore table error, open dump file failed, file: {:?}, err: {:?}", file_copy, e), Box::into_raw(callback)),
			Ok(f) => get_dump_meta(Arc::new(f), mgr_copy, ware, tab, Box::into_raw(callback)),
		}
	});
	AsyncFile::open((&file).to_string(), AsyncFileOptions::OnlyRead(1), open);
}

//获取备份文件中的元信息
fn get_dump_meta(file: SharedFile, mgr: Mgr, ware: Atom, tab: Atom, callback: *mut FnBox(Result<(), String>)) {
	let read = Box::new(move |f: SharedFile, result: IOResult<Vec<u8>>| {
		match result {
			Err(e) => callback_error(format!("restore table error, get dump meta failed, table: {}, err: {:?}", (&ware).to_string() + "/" + &tab, e), callback),
			Ok(bin) => {
				if bin.len() < 28 {
					return callback_error(format!("restore table error, invalid dump meta, table: {}, bin: {:?}", (&ware).to_string() + "/" + &tab, bin), callback);
				}

				let _vsn: u32;		//版本号
				let _time: u64;		//备份时间
				let count: u64;		//备份记录数
				let checksum: u64;	//备份校验和
				unsafe {
					_vsn = u32::from_le(*(bin[0..4].as_ptr() as *mut u32));
					_time = u64::from_le(*(bin[4..12].as_ptr() as *mut u64));
					count = u64::from_le(*(bin[12..20].as_ptr() as *mut u64));
					checksum = u64::from_le(*(bin[20..28].as_ptr() as *mut u64));
				}
				check_table_meta(f, mgr, ware, tab, count, checksum, callback);
			},
		}
	});
	file.pread(0, 28, read);
}

//读头信息
fn read_head(file: SharedFile, pos: u64, ware: Atom, 
	tab: Atom, read_body: Box<FnBox(SharedFile, u64, usize)>, callback: *mut FnBox(Result<(), String>)) {
		let read_head = Box::new(move |f: SharedFile, result: IOResult<Vec<u8>>| {
			match result {
				Err(e) => {
					//读头信息失败
					callback_error(format!("restore table error, get head failed, pos: {}, table: {}, err: {:?}", pos, (&ware).to_string() + "/" + &tab, e), callback);
				}
				Ok(bin) => {
					let bin_len = bin.len();
					if (bin_len > 0) && (bin_len < 5) {
						//头信息不完整
						callback_error(format!("restore table error, invalid head, pos: {}, table: {}, bin: {:?}", pos, (&ware).to_string() + "/" + &tab, bin), callback);
					} else if bin_len == 0 {
						//头信息不存在，则继续
						read_body(f, pos, bin_len);
					} else {
						//头信息存在，则继续读体
						let mut data = ReadBuffer::new(&bin[..], 0);
						let len = match data.read_lengthen() {
							Ok(l) => l,
							Err(e) => {
								callback_error(e.to_string(), callback);
								return;
							},
						};
						let next_pos = pos + data.head as u64;
						read_body(f, next_pos, len as usize);
					}
				},
			}
		});
		file.pread(pos, 5, read_head);
}

//检查备份文件中表的元信息
fn check_table_meta(file: SharedFile, mgr: Mgr, ware: Atom, tab: Atom, count: u64, checksum: u64, callback: *mut FnBox(Result<(), String>)) {
	let ware_copy = ware.clone();
	let tab_copy = tab.clone();
	let read_body = Box::new(move |shared: SharedFile, pos: u64, len: usize| {
		if len == 0 {
			let callback = unsafe{Box::from_raw(callback)} ;
			return callback(Ok(()));
			//表元信息不存在
			//return callback_error(format!("restore table error, table meta empty, table: {}", (&ware_copy).to_string() + "/" + &tab_copy), callback);
		}

		let read = Box::new(move |f: SharedFile, result: IOResult<Vec<u8>>| {
			match result {
				Err(e) => {
					//读元信息失败
					callback_error(format!("restore table error, get table meta failed, table: {}, err: {:?}", (&ware_copy).to_string() + "/" + &tab_copy, e), callback);
				}
				Ok(bin) => {
					if bin.len() < len {
						//元信息不完整
						return callback_error(format!("restore table error, invalid table meta, table: {}, bin: {:?}", (&ware_copy).to_string() + "/" + &tab_copy, bin), callback);
					}

					//let mut data = ReadBuffer::new(&bin[..], 0);
					match mgr.tab_info(&ware_copy, &tab_copy) {
						None => {
							//表未初始化
							callback_error(format!("restore table error, table not init, table: {}", (&ware_copy).to_string() + "/" + &tab_copy), callback);
						},
						Some(meta) => {
							let mut buf = WriteBuffer::new();
							meta.encode(&mut buf);
							if &bin[0] != &buf.get_byte()[0] {
								//元信息不匹配
								return callback_error(format!("restore table error, table meta not match, table: {}, meta: {:?}, data:{:?}", (&ware_copy).to_string() + "/" + &tab_copy, &buf.get_byte()[..], &bin ), callback);
							}

							let digest = Box::into_raw(Box::new(Digest::new_with_initial(crc64::ECMA, 0u64)));
							read_key(f, pos + len as u64, mgr, ware_copy, tab_copy, count, checksum, Vec::new(), 0, digest, callback);
						},
					}
				},
			}
		});
		shared.pread(pos, len, read);
	});
	read_head(file, 28, ware, tab, read_body, callback);
}

//读关键字
fn read_key(file: SharedFile, pos: u64, mgr: Mgr, 
	ware: Atom, tab: Atom, count: u64, 
	checksum: u64, kvs: Vec<TabKV>, c: u64, digest: *mut Digest, callback: *mut FnBox(Result<(), String>)) {
		let ware_copy = ware.clone();
		let tab_copy = tab.clone();
		let read_body = Box::new(move |shared: SharedFile, pos: u64, len: usize| {
			if len == 0 {
				//读备份文件完成
				if kvs.len() == 0 {
					//恢复完成
					return callback_ok(count, checksum, c, digest, callback);
				}

				//恢复剩余数据
				return restore_table(shared, pos + len as u64, mgr, ware_copy, tab_copy, count, checksum, kvs, c, digest, callback); 
			}

			let read = Box::new(move |f: SharedFile, result: IOResult<Vec<u8>>| {
				match result {
					Err(e) => {
						//读关键字失败
						callback_error(format!("restore table error, get key failed, pos: {}, len: {}, table: {}, err: {:?}", pos, len, (&ware_copy).to_string() + "/" + &tab_copy, e), callback);
					},
					Ok(bin) => {
						let bin_len =  bin.len();
						if bin_len < len {
							//关键字不完整
							return callback_error(format!("restore table error, invalid key, pos: {}, len: {}, table: {}", pos, len, (&ware_copy).to_string() + "/" + &tab_copy), callback);
						}
					
						let key = WriteBuffer::with_bytes(bin, bin_len);
						read_value(f, pos + len as u64, mgr, ware_copy, tab_copy, count, checksum, key, kvs, c, digest, callback);
					},
				}
			});
			shared.pread(pos, len, read);
		});
		read_head(file, pos, ware, tab, read_body, callback);
}

//读值
fn read_value(file: SharedFile, pos: u64, mgr: Mgr, 
	ware: Atom, tab: Atom, count: u64, 
	checksum: u64, key: WriteBuffer, kvs: Vec<TabKV>, c: u64, digest: *mut Digest, callback: *mut FnBox(Result<(), String>)) {
		let ware_copy = ware.clone();
		let tab_copy = tab.clone();
		let read_body = Box::new(move |shared: SharedFile, pos: u64, len: usize| {
			let read = Box::new(move |f: SharedFile, result: IOResult<Vec<u8>>| {
				match result {
					Err(e) => {
						//读值失败
						callback_error(format!("restore table error, get value failed, pos: {}, len: {}, table: {}, err: {:?}", pos, len, (&ware_copy).to_string() + "/" + &tab_copy, e), callback);
					},
					Ok(bin) => {
						let bin_len = bin.len();
						if bin_len < len {
							//值不完整
							return callback_error(format!("restore table error, invalid value, pos: {}, len: {}, table: {}", pos, len, (&ware_copy).to_string() + "/" + &tab_copy), callback);
						}
					
						let value = WriteBuffer::with_bytes(bin, bin_len);
						let mut digest_mut = unsafe { Box::from_raw(digest) };
						checksum_kv(&mut digest_mut, key.get_byte(), value.get_byte());

						let kv = TabKV {
							ware: ware_copy.clone(),
							tab: tab_copy.clone(),
							key: Arc::new(key.unwrap()),
							index: 0,
							value: Some(Arc::new(value.unwrap())),
						};
						let mut kvs_mut = kvs;
						(&mut kvs_mut).push(kv);

						let new_c = c + 1;
						if (new_c % BATCH_RESTORE_LIMIT) == 0 {
							//恢复
							restore_table(f, pos + len as u64, mgr, ware_copy, tab_copy, count, checksum, kvs_mut, new_c, Box::into_raw(digest_mut), callback);
						} else {
							//继续加载键值对
							read_key(f, pos + len as u64, mgr, ware_copy, tab_copy, count, checksum, kvs_mut, new_c, Box::into_raw(digest_mut), callback);
						}
					},
				}
			});
			shared.pread(pos, len, read);
		});
		read_head(file, pos, ware, tab, read_body, callback);
}

//恢复表
fn restore_table(file: SharedFile, pos: u64, mgr: Mgr, 
	ware: Atom, tab: Atom, count: u64, 
	checksum: u64, kvs: Vec<TabKV>, c: u64, digest: *mut Digest, callback: *mut FnBox(Result<(), String>)) {
		let tr = mgr.transaction(true);
		let tr_copy0 = tr.clone();
		let modify = Arc::new(move |r: SResult<()>| {
			let file_copy0 = file.clone();
			let mgr_copy0 = mgr.clone();
			let ware_copy0 = ware.clone();
			let tab_copy0 = tab.clone();
			let tr_copy1 = tr_copy0.clone();
			match r {
				Err(e) => {
					//写失败
					callback_error(format!("restore table error, write table failed, count: {}, pos: {}, table: {}, err: {:?}", c, pos, (&ware).to_string() + "/" + &tab, e), callback);
				},
				Ok(_) => {
					//写成功
					let prepare = Arc::new(move |rr: SResult<()>| {
						let file_copy1 = file_copy0.clone();
						let mgr_copy1 = mgr_copy0.clone();
						let ware_copy1 = ware_copy0.clone();
						let tab_copy1 = tab_copy0.clone();
						match rr {
							Err(ee) => {
								//预提交失败
								callback_error(format!("restore table error, prepare table failed, count: {}, pos: {}, table: {}, err: {:?}", c, pos, (&ware_copy0).to_string() + "/" + &tab_copy0, ee), callback);
							},
							Ok(_) => {
								//预提交成功
								let commit = Arc::new(move |rrr: SResult<()>| {
									match rrr {
										Err(eee) => {
											//提交失败
											callback_error(format!("restore table error, commit table failed, count: {}, pos: {}, table: {}, err: {:?}", c, pos, (&ware_copy1).to_string() + "/" + &tab_copy1, eee), callback);
										}
										Ok(_) => {
											//提交成功，继续恢复下一个键值对
											read_key(file_copy1.clone(), pos, mgr_copy1.clone(), ware_copy1.clone(), tab_copy1.clone(), count, checksum, Vec::new(), c, digest, callback);
										},
									};
								});
								match tr_copy1.clone().commit(commit.clone()) {
									None => (),
									Some(rrr) => commit(rrr), //提交
								};
							},
						};
					});
					match tr_copy0.clone().prepare(prepare.clone()) {
						None => (),
						Some(rr) => prepare(rr), //预提交
					};
				},
			};
		});
		match tr.modify(kvs, Some(100), false, modify.clone()) {
			None => (),
			Some(r) => modify(r), //同步写
		}
}

//累计校验和
fn checksum_kv(digest: &mut Box<Digest>, key: &Vec<u8>, value: &Vec<u8>) {
	digest.write(&key[..]);
	digest.write(&value[..]);
}

//正确回调
fn callback_ok(count: u64, checksum: u64, c: u64, d: *mut Digest, func: *mut FnBox(Result<(), String>)) {
	unsafe {
		let callback = Box::from_raw(func);
		let digest = Box::from_raw(d);
		let s = digest.sum64();
		if (s == checksum) && (c == count) {
			//校验成功
			callback(Ok(()));
		} else {
			callback(Err(format!("restore table error, table checksum failed, checksum: {:?}, count: {:?}", (s, checksum), (c, count))));
		}
	}
}

//错误回调
fn callback_error(err: String, func: *mut FnBox(Result<(), String>)) {
	unsafe {
		let callback = Box::from_raw(func);
		callback(Err(err));
	}
}

/**
* 备份指定表
*
* 协议--------------
* 版本：4字节，  时间：8字节，  记录条目数量： 8字节，  crc： 8字节， 元信息长度： 动态长度（使用write_lengthen方法写入）， 元信息
* 每记录的长度： 动态长度（使用write_lengthen方法写入）， 记录
* 每记录的长度： 动态长度（使用write_lengthen方法写入）， 记录
* ......
*/
pub fn dump(mgr: &Mgr, ware: Atom, tab: Atom, file: String, callback: Arc<Fn(Result<(), String>)>) {
let file_temp = file.clone()+ ".temp";
	let tr = mgr.transaction(false);
	//先将数据写入临时表中
	AsyncFile::open(file_temp.clone(), AsyncFileOptions::OnlyAppend(1), Box::new(move |f: IOResult<AsyncFile>|{
		//写入数据回调，r：Result<(写入条目数量, 写入条目的crc值), String>
		let callback1 = callback.clone();
		let file_temp = file_temp.clone();
		let file = file.clone();
		//数据库数据写入完成后回调， 根据写入的条目数量和crc校验值改写文件头信息， 并删除原文件， 从命名temp文件
		let iter_cb = Arc::new(move |r: Result<(u64, u64), String>|{
			match r {
				Ok(v) => {
					let mut arr = Vec::with_capacity(16);
					arr.extend_from_slice(&v.0.to_le_bytes());
					arr.extend_from_slice(&v.1.to_le_bytes());
					let callback2 = callback1.clone();
					let file = file.clone();
					let file_temp1 = file_temp.clone();
					AsyncFile::open(file_temp.clone(), AsyncFileOptions::ReadWrite(1), Box::new(move |f: IOResult<AsyncFile>|{
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
						f.pwrite(WriteOptions::None, 12, arr, Box::new(move |_f: SharedFile, _r: IOResult<usize>|{
							let file1 = file.clone();
							let file_temp = file_temp.clone();
							let callback2 = callback2.clone();
							AsyncFile::remove(file, Box::new(move |_r|{
								let callback2 = callback2.clone();
								AsyncFile::rename(file_temp.clone(), file1.clone(), Box::new(move |_r1: String, _r2: String, r3: IOResult<()>|{
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

			let mut bytes = Vec::with_capacity(28);
			bytes.extend_from_slice(&(0 as u32).to_le_bytes());//版本
			bytes.extend_from_slice(&(0 as u64).to_le_bytes());////时间
			bytes.extend_from_slice(&(0 as u64).to_le_bytes());//记录条目数量
			bytes.extend_from_slice(&(0 as u64).to_le_bytes());//crc
			let mut bytes = {
				let len = bytes.len();
				let mut bb = WriteBuffer::with_bytes(bytes, len);
				bb.write_lengthen(meta_bytes.len() as u32); //meta 长度
				bb.unwrap()
			};
			bytes.extend_from_slice(&meta_bytes); //meta

			f.clone().pwrite(WriteOptions::None, 0, bytes, Box::new(move |_f: SharedFile, _r: IOResult<usize>|{
				let crc = Arc::new(RefCell::new(Digest::new_with_initial(crc64::ECMA, 0u64)));
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
			})); 
		}
	}));
}

//遍历数据库并将数据写入文件， 同时累计计算crc的值
fn iter_ware_write(it: Arc<RefCell<Box<Iter<Item = (Bin, Bin)>>>>, f: SharedFile, crc: Arc<RefCell<Digest>>, count: u64, call_back: Arc<Fn(Result<(u64, u64), String>)>){
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
fn iter_ware_write1(r: Result<Option<(Bin, Bin)>, String>, it: Arc<RefCell<Box<Iter<Item = (Bin, Bin)>>>>, f: SharedFile, crc: Arc<RefCell<Digest>>, mut count: u64, call_back: Arc<Fn(Result<(u64, u64), String>)>){
	match r {
			Ok(v) => {
				match v {
					Some(r) => {
						count += 1;
						let mut arr = {
							let mut bb = WriteBuffer::with_capacity(8 + r.0.len()+ r.1.len());
							bb.write_lengthen(r.0.len() as u32);
							bb.unwrap()
						};
						arr.extend_from_slice(r.0.as_slice());
						{
							let mut bb = WriteBuffer::with_capacity(4);
							bb.write_lengthen(r.1.len() as u32);
							arr.extend_from_slice(bb.unwrap().as_slice());
						}
						arr.extend_from_slice(r.1.as_slice());
						crc.borrow_mut().write(r.0.as_slice());
						crc.borrow_mut().write(r.1.as_slice());
						f.clone().pwrite(WriteOptions::None, 0, arr, Box::new(move |_f: SharedFile, _r: IOResult<usize>|{
							iter_ware_write(it.clone(), f.clone(), crc.clone(), count, call_back.clone()); //结果或通过回调给出
						}));//写len,key,value
					},
					None =>call_back(Ok((count, crc.borrow().sum64()))),
				}
			},
			Err(s) => call_back(Err(s)),
		}
}