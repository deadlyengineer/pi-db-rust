#![crate_type = "rlib"]
#![feature(asm,box_syntax,box_patterns)]
#![feature(core_intrinsics)]
#![feature(type_ascription)]
#![feature(nll)]
#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

#[macro_use]
extern crate lazy_static;

pub mod db;
pub mod mgr;
pub mod tabs;
pub mod memery_db;
pub mod util;
