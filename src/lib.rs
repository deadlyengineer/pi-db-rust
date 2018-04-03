
#![crate_type = "rlib"]
#![feature(custom_derive,asm,box_syntax,box_patterns)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(i128_type)]
#![feature(i128)]
#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]
#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

extern crate pi_lib;

pub mod db;
pub mod mgr;
