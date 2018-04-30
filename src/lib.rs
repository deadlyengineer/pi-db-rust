
#![crate_type = "rlib"]
#![feature(custom_derive,asm,box_syntax,box_patterns)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(type_ascription)]
#![feature(i128_type)]
#![feature(i128)]
#![feature(nll)]
#![feature(universal_impl_trait)]
#![feature(conservative_impl_trait)]
#![feature(match_default_bindings)]
#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

extern crate pi_lib;
extern crate fnv;

pub mod db;
pub mod mgr;
