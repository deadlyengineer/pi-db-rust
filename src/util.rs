use std::boxed::FnBox;

use mgr::Mgr;
use pi_lib::atom::Atom;

pub fn restore(mgr: &Mgr, ware: Atom, tab: Atom, file: Atom, callback: Box<FnBox(Result<(), String>)>) {

}

pub fn dump(mgr: &Mgr, ware: Atom, tab: Atom, file: Atom, callback: Box<FnBox(Result<(), String>)>) {

}