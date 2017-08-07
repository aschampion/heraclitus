use std::mem;

use enum_set;
use enum_set::EnumSet;

//pub mod postgres;

#[derive(Clone, Copy)]
#[repr(u32)]
pub enum Store {
    Filesystem,
    Memory,
    Postgres,
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for Store {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> Store {
        mem::transmute(v)
    }
}
