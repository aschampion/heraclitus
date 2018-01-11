use std::mem;

use enum_set;

pub mod postgres;

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


pub enum Stored<T> {
    Postgres(T),
}

impl<T> Stored<T> {
    pub fn inner(&mut self) -> &mut T {
        use self::Stored::*;

        match *self {
            Postgres(ref mut i) => i
        }
    }
}
