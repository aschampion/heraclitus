use std::mem;

use enum_set;


#[cfg(feature="backend-debug-filesystem")]
pub mod debug_filesystem;
#[cfg(feature="backend-postgres")]
pub mod postgres;


#[derive(Clone, Copy)]
#[repr(u32)]
pub enum Backend {
    Memory,
    #[cfg(feature="backend-debug-filesystem")]
    DebugFilesystem,
    #[cfg(feature="backend-postgres")]
    Postgres,
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for Backend {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> Backend {
        mem::transmute(v)
    }
}
