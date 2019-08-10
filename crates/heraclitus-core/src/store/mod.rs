use enumset::EnumSetType;


#[cfg(feature="backend-debug-filesystem")]
pub mod debug_filesystem;
#[cfg(feature="backend-postgres")]
pub mod postgres;


#[derive(EnumSetType)]
#[repr(u8)]
pub enum Backend {
    Memory,
    #[cfg(feature="backend-debug-filesystem")]
    DebugFilesystem,
    #[cfg(feature="backend-postgres")]
    Postgres,
}
