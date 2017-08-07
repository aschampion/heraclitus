extern crate enum_set;


use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use self::enum_set::EnumSet;


pub mod blob;

#[derive(Clone, Copy)]
#[repr(u32)]
pub enum Store {
    Memory,
    Postgres,
}

// Boilerplate necessary for EnumSet compatibility.
impl self::enum_set::CLike for Store {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> Store {
        mem::transmute(v)
    }
}

pub struct Description {
    name: &'static str,
    version: u64,
    dependencies: Vec<DependencyDescription>,
}

impl Hash for Description {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.version.hash(state);
    }
}

pub enum DependencyStoreRestriction {
    Any,
    Same,
    Stores(EnumSet<Store>),
}

pub struct DependencyDescription {
    name: &'static str,
    datatype_name: &'static str,
    store_restriction: DependencyStoreRestriction,
}

impl DependencyDescription {
    fn new(name: &'static str,
           datatype_name: &'static str,
           store_restriction: DependencyStoreRestriction) -> DependencyDescription {
        DependencyDescription {
            name,
            datatype_name,
            store_restriction,
        }
    }
}

pub trait Model {
    type Controller: ModelController + ?Sized;

    fn info() -> Description;

    fn controller(Store) -> Option<Arc<Mutex<Self::Controller>>>;
}

pub trait ModelController {}  // Are there any general controller fns?

// TODO:
// - Datatypes register with a central controller
// - Sync/compare datatype defs with store
