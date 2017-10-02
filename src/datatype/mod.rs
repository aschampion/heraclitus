use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use enum_set;
use enum_set::EnumSet;

use super::store::Store;


pub mod blob;

pub struct Description {
    datatype: super::Datatype,
    dependencies: Vec<DependencyDescription>,
}

impl Hash for Description {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datatype.hash(state);
    }
}

pub enum DependencyStoreRestriction {
    Any,
    Same,
    Stores(EnumSet<Store>),
}

pub struct DependencyDescription {
    // TODO: strs or Identities or ??
    name: &'static str,
    datatype_name: &'static str,
    store_restriction: DependencyStoreRestriction,
}

impl DependencyDescription {
    fn new(
        name: &'static str,
        datatype_name: &'static str,
        store_restriction: DependencyStoreRestriction,
    ) -> DependencyDescription {
        DependencyDescription {
            name,
            datatype_name,
            store_restriction,
        }
    }
}

// TODO:
// This doesn't work because it's impossible to collect a set of generic MCs
// without knowning the associated types, which is necessary for programs
// consuming this lib to register their own dtypes.
// Consumers of the model controller (dependent dtypes) will know the dtype-
// specific MC trait, but not the concrete impl.
// What is the purpose of this method? So other datatype controllers can
// retrieve data from this controller without needing to know anything about
// the configuration of the hera repo.

// Alternatives:
// Models just register w/o MC type, consumers call a concrete method in the...
// ...doesn't work. Every consumer would rebuild, etc.
//

pub trait Model {
    type Controller: ModelController + ?Sized;

    fn info() -> Description;

    fn controller(Store) -> Option<Box<Self::Controller>>;
}

pub trait ModelController {} // TODO: Are there any general controller fns?

// TODO:
// - When/where are UUIDs generated? Do UUIDs change on versions? How does the map with hash equality?
// - Datatypes register with a central controller
//    - How does this mesh with extensions, e.g., VISAG
// - Sync/compare datatype defs with store
//    - Fresh init vs diff update

pub fn build_module_datatype_models() -> Vec<Box<Model>> {
    vec![
        blob::Blob{},
    ]
}

pub struct DatatypesController {
    datatype_models: Vec<Box<Model>>,
}

impl DatatypesController {
    fn default() -> DatatypesController {
        let dcon = DatatypesController {};
        dcon.register_datatype_models(build_module_datatype_models());
        dcon
    }

    fn register_datatype_models(&mut self, models: &mut Vec<Box<Model>>) {
        self.datatype_models.append(models);
    }
}
