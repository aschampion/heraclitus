extern crate daggy;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use enum_set;
use enum_set::EnumSet;

use super::Datatype;
use super::store::Store;


pub mod artifact_graph;
pub mod blob;

pub struct Description {
    pub datatype: Datatype,
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

pub trait MetaController<T: ::repo::RepoController> {
    // fn register_with_repo(&self, repo_controller: &mut T);

    // Content hashing, etc.
}

pub trait Model {
    // Necessary to be able to create this as a trait object. See:
    // https://www.reddit.com/r/rust/comments/620m1v/never_hearing_the_trait_x_cannot_be_made_into_an/dfirs5s/
    //fn clone(&self) -> Self where Self: Sized;

    fn info(&self) -> Description;

    fn controller(&self, Store) -> Option<StoreMetaController>;
}

pub trait ModelController {} // TODO: Are there any general controller fns?

// TODO:
// - When/where are UUIDs generated? Do UUIDs change on versions? How does the map with hash equality?
// - Datatypes register with a central controller
//    - How does this mesh with extensions, e.g., VISAG
// - Sync/compare datatype defs with store
//    - Fresh init vs diff update

pub enum StoreMetaController {
    Postgres(Box<::repo::PostgresMigratable>),
}

impl Into<Box<::repo::PostgresMigratable>> for StoreMetaController {
    fn into(self) -> Box<::repo::PostgresMigratable> {
        match self {
            StoreMetaController::Postgres(smc) => smc,
            _ => panic!("Unknown store"),
        }
    }
}

pub fn build_module_datatype_models() -> Vec<Box<Model>> {
    vec![
        Box::new(artifact_graph::ArtifactGraphDtype {}),
        Box::new(blob::Blob {}),
    ]
}

pub struct DatatypesRegistry {
    graph: super::Metadata,
    types_idx: HashMap<String, daggy::NodeIndex>,
    pub models: HashMap<String, Box<Model>>,
}

impl DatatypesRegistry {
    pub fn new() -> DatatypesRegistry {
        DatatypesRegistry {
            graph: ::Metadata { datatypes: daggy::Dag::new() },
            types_idx: HashMap::new(),
            models: HashMap::new(),
        }
    }

    pub fn get_datatype(&self, name: &str) -> Option<&Datatype> {
        match self.types_idx.get(name) {
            Some(idx) => self.graph.datatypes.node_weight(*idx),
            None => None,
        }
    }

    pub fn register_datatype_models(&mut self, models: Vec<Box<Model>>) {
        for model in &models {
            // Add datatype nodes.
            let description = model.info();
            let name = description.datatype.name.clone();
            let idx = self.graph.datatypes.add_node(description.datatype);
            self.types_idx.insert(name, idx);
        }

        for model in &models {
            // Add dependency edges.
            let description = model.info();
            let node_idx = self.types_idx.get(&description.datatype.name).expect("Unknown datatype.");
            for dependency in description.dependencies {
                let dep_idx = self.types_idx.get(dependency.datatype_name).expect("Unknown datatype.");
                let relation = ::DatatypeRelation{name: dependency.name.into()};
                self.graph.datatypes.add_edge(*node_idx, *dep_idx, relation).unwrap();
            }
        }

        // Add model lookup.
        for model in models {
            let description = model.info();
            self.models.insert(description.datatype.name, model);
        }
    }
}

// pub trait DatatypesLibrary {
//     fn walk_foo<T> {
//         (blob::Blob as &T)
//     }
// }

pub struct DatatypesController {
    datatype_models: Vec<Box<Model>>,
}

impl DatatypesController {
    fn default() -> DatatypesController {
        let mut dcon = DatatypesController {datatype_models: Vec::new()};
        dcon.register_datatype_models(&mut build_module_datatype_models());
        dcon
    }

    fn register_datatype_models(&mut self, models: &mut Vec<Box<Model>>) {
        self.datatype_models.append(models);
    }
}
