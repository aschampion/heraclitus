extern crate daggy;

use std;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use enum_set;
use enum_set::EnumSet;

use super::Datatype;
use super::store::Store;
use self::interface::{PartitioningController, ProducerController};


pub mod artifact_graph;
pub mod blob;
pub mod interface;
pub mod partitioning;
pub mod producer;

pub struct Description {
    name: String,
    version: u64,
    representations: EnumSet<::DatatypeRepresentationKind>,
    implements: Vec<&'static str>,
    dependencies: Vec<DependencyDescription>,
}

impl Description {
    fn to_datatype(self, interfaces: &InterfaceRegistry) -> Datatype {
        Datatype::new(
            self.name,
            self.version,
            self.representations,
            self.implements.iter().map(|name| interfaces.get_index(name)).collect(),
        )
    }
}

// impl Hash for Description {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.datatype.hash(state);
//     }
// }

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

pub struct InterfaceDescription {
    interface: ::Interface,
    extends: HashSet<&'static str>,
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

pub trait MetaController {
    // fn version_graph<'a>(
    //         &self,
    //         repo_control: &mut ::repo::StoreRepoController,
    //         artifact: &'a Artifact
    // ) -> Result<VersionGraph<'a>, Error>;
}

pub trait Model<T> {
    // Necessary to be able to create this as a trait object. See:
    // https://www.reddit.com/r/rust/comments/620m1v/never_hearing_the_trait_x_cannot_be_made_into_an/dfirs5s/
    //fn clone(&self) -> Self where Self: Sized;

    fn info(&self) -> Description;

    fn meta_controller(&self, Store) -> Option<StoreMetaController>;

    /// If this datatype acts as a partitioning controller, construct one.
    fn interface_controller(&self, store: Store, name: &str) -> Option<T>;
}

pub trait Control<T, U: ?Sized> where T: InterfaceController<U> {
    fn interface_controller(&self, store: Store) -> Option<T> {
        None
    }
}

pub trait ModelController {}

// TODO:
// - When/where are UUIDs generated? Do UUIDs change on versions? How does the map with hash equality?
// - Datatypes register with a central controller
//    - How does this mesh with extensions, e.g., VISAG
// - Sync/compare datatype defs with store
//    - Fresh init vs diff update

pub trait PostgresMetaController: MetaController + ::repo::PostgresMigratable {}

pub enum StoreMetaController {
    Postgres(Box<PostgresMetaController>),
}

impl Into<Box<PostgresMetaController>> for StoreMetaController {
    fn into(self) -> Box<PostgresMetaController> {
        match self {
            StoreMetaController::Postgres(smc) => smc,
            _ => panic!("Wrong store type."),
        }
    }
}

// pub fn module_datatype_models() -> Vec<Box<Model>> {
//     vec![
//         Box::new(artifact_graph::ArtifactGraphDtype {}),
//         Box::new(partitioning::UnaryPartitioning {}),
//         Box::new(blob::Blob {}),
//         Box::new(producer::NoopProducer {}),
//     ]
// }

pub trait InterfaceController<T: ?Sized> : From<Box<T>>
        //where Box<T>: From<Self>
        {}

pub trait InterfaceControllerEnum {
    fn all_descriptions() -> Vec<&'static InterfaceDescription>;
}

#[macro_export]
macro_rules! interface_controller_enum {
    ( $enum_name:ident, ( $( ( $i_name:ident, $i_control:ident, $i_desc:expr ) ),*  $(,)* ) ) => {
        pub enum $enum_name {
            $(
                $i_name(Box<$i_control>),
            )*
        }

        impl InterfaceControllerEnum for $enum_name {
            fn all_descriptions() -> Vec<&'static $crate::datatype::InterfaceDescription> {
                vec![
                    $($i_desc,)*
                ]
            }
        }

        $(
            impl $crate::datatype::InterfaceController<$i_control> for $enum_name {}

            impl std::convert::From<Box<$i_control>> for $enum_name {
                fn from(inner: Box<$i_control>) -> $enum_name {
                    $enum_name::$i_name(inner)
                }
            }

            impl std::convert::From<$enum_name> for Box<$i_control> {
                fn from(iface_control: $enum_name) -> Box<$i_control> {
                    match iface_control {
                        $enum_name::$i_name(inner) => inner,
                        _ => panic!("Attempt to unwrap interface controller into wrong type!"),
                    }
                }
            }
        )*
    };
}

pub trait DatatypeEnum: Sized {
    type InterfaceControllerType: InterfaceControllerEnum;

    fn variant_names() -> Vec<&'static str>;

    fn from_name(name: &str) -> Option<Self>;

    fn as_model(&self) -> &Model<Self::InterfaceControllerType>;

    fn interface_controller<T>(&self, store: Store) -> Option<Self::InterfaceControllerType>
        where Self::InterfaceControllerType: InterfaceController<T>;

    fn all_variants() -> Vec<Self> {
        Self::variant_names()
            .iter()
            .map(|name| Self::from_name(name).expect("Impossible"))
            .collect()
    }
}

#[macro_export]
macro_rules! datatype_enum {
    ( $enum_name:ident, $iface_enum:ty, ( $( ( $d_name:ident, $d_type:ty ) ),* $(,)* ) ) => {
        pub enum $enum_name {
            $(
                $d_name($d_type),
            )*
        }

        impl DatatypeEnum for $enum_name {
            type InterfaceControllerType = $iface_enum;

            fn variant_names() -> Vec<&'static str> {
                vec![
                    $(stringify!($d_name),)*
                ]
            }

            fn from_name(name: &str) -> Option<$enum_name> {
                match name {
                    $(
                        stringify!($d_name) => Some($enum_name::$d_name(<$d_type as Default>::default())),
                    )*
                    _ => None,
                }
            }

            fn as_model(&self) -> &Model<Self::InterfaceControllerType> {
                match *self {
                    $(
                        $enum_name::$d_name(ref d) => d,
                    )*
                }
            }

            fn interface_controller<T>(&self, store: Store) -> Option<Self::InterfaceControllerType>
                where Self::InterfaceControllerType: InterfaceController<T> {
                match *self {
                    $(
                        $enum_name::$d_name(ref d) =>
                            Control::<Self::InterfaceControllerType, T>::interface_controller(d, store),
                    )*
                }
            }
        }
    };
}

interface_controller_enum!(DefaultInterfaceController, (
        (Partitioning, PartitioningController, &*interface::INTERFACE_PARTITIONING_DESC),
        (Producer, ProducerController, &*interface::INTERFACE_PRODUCER_DESC),
    ));

datatype_enum!(DefaultDatatypes, DefaultInterfaceController, (
        (ArtifactGraph, artifact_graph::ArtifactGraphDtype),
        (UnaryPartitioning, partitioning::UnaryPartitioning),
        (Blob, blob::Blob),
        (NoopProducer, producer::NoopProducer),
    ));


pub struct InterfaceRegistry {
    extension: ::InterfaceExtension,
    ifaces_idx: HashMap<&'static str, ::InterfaceIndex>,
}

impl InterfaceRegistry {
    pub fn new() -> InterfaceRegistry {
        InterfaceRegistry {
            extension: ::InterfaceExtension::new(),
            ifaces_idx: HashMap::new(),
        }
    }

    pub fn get_index(&self, name: &str) -> ::InterfaceIndex {
        *self.ifaces_idx.get(name).expect("Unknown interface")
    }

    pub fn register_interfaces(&mut self, interfaces: Vec<&InterfaceDescription>) {
        for iface in &interfaces {
            let idx = self.extension.add_node(iface.interface.clone());
            self.ifaces_idx.insert(iface.interface.name, idx);
        }

        for iface in &interfaces {
            let idx = self.ifaces_idx.get(iface.interface.name).expect("Impossible");
            for super_iface in &iface.extends {
                let super_idx = self.ifaces_idx.get(super_iface).expect("Unknown super interface");
                self.extension.add_edge(*super_idx, *idx, ());
            }
        }
    }
}

pub struct DatatypesRegistry<T: DatatypeEnum> {
    interfaces: InterfaceRegistry,
    graph: super::DatatypeGraph,
    dtypes_idx: HashMap<String, daggy::NodeIndex>,
    pub models: HashMap<String, T>,
}

impl<T: DatatypeEnum> DatatypesRegistry<T> {
    pub fn new() -> DatatypesRegistry<T> {
        DatatypesRegistry {
            interfaces: InterfaceRegistry::new(),
            graph: super::DatatypeGraph::new(),
            dtypes_idx: HashMap::new(),
            models: HashMap::new(),
        }
    }

    pub fn get_datatype(&self, name: &str) -> Option<&Datatype> {
        match self.dtypes_idx.get(name) {
            Some(idx) => self.graph.node_weight(*idx),
            None => None,
        }
    }

    pub fn iter_dtypes<'a>(&'a self) -> impl Iterator<Item = &'a Datatype> {
        self.graph.raw_nodes().iter().map(|node| &node.weight)
    }

    pub fn register_interfaces(&mut self, interfaces: Vec<&InterfaceDescription>) {
        self.interfaces.register_interfaces(interfaces);
    }

    pub fn register_datatype_models(&mut self, models: Vec<T>) {
        for model in &models {
            // Add datatype nodes.
            let description = model.as_model().info();
            let name = description.name.clone();
            let idx = self.graph.add_node(description.to_datatype(&self.interfaces));
            self.dtypes_idx.insert(name, idx);
        }

        for model in &models {
            // Add dependency edges.
            let description = model.as_model().info();
            let node_idx = self.dtypes_idx.get(&description.name).expect("Unknown datatype.");
            for dependency in description.dependencies {
                let dep_idx = self.dtypes_idx.get(dependency.datatype_name).expect("Depends on unknown datatype.");
                let relation = ::DatatypeRelation{name: dependency.name.into()};
                self.graph.add_edge(*node_idx, *dep_idx, relation).unwrap();
            }
        }

        // Add model lookup.
        for model in models {
            let description = model.as_model().info();
            self.models.insert(description.name, model);
        }
    }
}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn init_default_dtypes_registry() -> DatatypesRegistry<DefaultDatatypes> {
        let mut dtypes_registry = DatatypesRegistry::new();
        dtypes_registry.register_interfaces(<DefaultDatatypes as DatatypeEnum>::InterfaceControllerType::all_descriptions());
        let models = DefaultDatatypes::all_variants();
            // .iter()
            // .map(|v| v.as_model())
            // .collect();
        dtypes_registry.register_datatype_models(models);
        dtypes_registry
    }
}
