extern crate daggy;

use std;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use enum_set::EnumSet;

use ::{Composition, Datatype, Error, Hunk};
use ::store::Store;
use self::interface::{
    PartitioningController,
    ProducerController,
    CustomProductionPolicyController,
};


#[macro_use]
pub mod macros;
pub mod artifact_graph;
pub mod blob;
pub mod interface;
pub mod partitioning;
pub mod producer;
pub mod reference;
pub mod tracking_branch_producer;

pub struct Description {
    name: String,
    version: u64,
    representations: EnumSet<::RepresentationKind>,
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


/// Specifies what source dependency datatypes are allowed for this
/// relationship.
pub enum DependencyTypeRestriction {
    /// Match datatype by its name.
    Datatype(HashSet<&'static str>),
    /// Match datatype by name of interface it implements (disjunctive).
    ImplementsInterface(HashSet<&'static str>),
    /// Match any datatype.
    Any,
}

/// Specifies how many incoming dependency relationships of this type may exist
/// for a particular artifact in an artifact graph.
pub enum DependencyCardinalityRestriction {
    Exact(u64),
    // Could represent all restrictions with just this variant:
    InclusiveRange(Option<u64>, Option<u64>),
    Unbounded,
}

impl DependencyCardinalityRestriction {
    pub fn allows(&self, size: u64) -> bool {
        match *self {
            DependencyCardinalityRestriction::Exact(v) => size == v,
            DependencyCardinalityRestriction::InclusiveRange(ref from, ref to) => {
                match (*from, *to) {
                    (None, None) => true,
                    (Some(low), None) => low <= size,
                    (None, Some(high)) => size <= high,
                    (Some(low), Some(high)) => low <= size && size <= high,
                }
            },
            DependencyCardinalityRestriction::Unbounded => true
        }
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
    datatype_restriction: DependencyTypeRestriction,
    cardinality_restriction: DependencyCardinalityRestriction,
    // TODO: not yet used/implemented.
    store_restriction: DependencyStoreRestriction,
}

impl DependencyDescription {
    fn new(
        name: &'static str,
        datatype_restriction: DependencyTypeRestriction,
        cardinality_restriction: DependencyCardinalityRestriction,
        store_restriction: DependencyStoreRestriction,
    ) -> DependencyDescription {
        DependencyDescription {
            name,
            datatype_restriction,
            cardinality_restriction,
            store_restriction,
        }
    }
}

pub struct InterfaceDescription {
    interface: ::Interface,
    extends: HashSet<&'static str>,
}

/// Common interface to all datatypes that does not involve their state or
/// types associated with their state.
pub trait MetaController {
}

pub trait Model<T> {
    // Necessary to be able to create this as a trait object. See:
    // https://www.reddit.com/r/rust/comments/620m1v//dfirs5s/
    //fn clone(&self) -> Self where Self: Sized;

    fn info(&self) -> Description;

    fn meta_controller(&self, Store) -> Option<StoreMetaController>;

    /// If this datatype acts as a partitioning controller, construct one.
    fn interface_controller(&self, store: Store, name: &str) -> Option<T>;
}

#[derive(Debug, Hash, PartialEq)]
pub enum Payload<S, D> {
    State(S),
    Delta(D),
}

/// Common interface to all datatypes that involves state.
pub trait ModelController {
    type StateType: Debug + Hash + PartialEq;
    type DeltaType: Debug + Hash + PartialEq;

    fn hash_payload(
        &self,
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> ::HashType {
        let mut s = DefaultHasher::new();
        payload.hash(&mut s);
        s.finish()
    }

    fn write_hunk(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> Result<(), Error>;

    fn read_hunk(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<Payload<Self::StateType, Self::DeltaType>, Error>;

    /// Compose state from a composition of sufficient hunks.
    ///
    /// Datatypes' store types may choose to implement this more efficiently.
    fn get_composite_state(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        composition: &Composition,
    ) -> Result<Self::StateType, Error> {
            let mut hunk_iter = composition.iter().rev();

            let mut state = match self.read_hunk(repo_control, hunk_iter.next().expect("TODO"))? {
                Payload::State(mut state) => state,
                _ => panic!("Composition rooted in non-state hunk"),
            };

            for hunk in hunk_iter {
                match self.read_hunk(repo_control, hunk)? {
                    Payload::State(_) => panic!("TODO: shouldn't have non-root state"),
                    Payload::Delta(ref delta) => {
                        self.compose_state(&mut state, delta);
                    }
                }
            }

            Ok(state)
        }

    fn compose_state(
        &self,
        state: &mut Self::StateType,
        delta: &Self::DeltaType,
    );
}

// TODO:
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

pub trait InterfaceController<T: ?Sized> : From<Box<T>>
        //where Box<T>: From<Self>
        {}

pub trait InterfaceControllerEnum {
    fn all_descriptions() -> Vec<&'static InterfaceDescription>;
}

pub trait DatatypeEnum: Sized {
    type InterfaceControllerType: InterfaceControllerEnum;

    fn variant_names() -> Vec<&'static str>;

    fn from_name(name: &str) -> Option<Self>;

    fn as_model(&self) -> &Model<Self::InterfaceControllerType>;

    fn all_variants() -> Vec<Self> {
        Self::variant_names()
            .iter()
            .map(|name| Self::from_name(name).expect("Impossible"))
            .collect()
    }
}

interface_controller_enum!(DefaultInterfaceController, (
        (Partitioning, PartitioningController, &*interface::INTERFACE_PARTITIONING_DESC),
        (Producer, ProducerController, &*interface::INTERFACE_PRODUCER_DESC),
        (CustomProductionPolicy, CustomProductionPolicyController, &*interface::INTERFACE_CUSTOM_PRODUCTION_POLICY_DESC)
    ));

datatype_enum!(DefaultDatatypes, DefaultInterfaceController, (
        (ArtifactGraph, artifact_graph::ArtifactGraphDtype),
        (Ref, reference::Ref),
        (UnaryPartitioning, partitioning::UnaryPartitioning),
        (ArbitraryPartitioning, partitioning::arbitrary::ArbitraryPartitioning),
        (Blob, blob::Blob),
        (NoopProducer, producer::NoopProducer),
        (TrackingBranchProducer, tracking_branch_producer::TrackingBranchProducer),
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
                self.extension.add_edge(*super_idx, *idx, ()).expect("Interfaces have non-DAG relationship");
            }
        }
    }
}

pub struct DatatypesRegistry<T: DatatypeEnum> {
    interfaces: InterfaceRegistry,
    dtypes: HashMap<String, Datatype>,
    pub models: HashMap<String, T>,
}

impl<T: DatatypeEnum> DatatypesRegistry<T> {
    pub fn new() -> DatatypesRegistry<T> {
        DatatypesRegistry {
            interfaces: InterfaceRegistry::new(),
            dtypes: HashMap::new(),
            models: HashMap::new(),
        }
    }

    pub fn get_datatype(&self, name: &str) -> Option<&Datatype> {
        self.dtypes.get(name)
    }

    /// Iterate over datatypes.
    pub fn iter_dtypes<'a>(&'a self) -> impl Iterator<Item = &'a Datatype> {
        self.dtypes.values()
    }

    pub fn register_interfaces(&mut self, interfaces: Vec<&InterfaceDescription>) {
        self.interfaces.register_interfaces(interfaces);
    }

    pub fn register_datatype_models(&mut self, models: Vec<T>) {
        for model in models {
            let description = model.as_model().info();
            self.models.insert(description.name.clone(), model);
            self.dtypes.insert(description.name.clone(), description.to_datatype(&self.interfaces));
        }
    }
}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn init_default_dtypes_registry() -> DatatypesRegistry<DefaultDatatypes> {
        init_dtypes_registry::<DefaultDatatypes>()
    }

    pub fn init_dtypes_registry<T: DatatypeEnum>() -> DatatypesRegistry<T> {
        let mut dtypes_registry = DatatypesRegistry::new();
        dtypes_registry.register_interfaces(<T as DatatypeEnum>::InterfaceControllerType::all_descriptions());
        let models = T::all_variants();
            // .iter()
            // .map(|v| v.as_model())
            // .collect();
        dtypes_registry.register_datatype_models(models);
        dtypes_registry
    }
}
