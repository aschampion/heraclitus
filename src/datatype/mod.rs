use std;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use enum_set::EnumSet;

use ::{Artifact, Composition, Datatype, Error, Hunk};
use ::store::Store;
use ::store::postgres::datatype::PostgresMetaController;
use self::interface::{
    ProducerController,
    CustomProductionPolicyController,
};


#[macro_use]
pub mod macros;
pub mod artifact_graph;
#[macro_use]
pub mod blob;
pub mod interface;
pub mod partitioning;
pub mod producer;
pub mod reference;
pub mod tracking_branch_producer;

pub struct Description<T: InterfaceControllerEnum> {
    pub name: String,
    pub version: u64,
    pub representations: EnumSet<::RepresentationKind>,
    // TODO: Not yet clear that this reflection of interfaces is useful.
    pub implements: Vec<T>,
    pub dependencies: Vec<DependencyDescription>,
}

impl<T: InterfaceControllerEnum> Description<T> {
    fn into_datatype(self, interfaces: &InterfaceRegistry) -> Datatype {
        use std::string::ToString;
        Datatype::new(
            self.name,
            self.version,
            self.representations,
            self.implements.iter().map(|iface| interfaces.get_index(&iface.to_string())).collect(),
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
    pub fn new(
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
    pub interface: ::Interface,
    pub extends: HashSet<&'static str>,
}

/// Common interface to all datatypes that does not involve their state or
/// types associated with their state.
pub trait MetaController {
    /// This allows the model controller to initialize any structures necessary
    /// for a new version (without involving state for that version).
    fn init_artifact(
        &mut self,
        _repo_control: &mut ::repo::StoreRepoController,
        _artifact: &Artifact,
    ) -> Result<(), Error> {
        Ok(())
    }
}

pub trait Model<T: InterfaceControllerEnum> {
    // Necessary to be able to create this as a trait object. See:
    // https://www.reddit.com/r/rust/comments/620m1v//dfirs5s/
    //fn clone(&self) -> Self where Self: Sized;

    fn info(&self) -> Description<T>;

    fn meta_controller(&self, Store) -> Option<StoreMetaController>;

    /// If this datatype acts as a partitioning controller, construct one.
    fn interface_controller(&self, store: Store, iface: T) -> Option<T>;
}

pub trait GetInterfaceController<T: ?Sized> {
    fn get_controller(&self, store: Store) -> Option<Box<T>>;
}

impl<'a, T, IC> GetInterfaceController<T> for Model<IC> + 'a
        where
            T: ?Sized,
            IC: InterfaceController<T> {
    fn get_controller(&self, store: Store) -> Option<Box<T>> {
        self.interface_controller(store, IC::VARIANT)
            .map(|ic| ic.into())
    }
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
    ) -> Result<(), Error> {

        self.write_hunks(repo_control, &[hunk], &[payload])
    }

    fn write_hunks<'a: 'b, 'b: 'c + 'd, 'c, 'd, H, P>(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunks: &[H],
        payloads: &[P],
    ) -> Result<(), Error>
            where H: std::borrow::Borrow<Hunk<'a, 'b, 'c, 'd>>,
                P: std::borrow::Borrow<Payload<Self::StateType, Self::DeltaType>> {

        for (hunk, payload) in hunks.iter().zip(payloads) {
            let hunk: &Hunk = hunk.borrow();
            let payload = payload.borrow();

            self.write_hunk(repo_control, hunk, payload)?;
        }

        Ok(())
    }

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

/// A type for a representation kind that is not supported by a model. This
/// allows, for example, models to implement `ModelController` if they do not
/// support deltas.
///
/// The type is uninstantiable.
#[allow(unreachable_code, unreachable_patterns)]
#[derive(Debug, PartialEq)]
pub struct UnrepresentableType (!);

impl Hash for UnrepresentableType {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unreachable!()
    }
}


// Previous implementation of state interfaces before moving to macro-generated
// traits. Left here for reference.
// pub trait StateInterface<I: ?Sized> {
//     fn get_composite_interface(
//         &self,
//         repo_control: &mut ::repo::StoreRepoController,
//         composition: &Composition,
//     ) -> Result<Box<I>, Error>;
// }


pub enum StoreMetaController {
    Postgres(Box<PostgresMetaController>),
}

// TODO: ugly kludge, but getting deref/borrow to work for variants is fraught.
impl MetaController for StoreMetaController {
    fn init_artifact(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact,
    ) -> Result<(), Error> {
        match *self {
            StoreMetaController::Postgres(ref mut pmc) => pmc.init_artifact(repo_control, artifact),
        }
    }
}

pub trait InterfaceController<T: ?Sized> : From<Box<T>> + Into<Box<T>> + InterfaceControllerEnum {
    const VARIANT : Self;
}

pub trait InterfaceControllerEnum : PartialEq + std::fmt::Display {
    fn all_descriptions() -> Vec<&'static InterfaceDescription>;
}

pub trait DatatypeEnum: Sized {
    type InterfaceControllerType: InterfaceControllerEnum;

    fn variant_names() -> Vec<&'static str>;

    fn from_name(name: &str) -> Option<Self>;

    fn as_model<'a>(&self) -> &(Model<Self::InterfaceControllerType> + 'a);

    fn all_variants() -> Vec<Self> {
        Self::variant_names()
            .iter()
            .map(|name| Self::from_name(name).expect("Impossible"))
            .collect()
    }
}

interface_controller_enum!(DefaultInterfaceController, (
        (Partitioning, partitioning::PartitioningState, &*interface::INTERFACE_PARTITIONING_DESC),
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

    pub fn register_interfaces(&mut self, interfaces: &[&InterfaceDescription]) {
        for iface in interfaces {
            let idx = self.extension.add_node(iface.interface.clone());
            self.ifaces_idx.insert(iface.interface.name, idx);
        }

        for iface in interfaces {
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
    models: HashMap<String, T>,
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

    // TODO: Kludge around Model/Interface controller mess
    // TODO: Unable to implement as Index trait because of trait obj lifetime?
    pub fn get_model<'a>(&self, name: &str) -> &(Model<T::InterfaceControllerType> + 'a) {
        self.models.get(name).expect("Datatype must be known").as_model()
    }

    /// Iterate over datatypes.
    pub fn iter_dtypes(&self) -> impl Iterator<Item = &Datatype> {
        self.dtypes.values()
    }

    pub fn register_interfaces(&mut self, interfaces: &[&InterfaceDescription]) {
        self.interfaces.register_interfaces(interfaces);
    }

    pub fn register_datatype_models(&mut self, models: Vec<T>) {
        for model in models {
            let description = model.as_model().info();
            self.models.insert(description.name.clone(), model);
            self.dtypes.insert(description.name.clone(), description.into_datatype(&self.interfaces));
        }
    }
}


/// Testing utilities.
///
/// This module is public so dependent libraries can reuse these utilities to
/// test custom datatypes.
pub mod testing {
    use super::*;

    pub fn init_default_dtypes_registry() -> DatatypesRegistry<DefaultDatatypes> {
        init_dtypes_registry::<DefaultDatatypes>()
    }

    pub fn init_dtypes_registry<T: DatatypeEnum>() -> DatatypesRegistry<T> {
        let mut dtypes_registry = DatatypesRegistry::new();
        dtypes_registry.register_interfaces(&<T as DatatypeEnum>::InterfaceControllerType::all_descriptions());
        let models = T::all_variants();
            // .iter()
            // .map(|v| v.as_model())
            // .collect();
        dtypes_registry.register_datatype_models(models);
        dtypes_registry
    }
}
