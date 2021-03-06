use std;
use std::collections::{HashMap, HashSet};

use enumset::{
    EnumSet,
};
use lazy_static::lazy_static;
use uuid::Uuid;

use crate::Datatype;
use crate::repo::{Repository, RepoController};
use crate::store::Backend;

#[cfg(feature="backend-debug-filesystem")]
use crate::store::debug_filesystem::datatype::DebugFilesystemMetaController;
#[cfg(feature="backend-postgres")]
use crate::store::postgres::datatype::PostgresMetaController;


#[macro_use]
pub mod macros;

pub trait StoreOrBackendMarker {}
pub struct StoreMarker; impl StoreOrBackendMarker for StoreMarker {}
pub struct BackendMarker; impl StoreOrBackendMarker for BackendMarker {}

pub trait StoreOrBackend {
    type Datatype: DatatypeMarker;
    // type Disjunctive: StoreOrBackendMarker;
}

pub trait StoreBackend: StoreOrBackend {
    type Base: Store;

    fn new() -> Self;
}

pub trait Store: Sized + StoreOrBackend {
    #[cfg(feature="backend-debug-filesystem")]
    type BackendDebugFilesystem: StoreBackend;
    #[cfg(feature="backend-postgres")]
    type BackendPostgres: StoreBackend;

    fn backend(&self) -> Backend;

    fn for_backend(backend: Backend) -> Self;

    fn new(repo: &Repository) -> Self {
        Self::for_backend(repo.backend())
    }
}

pub trait DatatypeMarker: 'static {
    type Store: Store;

    fn store(repo: &Repository) -> Self::Store {
        Self::Store::new(repo)
    }
}

lazy_static! {
    static ref DATATYPES_UUID_NAMESPACE: Uuid =
        Uuid::parse_str("a95d827d-3a11-405e-b9e0-e43ffa620d33").unwrap();
}

pub trait DatatypeMeta {
    const NAME: &'static str;
    const VERSION: u64;

    fn uuid() -> Uuid {
        Uuid::new_v5(&DATATYPES_UUID_NAMESPACE, Self::NAME)
    }
}

/// Rust stupidly can't resolve consts through trait objects
/// even though they should just be part of the static vtable.
pub trait DatatypeMetaIndirection {
    fn name(&self) -> &'static str;

    fn uuid(&self) -> Uuid;

    fn version(&self) -> u64;
}

impl<T: DatatypeMeta> DatatypeMetaIndirection for T {
    fn name(&self) -> &'static str {
        <Self as DatatypeMeta>::NAME
    }

    fn uuid(&self) -> Uuid {
        <Self as DatatypeMeta>::uuid()
    }

    fn version(&self) -> u64 {
        <Self as DatatypeMeta>::VERSION
    }
}

pub trait Implements<I: ?Sized + interface::InterfaceMeta> {}

pub struct Description<T: InterfaceControllerEnum> {
    pub name: &'static str,
    pub uuid: Uuid,
    pub version: u64,
    pub reflection: Reflection<T>,
}

pub struct Reflection<T: InterfaceControllerEnum> {
    pub representations: EnumSet<crate::RepresentationKind>,
    // TODO: Not yet clear that this reflection of interfaces is useful.
    pub implements: Vec<T>,
    pub dependencies: Vec<DependencyDescription>,
}

impl<T: InterfaceControllerEnum> Description<T> {
    fn into_datatype(self, interfaces: &InterfaceRegistry) -> Datatype {
        Datatype::new(
            self.name,
            self.uuid,
            self.version,
            self.reflection.representations,
            self.reflection.implements.iter().map(|iface| interfaces.get_index(&iface.to_string())).collect(),
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
    Stores(EnumSet<Backend>),
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
    pub interface: crate::Interface,
    pub extends: HashSet<&'static str>,
}

pub trait Model<T: InterfaceControllerEnum>: DatatypeMetaIndirection {
    // Necessary to be able to create this as a trait object. See:
    // https://www.reddit.com/r/rust/comments/620m1v//dfirs5s/
    //fn clone(&self) -> Self where Self: Sized;

    fn info(&self) -> Description<T> {
        Description {
            name: self.name(),
            uuid: self.uuid(),
            version: self.version(),
            reflection: self.reflection(),
        }
    }

    fn reflection(&self) -> Reflection<T>;

    fn meta_controller(&self, repo: crate::store::Backend) -> StoreMetaController;

    /// If this datatype acts as a partitioning controller, construct one.
    fn interface_controller(&self, iface: T) -> Option<T>;
}

pub trait GetInterfaceController<T: ?Sized + interface::InterfaceMeta> {
    fn get_controller(&self) -> Option<T::Generator>;
}

impl<'a, T, IC> GetInterfaceController<T> for dyn Model<IC> + 'a
        where
            T: ?Sized + interface::InterfaceMeta,
            IC: InterfaceController<T> {
    fn get_controller(&self) -> Option<T::Generator> {
        self.interface_controller(IC::VARIANT)
            .and_then(|ic| ic.into_controller_generator())
    }
}


pub enum StoreMetaController {
    #[cfg(feature="backend-debug-filesystem")]
    DebugFilesystem(Box<dyn DebugFilesystemMetaController>),
    #[cfg(feature="backend-postgres")]
    Postgres(Box<dyn PostgresMetaController>),
}

pub trait InterfaceController<T: ?Sized + interface::InterfaceMeta> :
        From<T::Generator> +
        // Into<T::Generator> +
        InterfaceControllerEnum {
    const VARIANT : Self;

    fn into_controller_generator(self) -> Option<T::Generator>;
}

/// Trait for coproduct type of all an application's `InterfaceController` types.
pub trait InterfaceControllerEnum : PartialEq + std::fmt::Display {
    fn all_descriptions() -> Vec<&'static InterfaceDescription>;
}

/// Trait for coproduct type of all an application's datatype `Model` types.
pub trait DatatypeEnum: Sized {
    type InterfaceControllerType: InterfaceControllerEnum;

    fn variant_names() -> Vec<&'static str>;

    fn from_name(name: &str) -> Option<Self>;

    fn as_model<'a>(&self) -> &(dyn Model<Self::InterfaceControllerType> + 'a);

    fn all_variants() -> Vec<Self> {
        Self::variant_names()
            .iter()
            .map(|name| Self::from_name(name).expect("Impossible"))
            .collect()
    }
}


pub struct InterfaceRegistry {
    extension: crate::InterfaceExtension,
    ifaces_idx: HashMap<&'static str, crate::InterfaceIndex>,
}

impl InterfaceRegistry {
    pub fn new() -> InterfaceRegistry {
        InterfaceRegistry {
            extension: crate::InterfaceExtension::new(),
            ifaces_idx: HashMap::new(),
        }
    }

    pub fn get_index(&self, name: &str) -> crate::InterfaceIndex {
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
    models: HashMap<Uuid, T>,
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
    pub fn get_model<'a>(&self, uuid: &Uuid) -> &(dyn Model<T::InterfaceControllerType> + 'a) {
        self.models.get(uuid).expect("Datatype must be known").as_model()
    }

    pub fn get_model_interface<I: ?Sized + interface::InterfaceMeta>(&self, uuid: &Uuid)
            -> Option<<I as interface::InterfaceMeta>::Generator>
            where T::InterfaceControllerType: InterfaceController<I> {

        self.get_model(uuid).get_controller()
    }

    /// Iterate over datatypes.
    pub fn iter_dtypes(&self) -> impl Iterator<Item = &Datatype> {
        self.dtypes.values()
    }

    pub fn iter_models(&self) -> impl Iterator<Item =&(dyn Model<T::InterfaceControllerType> + '_)> {
        self.models.values().map(|m| m.as_model())
    }

    pub fn register_interfaces(&mut self, interfaces: &[&InterfaceDescription]) {
        self.interfaces.register_interfaces(interfaces);
    }

    pub fn register_datatype_models(&mut self, models: Vec<T>) {
        for model in models {
            let description = model.as_model().info();
            let name = description.name.to_string();
            let dtype = description.into_datatype(&self.interfaces);
            self.models.insert(dtype.id.uuid.clone(), model);
            self.dtypes.insert(name, dtype);
        }
    }
}

interface_controller_enum!(EmptyInterfaceController, ());

datatype_enum!(EmptyDatatypes, EmptyInterfaceController, ());


pub mod interface {
    pub trait InterfaceMeta {
        type Generator;
    }
}


/// Testing utilities.
///
/// This module is public so dependent libraries can reuse these utilities to
/// test custom datatypes.
pub mod testing {
    use super::*;

    pub fn init_empty_dtypes_registry() -> DatatypesRegistry<EmptyDatatypes> {
        init_dtypes_registry::<EmptyDatatypes>()
    }

    pub fn init_dtypes_registry<T: DatatypeEnum>() -> DatatypesRegistry<T> {
        let mut dtypes_registry = DatatypesRegistry::new();
        dtypes_registry.register_interfaces(&<T as DatatypeEnum>::InterfaceControllerType::all_descriptions());
        let models = T::all_variants();
        dtypes_registry.register_datatype_models(models);
        dtypes_registry
    }
}
