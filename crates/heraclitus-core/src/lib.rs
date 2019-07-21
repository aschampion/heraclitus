#![feature(const_fn)]

pub extern crate daggy;
pub extern crate enum_set;
pub extern crate lazy_static;
pub extern crate petgraph;
#[cfg(any(feature="backend-postgres"))]
#[macro_use]
pub extern crate schemer;
pub extern crate url;
pub extern crate uuid;


#[cfg(feature="backend-postgres")]
#[macro_use]
pub extern crate postgres;
#[cfg(feature="backend-postgres")]
pub extern crate postgres_array;
#[cfg(feature="backend-postgres")]
pub extern crate postgres_derive;
#[cfg(feature="backend-postgres")]
pub extern crate schemer_postgres;


// Necessary for names to resolve when using heraclitus-macros within the
// heraclitus-core crate itself;
extern crate self as heraclitus;


use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io;
use std::mem;

use enum_set::EnumSet;
use lazy_static::lazy_static;
#[cfg(feature="backend-postgres")]
use postgres_derive::{ToSql, FromSql};
use url::Url;
use uuid::Uuid;


#[macro_use]
pub mod datatype;
pub mod repo;
pub mod store;


#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Store(String),
    Model(String),
    TODO(&'static str),
}

impl<T: Debug> From<daggy::WouldCycle<T>> for Error {
    fn from(_e: daggy::WouldCycle<T>) -> Self {
        Error::Store("TODO: Daggy cycle".into())
    }
}


//struct InternalId(u64);
pub type HashType = u64;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Identity {
    pub uuid: Uuid,
    pub hash: HashType,
    // TODO: does, e.g., a delta version hash its whole state or the delta state?
    // could be multiple hashes for these.
    // For now say that verions hash state/delta of hunks they own. A complete
    // composite content hash requires partition mapping operations (later this
    // could be memoized somewhere with the version).
    //internal: InternalId,
}

pub trait Identifiable {
    fn id(&self) -> &Identity;
}

#[derive(Clone, Debug, Hash)]
pub struct Interface {
    pub name: &'static str,
}

type InterfaceIndexType = petgraph::graph::DefaultIx;
pub type InterfaceIndex = petgraph::graph::NodeIndex<InterfaceIndexType>;
type InterfaceExtension = daggy::Dag<Interface, (), InterfaceIndexType>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
#[cfg_attr(feature="backend-postgres", derive(ToSql, FromSql))]
#[cfg_attr(feature="backend-postgres", postgres(name = "representation_kind"))]
pub enum RepresentationKind {
    /// Contains independent representation of the state of its datatype.
    /// That is, a single hunk per partition is sufficient.
    #[cfg_attr(feature="backend-postgres", postgres(name = "state"))]
    State,
    /// Contains a dependent representation given a *single* prior state of a
    /// datatype. That is, a single state hunk and this delta is sufficient.
    #[cfg_attr(feature="backend-postgres", postgres(name = "cumulative_delta"))]
    CumulativeDelta,
    /// Contains a dependent representation given prior state of a datatype.
    /// This dependent representation may be a sequence of multiple prior
    /// representations.
    #[cfg_attr(feature="backend-postgres", postgres(name = "delta"))]
    Delta,
}

impl RepresentationKind {
    pub fn all() -> EnumSet<Self> {
        let mut all_rep = EnumSet::new();
        all_rep.insert(RepresentationKind::State);
        all_rep.insert(RepresentationKind::CumulativeDelta);
        all_rep.insert(RepresentationKind::Delta);

        all_rep
    }
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for RepresentationKind {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> RepresentationKind {
        mem::transmute(v)
    }
}

lazy_static! {
    static ref DATATYPES_UUID_NAMESPACE: Uuid =
        Uuid::parse_str("a95d827d-3a11-405e-b9e0-e43ffa620d33").unwrap();
}

#[derive(Debug)]
pub struct Datatype {
    // TODO: Not clear that identity is needed as canonical resolution is
    // through name, but here for consistency with other data structures.
    id: Identity,
    pub name: String,
    version: u64,
    representations: EnumSet<RepresentationKind>,
    implements: HashSet<InterfaceIndex>,
}

impl Datatype {
    fn new(
        name: String,
        version: u64,
        representations: EnumSet<RepresentationKind>,
        implements: HashSet<InterfaceIndex>,
    ) -> Datatype {
        let uuid = Uuid::new_v5(&DATATYPES_UUID_NAMESPACE, &name);
        let mut dtype = Datatype {
            id: Identity { uuid, hash: 0 },
            name,
            version,
            representations,
            implements,
        };
        let mut s = DefaultHasher::new();
        dtype.hash(&mut s);
        dtype.id.hash = s.finish();
        dtype
    }
}

impl Identifiable for Datatype {
    fn id(&self) -> &Identity {
        &self.id
    }
}

impl Hash for Datatype {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.version.hash(state);
        self.representations.hash(state);
        // self.implements.hash(state);
    }
}

// TODO: not clear this is necessary
pub struct RepositoryLocation {
    pub url: Url,
}
