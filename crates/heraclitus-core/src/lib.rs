#![feature(const_fn)]

pub extern crate daggy;
pub extern crate enumset;
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

use enumset::{
    EnumSet,
    EnumSetType,
};
#[cfg(feature="backend-postgres")]
use postgres_derive::{ToSql, FromSql};
use serde_derive::{Serialize, Deserialize};
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
    Model(ModelError),
    TODO(&'static str),
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Self {
        Error::Io(io_error)
    }
}

impl From<ModelError> for Error {
    fn from(model_error: ModelError) -> Self {
        Error::Model(model_error)
    }
}

#[derive(Debug)]
pub enum ModelError {
    HashMismatch {
        uuid: Uuid,
        expected: HashType,
        found: HashType,
    },
    NotFound(Uuid),
    Other(String),
}

impl<T: Debug> From<daggy::WouldCycle<T>> for Error {
    fn from(_e: daggy::WouldCycle<T>) -> Self {
        Error::Store("TODO: Daggy cycle".into())
    }
}


//struct InternalId(u64);
pub type HashType = u64;

// TODO: shouldn't ID have a custom hash that just hashes against its hash
// (ignoring its uuid)?
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

impl From<HashType> for Identity {
    fn from(hash: HashType) -> Self {
        Identity {
            uuid: Uuid::new_v4(),
            hash
        }
    }
}

// TODO: this should be majorly revised with the content-vs-structure hash
// refactor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct PartialIdentity {
    pub uuid: Uuid,
    pub hash: Option<HashType>,
}

impl From<Identity> for PartialIdentity {
    fn from(id: Identity) -> Self {
        PartialIdentity {
            uuid: id.uuid,
            hash: Some(id.hash),
        }
    }
}

impl From<PartialIdentity> for Identity {
    fn from(id: PartialIdentity) -> Self {
        Identity {
            uuid: id.uuid,
            hash: id.hash.unwrap_or_else(|| 0),
        }
    }
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

#[derive(Debug)]
#[derive(EnumSetType)]
#[enumset(serialize_as_list)]
#[derive(Deserialize, Serialize)]
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
        EnumSet::all()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Datatype {
    // TODO: Not clear that identity is needed as canonical resolution is
    // through name, but here for consistency with other data structures.
    id: Identity,
    pub name: &'static str,
    version: u64,
    #[serde(skip)]
    representations: EnumSet<RepresentationKind>,
    #[serde(skip)]
    implements: HashSet<InterfaceIndex>,
}

impl Datatype {
    fn new(
        name: &'static str,
        uuid: Uuid,
        version: u64,
        representations: EnumSet<RepresentationKind>,
        implements: HashSet<InterfaceIndex>,
    ) -> Datatype {
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
