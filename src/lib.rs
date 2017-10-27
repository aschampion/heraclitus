#![feature(conservative_impl_trait)]

extern crate daggy;
extern crate enum_set;
extern crate postgres;
#[macro_use]
extern crate schemamama;
extern crate schemamama_postgres;
extern crate url;
extern crate uuid;


use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

use enum_set::EnumSet;
use url::Url;
use uuid::Uuid;
// use schemamama;


mod datatype;
mod repo;
mod store;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}

pub fn noop() {
    println!("Test");
}

//struct InternalId(u64);

struct Identity {
    uuid: Uuid,
    hash: u64,
    //internal: InternalId,
}

#[derive(Clone, Copy)]
#[repr(u32)]
pub enum DatatypeRepresentationKind {
    State,
    Delta,
    CumulativeDelta,
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for DatatypeRepresentationKind {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> DatatypeRepresentationKind {
        mem::transmute(v)
    }
}

pub struct Datatype {
    id: Identity,
    name: String,
    version: u64,
    representations: EnumSet<DatatypeRepresentationKind>,
}

impl Datatype {
    fn new(
        uuid: Uuid,
        name: String,
        version: u64,
        representations: EnumSet<DatatypeRepresentationKind>,
    ) -> Datatype {
        let mut dtype = Datatype {
            id: Identity { uuid, hash: 0 },
            name,
            version,
            representations,
        };
        let mut s = DefaultHasher::new();
        dtype.hash(&mut s);
        dtype.id.hash = s.finish();
        dtype
    }
}

impl Hash for Datatype {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.version.hash(state);
        self.representations.hash(state);
    }
}

#[derive(Debug)]
struct DatatypeRelation {
    name: String,
}

struct Metadata {
    datatypes: daggy::Dag<Datatype, DatatypeRelation>,
}

struct Repository {
    id: Identity,
    name: String,
    url: Url,
}

struct Context<T> where T: repo::RepoController {
    // repo: Repository,
    store_type: T
}

/// A graph expressing the dependence structure between sets of data artifacts.
struct ArtifactGraph<'a> {
    id: Identity,
    artifacts: daggy::Dag<ArtifactNode<'a>, ArtifactRelation>,
}

struct Artifact<'a> {
    id: Identity,
    name: Option<String>,
    dtype: &'a Datatype,
}

struct Producer {
    id: Identity,
    name: String,
}

enum ArtifactNode<'a> {
    Producer(Producer),
    Artifact(Artifact<'a>),
}

enum ArtifactRelation {
    DtypeDepends(DatatypeRelation),
    ProducedBy(String),
    PruducedFrom(String),
}

enum VersionStatus {
    Staging,
    Committed,
}

type VersionGraph<'a> = daggy::Dag<Version<'a>, ArtifactRelation>;

struct Version<'a> {
    id: Identity,
    artifact: &'a Artifact<'a>,
    status: VersionStatus,
    representation: DatatypeRepresentationKind,
}

struct Partition<'a> {
    partitioning: &'a Version<'a>,
    index: u64,
}

enum PartCompletion {
    Complete,
    Ragged,
}

struct Hunk<'a> {
    // Is this a Hunk or a Patch (in which case changeset items would be hunks)?
    id: Identity,
    version: &'a Version<'a>,
    partition: Partition<'a>,
    completion: PartCompletion,
}
