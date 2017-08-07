#![feature(conservative_impl_trait)]

extern crate daggy;
extern crate url;
extern crate uuid;

use url::Url;
use uuid::Uuid;

mod datatype;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
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
enum DatatypeRepresentationKind {
    State,
    Delta,
    CumulativeDelta,
}

// Boilerplate necessary for EnumSet compatibility.
impl self::enum_set::CLike for DatatypeRepresentationKind {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> DatatypeRepresentationKind {
        mem::transmute(v)
    }
}

struct Datatype {
    id: Identity,
    name: String,
    version: u64,
    representations: EnumSet<DatatypeRepresentationKind>,
}

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

struct Context {
    repo: Repository,
}

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
    id: Identity,
    version: &'a Version<'a>,
    partition: Partition<'a>,
    completion: PartCompletion,
}

