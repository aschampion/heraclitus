#![feature(conservative_impl_trait)]

extern crate daggy;
extern crate enum_set;
#[macro_use]
extern crate lazy_static;
extern crate petgraph;
extern crate postgres;
#[macro_use]
extern crate schemamama;
extern crate schemamama_postgres;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate url;
extern crate uuid;


use std::collections::{BTreeMap, HashMap, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::mem;

use daggy::Walker;
use enum_set::EnumSet;
use url::Url;
use uuid::Uuid;
// use schemamama;

use datatype::{DatatypesRegistry};
use datatype::artifact_graph::{ArtifactGraphDescription, ArtifactNodeDescription};


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

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Store(String),
}

//struct InternalId(u64);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Identity {
    uuid: Uuid,
    hash: u64, // TODO: does, e.g., a delta version hash its whole state or the delta state?
    //internal: InternalId,
}

#[derive(Clone, Copy, Debug)]
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

lazy_static! {
    static ref DATATYPES_UUID_NAMESPACE: Uuid = Uuid::parse_str("a95d827d-3a11-405e-b9e0-e43ffa620d33").unwrap();
}

#[derive(Debug)]
pub struct Datatype {
    // TODO: Not clear that identity is needed as canonical resolution is
    // through name, but here for consistency with other data structures.
    id: Identity,
    name: String,
    version: u64,
    representations: EnumSet<DatatypeRepresentationKind>,
}

impl Datatype {
    fn new(
        name: String,
        version: u64,
        representations: EnumSet<DatatypeRepresentationKind>,
    ) -> Datatype {
        let uuid = Uuid::new_v5(&DATATYPES_UUID_NAMESPACE, &name);
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatatypeRelation {
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

pub struct Context {
    dtypes_registry: datatype::DatatypesRegistry,
    repo_control: repo::StoreRepoController,
}

type ArtifactGraphIndexType = petgraph::graph::DefaultIx;
pub type ArtifactGraphIndex = petgraph::graph::NodeIndex<ArtifactGraphIndexType>;
/// A graph expressing the dependence structure between sets of data artifacts.
pub struct ArtifactGraph<'a> {
    id: Identity,
    artifacts: daggy::Dag<ArtifactNode<'a>, ArtifactRelation, ArtifactGraphIndexType>,
}

impl<'a> ArtifactGraph<'a> {
    fn from_description(
        desc: &ArtifactGraphDescription,
        dtypes_registry: &'a DatatypesRegistry
    ) -> (ArtifactGraph<'a>, BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex>) {
        let desc_graph = desc.artifacts.graph();
        let mut to_visit = desc_graph.externals(petgraph::Direction::Incoming)
                .collect::<VecDeque<_>>();

        let mut artifacts: daggy::Dag<ArtifactNode, ArtifactRelation> = daggy::Dag::new();
        let mut idx_map = BTreeMap::new();
        let mut ag_hash = DefaultHasher::new();

        // Walk the description graph in descending dependency order to build
        // up hashes for artifacts while copying to the new graph.
        loop {
            match to_visit.pop_front() {
                Some(node_idx) => {
                    let mut id = Identity { uuid: Uuid::new_v4(), hash: 0 };
                    let mut s = DefaultHasher::new();

                    // TODO: replace with petgraph neighbors
                    // TODO: this ordering needs to be deterministic
                    for (_, p_idx) in desc.artifacts.parents(node_idx).iter(&desc.artifacts) {
                        let new_p_idx = idx_map.get(&p_idx).expect("Graph is malformed.");
                        let new_p = artifacts.node_weight(*new_p_idx).expect("Graph is malformed.");
                        match new_p {
                            &ArtifactNode::Producer(ref inner) => inner.id.hash.hash(&mut s),
                            &ArtifactNode::Artifact(ref inner) => inner.id.hash.hash(&mut s),
                        }
                    }

                    let node = desc.artifacts.node_weight(node_idx).expect("Graph is malformed.");
                    let artifact = match node {
                        &ArtifactNodeDescription::Producer(ref p_desc) => {
                            let mut producer = Producer { id: id, name: p_desc.name.clone() };
                            producer.hash(&mut s);
                            producer.id.hash = s.finish();
                            producer.id.hash.hash(&mut ag_hash);
                            ArtifactNode::Producer(producer)
                        },
                        &ArtifactNodeDescription::Artifact(ref a_desc) => {
                            let mut art = Artifact {
                                id: id,
                                name: a_desc.name.clone(),
                                dtype: dtypes_registry.get_datatype(&*a_desc.dtype).expect("Unknown datatype."),
                            };
                            art.hash(&mut s);
                            art.id.hash = s.finish();
                            art.id.hash.hash(&mut ag_hash);
                            ArtifactNode::Artifact(art)
                        },
                    };

                    let new_idx = artifacts.add_node(artifact);
                    idx_map.insert(node_idx, new_idx);

                    for (e_idx, p_idx) in desc.artifacts.parents(node_idx).iter(&desc.artifacts) {
                        let edge = desc.artifacts.edge_weight(e_idx).expect("Graph is malformed.").clone();
                        artifacts.add_edge(*idx_map.get(&p_idx).expect("Graph is malformed."), new_idx, edge)
                                 .expect("Graph is malformed.");
                    }

                    for (_, c_idx) in desc.artifacts.children(node_idx).iter(&desc.artifacts) {
                        to_visit.push_back(c_idx);
                    }
                },
                None => break
            }
        }

        (ArtifactGraph {
            id: Identity {
                uuid: Uuid::new_v4(),
                hash: ag_hash.finish(),
            },
            artifacts: artifacts,
        }, idx_map)
    }

    fn verify_hash(&self) -> bool {
        let desc_graph = self.artifacts.graph();
        let mut to_visit = desc_graph.externals(petgraph::Direction::Incoming)
                .collect::<VecDeque<_>>();

        let mut ag_hash = DefaultHasher::new();

        // Walk the description graph in descending dependency order.
        loop {
            match to_visit.pop_front() {
                Some(node_idx) => {
                    let mut s = DefaultHasher::new();

                    // TODO: replace with petgraph neighbors
                    // TODO: this ordering needs to be deterministic
                    for (_, p_idx) in self.artifacts.parents(node_idx).iter(&self.artifacts) {
                        match self.artifacts.node_weight(p_idx).expect("Graph is malformed.") {
                            &ArtifactNode::Producer(ref inner) => inner.id.hash.hash(&mut s),
                            &ArtifactNode::Artifact(ref inner) => inner.id.hash.hash(&mut s),
                        }
                    }

                    let node = self.artifacts.node_weight(node_idx).expect("Graph is malformed.");
                    match node {
                        &ArtifactNode::Producer(ref p) => {
                            p.hash(&mut s);
                            if s.finish() != p.id.hash { return false; }
                            p.id.hash.hash(&mut ag_hash);
                        },
                        &ArtifactNode::Artifact(ref a) => {
                            a.hash(&mut s);
                            if s.finish() != a.id.hash { return false; }
                            a.id.hash.hash(&mut ag_hash);
                        },
                    };

                    for (_, c_idx) in self.artifacts.children(node_idx).iter(&self.artifacts) {
                        to_visit.push_back(c_idx);
                    }
                },
                None => break
            }
        }

        self.id.hash == ag_hash.finish()
    }

    fn find_artifact_by_id(
        &self,
        id: &Identity
    ) -> Option<(ArtifactGraphIndex, &ArtifactNode)> {
        let graph = self.artifacts.graph();
        for node_idx in graph.node_indices() {
            let node = graph.node_weight(node_idx).expect("Graph is malformed");
            if node.id() == id {
                return Some((node_idx, node))
            }
        }

        None
    }
}

/// An `Artifact` represents a collection of instances of a `Datatype` that can
/// exist in dependent relationships with other artifacts and producers.
#[derive(Debug)]
pub struct Artifact<'a> {
    id: Identity,
    name: Option<String>,
    dtype: &'a Datatype,
}

impl<'a> Hash for Artifact<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dtype.id.hash.hash(state);
        self.name.hash(state);
    }
}

#[derive(Debug)]
pub struct Producer {
    id: Identity,
    name: String,
    // TODO:
    // - Does this producer handle changes incrementally or cumulatively/in batches?
    // - Types:
    //   - Handle only
    //   - RPC/push notify
    //   - Directly invoked
    //     - Rust/FFI call (how is this serialized?)
    //     - Script?
}

impl Hash for Producer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

#[derive(Debug)]
pub enum ArtifactNode<'a> {
    Producer(Producer),
    Artifact(Artifact<'a>),
}

impl<'a> ArtifactNode<'a> {
    fn id(&self) -> &Identity {
        match self {
            &ArtifactNode::Producer(ref p) => &p.id,
            &ArtifactNode::Artifact(ref a) => &a.id,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ArtifactRelation {
    DtypeDepends(DatatypeRelation),
    ProducedFrom(String),
}


type VersionGraphIndexType = petgraph::graph::DefaultIx;
pub type VersionGraphIndex = petgraph::graph::NodeIndex<VersionGraphIndexType>;
pub type VersionGraph<'a> = daggy::Dag<Version<'a>, VersionRelation<'a>, VersionGraphIndexType>;
// TODO: should either use the below in most interfaces or make the above also have pruning.
pub type VersionSubgraph<'a> = daggy::Dag<VersionNode<'a>, VersionRelation<'a>, VersionGraphIndexType>;

#[derive(Debug)]
pub enum VersionRelation<'a>{
    Dependence(&'a ArtifactRelation),
    Parent,
}

#[derive(Debug)]
pub enum VersionStatus {
    Staging,
    Committed,
}

#[derive(Debug)]
pub enum VersionNode<'a> {
    Complete(Version<'a>),
    Pruned(Version<'a>),
}

#[derive(Debug)]
pub struct Version<'a> {
    id: Identity,
    artifact: &'a ArtifactNode<'a>,
    status: VersionStatus,
    representation: DatatypeRepresentationKind,
}

pub type PartitionIndex = u64;
const UNARY_PARTITION_INDEX: PartitionIndex = 0;

#[derive(Debug)]
pub struct Partition<'a> {
    partitioning: &'a Version<'a>,
    index: PartitionIndex,
    // TODO: also need to be able to handle partition types (leaf v. neighborhood, level, arbitrary)
}

#[derive(Debug)]
pub enum PartCompletion {
    Complete,
    Ragged,
}

#[derive(Debug)]
pub struct Hunk<'a> {
    // Is this a Hunk or a Patch (in which case changeset items would be hunks)?
    id: Identity,
    version: &'a Version<'a>,
    partition: Option<Partition<'a>>,
    completion: PartCompletion,
    // TODO: do hunks also need a DatatypeRepresentationKind?
}
