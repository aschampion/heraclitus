#![feature(conservative_impl_trait)]

extern crate daggy;
extern crate enum_set;
extern crate failure;
#[macro_use]
extern crate lazy_static;
extern crate petgraph;
extern crate postgres;
#[macro_use]
extern crate schemer;
extern crate schemer_postgres;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate url;
extern crate uuid;


use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::mem;

use daggy::Walker;
use enum_set::EnumSet;
use petgraph::visit::EdgeRef;
use url::Url;
use uuid::Uuid;
// use schemer;

use datatype::{DatatypeEnum, DatatypesRegistry};
use datatype::artifact_graph::{ArtifactGraphDescription, ArtifactDescription};


mod datatype;
mod repo;
mod store;

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
    hash: u64,
    // TODO: does, e.g., a delta version hash its whole state or the delta state?
    // could be multiple hashees for these, for now say that state versions
    // hash whole state, delta versions hash deltas (but this is garbage, same
    // delta versions would be ident even if state is different).
    //internal: InternalId,
}

#[derive(Clone, Debug, Hash)]
pub struct Interface {
    name: &'static str,
}

type InterfaceIndexType = petgraph::graph::DefaultIx;
pub type InterfaceIndex = petgraph::graph::NodeIndex<InterfaceIndexType>;
type InterfaceExtension = daggy::Dag<Interface, (), InterfaceIndexType>;

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
    implements: HashSet<InterfaceIndex>,
}

impl Datatype {
    fn new(
        name: String,
        version: u64,
        representations: EnumSet<DatatypeRepresentationKind>,
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

impl Hash for Datatype {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.version.hash(state);
        self.representations.hash(state);
        // self.implements.hash(state);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DatatypeRelation {
    name: String,
}

type DatatypeGraph =  daggy::Dag<Datatype, DatatypeRelation>;

struct Repository {
    id: Identity,  // TODO: no clear reason that repos need an identity. Except: cross-store deps.
    name: String,
    url: Url,
}

pub struct Context<T: DatatypeEnum> {
    dtypes_registry: datatype::DatatypesRegistry<T>,
    repo_control: repo::StoreRepoController,
}

type ArtifactGraphIndexType = petgraph::graph::DefaultIx;
pub type ArtifactGraphIndex = petgraph::graph::NodeIndex<ArtifactGraphIndexType>;
pub(crate) type ArtifactGraphType<'a> = daggy::Dag<Artifact<'a>, ArtifactRelation, ArtifactGraphIndexType>;
/// A graph expressing the dependence structure between sets of data artifacts.
pub struct ArtifactGraph<'a> {
    id: Identity,
    artifacts: ArtifactGraphType<'a>,
}

impl<'a> ArtifactGraph<'a> {
    fn new_singleton(
        datatype: &'a Datatype,
        graph_uuid: Uuid,
        artifact_uuid: Uuid,
    ) -> ArtifactGraph<'a> {
        let mut art_graph = ArtifactGraph {
            id: Identity {
                uuid: graph_uuid.clone(),
                hash: 0,
            },
            artifacts: ArtifactGraphType::new(),
        };
        let mut s = DefaultHasher::new();
        let mut ag_hash = DefaultHasher::new();
        let mut art = Artifact {
            id: Identity {uuid: artifact_uuid.clone(), hash: 0},
            name: None,
            dtype: datatype,
        };
        art.hash(&mut s);
        art.id.hash = s.finish();
        art.id.hash.hash(&mut ag_hash);
        art_graph.artifacts.add_node(art);
        art_graph.id.hash = ag_hash.finish();
        art_graph
    }

    fn from_description<T: DatatypeEnum>(
        desc: &ArtifactGraphDescription,
        dtypes_registry: &'a DatatypesRegistry<T>
    ) -> (ArtifactGraph<'a>, BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex>) {
        let desc_graph = desc.artifacts.graph();
        let mut to_visit = desc_graph.externals(petgraph::Direction::Incoming)
                .collect::<VecDeque<_>>();

        let mut artifacts = ArtifactGraphType::new();
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
                        new_p.id.hash.hash(&mut s);
                    }

                    let a_desc = desc.artifacts.node_weight(node_idx).expect("Graph is malformed.");
                    let artifact = {
                        let mut art = Artifact {
                            id: id,
                            name: a_desc.name.clone(),
                            dtype: dtypes_registry.get_datatype(&*a_desc.dtype).expect("Unknown datatype."),
                        };
                        art.hash(&mut s);
                        art.id.hash = s.finish();
                        art.id.hash.hash(&mut ag_hash);
                        art
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
        // TODO: should use a topological sort instead
        loop {
            match to_visit.pop_front() {
                Some(node_idx) => {
                    let mut s = DefaultHasher::new();

                    // TODO: replace with petgraph neighbors
                    // TODO: this ordering needs to be deterministic
                    for (_, p_idx) in self.artifacts.parents(node_idx).iter(&self.artifacts) {
                        let artifact = self.artifacts.node_weight(p_idx).expect("Graph is malformed.");
                        artifact.id.hash.hash(&mut s);
                    }

                    let artifact = self.artifacts.node_weight(node_idx).expect("Graph is malformed.");
                    artifact.hash(&mut s);
                    if s.finish() != artifact.id.hash { return false; }
                    artifact.id.hash.hash(&mut ag_hash);

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
    ) -> Option<(ArtifactGraphIndex, &Artifact)> {
        let graph = self.artifacts.graph();
        for node_idx in graph.node_indices() {
            let node = graph.node_weight(node_idx).expect("Graph is malformed");
            if node.id == *id {
                return Some((node_idx, node))
            }
        }

        None
    }

    fn find_artifact_by_uuid(
        &self,
        uuid: &Uuid
    ) -> Option<(ArtifactGraphIndex, &'a Artifact)> {
        let graph = self.artifacts.graph();
        for node_idx in graph.node_indices() {
            let node = graph.node_weight(node_idx).expect("Graph is malformed");
            if node.id.uuid == *uuid {
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ArtifactRelation {
    DtypeDepends(DatatypeRelation),
    ProducedFrom(String),
}


type VersionGraphIndexType = petgraph::graph::DefaultIx;
pub type VersionGraphIndex = petgraph::graph::NodeIndex<VersionGraphIndexType>;
pub struct VersionGraph<'a> {
    versions: daggy::Dag<Version<'a>, VersionRelation<'a>, VersionGraphIndexType>,
}
// TODO: should either use the below in most interfaces or make the above also have pruning.
pub type VersionSubgraph<'a> = daggy::Dag<VersionNode<'a>, VersionRelation<'a>, VersionGraphIndexType>;

impl<'a> VersionGraph<'a> {
    fn new() -> VersionGraph<'a> {
        VersionGraph {
            versions: daggy::Dag::new()
        }
    }

    fn get_related_version(
        &self,
        v_idx: VersionGraphIndex,
        relation: &VersionRelation,
        dir: petgraph::Direction,
    ) -> Option<VersionGraphIndex> {
        self.versions.graph().edges_directed(v_idx, dir)
            .find(|e| e.weight() == relation)
            .map(|e| match dir {
                petgraph::Direction::Outgoing => e.target(),
                petgraph::Direction::Incoming => e.source(),
            })
    }

    fn get_partitioning(
        &self,
        v_idx: VersionGraphIndex
    ) -> Option<&Version> {
        let partitioning_art_relation = ArtifactRelation::DtypeDepends(DatatypeRelation {
                name: datatype::interface::PARTITIONING_RELATION_NAME.clone(),
            });
        let partitioning_relation = VersionRelation::Dependence(&partitioning_art_relation);
        self.get_related_version(
                v_idx,
                &partitioning_relation,
                petgraph::Direction::Incoming)
            .map(|p_idx| self.versions.node_weight(p_idx).expect("Impossible non-existent index"))
    }
}

#[derive(Debug, PartialEq)]
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
    artifact: &'a Artifact<'a>,
    status: VersionStatus,
    representation: DatatypeRepresentationKind,
}

impl<'a> Version<'a> {
    fn new_singleton(
        artifact: &'a Artifact<'a>,
        uuid: Uuid,
    ) -> Version<'a> {
        Version {
            id: Identity {
                uuid: uuid,
                hash: 0,
            },
            artifact: artifact,
            status: ::VersionStatus::Committed,
            representation: ::DatatypeRepresentationKind::State,
        }
    }
}

pub type PartitionIndex = u64;

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
    partition: Partition<'a>,
    completion: PartCompletion,
    // TODO: do hunks also need a DatatypeRepresentationKind?
}
