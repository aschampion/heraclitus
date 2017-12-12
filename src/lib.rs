#![feature(conservative_impl_trait)]
#![feature(entry_and_modify)]
#![feature(vec_remove_item)]

extern crate daggy;
extern crate enum_set;
extern crate failure;
#[macro_use]
extern crate lazy_static;
extern crate petgraph;
#[macro_use]
extern crate postgres;
extern crate postgres_array;
#[macro_use]
extern crate postgres_derive;
#[macro_use]
extern crate rental;
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
use petgraph::Direction;
use petgraph::visit::EdgeRef;
use url::Url;
use uuid::Uuid;
// use schemer;

use datatype::{DatatypeEnum, DatatypesRegistry};
use datatype::artifact_graph::{ArtifactGraphDescription, ArtifactDescription};


pub mod datatype;
pub mod repo;
pub mod store;

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

pub trait Identifiable {
    fn id(&self) -> &Identity;
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

pub trait IdentifiableGraph<'s, N: Identifiable, E: 's, IT: petgraph::csr::IndexType> {
    fn graph(&self) -> &daggy::Dag<N, E, IT>;

    fn graph_mut(&mut self) -> &mut daggy::Dag<N, E, IT>;

    fn find_by_id<'b>(
        &'b self,
        id: &Identity
    ) -> Option<(petgraph::graph::NodeIndex<IT>, &N)> where 's: 'b {
        for node_idx in self.graph().graph().node_indices() {
            let node = self.graph().node_weight(node_idx).expect("Graph is malformed");
            if node.id() == id {
                return Some((node_idx, node))
            }
        }

        None
    }

    fn find_by_uuid<'b>(
        &'b self,
        uuid: &Uuid
    ) -> Option<(petgraph::graph::NodeIndex<IT>, &N)>  where 's: 'b {
        for node_idx in self.graph().graph().node_indices() {
            let node = self.graph().node_weight(node_idx).expect("Graph is malformed");
            if node.id().uuid == *uuid {
                return Some((node_idx, node))
            }
        }

        None
    }

    /// If a node with a given identity does not exist, construct and
    /// insert it into the graph. Return the index of the existing or created
    /// node.
    fn emplace<F>(
        &mut self,
        id: &Identity,
        constructor: F
    ) -> petgraph::graph::NodeIndex<IT>
            where F: FnOnce() -> N {
        match self.find_by_id(id) {
            Some((idx, _)) => idx,
            None => self.graph_mut().add_node(constructor()),
        }
    }
}

type ArtifactGraphIndexType = petgraph::graph::DefaultIx;
pub type ArtifactGraphIndex = petgraph::graph::NodeIndex<ArtifactGraphIndexType>;
pub type ArtifactGraphEdgeIndex = petgraph::graph::EdgeIndex<ArtifactGraphIndexType>;
pub(crate) type ArtifactGraphType<'a> = daggy::Dag<Artifact<'a>, ArtifactRelation, ArtifactGraphIndexType>;
/// A graph expressing the dependence structure between sets of data artifacts.
pub struct ArtifactGraph<'a> {
    id: Identity,
    artifacts: ArtifactGraphType<'a>,
    // unary_partitioning_idx: ArtifactGraphIndex,
    // unary_partitioning_ver: Option<Version<'a, 'b>>,
}

impl<'a> ArtifactGraph<'a> {
    // fn new_singleton(
    //     datatype: &'a Datatype,
    //     graph_uuid: Uuid,
    //     artifact_uuid: Uuid,
    // ) -> ArtifactGraph<'a> {
    //     let mut art_graph = ArtifactGraph {
    //         id: Identity {
    //             uuid: graph_uuid.clone(),
    //             hash: 0,
    //         },
    //         artifacts: ArtifactGraphType::new(),
    //     };
    //     let mut s = DefaultHasher::new();
    //     let mut ag_hash = DefaultHasher::new();
    //     let mut art = Artifact {
    //         id: Identity {uuid: artifact_uuid.clone(), hash: 0},
    //         name: None,
    //         dtype: datatype,
    //     };
    //     art.hash(&mut s);
    //     art.id.hash = s.finish();
    //     art.id.hash.hash(&mut ag_hash);
    //     art_graph.artifacts.add_node(art);
    //     art_graph.id.hash = ag_hash.finish();
    //     art_graph
    // }

    fn from_description<T: DatatypeEnum>(
        desc: &ArtifactGraphDescription,
        dtypes_registry: &'a DatatypesRegistry<T>,
        unary_partitioning_idx: Option<ArtifactGraphIndex>,
    ) -> (ArtifactGraph<'a>, BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex>) {

        fn create_node<'a, T: DatatypeEnum>(
            desc: &ArtifactGraphDescription,
            dtypes_registry: &'a DatatypesRegistry<T>,
            artifacts: &mut ArtifactGraphType<'a>,
            idx_map: &mut BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex>,
            ag_hash: &mut DefaultHasher,
            node_idx: ArtifactGraphIndex,
            inner_unary_partitioning_idx: Option<ArtifactGraphIndex>,
        ) -> ArtifactGraphIndex {
            let mut id = Identity { uuid: Uuid::new_v4(), hash: 0 };
            let mut s = DefaultHasher::new();

            // TODO: replace with petgraph neighbors
            // Order hashing based on hash, not ID, so that artifact content
            // hashes are ID-independent.
            let mut sorted_parent_hashes = desc.artifacts.parents(node_idx)
                .iter(&desc.artifacts)
                .map(|(_, p_idx)| {
                    let new_p_idx = idx_map.get(&p_idx).expect("Graph is malformed.");
                    artifacts.node_weight(*new_p_idx).expect("Graph is malformed.").id.hash
                })
                .collect::<Vec<u64>>();
            sorted_parent_hashes.sort();
            for hash in &sorted_parent_hashes {
                hash.hash(&mut s);
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
                art.id.hash.hash(ag_hash);
                art
            };

            let new_idx = artifacts.add_node(artifact);
            idx_map.insert(node_idx, new_idx);

            let mut has_partitioning = false;
            for (e_idx, p_idx) in desc.artifacts.parents(node_idx).iter(&desc.artifacts) {
                let edge = desc.artifacts.edge_weight(e_idx).expect("Graph is malformed.").clone();
                has_partitioning = has_partitioning || match edge {
                    ArtifactRelation::DtypeDepends(ref rel) => rel.name == "Partitioning",
                    _ => false,
                };
                artifacts.add_edge(*idx_map.get(&p_idx).expect("Graph is malformed."), new_idx, edge)
                         .expect("Graph is malformed.");
            }

            if !has_partitioning && inner_unary_partitioning_idx.is_some() {
                let edge = ArtifactRelation::DtypeDepends(DatatypeRelation {name: "Partitioning".into()});
                artifacts.add_edge(inner_unary_partitioning_idx.expect("TODO"), new_idx, edge)
                         .expect("Graph is malformed.");
            }

            new_idx

            // for (_, c_idx) in desc.artifacts.children(node_idx).iter(&desc.artifacts) {
            //     to_visit.push_back(c_idx);
            // }
        }

        let desc_graph = desc.artifacts.graph();
        let mut to_visit = daggy::petgraph::algo::toposort(desc.artifacts.graph(), None)
            .expect("TODO: not a DAG");

        let mut artifacts = ArtifactGraphType::new();
        let mut idx_map = BTreeMap::new();
        let mut ag_hash = DefaultHasher::new();

        // let up_idx = match unary_partitioning_idx {
        //     Some(desc_idx) => {
        //         to_visit.remove_item(&desc_idx);
        //         create_node(
        //             desc,
        //             dtypes_registry,
        //             &mut artifacts,
        //             &mut idx_map,
        //             &mut ag_hash,
        //             desc_idx,
        //             None)
        //     },
        //     None => {
        //         let mut singleton_agd = ArtifactGraphDescription::new();
        //         let desc_idx = singleton_agd.artifacts.add_node(ArtifactDescription{
        //             name: Some("Unary Partitioning Singleton".into()),
        //             dtype: "UnaryPartitioning".into(),
        //         });
        //         create_node(
        //             &singleton_agd,
        //             dtypes_registry,
        //             &mut artifacts,
        //             &mut idx_map,
        //             &mut ag_hash,
        //             desc_idx,
        //             None)
        //     },
        // };

        for node_idx in to_visit {
            create_node(
                desc,
                dtypes_registry,
                &mut artifacts,
                &mut idx_map,
                &mut ag_hash,
                node_idx,
                None);
        }

        // Walk the description graph in descending dependency order to build
        // up hashes for artifacts while copying to the new graph.
        // for node_idx in to_visit {
        //         // Some(node_idx) => {
        //             let mut id = Identity { uuid: Uuid::new_v4(), hash: 0 };
        //             let mut s = DefaultHasher::new();

        //             // TODO: replace with petgraph neighbors
        //             // TODO: this ordering needs to be deterministic
        //             for (_, p_idx) in desc.artifacts.parents(node_idx).iter(&desc.artifacts) {
        //                 let new_p_idx = idx_map.get(&p_idx).expect("Graph is malformed.");
        //                 let new_p = artifacts.node_weight(*new_p_idx).expect("Graph is malformed.");
        //                 new_p.id.hash.hash(&mut s);
        //             }

        //             let a_desc = desc.artifacts.node_weight(node_idx).expect("Graph is malformed.");
        //             let artifact = {
        //                 let mut art = Artifact {
        //                     id: id,
        //                     name: a_desc.name.clone(),
        //                     dtype: dtypes_registry.get_datatype(&*a_desc.dtype).expect("Unknown datatype."),
        //                 };
        //                 art.hash(&mut s);
        //                 art.id.hash = s.finish();
        //                 art.id.hash.hash(&mut ag_hash);
        //                 art
        //             };

        //             let new_idx = artifacts.add_node(artifact);
        //             idx_map.insert(node_idx, new_idx);

        //             for (e_idx, p_idx) in desc.artifacts.parents(node_idx).iter(&desc.artifacts) {
        //                 let edge = desc.artifacts.edge_weight(e_idx).expect("Graph is malformed.").clone();
        //                 artifacts.add_edge(*idx_map.get(&p_idx).expect("Graph is malformed."), new_idx, edge)
        //                          .expect("Graph is malformed.");
        //             }

        //             // for (_, c_idx) in desc.artifacts.children(node_idx).iter(&desc.artifacts) {
        //             //     to_visit.push_back(c_idx);
        //             // }
        //         // },
        //         // None => break
        //     // }
        // }

        (ArtifactGraph {
            id: Identity {
                uuid: Uuid::new_v4(),
                hash: ag_hash.finish(),
            },
            artifacts: artifacts,
            // unary_partitioning_idx: up_idx,
        }, idx_map)
    }

    fn verify_hash(&self) -> bool {
        let to_visit = daggy::petgraph::algo::toposort(self.artifacts.graph(), None)
            .expect("TODO: not a DAG");

        let mut ag_hash = DefaultHasher::new();

        // Walk the description graph in descending dependency order.
        for node_idx in to_visit {
            let mut s = DefaultHasher::new();

            // TODO: replace with petgraph neighbors
            let mut sorted_parent_hashes = self.artifacts.parents(node_idx)
                .iter(&self.artifacts)
                .map(|(_, p_idx)| {
                    self.artifacts[p_idx].id.hash
                })
                .collect::<Vec<u64>>();
            sorted_parent_hashes.sort();
            for hash in &sorted_parent_hashes {
                hash.hash(&mut s);
            }

            let artifact = self.artifacts.node_weight(node_idx).expect("Graph is malformed.");
            artifact.hash(&mut s);
            if s.finish() != artifact.id.hash { return false; }
            artifact.id.hash.hash(&mut ag_hash);
        };

        self.id.hash == ag_hash.finish()
    }

    // fn get_unary_partitioning<'b>(
    //     &'b mut self
    // ) -> &Version<'a, 'b> {
    //     if self.unary_partitioning_ver.is_none() {
    //         let v = Version {
    //             id: Identity {
    //                 uuid: ::datatype::partitioning::UNARY_PARTITIONING_VERSION_UUID.clone(),
    //                 hash: 0,
    //             },
    //             artifact: self.artifacts[self.unary_partitioning_idx],
    //             status: ::VersionStatus::Committed,
    //             representation: ::DatatypeRepresentationKind::State,
    //         };
    //         self.unary_partitioning_ver = Some(v);
    //     }

    //     match self.unary_partitioning_ver {
    //         Some(ref v) => v,
    //         None => unreachable!(),
    //     }
    // }
}

impl<'a, 's> IdentifiableGraph<'s, Artifact<'a>, ArtifactRelation, ArtifactGraphIndexType> for ArtifactGraph<'a> {
    fn graph(&self) -> &daggy::Dag<Artifact<'a>, ArtifactRelation, ArtifactGraphIndexType> {
        &self.artifacts
    }

    fn graph_mut(&mut self) -> &mut daggy::Dag<Artifact<'a>, ArtifactRelation, ArtifactGraphIndexType> {
        &mut self.artifacts
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

// impl<'a> Artifact<'a> {
//     fn borrow(&self) -> &Datatype { self.dtype }
//     fn try_borrow(&self) -> Result<&Datatype, ()> { Ok(self.dtype) }
//     fn fail_borrow(&self) -> Result<&Datatype, ()> { Err(()) }
// }

impl<'a> Identifiable for Artifact<'a> {
    fn id(&self) -> &Identity {
        &self.id
    }
}

impl<'a> Hash for Artifact<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dtype.id.hash.hash(state);
        self.name.hash(state);
    }
}


// pub struct SingletonVersion<'a> {
//     pub artifact: Box<Artifact<'a>>,
//     pub version: Version<'a>,
//     // version: Version<'a>,
// }

// fn new_singleton_version<'a, T: DatatypeEnum>(
//     dtypes_registry: &'a DatatypesRegistry<T>,
//     artifact_id: Identity,
//     version_id: Identity,
//     datatype_name: &str,
// ) -> SingletonVersion<'a> {
//     let art = Box::new(Artifact {
//             id: artifact_id,
//             name: None,
//             dtype: dtypes_registry.get_datatype(datatype_name)
//                 .expect("Singleton datatypemissing from registry"),
//         });
//     // let art_ref: &Artifact = art.as_ref();
//     SingletonVersion {
//         artifact: art,
//         version: Version {
//             id: version_id,
//             artifact: art.as_ref(),
//             status: VersionStatus::Committed,
//             representation: DatatypeRepresentationKind::State,
//         }
//     }
// }


// rental! {
//     mod singleton {
//         use super::*;

//         /// A single artifact and version existing outsite of any graphs.
//         // #[rental(deref_suffix)]
//         #[rental]
//         pub struct SingletonVersion<'a> {
//             datatype: &'a Datatype,
//             artifact: Box<Artifact<'datatype>>,
//             version: Version<'datatype, 'artifact>,
//             // v_ref: &'version Version<'artifact>,
//         }
//     }
// }

// fn new_singleton_version<'a, T: DatatypeEnum>(
//     dtypes_registry: &'a DatatypesRegistry<T>,
//     artifact_id: Identity,
//     version_id: Identity,
//     datatype_name: &str,
// ) -> singleton::SingletonVersion<'a> {
//     singleton::SingletonVersion::new(
//         dtypes_registry.get_datatype(datatype_name)
//                 .expect("Singleton datatypemissing from registry"),
//         |dtype| Box::new(Artifact {
//             id: artifact_id,
//             name: None,
//             dtype: dtype,
//         }),
//         |art, _| Version {
//             id: version_id,
//             artifact: art,
//             status: VersionStatus::Committed,
//             representation: DatatypeRepresentationKind::State,
//         },
//         // |version, _| version,
//     )
// }

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ArtifactRelation {
    DtypeDepends(DatatypeRelation),
    ProducedFrom(String),
}


type VersionGraphIndexType = petgraph::graph::DefaultIx;
pub type VersionGraphIndex = petgraph::graph::NodeIndex<VersionGraphIndexType>;
pub type VersionGraphEdgeIndex = petgraph::graph::EdgeIndex<VersionGraphIndexType>;
pub struct VersionGraph<'a: 'b, 'b> {
    versions: daggy::Dag<Version<'a, 'b>, VersionRelation<'b>, VersionGraphIndexType>,
}
// TODO: should either use the below in most interfaces or make the above also have pruning.
pub type VersionSubgraph<'a, 'b> = daggy::Dag<VersionNode<'a, 'b>, VersionRelation<'b>, VersionGraphIndexType>;

impl<'a: 'b, 'b> VersionGraph<'a, 'b> {
    fn new() -> VersionGraph<'a, 'b> {
        VersionGraph {
            versions: daggy::Dag::new()
        }
    }

    fn new_from_source_artifacts(
        art_graph: &'b ArtifactGraph<'a>,
    ) -> VersionGraph<'a, 'b> {
        let mut versions = daggy::Dag::new();
        for node_idx in art_graph.artifacts.graph().externals(Direction::Incoming) {
            let art = &art_graph.artifacts[node_idx];
            versions.add_node(Version {
                id: Identity {uuid: Uuid::new_v4(), hash: 0},
                artifact: art,
                status: VersionStatus::Staging,
                representation: DatatypeRepresentationKind::State,
            });
        }

        VersionGraph {
            versions
        }
    }

    /// Find all versions of an artifact in this graph.
    fn artifact_versions(
        &self,
        artifact: &Artifact
    ) -> Vec<VersionGraphIndex> {
        self.versions.graph().node_indices()
            .filter(|&node_idx| {
                self.versions.node_weight(node_idx)
                    .expect("Impossible: indices from this graph")
                    .artifact.id == artifact.id
            })
            .collect()
    }

    // TODO: collecting result for quick prototyping. Should return mapped iter.
    fn get_related_versions(
        &self,
        v_idx: VersionGraphIndex,
        relation: &VersionRelation,
        dir: petgraph::Direction,
    ) -> Vec<VersionGraphIndex> {
        self.versions.graph().edges_directed(v_idx, dir)
            .filter(|e| e.weight() == relation)
            .map(|e| match dir {
                petgraph::Direction::Outgoing => e.target(),
                petgraph::Direction::Incoming => e.source(),
            })
            .collect()
    }

    fn get_partitioning(
        &self,
        v_idx: VersionGraphIndex
    ) -> Option<(VersionGraphIndex, &Version)> {
        let partitioning_art_relation = ArtifactRelation::DtypeDepends(DatatypeRelation {
                name: datatype::interface::PARTITIONING_RELATION_NAME.clone(),
            });
        let partitioning_relation = VersionRelation::Dependence(&partitioning_art_relation);
        self.get_related_versions(
                v_idx,
                &partitioning_relation,
                petgraph::Direction::Incoming)
            .iter()
            .next()
            .map(|p_idx| (*p_idx, &self.versions[*p_idx]))
    }
}

impl<'a: 'b, 'b: 's, 's>
IdentifiableGraph<
        's,
        Version<'a, 'b>,
        VersionRelation<'b>,
        VersionGraphIndexType>
for VersionGraph<'a, 'b> {
    fn graph(&self) -> &daggy::Dag<Version<'a, 'b>, VersionRelation<'b>, VersionGraphIndexType> {
        &self.versions
    }

    fn graph_mut(&mut self) -> &mut daggy::Dag<Version<'a, 'b>, VersionRelation<'b>, VersionGraphIndexType> {
        &mut self.versions
    }
}

#[derive(Debug, PartialEq)]
pub enum VersionRelation<'b>{
    Dependence(&'b ArtifactRelation),
    Parent,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "version_status")]
pub enum VersionStatus {
    #[postgres(name = "staging")]
    Staging,
    #[postgres(name = "committed")]
    Committed,
}

#[derive(Debug)]
pub enum VersionNode<'a: 'b, 'b> {
    Complete(Version<'a, 'b>),
    Pruned(Version<'a, 'b>),
}

#[derive(Debug)]
pub struct Version<'a: 'b, 'b> {
    id: Identity,
    artifact: &'b Artifact<'a>,
    status: VersionStatus,
    representation: DatatypeRepresentationKind,
}

impl<'a: 'b, 'b> Version<'a, 'b> {
    fn new(
        artifact: &'b Artifact<'a>,
        representation: DatatypeRepresentationKind,
    ) -> Self {
        Version {
            id: Identity {uuid: Uuid::new_v4(), hash: 0},
            artifact: artifact,
            status: VersionStatus::Staging,
            representation: representation,
        }
    }

    // fn new_singleton(
    //     artifact: &'a Artifact<'a>,
    //     uuid: Uuid,
    // ) -> Version<'a> {
    //     Version {
    //         id: Identity {
    //             uuid: uuid,
    //             hash: 0,
    //         },
    //         artifact: artifact,
    //         status: ::VersionStatus::Committed,
    //         representation: ::DatatypeRepresentationKind::State,
    //     }
    // }
}

impl<'a, 'b> Identifiable for Version<'a, 'b> {
    fn id(&self) -> &Identity {
        &self.id
    }
}

pub type PartitionIndex = u64;

#[derive(Clone, Debug)]
pub struct Partition<'a: 'b, 'b: 'c, 'c> {
    partitioning: &'c Version<'a, 'b>,
    index: PartitionIndex,
    // TODO: also need to be able to handle partition types (leaf v. neighborhood, level, arbitrary)
}

#[derive(Debug)]
pub enum PartCompletion {
    Complete,
    Ragged,
}

#[derive(Debug)]
pub struct Hunk<'a: 'b, 'b: 'c + 'd, 'c, 'd> {
    // Is this a Hunk or a Patch (in which case changeset items would be hunks)?
    id: Identity, // TODO: Not clear hunk needs a UUID.
    version: &'d Version<'a, 'b>,
    partition: Partition<'a, 'b, 'c>,
    completion: PartCompletion,
    // TODO: do hunks also need a DatatypeRepresentationKind?
}
