#![feature(associated_type_bounds)]
#![feature(never_type)]
#![feature(specialization)]
#![feature(todo_macro)]
#![feature(trivial_bounds)]
#![feature(vec_remove_item)]

#[macro_use]
pub extern crate heraclitus_core;

pub use heraclitus_core::*;

// Necessary for names to resolve when using heraclitus-macros within the
// heraclitus crate itself;
extern crate self as heraclitus;


use std::collections::{BTreeMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::{Index, IndexMut};

use daggy::Walker;
use petgraph::Direction;
use petgraph::visit::EdgeRef;
#[cfg(feature="backend-postgres")]
use postgres::to_sql_checked;
#[cfg(feature="backend-postgres")]
use postgres_derive::{ToSql, FromSql};
use serde_derive::{Serialize, Deserialize};
use uuid::Uuid;

use heraclitus_core::datatype::{DatatypeEnum, DatatypesRegistry};
use crate::datatype::artifact_graph::{
    ArtifactDescription,
    ArtifactGraphDescription,
};


#[macro_use]
pub mod datatype;
pub mod store;
mod util;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DatatypeRelation {
    pub name: String,
}

pub trait IdentifiableGraph {
    type N: Identifiable;
    type E;
    type IT: petgraph::csr::IndexType;

    fn graph(&self) -> &daggy::Dag<Self::N, Self::E, Self::IT>;

    fn graph_mut(&mut self) -> &mut daggy::Dag<Self::N, Self::E, Self::IT>;

    fn get_by_id<'b>(
        &'b self,
        id: &Identity
    ) -> Option<(petgraph::graph::NodeIndex<Self::IT>, &Self::N)> {
        for node_idx in self.graph().graph().node_indices() {
            let node = self.graph().node_weight(node_idx).expect("Graph is malformed");
            if node.id() == id {
                return Some((node_idx, node))
            }
        }

        None
    }

    fn get_by_uuid<'b>(
        &'b self,
        uuid: &Uuid
    ) -> Option<(petgraph::graph::NodeIndex<Self::IT>, &Self::N)> {
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
    ) -> petgraph::graph::NodeIndex<Self::IT>
            where F: FnOnce() -> Self::N {
        match self.get_by_id(id) {
            Some((idx, _)) => idx,
            None => self.graph_mut().add_node(constructor()),
        }
    }
}

type ArtifactGraphIndexType = petgraph::graph::DefaultIx;
pub type ArtifactGraphIndex = petgraph::graph::NodeIndex<ArtifactGraphIndexType>;
pub type ArtifactGraphEdgeIndex = petgraph::graph::EdgeIndex<ArtifactGraphIndexType>;
pub(crate) type ArtifactGraphType<'a> = daggy::Dag<Artifact<'a>, ArtifactRelation, ArtifactGraphIndexType>;
pub type ArtifactIndexMap = BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex>;

/// A graph expressing the dependence structure between sets of data artifacts.
pub struct ArtifactGraph<'a> {
    id: Identity, // Note: currently this hash is the hash of the **version**, not the AG artifact.
    pub artifacts: ArtifactGraphType<'a>,
}

impl<'a> ArtifactGraph<'a> {
    pub fn from_description<T: DatatypeEnum>(
        desc: &ArtifactGraphDescription,
        dtypes_registry: &'a DatatypesRegistry<T>,
    ) -> (ArtifactGraph<'a>, ArtifactIndexMap) {

        let to_visit = daggy::petgraph::algo::toposort(desc.artifacts.graph(), None)
            .expect("TODO: not a DAG");

        let mut ag = ArtifactGraph {
            id: 0.into(),
            artifacts: ArtifactGraphType::new(),
        };
        let mut idx_map = ArtifactIndexMap::new();
        let mut ag_hash = DefaultHasher::new();

        for node_idx in to_visit {
            let idx = ag.add_description_node(
                desc,
                dtypes_registry,
                &mut idx_map,
                node_idx);
            ag.artifacts[idx].id.hash.hash(&mut ag_hash);
        }

        ag.id.hash = ag_hash.finish();

        (ag, idx_map)
    }

    pub fn apply_delta<T: DatatypeEnum>(
        &mut self,
        delta: &crate::datatype::artifact_graph::ArtifactGraphDelta,
        dtypes_registry: &'a DatatypesRegistry<T>,
    ) -> ArtifactIndexMap {

        for art_uuid in delta.removals() {
            let (found_idx, _) = self.get_by_uuid(art_uuid).expect("TODO");
            self.artifacts.remove_node(found_idx);
        }

        let to_visit = daggy::petgraph::algo::toposort(delta.additions().artifacts.graph(), None)
            .expect("TODO: not a DAG");

        let mut idx_map = ArtifactIndexMap::new();

        for node_idx in to_visit {
            self.add_description_node(
                delta.additions(),
                dtypes_registry,
                &mut idx_map,
                node_idx);
        }

        self.id.hash = self.hash_current_state().expect("TODO: existing art hash is wrong");

        idx_map
    }

    fn add_description_node<T: DatatypeEnum>(
        &mut self,
        desc: &ArtifactGraphDescription,
        dtypes_registry: &'a DatatypesRegistry<T>,
        idx_map: &mut ArtifactIndexMap,
        node_idx: ArtifactGraphIndex,
    ) -> ArtifactGraphIndex {
        let mut s = DefaultHasher::new();

        // TODO: replace with petgraph neighbors
        // Order hashing based on hash, not ID, so that artifact content
        // hashes are ID-independent.
        let mut sorted_parent_hashes = desc.artifacts.parents(node_idx)
            .iter(&desc.artifacts)
            .map(|(_, p_idx)| {
                let new_p_idx = idx_map.get(&p_idx).expect("Graph is malformed.");
                self.artifacts.node_weight(*new_p_idx).expect("Graph is malformed.").id.hash
            })
            .collect::<Vec<HashType>>();
        sorted_parent_hashes.sort();
        for hash in &sorted_parent_hashes {
            hash.hash(&mut s);
        }

        let a_desc = desc.artifacts.node_weight(node_idx).expect("Graph is malformed.");
        let new_idx = match a_desc {
            ArtifactDescription::New { id, name, self_partitioning, dtype } => {
                let id_new = id.unwrap_or_else(|| 0.into());
                let artifact = {
                    let mut art = Artifact {
                        id: id_new,
                        name: name.clone(),
                        self_partitioning: *self_partitioning,
                        dtype: dtypes_registry.get_datatype(&*dtype).expect("Unknown datatype."),
                    };
                    art.hash(&mut s);
                    let new_hash = s.finish();
                    if id.is_some() {
                        // TODO: hash verification should return an error
                        assert_eq!(art.id.hash, new_hash, "ID mismatch for artifact: {:?}", art);
                    }
                    art.id.hash = new_hash;
                    art
                };

                self.artifacts.add_node(artifact)
            },
            ArtifactDescription::Existing(uuid) => {
                self.get_by_uuid(uuid).expect("TODO").0
            },
        };
        idx_map.insert(node_idx, new_idx);

        for (e_idx, p_idx) in desc.artifacts.parents(node_idx).iter(&desc.artifacts) {
            let edge = desc.artifacts.edge_weight(e_idx).expect("Graph is malformed.").clone();
            self.artifacts.add_edge(*idx_map.get(&p_idx).expect("Graph is malformed."), new_idx, edge)
                     .expect("Graph is malformed.");
        }

        new_idx
    }

    pub fn as_description(&self) -> ArtifactGraphDescription {
        let mut idx_map = ArtifactIndexMap::new();

        let mut desc = ArtifactGraphDescription::new();

        for node_idx in self.artifacts.graph().node_indices() {
            let art = &self.artifacts[node_idx];
            let desc_node_idx = desc.artifacts.add_node(ArtifactDescription::new_from_artifact(art));

            idx_map.insert(node_idx, desc_node_idx);
        }

        for edge in self.artifacts.graph().raw_edges() {
            let source = idx_map[&edge.source()];
            let target = idx_map[&edge.target()];

            desc.artifacts.add_edge(source, target, edge.weight.clone()).expect("TODO");
        }

        desc
    }

    /// Compute the overall state hash, or return None if any artifact's hash
    /// is incorrect.
    fn hash_current_state(&self) -> Option<HashType> {
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
                .collect::<Vec<HashType>>();
            sorted_parent_hashes.sort();
            for hash in &sorted_parent_hashes {
                hash.hash(&mut s);
            }

            let artifact = self.artifacts.node_weight(node_idx).expect("Graph is malformed.");
            artifact.hash(&mut s);
            if s.finish() != artifact.id.hash { return None; }
            artifact.id.hash.hash(&mut ag_hash);
        };

        Some(ag_hash.finish())
    }

    pub fn verify_hash(&self) -> bool {
        match self.hash_current_state() {
            Some(hash) => self.id.hash == hash,
            None => false,
        }
    }

    pub fn get_related_artifacts(
        &self,
        a_idx: ArtifactGraphIndex,
        relation: &ArtifactRelation,
        dir: petgraph::Direction,
    ) -> Vec<ArtifactGraphIndex> {
        self.artifacts.graph().edges_directed(a_idx, dir)
            .filter(|e| e.weight() == relation)
            .map(|e| match dir {
                petgraph::Direction::Outgoing => e.target(),
                petgraph::Direction::Incoming => e.source(),
            })
            .collect()
    }

    pub fn get_unary_partitioning(&self) -> Option<ArtifactGraphIndex> {
        // TODO: brittle
        self.artifacts.graph().node_indices()
            .find(|i| self.artifacts[*i].dtype.name == "UnaryPartitioning")
    }

    pub fn find_by_name(&self, name: &str) -> Option<ArtifactGraphIndex> {
        // TODO: brittle
        self.artifacts.graph().node_indices()
            .find(|i| self.artifacts[*i].name.as_ref().filter(|n| n.as_str() == name).is_some())
    }
}

impl<'a> IdentifiableGraph for ArtifactGraph<'a> {
    type N = Artifact<'a>;
    type E = ArtifactRelation;
    type IT = ArtifactGraphIndexType;

    fn graph(&self) -> &daggy::Dag<Self::N, Self::E, Self::IT> {
        &self.artifacts
    }

    fn graph_mut(&mut self) -> &mut daggy::Dag<Self::N, Self::E, Self::IT> {
        &mut self.artifacts
    }
}

// Annoyingly, cannot write these impl generically for IdentifiableGraph because
// of https://doc.rust-lang.org/error-index.html#E0210.
impl<'a> Index<ArtifactGraphIndex> for ArtifactGraph<'a>
{
    type Output = Artifact<'a>;

    fn index(&self, index: ArtifactGraphIndex) -> &Artifact<'a> {
        &self.graph()[index]
    }
}

impl<'a> IndexMut<ArtifactGraphIndex> for ArtifactGraph<'a>
{
    fn index_mut(&mut self, index: ArtifactGraphIndex) -> &mut Artifact<'a> {
        &mut self.graph_mut()[index]
    }
}

impl<'a> Index<ArtifactGraphEdgeIndex> for ArtifactGraph<'a>
{
    type Output = ArtifactRelation;

    fn index(&self, index: ArtifactGraphEdgeIndex) -> &ArtifactRelation {
        &self.graph()[index]
    }
}


/// An `Artifact` represents a collection of instances of a `Datatype` that can
/// exist in dependent relationships with other artifacts and producers.
#[derive(Debug)]
pub struct Artifact<'a> {
    pub id: Identity,
    /// Name identifier for this artifact. Can not start with '@'.
    name: Option<String>,
    pub dtype: &'a Datatype,
    /// Because partitioning is a relationship with special status, it is
    /// allowed to be self-cyclic for datatype artifacts if appropriate. For
    /// example, unary partitioning or a self-balancing point octree are both
    /// structures that may be self-partitioning. Rather than allow cyclic
    /// edges in the DAG for this special case, make it a property.
    pub self_partitioning: bool,
}

impl<'a> Artifact<'a> {
    pub fn name(&self) -> &Option<String> {
        &self.name
    }
}

impl<'a> Identifiable for Artifact<'a> {
    fn id(&self) -> &Identity {
        &self.id
    }
}

impl<'a> Hash for Artifact<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: is there a reason I had this hash the id directly, rather than
        // use the dtype hash? If there is, it should be commented!
        self.dtype.id().hash.hash(state);
        self.name.hash(state);
        self.self_partitioning.hash(state);
    }
}

/// Note: relations in heraclitus are directed from the dependency to the dependent.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ArtifactRelation {
    DtypeDepends(DatatypeRelation),
    ProducedFrom(String),
}


type VersionGraphIndexType = petgraph::graph::DefaultIx;
pub type VersionGraphIndex = petgraph::graph::NodeIndex<VersionGraphIndexType>;
pub type VersionGraphEdgeIndex = petgraph::graph::EdgeIndex<VersionGraphIndexType>;
pub struct VersionGraph<'a: 'b, 'b> {
    pub versions: daggy::Dag<Version<'a, 'b>, VersionRelation<'b>, VersionGraphIndexType>,
}
// TODO: should either use the below in most interfaces or make the above also have pruning.
pub type VersionSubgraph<'a, 'b> = daggy::Dag<VersionNode<'a, 'b>, VersionRelation<'b>, VersionGraphIndexType>;

impl<'a: 'b, 'b> VersionGraph<'a, 'b> {
    pub fn new() -> VersionGraph<'a, 'b> {
        VersionGraph {
            versions: daggy::Dag::new()
        }
    }

    pub fn new_from_source_artifacts(
        art_graph: &'b ArtifactGraph<'a>,
    ) -> VersionGraph<'a, 'b> {
        let mut versions = daggy::Dag::new();
        for node_idx in art_graph.artifacts.graph().externals(Direction::Incoming) {
            let art = &art_graph.artifacts[node_idx];
            versions.add_node(Version::new(art, RepresentationKind::State));
        }

        VersionGraph {
            versions
        }
    }

    /// Find all versions of an artifact in this graph.
    pub fn artifact_versions(
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

    pub fn artifact_tips(
        &self,
        artifact: &Artifact
    ) -> Vec<VersionGraphIndex> {
        self.artifact_versions(artifact).into_iter()
            .filter(|&node_idx| {
                !self.versions.graph()
                    .edges_directed(node_idx, petgraph::Direction::Outgoing)
                    .any(|e| e.weight() == &VersionRelation::Parent)
            })
            .collect()
    }

    // TODO: collecting result for quick prototyping. Should return mapped iter.
    pub fn get_related_versions(
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

    pub fn get_parents(
        &self,
        v_idx: VersionGraphIndex,
    ) -> Vec<VersionGraphIndex> {
        self.get_related_versions(
            v_idx,
            &VersionRelation::Parent,
            petgraph::Direction::Incoming)
    }

    pub fn new_child(
        &mut self,
        parent_idx: VersionGraphIndex,
        representation: RepresentationKind,
    ) -> VersionGraphIndex {
        let parent = &self.versions[parent_idx];
        let child = Version::new(parent.artifact, representation);
        let child_idx = self.versions.add_node(child);
        self.versions.add_edge(parent_idx, child_idx, VersionRelation::Parent).expect("TODO");

        child_idx
    }

    pub fn get_partitioning(
        &self,
        v_idx: VersionGraphIndex
    ) -> Option<(VersionGraphIndex, &Version)> {
        if self.versions[v_idx].artifact.self_partitioning {
            return Some((v_idx, &self.versions[v_idx]));
        }

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

impl<'a: 'b, 'b> IdentifiableGraph for VersionGraph<'a, 'b> {
    type N = Version<'a, 'b>;
    type E = VersionRelation<'b>;
    type IT = VersionGraphIndexType;

    fn graph(&self) -> &daggy::Dag<Self::N, Self::E, Self::IT> {
        &self.versions
    }

    fn graph_mut(&mut self) -> &mut daggy::Dag<Self::N, Self::E, Self::IT> {
        &mut self.versions
    }
}

impl<'a, 'b> Index<VersionGraphIndex> for VersionGraph<'a, 'b>
{
    type Output = Version<'a, 'b>;

    fn index(&self, index: VersionGraphIndex) -> &Version<'a, 'b> {
        &self.graph()[index]
    }
}

impl<'a, 'b> IndexMut<VersionGraphIndex> for VersionGraph<'a, 'b>
{
    fn index_mut(&mut self, index: VersionGraphIndex) -> &mut Version<'a, 'b> {
        &mut self.graph_mut()[index]
    }
}

impl<'a, 'b> Index<VersionGraphEdgeIndex> for VersionGraph<'a, 'b>
{
    type Output = VersionRelation<'b>;

    fn index(&self, index: VersionGraphEdgeIndex) -> &VersionRelation<'b> {
        &self.graph()[index]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionRelation<'b>{
    /// The target version is dependent on the source version congruent with
    /// a artifact dependence relationship.
    Dependence(&'b ArtifactRelation),
    /// The target version is a child version of the source version.
    Parent,
    // /// The target version must walk the ancestry tree back to the source
    // /// version to materialize a complete state.
    // /// *Note:* The materialized state may still contain sparse or ragged
    // /// `PartitionCompletion`.
    // SufficientAncestor,
}

#[derive(Debug)]
#[cfg_attr(feature="backend-postgres", derive(ToSql, FromSql))]
#[cfg_attr(feature="backend-postgres", postgres(name = "version_status"))]
pub enum VersionStatus {
    #[cfg_attr(feature="backend-postgres", postgres(name = "staging"))]
    Staging,
    #[cfg_attr(feature="backend-postgres", postgres(name = "committed"))]
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
    pub artifact: &'b Artifact<'a>,
    status: VersionStatus,
    representation: RepresentationKind,
}

impl<'a: 'b, 'b> Version<'a, 'b> {
    pub fn new(
        artifact: &'b Artifact<'a>,
        representation: RepresentationKind,
    ) -> Self {
        Version {
            id: Identity {uuid: Uuid::new_v4(), hash: 0},
            artifact,
            status: VersionStatus::Staging,
            representation,
        }
    }
}

impl<'a, 'b> Identifiable for Version<'a, 'b> {
    fn id(&self) -> &Identity {
        &self.id
    }
}

pub type PartitionIndex = u64;

#[derive(Clone, Debug)]
pub struct Partition<'a: 'b, 'b: 'c, 'c> {
    pub partitioning: &'c Version<'a, 'b>,
    pub index: PartitionIndex,
    // TODO: also need to be able to handle partition types (leaf v. neighborhood, level, arbitrary)
}

#[derive(Debug)]
#[cfg_attr(feature="backend-postgres", derive(ToSql, FromSql))]
#[cfg_attr(feature="backend-postgres", postgres(name = "part_completion"))]
pub enum PartCompletion {
    #[cfg_attr(feature="backend-postgres", postgres(name = "complete"))]
    Complete,
    #[cfg_attr(feature="backend-postgres", postgres(name = "ragged"))]
    Ragged,
}

/// Metadata for a version's data for a particular partition.
///
/// The absence of a hunk for a partition for a version indicates the version
/// made no changes relative to its parent versions' hunks for that partition.
#[derive(Debug)]
pub struct Hunk<'a: 'b, 'b: 'c + 'd, 'c, 'd> {
    // Is this a Hunk or a Patch (in which case changeset items would be hunks)?
    pub id: Identity, // TODO: Not clear hunk needs a UUID.
    pub version: &'d Version<'a, 'b>,
    pub partition: Partition<'a, 'b, 'c>,
    /// Representation kind of this hunk's contents. `State` versions may
    /// contains only `State` hunks, `CumulativeDelta` versions may contain either
    /// `State` or `CumulativeDelta` hunks, and 'Delta' versions may contain
    /// combinations of any hunk representations.
    pub representation: RepresentationKind,
    pub completion: PartCompletion,
    /// Indicates for a merge version which ancestral version's hunk takes
    /// precedence.
    pub precedence: Option<Uuid>,
}

impl<'a: 'b, 'b: 'c + 'd, 'c, 'd> Hunk<'a, 'b, 'c, 'd> {
    /// Check that local properties of this hunk are consistent with the model
    /// constraints. Note that this does *not* verify data contained by this
    /// hunk, graph-level constraints, or consistency with the stored
    /// representation.
    fn is_valid(&self) -> bool {
        (match self.version.representation {
            RepresentationKind::State => self.representation == RepresentationKind::State,
            RepresentationKind::CumulativeDelta =>
                self.representation != RepresentationKind::Delta,
            RepresentationKind::Delta => true,
        })
        &&
        (match self.precedence {
            // State hunks should not need precedence.
            Some(_) => self.version.representation != RepresentationKind::State,
            None => true
        })
    }
}

// /// Indicates for a merge version which ancestral hunk takes precedence.
// pub struct HunkPrecedence<'a: 'b, 'b: 'c + 'd + 'e, 'c, 'd, 'e> {
//     merge_version: &'e Version<'a, 'b>,
//     preceding_version: &'d Version<'a, 'b>,
//     partition: Partition<'a, 'b, 'c>,
// }

/// A sequence of hunks for a partition sufficient to compose state for that
/// partition's data.
pub type Composition<'a, 'b, 'c, 'd> = Vec<Hunk<'a, 'b, 'c, 'd>>;

/// A mapping of compositions for a set of partitions.
pub type CompositionMap<'a, 'b, 'c, 'd> = BTreeMap<PartitionIndex, Composition<'a, 'b, 'c, 'd>>;
