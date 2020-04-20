#![feature(associated_type_bounds)]
#![feature(never_type)]
#![feature(specialization)]
#![feature(todo_macro)]
#![feature(trivial_bounds)]
#![feature(vec_remove_item)]

// Inject mermaid into doc in a way that works with both `rust doc` and docs.rs.
// Adapted from: https://docs.rs/crate/horrible-katex-hack/
#![doc(html_favicon_url = r#"
">
<script defer
    src="https://cdn.jsdelivr.net/npm/mermaid@8.5.0/dist/mermaid.min.js"
    integrity="sha256-bTMqpr7baOlzavIdddfmnQZsEBdfnK5p6KG8FcrwwD8="
    crossorigin="anonymous">
</script>
<script>
document.addEventListener("DOMContentLoaded", function () {
    let to_do = [];
    for (let e of document.querySelectorAll("code.language-mermaid")) {
        let x = document.createElement("p");
        x.innerHTML = e.innerHTML;
        x.classList.add("mermaid");
        e.parentNode.parentNode.replaceChild(x, e.parentNode);
    }
    mermaid.initialize({startOnLoad:true});
});
</script>
" // Errant character to fix broken rls-vscode parsing of raw string literals in attributes.
"#)]

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
pub(crate) type ArtifactGraphType = daggy::Dag<Artifact, ArtifactRelation, ArtifactGraphIndexType>;
pub type ArtifactIndexMap = BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex>;

/// A graph expressing the dependence structure between sets of data artifacts.
pub struct ArtifactGraph {
    id: Identity, // Note: currently this hash is the hash of the **version**, not the AG artifact.
                  // The UUID is usually the UUID of the **hunk** containing this AG.
    pub artifacts: ArtifactGraphType,
}

impl ArtifactGraph {
    pub fn from_description<T: DatatypeEnum>(
        desc: &ArtifactGraphDescription,
        dtypes_registry: &DatatypesRegistry<T>,
        uuid: Option<Uuid>,
    ) -> (ArtifactGraph, ArtifactIndexMap) {

        let to_visit = daggy::petgraph::algo::toposort(desc.artifacts.graph(), None)
            .expect("TODO: not a DAG");

        let mut ag = ArtifactGraph {
            id: Identity {
                uuid: uuid.unwrap_or_else(Uuid::new_v4),
                hash: 0,
            },
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
        dtypes_registry: &DatatypesRegistry<T>,
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
        dtypes_registry: &DatatypesRegistry<T>,
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
                let id_new: Identity = id.map(|i| i.into()).unwrap_or_else(|| 0.into());
                let artifact = {
                    let mut art = Artifact {
                        id: id_new,
                        name: name.clone(),
                        self_partitioning: *self_partitioning,
                        dtype_uuid: dtypes_registry.get_datatype(&*dtype).expect("Unknown datatype.")
                            .id().uuid,
                    };
                    art.hash(&mut s);
                    let new_hash = s.finish();
                    if let Some(PartialIdentity {hash: Some(expected_hash), ..}) = id {
                        // TODO: hash verification should return an error
                        assert_eq!(*expected_hash, new_hash, "ID mismatch for artifact: {:?}", art);
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

    pub fn as_description<T: DatatypeEnum>(
        &self,
        dtypes_registry: &DatatypesRegistry<T>,
    ) -> ArtifactGraphDescription {
        let mut idx_map = ArtifactIndexMap::new();

        let mut desc = ArtifactGraphDescription::new();

        for node_idx in self.artifacts.graph().node_indices() {
            let art = &self.artifacts[node_idx];
            let desc_node_idx = desc.artifacts.add_node(
                ArtifactDescription::new_from_artifact(art, dtypes_registry));

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

    pub fn get_neighbors(
        &self,
        a_idx: ArtifactGraphIndex,
        dir: petgraph::Direction,
    ) -> impl Iterator<Item=ArtifactGraphIndex> + '_ {
        self.artifacts.graph().edges_directed(a_idx, dir)
            .map(move |e| match dir {
                petgraph::Direction::Outgoing => e.target(),
                petgraph::Direction::Incoming => e.source(),
            })
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
        use crate::datatype::DatatypeMeta;
        // TODO: brittle
        self.artifacts.graph().node_indices()
            .find(|i| self.artifacts[*i].dtype_uuid ==
                crate::datatype::partitioning::UnaryPartitioning::uuid())
    }

    pub fn find_by_name(&self, name: &str) -> Option<ArtifactGraphIndex> {
        // TODO: brittle
        self.artifacts.graph().node_indices()
            .find(|i| self.artifacts[*i].name.as_ref().filter(|n| n.as_str() == name).is_some())
    }
}

impl IdentifiableGraph for ArtifactGraph {
    type N = Artifact;
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
impl Index<ArtifactGraphIndex> for ArtifactGraph
{
    type Output = Artifact;

    fn index(&self, index: ArtifactGraphIndex) -> &Artifact {
        &self.graph()[index]
    }
}

impl IndexMut<ArtifactGraphIndex> for ArtifactGraph
{
    fn index_mut(&mut self, index: ArtifactGraphIndex) -> &mut Artifact {
        &mut self.graph_mut()[index]
    }
}

impl Index<ArtifactGraphEdgeIndex> for ArtifactGraph
{
    type Output = ArtifactRelation;

    fn index(&self, index: ArtifactGraphEdgeIndex) -> &ArtifactRelation {
        &self.graph()[index]
    }
}


/// An `Artifact` represents a collection of instances of a `Datatype` that can
/// exist in dependent relationships with other artifacts and producers.
#[derive(Debug)]
pub struct Artifact {
    pub id: Identity,
    /// Name identifier for this artifact. Can not start with '@'.
    name: Option<String>,
    pub dtype_uuid: Uuid,
    /// Because partitioning is a relationship with special status, it is
    /// allowed to be self-cyclic for datatype artifacts if appropriate. For
    /// example, unary partitioning or a self-balancing point octree are both
    /// structures that may be self-partitioning. Rather than allow cyclic
    /// edges in the DAG for this special case, make it a property.
    pub self_partitioning: bool,
}

impl Artifact {
    pub fn name(&self) -> &Option<String> {
        &self.name
    }
}

impl Identifiable for Artifact {
    fn id(&self) -> &Identity {
        &self.id
    }
}

impl Hash for Artifact {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dtype_uuid.hash(state);
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
pub struct VersionGraph<'ag> {
    pub versions: daggy::Dag<Version<'ag>, VersionRelation<'ag>, VersionGraphIndexType>,
}
// TODO: should either use the below in most interfaces or make the above also have pruning.
pub type VersionSubgraph<'ag> = daggy::Dag<VersionNode<'ag>, VersionRelation<'ag>, VersionGraphIndexType>;

impl<'ag> VersionGraph<'ag> {
    pub fn new() -> VersionGraph<'ag> {
        VersionGraph {
            versions: daggy::Dag::new()
        }
    }

    pub fn new_from_source_artifacts(
        art_graph: &'ag ArtifactGraph,
    ) -> VersionGraph<'ag> {
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

    pub fn new_child_same_dependencies(
        &mut self,
        parent_idx: VersionGraphIndex,
        representation: RepresentationKind,
    ) -> VersionGraphIndex {
        let parent = &self.versions[parent_idx];
        let child = Version::new(parent.artifact, representation);
        let child_idx = self.versions.add_node(child);

        let to_add: Vec<_> = self.versions.graph()
            .edges_directed(parent_idx, petgraph::Direction::Incoming)
            .filter(|e| match e.weight() {
                VersionRelation::Dependence(_) => true,
                _ => false,
            })
            .map(|e| (e.source(), e.weight().clone()))
            .collect();
        to_add.into_iter()
            .for_each(|(source, weight)| {
                self.versions.add_edge(source, child_idx, weight).expect("TODO");
            });

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
                name: datatype::interface::PARTITIONING_RELATION_NAME.to_owned(),
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

impl<'ag> IdentifiableGraph for VersionGraph<'ag> {
    type N = Version<'ag>;
    type E = VersionRelation<'ag>;
    type IT = VersionGraphIndexType;

    fn graph(&self) -> &daggy::Dag<Self::N, Self::E, Self::IT> {
        &self.versions
    }

    fn graph_mut(&mut self) -> &mut daggy::Dag<Self::N, Self::E, Self::IT> {
        &mut self.versions
    }
}

impl<'ag> Index<VersionGraphIndex> for VersionGraph<'ag>
{
    type Output = Version<'ag>;

    fn index(&self, index: VersionGraphIndex) -> &Version<'ag> {
        &self.graph()[index]
    }
}

impl<'ag> IndexMut<VersionGraphIndex> for VersionGraph<'ag>
{
    fn index_mut(&mut self, index: VersionGraphIndex) -> &mut Version<'ag> {
        &mut self.graph_mut()[index]
    }
}

impl<'ag> Index<VersionGraphEdgeIndex> for VersionGraph<'ag>
{
    type Output = VersionRelation<'ag>;

    fn index(&self, index: VersionGraphEdgeIndex) -> &VersionRelation<'ag> {
        &self.graph()[index]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VersionRelation<'ag>{
    /// The target version is dependent on the source version congruent with
    /// a artifact dependence relationship.
    Dependence(&'ag ArtifactRelation),
    /// The target version is a child version of the source version.
    Parent,
    // /// The target version must walk the ancestry tree back to the source
    // /// version to materialize a complete state.
    // /// *Note:* The materialized state may still contain sparse or ragged
    // /// `PartitionCompletion`.
    // SufficientAncestor,
}

#[derive(Clone, Debug)]
#[derive(Deserialize, Serialize)]
#[cfg_attr(feature="backend-postgres", derive(ToSql, FromSql))]
#[cfg_attr(feature="backend-postgres", postgres(name = "version_status"))]
pub enum VersionStatus {
    #[cfg_attr(feature="backend-postgres", postgres(name = "staging"))]
    Staging,
    #[cfg_attr(feature="backend-postgres", postgres(name = "committed"))]
    Committed,
}

#[derive(Debug)]
pub enum VersionNode<'ag> {
    Complete(Version<'ag>),
    Pruned(Version<'ag>),
}

#[derive(Debug)]
pub struct Version<'ag> {
    id: Identity,
    pub artifact: &'ag Artifact,
    status: VersionStatus,
    representation: RepresentationKind,
}

impl<'ag> Version<'ag> {
    pub fn new(
        artifact: &'ag Artifact,
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

impl<'ag> Identifiable for Version<'ag> {
    fn id(&self) -> &Identity {
        &self.id
    }
}

pub type PartitionIndex = u64;

#[derive(Clone, Debug)]
#[derive(Serialize)]
pub struct Partition<'ag: 'vg, 'vg> {
    #[serde(skip_serializing)]
    pub partitioning: &'vg Version<'ag>,
    pub index: PartitionIndex,
    // TODO: also need to be able to handle partition types (leaf v. neighborhood, level, arbitrary)
}

#[derive(Debug)]
#[derive(Deserialize, Serialize)]
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
#[derive(Serialize)]
pub struct Hunk<'ag: 'vg1 + 'vg2, 'vg1, 'vg2> {
    // Is this a Hunk or a Patch (in which case changeset items would be hunks)?
    pub id: Identity, // TODO: Not clear hunk needs a UUID.
    #[serde(skip_serializing)]
    pub version: &'vg2 Version<'ag>,
    pub partition: Partition<'ag, 'vg1>,
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

impl<'ag: 'vg1 + 'vg2, 'vg1, 'vg2> Hunk<'ag, 'vg1, 'vg2> {
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

    fn uuid_spec(&self) -> HunkUuidSpec {
        HunkUuidSpec {
            artifact_uuid: self.version.artifact.id.uuid,
            version_uuid: self.version.id.uuid,
            hunk_uuid: self.id.uuid,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct HunkUuidSpec {
    pub artifact_uuid: Uuid,
    pub version_uuid: Uuid,
    pub hunk_uuid: Uuid,
}

// /// Indicates for a merge version which ancestral hunk takes precedence.
// pub struct HunkPrecedence<'a: 'b, 'b: 'c + 'd + 'e, 'c, 'd, 'e> {
//     merge_version: &'e Version<'a, 'b>,
//     preceding_version: &'d Version<'a, 'b>,
//     partition: Partition<'a, 'b, 'c>,
// }

/// A sequence of hunks for a partition sufficient to compose state for that
/// partition's data.
pub type Composition<'ag, 'vg1, 'vg2> = Vec<Hunk<'ag, 'vg1, 'vg2>>;

/// A mapping of compositions for a set of partitions.
pub type CompositionMap<'ag, 'vg1, 'vg2> = BTreeMap<PartitionIndex, Composition<'ag, 'vg1, 'vg2>>;
