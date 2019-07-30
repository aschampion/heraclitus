use std::collections::{
    BTreeSet,
    BTreeMap,
    HashMap,
    HashSet,
};
use std::hash::Hash;

use heraclitus_core::{
    daggy,
    enum_set,
    petgraph,
    uuid,
};
use daggy::{
    Walker,
};
use heraclitus_macros::{
    DatatypeMarker,
    interface,
    stored_datatype_controller,
    stored_interface_controller,
};
use enum_set::{
    EnumSet,
};
use serde_derive::{Deserialize, Serialize};
use uuid::{
    Uuid,
};

use crate::{
    Artifact,
    ArtifactGraph,
    ArtifactGraphIndex,
    ArtifactIndexMap,
    ArtifactRelation,
    CompositionMap,
    DatatypeRelation,
    RepresentationKind,
    Error,
    Hunk,
    Identity,
    IdentifiableGraph,
    Interface,
    ModelError,
    PartCompletion,
    Partition,
    PartitionIndex,
    Version,
    VersionGraph,
    VersionGraphIndex,
    VersionRelation,
    VersionStatus,
};
use crate::datatype::{
    ComposableState,
    partitioning::Partitioning,
    Payload,
};
use super::{
    DatatypeEnum,
    DatatypesRegistry,
    Description,
    InterfaceController,
    InterfaceDescription,
};
use crate::datatype::interface::{
    CustomProductionPolicyController,
    ProducerController,
    ProductionOutput,
};
use crate::repo::Repository;


pub mod production;
use self::production::*;

#[cfg(test)]
mod tests;


lazy_static! {
    pub static ref INTERFACE_ARTIFACT_META_DESC: InterfaceDescription = InterfaceDescription {
        interface: Interface {
            name: "ArtifactMeta",
        },
        extends: HashSet::new(),
    };
}

#[interface]
#[stored_interface_controller]
pub trait ArtifactMeta {
    /// This allows the model controller to initialize any structures necessary
    /// for a new version (without involving state for that version).
    fn init_artifact(
        &mut self,
        _repo: &Repository,
        _artifact: &Artifact,
    ) -> Result<(), Error> {
        Ok(())
    }
}


#[derive(Default, DatatypeMarker)]
pub struct ArtifactGraphDtype;

impl<T: InterfaceController<ArtifactMeta>> super::Model<T> for ArtifactGraphDtype {
    fn info(&self) -> Description<T> {
        Description {
            name: "ArtifactGraph".into(),
            version: 1,
            representations: vec![
                        RepresentationKind::State,
                        RepresentationKind::Delta,
                    ]
                    .into_iter()
                    .collect(),
            implements: vec![], // TODO: should artifact graph be an interface?
            dependencies: vec![],
        }
    }

    datatype_controllers!(ArtifactGraphDtype, ());
}


impl crate::datatype::ComposableState for ArtifactGraphDtype {
    type StateType = ArtifactGraphDescription;
    type DeltaType = ArtifactGraphDelta;

    fn compose_state(
        state: &mut Self::StateType,
        delta: &Self::DeltaType,
    ) {
        state.compose(delta).expect("TODO");
    }
}

struct OriginGraphTemplate<'d> {
    artifact_graph: ArtifactGraph<'d>,
    origin_idx: ArtifactGraphIndex,
    root_idx: ArtifactGraphIndex,
    up_idx: ArtifactGraphIndex,
}

/// An origin artifact graph which contains:
/// - Unary partitioning
/// - Recursive AG artifact itself
/// - Root AG artifact
impl<'d> OriginGraphTemplate<'d> {
    fn new<T: DatatypeEnum>(dtypes_registry: &'d DatatypesRegistry<T>) -> OriginGraphTemplate<'d> {
        let mut origin_ag = ArtifactGraphDescriptionType::new();

        let origin_art = ArtifactDescription::New {
            id: None,
            name: Some("origin".into()),
            dtype: "ArtifactGraph".into(),
            self_partitioning: false,
        };
        let root_art = ArtifactDescription::New {
            id: None,
            name: Some("root".into()),
            dtype: "ArtifactGraph".into(),
            self_partitioning: false,
        };
        let origin_desc_idx = origin_ag.add_node(origin_art);
        let root_desc_idx = origin_ag.add_node(root_art);
        let mut origin_ag_desc = ArtifactGraphDescription {
            artifacts: origin_ag,
        };
        let up_desc_idx = origin_ag_desc.add_unary_partitioning();

        // TODO: having to do this cycle because of inconsistency of hash between
        // AG and AG description.
        let (origin_ag, idx_map) = ArtifactGraph::from_description(
            &origin_ag_desc,
            dtypes_registry);

        OriginGraphTemplate {
            artifact_graph: origin_ag,
            origin_idx: idx_map[&origin_desc_idx],
            root_idx: idx_map[&root_desc_idx],
            up_idx: idx_map[&up_desc_idx],
        }
    }

    fn origin(&self) -> &Artifact {
        &self.artifact_graph[self.origin_idx]
    }

    fn root(&self) -> &Artifact {
        &self.artifact_graph[self.root_idx]
    }

    fn up(&self) -> &Artifact {
        &self.artifact_graph[self.up_idx]
    }
}

#[stored_datatype_controller(ArtifactGraphDtype)]
pub trait Storage: crate::datatype::Storage<StateType = ArtifactGraphDescription, DeltaType = ArtifactGraphDelta> {
    fn get_or_create_origin_root<'d, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &'d DatatypesRegistry<T>,
        repo: &Repository,
    ) -> Result<(ArtifactGraph<'d>, ArtifactGraph<'d>), Error> {
        let origin_hunk_uuid = match self.read_origin_hunk_uuid(repo)? {
            Some(uuid) => uuid,
            None => {
                self.create_origin_root(dtypes_registry, repo)?.uuid
            }
        };

        let origin_ag = self.read_origin_artifact_graph(dtypes_registry, repo, origin_hunk_uuid)?;
        // TODO: many temporary hacks
        // - using names instead of refs
        // - assuming unary root versions
        // - assuming tip of root versions
        let root_art_idx = origin_ag.find_by_name("root").expect("TODO: malformed origin AG");

        let origin_vg = self.get_version_graph(repo, &origin_ag)?;
        let root_tip_v_idx = origin_vg.artifact_tips(&origin_ag[root_art_idx])[0];

        let root_ag = self.get_artifact_graph(dtypes_registry, repo, &origin_vg, root_tip_v_idx)?;

        Ok((origin_ag, root_ag))
    }

    fn read_origin_artifact_graph<'d, T: DatatypeEnum>(
        &self,
        dtypes_registry: &'d DatatypesRegistry<T>,
        repo: &Repository,
        origin_hunk_uuid: Uuid,
    ) -> Result<ArtifactGraph<'d>, Error> {

        let fake_origin = OriginGraphTemplate::new(dtypes_registry);
        let fake_vg = VersionGraph::new_from_source_artifacts(&fake_origin.artifact_graph);
        let fake_origin_version = Version::new(fake_origin.origin(), RepresentationKind::State);
        let fake_origin_hunk = Hunk {
            id: Identity {uuid: origin_hunk_uuid, hash: 0},
            version: &fake_origin_version,
            partition: Partition {
                partitioning: &fake_vg[fake_vg.artifact_versions(fake_origin.up())[0]],
                index: crate::datatype::partitioning::UNARY_PARTITION_INDEX,
            },
            representation: RepresentationKind::State,
            completion: PartCompletion::Complete,
            precedence: None,
        };
        let comp = vec![fake_origin_hunk];
        let ag_desc = self.get_composite_state(repo, &comp)?;

        let (origin_ag, _) = ArtifactGraph::from_description(&ag_desc, dtypes_registry);

        Ok(origin_ag)
    }

    fn read_origin_hunk_uuid(
        &self,
        repo: &Repository,
    ) -> Result<Option<Uuid>, Error>;

    fn create_origin_root<'a, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &'a DatatypesRegistry<T>,
        repo: &Repository,
    ) -> Result<Identity, Error> {

        let origin = OriginGraphTemplate::new(dtypes_registry);
        let origin_art_id = origin.origin().id;

        // Create version graph for the origin AG.
        let mut ver_graph = VersionGraph::new_from_source_artifacts(&origin.artifact_graph);
        ver_graph.versions.node_weights_mut()
            .for_each(|n| n.status = VersionStatus::Committed);
        let up_ver_idx = ver_graph.artifact_versions(origin.up())[0];
        let part_id = crate::datatype::partitioning::UnaryPartitioningState
            .get_partition_ids().iter().cloned().nth(0).unwrap();

        // Create origin version.
        let (origin_ver_idx, origin_hunk_id) = {
            let mut origin_version = Version::new(
                origin.origin(),
                RepresentationKind::State);
            origin_version.status = VersionStatus::Committed;
            let origin_ver_idx = ver_graph.versions.add_node(origin_version);
            let up_origin_art_edge = origin.artifact_graph.artifacts.find_edge(
                origin.up_idx, origin.origin_idx).unwrap();
            ver_graph.versions.add_edge(up_ver_idx, origin_ver_idx,
                VersionRelation::Dependence(&origin.artifact_graph[up_origin_art_edge])).unwrap();

            // Create origin hunk.
            let origin_ag_complete = origin.artifact_graph.as_description();
            let complete_payload = Payload::State(origin_ag_complete);
            let origin_hunk = Hunk {
                id: ArtifactGraphDtype::hash_payload(&complete_payload).into(),
                version: &ver_graph[origin_ver_idx],
                partition: Partition {
                    partitioning: &ver_graph[up_ver_idx],
                    index: part_id,
                },
                representation: RepresentationKind::State,
                completion: PartCompletion::Complete,
                precedence: None,
            };

            self.bootstrap_origin(repo, &origin_hunk)?;

            // Adjust AG descrition so that origin artifact is existing reference.
            // HACK: Not only is this delta-like descript graph being used as a
            // state, it is not a valid delta either as the partitioning artifact
            // will have a relation with this existing origin artifact.
            let mut origin_ag_delta = origin.artifact_graph.as_description();
            let origin_art_idx = origin_ag_delta.get_by_uuid(&origin_art_id.uuid).unwrap().0;
            origin_ag_delta.artifacts[origin_art_idx] = ArtifactDescription::Existing(origin_art_id.uuid);
            let payload = Payload::State(origin_ag_delta);

            // Write AG description hunk.
            // HACK: This is violating many contracts, including that a state
            // hunk has a reference to an existing artifact.
            self.write_hunk(repo, &origin_hunk, &payload)?;

            (origin_ver_idx, origin_hunk.id)
        };

        // Write out remaining AG versions and hunks.
        // This should only be the UP version:
        self.create_staging_version(
            &repo,
            &ver_graph,
            up_ver_idx)?;
        for part_id in crate::datatype::partitioning::UnaryPartitioningState.get_partition_ids() {
            let hunk = Hunk {
                id: 0.into(),
                version: &ver_graph[up_ver_idx],
                partition: Partition {
                    partitioning: &ver_graph[up_ver_idx],
                    index: part_id,
                },
                representation: RepresentationKind::State,
                completion: PartCompletion::Complete,
                precedence: None,
            };
            self.create_hunk(repo, &hunk)?;
        }

        let mut root_ag_ver = Version::new(
            origin.root(),
            RepresentationKind::State);
        root_ag_ver.status = VersionStatus::Committed;
        let root_ag_ver_idx = ver_graph.versions.add_node(root_ag_ver);
        let up_root_art_edge = origin.artifact_graph.artifacts.find_edge(
            origin.up_idx, origin.root_idx).unwrap();
        ver_graph.versions.add_edge(up_ver_idx, root_ag_ver_idx,
            VersionRelation::Dependence(&origin.artifact_graph[up_root_art_edge])).unwrap();
        self.create_staging_version(
            repo,
            &ver_graph,
            root_ag_ver_idx.clone())?;

        // Empty root AG.
        let (root_ag, _) = ArtifactGraph::from_description(
            &ArtifactGraphDescription::new(),
            dtypes_registry);
        let root_ag_payload = Payload::State(root_ag.as_description());
        let root_ag_hunk = Hunk {
            id: ArtifactGraphDtype::hash_payload(&root_ag_payload).into(),
            version: &ver_graph[root_ag_ver_idx],
            partition: Partition {
                partitioning: &ver_graph[up_ver_idx],
                index: part_id,
            },
            representation: RepresentationKind::State,
            completion: PartCompletion::Complete,
            precedence: None,
        };
        self.create_hunk(repo, &root_ag_hunk)?;
        self.write_hunk(repo, &root_ag_hunk, &root_ag_payload)?;

        // Do any final cleanup necessary, e.g., adding version relations from
        // partitioning to origin artifact and marking commits.
        self.tie_off_origin(repo, &ver_graph, origin_ver_idx)?;

        Ok(origin_hunk_id)
    }

    /// Given an origin hunk, create a reference self-loop such that the
    /// hunk's artifact is in the graph specified by the hunk.
    ///
    /// # Warning
    ///
    /// This method is a hook called by heraclitus internals to be implemented
    /// by backends, and should never be called from client code.
    fn bootstrap_origin(
        &self,
        repo: &Repository,
        hunk: &Hunk,
    ) -> Result<(), Error>;

    /// When creating the origin, after the origin has been bootstrapped and
    /// the graph has been written, perform any cleanup. This must include
    /// relating the origin AG to its partitioning, and may include backend-
    /// specific hacks.
    ///
    /// # Warning
    ///
    /// This method is a hook called by heraclitus internals to be implemented
    /// by backends, and should never be called from client code.
    fn tie_off_origin(
        &self,
        repo: &Repository,
        ver_graph: &VersionGraph,
        origin_v_idx: VersionGraphIndex,
    ) -> Result<(), Error>;

    fn create_artifact_graph<'d, 'a, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &'d DatatypesRegistry<T>,
        repo: &Repository,
        art_graph_desc: ArtifactGraphDescription,
        parent: &'a mut ArtifactGraph<'d>,
        parent_v_idx: VersionGraphIndex,
        grandp_vg: &mut VersionGraph,
    ) -> Result<(VersionGraph<'d, 'a>, VersionGraphIndex, ArtifactGraph<'d>, ArtifactIndexMap), Error>
        where T::InterfaceControllerType: InterfaceController<ArtifactMeta>,
            // This is only necessary because `commit_version` is called for the
            // new AG's version, even though it will do nothing besides set
            // a status flag. TODO: reconsider.
            <T as DatatypeEnum>::InterfaceControllerType :
                    InterfaceController<ProducerController> +
                    InterfaceController<CustomProductionPolicyController>
    {

        // Create delta for parent graph, with new AG artifact related to UP.
        let mut parent_ag_delta_desc = ArtifactGraphDescription::new();
        let new_ag_art = ArtifactDescription::New {
            id: None, // TODO: hashes are wrong
            name: None,
            dtype: "ArtifactGraph".into(),
            self_partitioning: false,
        };
        let new_ag_art_idx = parent_ag_delta_desc.artifacts.add_node(new_ag_art);
        let parent_ag_up_idx = match parent.get_unary_partitioning() {
            Some(idx) => {
                let existing_up = ArtifactDescription::Existing(parent[idx].id.uuid);
                parent_ag_delta_desc.add_uniform_partitioning(existing_up)
            },
            None => parent_ag_delta_desc.add_unary_partitioning(),
        };
        let mut parent_ag_delta = ArtifactGraphDelta {
            additions: parent_ag_delta_desc,
            removals: vec![]
        };
        let parent_ag_idx_map = parent.apply_delta(&parent_ag_delta, dtypes_registry);

        // Set the correct ID and hash for the new AG artifacts.
        for node_idx in parent_ag_delta.additions.artifacts.graph().node_indices() {
            if let ArtifactDescription::New {ref mut id, ..} = parent_ag_delta.additions.artifacts[node_idx] {
                *id = Some(parent[parent_ag_idx_map[&node_idx]].id);
            }
        }

        // Create new version for parent graph delta.
        let new_parent_v_idx = grandp_vg.new_child(parent_v_idx, RepresentationKind::Delta);
        // TODO: have to do this because do not have access to grandparent AG.
        grandp_vg[new_parent_v_idx].status = VersionStatus::Committed;
        let parent_v_part_idx = grandp_vg.get_partitioning(parent_v_idx).expect("TODO").0;
        let parent_v_part_edge = grandp_vg.versions.find_edge(parent_v_part_idx, parent_v_idx).unwrap();
        grandp_vg.versions.add_edge(parent_v_part_idx, new_parent_v_idx, grandp_vg[parent_v_part_edge].clone()).unwrap();
        self.create_staging_version(repo, grandp_vg, new_parent_v_idx)?;

        // Write parent graph delta hunk.
        let parent_ag_payload = Payload::Delta(parent_ag_delta);
        let part_id = crate::datatype::partitioning::UnaryPartitioningState
            .get_partition_ids().iter().cloned().nth(0).unwrap();
        let parent_ag_hunk = Hunk {
            id: ArtifactGraphDtype::hash_payload(&parent_ag_payload).into(),
            version: &grandp_vg[new_parent_v_idx],
            partition: Partition {
                partitioning: &grandp_vg[parent_v_part_idx],
                index: part_id,
            },
            representation: RepresentationKind::Delta,
            completion: PartCompletion::Complete,
            precedence: None,
        };
        self.create_hunk(repo, &parent_ag_hunk)?;
        self.write_hunk(repo, &parent_ag_hunk, &parent_ag_payload)?;

        // Create new AG.
        // Done earlier here so ownership of the description can be transferred.
        let (art_graph, new_idx_map) = ArtifactGraph::from_description(&art_graph_desc, dtypes_registry);

        // Create version for new artifact graph's artifact.
        let new_ag_art = &parent[parent_ag_idx_map[&new_ag_art_idx]];
        let parent_ag_up_art = &parent[parent_ag_idx_map[&parent_ag_up_idx]];
        let mut parent_vg = self.get_version_graph(repo, parent)?;
        // Create partition version is necessary.
        let parent_ag_up_ver_idx = match parent_vg.artifact_versions(parent_ag_up_art).get(0) {
            None => {
                let up_ver = Version::new(parent_ag_up_art, RepresentationKind::State);
                let up_ver_idx = parent_vg.versions.add_node(up_ver);
                self.create_staging_version(repo, &parent_vg, up_ver_idx)?;
                up_ver_idx
            },
            Some(vers) => *vers,
        };
        let new_ag_version = Version::new(new_ag_art, RepresentationKind::State);
        let new_ag_v_idx = parent_vg.versions.add_node(new_ag_version);
        // Add partitioning edge.
        let up_art_edge = parent.artifacts.find_edge(
            parent_ag_idx_map[&parent_ag_up_idx], parent_ag_idx_map[&new_ag_art_idx]).unwrap();
        parent_vg.versions.add_edge(parent_ag_up_ver_idx, new_ag_v_idx,
            VersionRelation::Dependence(&parent[up_art_edge])).unwrap();
        self.create_staging_version(repo, &parent_vg, new_ag_v_idx)?;

        // Create hunk for new artifact graph's artifact.
        let new_ag_payload = Payload::State(art_graph.as_description());
        let new_ag_hunk = Hunk {
            id: ArtifactGraphDtype::hash_payload(&new_ag_payload).into(),
            version: &parent_vg[new_ag_v_idx],
            partition: Partition {
                partitioning: &grandp_vg[parent_v_part_idx], // TODO: this is wrong
                index: part_id,
            },
            representation: RepresentationKind::State,
            completion: PartCompletion::Complete,
            precedence: None,
        };
        self.create_hunk(repo, &new_ag_hunk)?;

        // Write new artifact graph's hunk.
        self.write_hunk(repo, &new_ag_hunk, &new_ag_payload)?;
        self.commit_version(dtypes_registry, repo, parent, &mut parent_vg, new_ag_v_idx)?;

        // Call initializers for artifacts the new graph.
        for idx in art_graph.artifacts.graph().node_indices() {
            let art = &art_graph[idx];
            let meta_controller = dtypes_registry
                .get_model_interface::<ArtifactMeta>(&art.dtype.name)
                .map(|gen| gen(&repo));
            if let Some(mut meta_controller) = meta_controller {
                meta_controller.init_artifact(repo, art)?;
            }
        }

        Ok((parent_vg, new_ag_v_idx, art_graph, new_idx_map))
    }

    fn get_artifact_graph<'d, T: DatatypeEnum>(
        &self,
        dtypes_registry: &'d DatatypesRegistry<T>,
        repo: &Repository,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> Result<ArtifactGraph<'d>, Error> {

        let up_part_id = crate::datatype::partitioning::UNARY_PARTITION_INDEX;
        let composition_map = self.get_composition_map(
            repo,
            ver_graph,
            v_idx,
            maplit::btreeset![up_part_id])?;
        let composition = &composition_map[&up_part_id];

        let ag_desc = self.get_composite_state(repo, composition)?;

        let ag = ArtifactGraph::from_description(&ag_desc, dtypes_registry).0;

        Ok(ag)
    }

    fn create_staging_version(
        &mut self,
        repo: &Repository,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>;

    /// Commit a version node and propagate any changes through the graph.
    /// Matches version based on UUID only and updates its hash.
    ///
    /// Constraints:
    /// - The version must not already be committed.
    fn commit_version<'a, 'b, T: DatatypeEnum>(
        &mut self,
        // TODO: dirty hack to work around mut/immut refs to context. Either
        // look at other Rust workarounds, or better yet finally design a way
        // to get model directly from datatypes.
        dtypes_registry: &DatatypesRegistry<T>,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>
            where
                <T as DatatypeEnum>::InterfaceControllerType :
                    InterfaceController<ProducerController> +
                    InterfaceController<CustomProductionPolicyController>;
    // TODO: many args to avoid reloading state. A 2nd-level API should just take an ID.

    fn cascade_notify_producers<'a, 'b, T:DatatypeEnum> (
        &mut self,
        dtypes_registry: &DatatypesRegistry<T>,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        seed_v_idx: VersionGraphIndex,
    ) -> Result<HashMap<Identity, ProductionOutput>, Error>
            where
                <T as DatatypeEnum>::InterfaceControllerType :
                    InterfaceController<ProducerController> +
                    InterfaceController<CustomProductionPolicyController>
            {
        let outputs = self.notify_producers(
            dtypes_registry,
            repo,
            art_graph,
            ver_graph,
            seed_v_idx)?;

        for output in outputs.values() {
            if let ProductionOutput::Synchronous(ref v_idxs) = *output {
                for v_idx in v_idxs {
                    self.commit_version(
                        dtypes_registry,
                        repo,
                        art_graph,
                        ver_graph,
                        *v_idx)?;
                }
            }
        }

        Ok(outputs)
    }

    fn notify_producers<'a, 'b, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &DatatypesRegistry<T>,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<HashMap<Identity, ProductionOutput>, Error>
            where
                <T as DatatypeEnum>::InterfaceControllerType :
                    InterfaceController<ProducerController> +
                    InterfaceController<CustomProductionPolicyController>
            {

        let default_production_policies: Vec<Box<dyn ProductionPolicy>> = vec![
            Box::new(ExtantProductionPolicy),
            Box::new(LeafBootstrapProductionPolicy),
        ];

        // TODO: should be configurable per-production artifact.
        let production_strategy_policy: Box<dyn ProductionStrategyPolicy> =
            Box::new(ParsimoniousRepresentationProductionStrategyPolicy);

        let (ver_art_idx, _) = {
            let new_ver = ver_graph.versions.node_weight(v_idx).expect("TODO");
            art_graph.get_by_id(&new_ver.artifact.id)
                .expect("TODO: Unknown artifact")
        };

        let dependent_arts = art_graph.artifacts.children(ver_art_idx).iter(&art_graph.artifacts);

        let mut new_prod_vers = HashMap::new();

        for (_e_idx, dep_art_idx) in dependent_arts {
            let dependent = &art_graph[dep_art_idx];
            let dtype = dependent.dtype;

            let producer_interface = dtypes_registry.get_model_interface::<ProducerController>(&dtype.name)
                .map(|gen| gen(&repo));
            if let Some(producer_controller) = producer_interface {

                let production_policies: Option<Vec<Box<dyn ProductionPolicy>>> =
                    self.get_production_policies(&repo, dependent)?
                    .map(|policies| policies.iter().filter_map(|p| match p {
                        ProductionPolicies::Extant =>
                            Some(Box::new(ExtantProductionPolicy) as Box<dyn ProductionPolicy>),
                        ProductionPolicies::LeafBootstrap =>
                            Some(Box::new(LeafBootstrapProductionPolicy) as Box<dyn ProductionPolicy>),
                        ProductionPolicies::Custom => {
                            let custom_policy_interface = dtypes_registry
                                .get_model_interface::<CustomProductionPolicyController>(&dtype.name)
                                // .get_model(&dtype.name)
                                // .get_controller()
                                .map(|gen| gen(&repo));
                            if let Some(custom_policy_controller) = custom_policy_interface {

                                Some(custom_policy_controller.get_custom_production_policy(
                                    &repo,
                                    art_graph,
                                    dep_art_idx).expect("TODO"))
                            } else {
                                None // TODO: custom policy for non-interface producer.
                            }
                        }
                    }).collect());

                let production_policy_reqs = match production_policies {
                        None => default_production_policies.iter(),
                        Some(ref p) => p.iter(),
                    }
                    .map(|policy| policy.requirements())
                    .fold(
                        ProductionPolicyRequirements::default(),
                        |mut max, ref p| {
                            max.producer = max.producer.max(p.producer.clone());
                            max.dependency = max.dependency.max(p.dependency.clone());
                            max
                        });

                self.fulfill_policy_requirements(
                    &repo,
                    art_graph,
                    ver_graph,
                    v_idx,
                    dep_art_idx,
                    &production_policy_reqs)?;

                let prod_specs = match production_policies {
                        None => default_production_policies.iter(),
                        Some(ref p) => p.iter(),
                    }
                    .map(|policy| policy.new_production_version_specs(
                        art_graph,
                        ver_graph,
                        v_idx,
                        dep_art_idx))
                    .fold(
                        ProductionVersionSpecs::default(),
                        |mut specs, x| {specs.merge(x); specs});

                let production_strategies = producer_controller.production_strategies();

                for (specs, parent_prod_vers) in &prod_specs.specs {
                    let new_prod_ver = Version::new(dependent, RepresentationKind::State);
                    let new_prod_ver_id = new_prod_ver.id;
                    let new_prod_ver_idx = ver_graph.versions.add_node(new_prod_ver);

                    for spec in specs {
                        let art_rel = art_graph.artifacts.edge_weight(spec.relation)
                            .expect("Impossible unknown artifact relation: indices from this graph");
                        ver_graph.versions.add_edge(
                            spec.version,
                            new_prod_ver_idx,
                            VersionRelation::Dependence(art_rel))?;
                    }

                    for parent_ver in parent_prod_vers {
                        if let Some(ref idx) = *parent_ver {
                            ver_graph.versions.add_edge(
                                *idx,
                                new_prod_ver_idx,
                                VersionRelation::Parent)?;
                        }
                    }

                    let strategy_specs = ProductionStrategySpecs {
                        representation: production_strategy_policy.select_representation(
                            ver_graph,
                            new_prod_ver_idx,
                            &production_strategies)
                                .expect("TODO: producer is incompatible with input versions"),
                    };

                    self.create_staging_version(
                        &repo,
                        ver_graph,
                        new_prod_ver_idx)?;

                    self.write_production_specs(
                        &repo,
                        &ver_graph[new_prod_ver_idx],
                        strategy_specs)?;

                    let output = producer_controller.notify_new_version(
                        &repo,
                        art_graph,
                        ver_graph,
                        new_prod_ver_idx)?;

                    new_prod_vers.insert(new_prod_ver_id, output);
                }
            }
        }

        Ok(new_prod_vers)
    }

    /// Fulfill policy requirements specified by `ProductionPolicyRequirements`
    /// by populating a version graph.
    ///
    /// Constraints:
    /// - The triggering new dependency version (`v_idx`) and all of its
    ///   relations must already be present in the graph.
    fn fulfill_policy_requirements<'a, 'b>(
        &self,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
        requirements: &ProductionPolicyRequirements,
    ) -> Result<(), Error>;

    fn get_version<'a, 'b>(
        &self,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'a, 'b>), Error>;

    fn get_version_graph<'a, 'b>(
        &self,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
    ) -> Result<VersionGraph<'a, 'b>, Error>;

    fn create_hunk(
        &mut self,
        repo: &Repository,
        hunk: &Hunk,
    ) -> Result<(), Error> {
        self.create_hunks(repo, &[hunk])
    }

    fn create_hunks<'a: 'b, 'b: 'c + 'd, 'c, 'd, H>(
        &mut self,
        repo: &Repository,
        hunks: &[H],
    ) -> Result<(), Error>
        where H: std::borrow::Borrow<Hunk<'a, 'b, 'c, 'd>>;

    /// Get hunks directly associated with a version.
    ///
    /// # Arguments
    ///
    /// - `partitions` - Partitions indices for which to return hunks. If
    ///                  `None`, return hunks for all partitions.
    fn get_hunks<'a, 'b, 'c, 'd>(
        &self,
        repo: &Repository,
        version: &'d Version<'a, 'b>,
        partitioning: &'c Version<'a, 'b>,
        partitions: Option<&BTreeSet<PartitionIndex>>,
    ) -> Result<Vec<Hunk<'a, 'b, 'c, 'd>>, Error>;

    /// Get hunk sets sufficient to reconstruct composite states for a set of
    /// partitions.
    ///
    /// Note that partition indices in `partitions` may be abset from the
    /// returned `CompositionMap` if they have never been populated.
    fn get_composition_map<'a: 'b, 'b: 'r, 'c, 'd, 'r: 'c + 'd>(
        &self,
        repo: &Repository,
        ver_graph: &'r VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
        partitions: BTreeSet<PartitionIndex>,
    ) -> Result<CompositionMap<'a, 'b, 'c, 'd>, Error>  {
        // TODO: assumes whole version graph is loaded.
        // TODO: not backend-specific, but could be optimized to be so.
        let ancestors = crate::util::petgraph::induced_stream_toposort(
            ver_graph.versions.graph(),
            &[v_idx],
            petgraph::Direction::Incoming,
            |e: &VersionRelation| *e == VersionRelation::Parent)?;

        let mut map = CompositionMap::new();
        // Partition indices that have not yet been resolved.
        let mut unresolved: BTreeSet<PartitionIndex> = partitions.clone();
        // Partition indices that have not yet been seen.
        // TODO: could be more efficient by instead asserting unresolved and
        // domain of map are disjoint.
        let mut unseen: BTreeSet<PartitionIndex> = partitions.clone();
        // Partition indices that are locked from composition changes because
        // they have received a hunk from an unreached version.
        let mut locked: BTreeMap<Uuid, BTreeSet<PartitionIndex>> = BTreeMap::new();

        for n_idx in ancestors {
            let version = &ver_graph[n_idx];

            if let Some(mut part_idxs) = locked.remove(&version.id.uuid) {
                unresolved.append(&mut part_idxs);
            }

            let hunks = self.get_hunks(
                &repo,
                version,
                &ver_graph[ver_graph.get_partitioning(n_idx).expect("TODO: comp map part").0],
                Some(&unresolved))?;

            for hunk in hunks {
                let part_idx = hunk.partition.index;
                unseen.remove(&part_idx);

                if hunk.representation == RepresentationKind::State {
                    unresolved.remove(&part_idx);
                }

                if let Some(ver_uuid) = hunk.precedence {
                    locked.entry(ver_uuid)
                        .or_insert_with(BTreeSet::new)
                        .insert(hunk.partition.index);
                    unresolved.remove(&hunk.partition.index);
                }

                map.entry(part_idx)
                    .or_insert_with(Vec::new)
                    .push(hunk);
            }

            // If sufficient ancestry is reached, return early.
            if unresolved.is_empty() && locked.is_empty() {
                return Ok(map)
            }
        }

        assert!(unresolved == unseen && locked.is_empty(), "Composition map was unfulfilled!");
        Ok(map)
    }

    fn write_production_policies<'a>(
        &mut self,
        repo: &Repository,
        artifact: &Artifact<'a>,
        policies: EnumSet<ProductionPolicies>,
    ) -> Result<(), Error>;

    fn get_production_policies<'a>(
        &self,
        repo: &Repository,
        artifact: &Artifact<'a>,
    ) -> Result<Option<EnumSet<ProductionPolicies>>, Error>;

    fn write_production_specs<'a, 'b>(
        &mut self,
        repo: &Repository,
        version: &Version<'a, 'b>,
        specs: ProductionStrategySpecs,
    ) -> Result<(), Error>;

    fn get_production_specs<'a, 'b>(
        &self,
        repo: &Repository,
        version: &Version<'a, 'b>,
    ) -> Result<ProductionStrategySpecs, Error>;
}


#[derive(Debug, Hash, PartialEq)]
pub struct ArtifactGraphDelta {
    additions: ArtifactGraphDescription,
    removals: Vec<Uuid>,
}

impl ArtifactGraphDelta {
    pub fn new(additions: ArtifactGraphDescription, removals: Vec<Uuid>) -> Self {
        Self {
            additions,
            removals,
        }
    }

    pub fn additions(&self) -> &ArtifactGraphDescription {
        &self.additions
    }

    pub fn removals(&self) -> &[Uuid] {
        &self.removals
    }
}

pub type ArtifactGraphDescriptionType = daggy::Dag<ArtifactDescription, ArtifactRelation>;
#[derive(Clone, Debug)]
pub struct ArtifactGraphDescription {
    pub artifacts: ArtifactGraphDescriptionType,
}

impl ArtifactGraphDescription {
    pub fn new() -> Self {
        Self {
            artifacts: ArtifactGraphDescriptionType::new(),
        }
    }

    pub fn add_unary_partitioning(&mut self) -> daggy::NodeIndex {
        self.add_uniform_partitioning(ArtifactDescription::New {
                    id: None,
                    name: Some("Unary Partitioning Singleton".into()),
                    dtype: "UnaryPartitioning".into(),
                    self_partitioning: true,
                })
    }

    pub fn add_uniform_partitioning(&mut self, partitioning: ArtifactDescription) -> daggy::NodeIndex {
        let part_idx = self.artifacts.add_node(partitioning);
        for node_idx in daggy::petgraph::algo::toposort(self.artifacts.graph(), None)
                .expect("TODO: not a DAG") {
            if node_idx == part_idx {
                continue;
            }
            let has_partitioning = self.artifacts.parents(node_idx).iter(&self.artifacts)
                .fold(false, |hp, (e_idx, _p_idx)| {
                    hp || match self.artifacts[e_idx] {
                        ArtifactRelation::DtypeDepends(ref rel) => rel.name == "Partitioning",
                        _ => false,
                    }
                });
            if !has_partitioning {
                let edge = ArtifactRelation::DtypeDepends(DatatypeRelation {name: "Partitioning".into()});
                self.artifacts.add_edge(part_idx, node_idx, edge).expect("Graph is malformed.");
            }
        }

        part_idx
    }

    /// Whether this description does not refer to any existing nodes.
    pub fn is_independent(&self) -> bool {
        for node_idx in self.artifacts.graph().node_indices() {
            if let ArtifactDescription::Existing(_) = self.artifacts[node_idx] {
                return false;
            }
        }

        true
    }

    /// Whether this description is already a valid payload for persisting.
    pub fn is_valid_payload(&self) -> bool {
        // All nodes must have an identity.
        for node_idx in self.artifacts.graph().node_indices() {
            if let ArtifactDescription::New { id, .. } = self.artifacts[node_idx] {
                if id.is_none() {
                    return false;
                }
            }
        }

        // Only new nodes may have dependent edges.
        for edge in self.artifacts.graph().raw_edges() {
            if let ArtifactDescription::Existing(_) = self.artifacts[edge.target()] {
                return false
            }
        }

        true
    }

    pub fn is_valid_state(&self) -> bool {
        self.is_independent() && self.is_valid_payload()
    }

    fn get_by_uuid(
        &self,
        uuid: &Uuid
    ) -> Option<(ArtifactGraphIndex, &ArtifactDescription)> {
        for node_idx in self.artifacts.graph().node_indices() {
            let node = self.artifacts.node_weight(node_idx).expect("Graph is malformed");
            if let ArtifactDescription::New {id: Some(id), ..} = node {
                if id.uuid == *uuid {
                    return Some((node_idx, node))
                }
            }
        }

        None
    }

    pub fn compose(&mut self, delta: &ArtifactGraphDelta) -> Result<(), Error> {
        for art_uuid in &delta.removals {
            let (found_idx, _) = self.get_by_uuid(art_uuid)
                .ok_or_else(|| Error::Model(ModelError::NotFound(*art_uuid)))?;
            self.artifacts.remove_node(found_idx);
        }

        let mut idx_map = ArtifactIndexMap::new();
        for delta_idx in delta.additions.artifacts.graph().node_indices() {
            let node = &delta.additions.artifacts[delta_idx];
            let self_idx = match node {
                ArtifactDescription::New {..} => {
                    self.artifacts.add_node(node.clone())
                },
                ArtifactDescription::Existing(uuid) => {
                    match self.get_by_uuid(uuid) {
                        Some((self_idx, _)) => self_idx,
                        None => self.artifacts.add_node(node.clone()),
                    }
                },
            };
            idx_map.insert(delta_idx, self_idx);
        }

        for edge in delta.additions.artifacts.graph().raw_edges() {
            self.artifacts.add_edge(idx_map[&edge.source()], idx_map[&edge.target()], edge.weight.clone())
                .expect("TODO");
        }

        Ok(())
    }
}

impl PartialEq for ArtifactGraphDescription {
    fn eq(&self, other: &Self) -> bool {
        let mut idx_map: BTreeMap<ArtifactGraphIndex, ArtifactGraphIndex> = BTreeMap::new();

        let mut other_node_idxs = other.artifacts.graph().node_indices().collect::<BTreeSet<_>>();

        for node_idx in self.artifacts.graph().node_indices() {
            let node = &self.artifacts[node_idx];
            let match_idx = other_node_idxs.iter()
                .find(|&idx| node == &other.artifacts[*idx])
                .copied();

            match match_idx {
                None => { return false; },
                Some(other_idx) => {
                    idx_map.insert(node_idx, other_idx);
                    other_node_idxs.remove(&other_idx);
                }
            }
        }

        if !other_node_idxs.is_empty() {
            return false;
        }

        true
    }
}

// TODO: not verified or compatible with ArtifactGraph
impl Hash for ArtifactGraphDescription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let to_visit = daggy::petgraph::algo::toposort(self.artifacts.graph(), None)
            .expect("TODO: not a DAG");

        // Walk the description graph in descending dependency order.
        for node_idx in to_visit {
            self.artifacts[node_idx].hash(state);
        };
    }
}


#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ArtifactDescription {
    New {
        id: Option<Identity>,
        name: Option<String>,
        dtype: String,
        self_partitioning: bool,
    },
    Existing(Uuid),
}

impl ArtifactDescription {
    pub fn new_from_artifact(art: &Artifact) -> Self {
        ArtifactDescription::New {
            id: Some(art.id.clone()),
            name: art.name.clone(),
            dtype: art.dtype.name.clone(),
            self_partitioning: art.self_partitioning,
        }
    }
}

// TODO: this can't match Artifact hash yet because of dtype.
impl Hash for ArtifactDescription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ArtifactDescription::New {id: Some(id), ..} => {
                id.hash.hash(state);
            },
            ArtifactDescription::New {id: None, name, self_partitioning, ..} => {
                // TODO: not using dtype for hash.
                name.hash(state);
                self_partitioning.hash(state);
            },
            ArtifactDescription::Existing(uuid) => {
                uuid.hash(state);
            }
        }
    }
}
