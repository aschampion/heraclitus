use std::collections::{
    BTreeSet,
    BTreeMap,
    HashMap,
};

use daggy::{
    self,
    Walker,
};
use heraclitus_macros::stored_controller;
use enum_set::{
    EnumSet,
};
use petgraph;
use uuid::{
    Uuid,
};

use crate::{
    Artifact,
    ArtifactGraph,
    ArtifactGraphIndex,
    ArtifactRelation,
    CompositionMap,
    DatatypeRelation,
    RepresentationKind,
    Error,
    Hunk,
    Identity,
    IdentifiableGraph,
    PartitionIndex,
    Version,
    VersionGraph,
    VersionGraphIndex,
    VersionRelation,
};
use super::{
    DatatypeEnum,
    DatatypeMarker,
    DatatypesRegistry,
    Description,
    InterfaceController,
    InterfaceControllerEnum,
    MetaController,
    StoreMetaController,
};
use crate::datatype::interface::{
    CustomProductionPolicyController,
    ProducerController,
    ProductionOutput,
};
use crate::repo::Repository;
use crate::repo::RepoController;


pub mod production;
use self::production::*;

#[cfg(test)]
mod tests;


#[derive(Default)]
pub struct ArtifactGraphDtype;

impl DatatypeMarker for ArtifactGraphDtype {}

impl<T: InterfaceControllerEnum> super::Model<T> for ArtifactGraphDtype {
    fn info(&self) -> Description<T> {
        Description {
            name: "ArtifactGraph".into(),
            version: 1,
            representations: vec![RepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec![], // TODO: should artifact graph be an interface?
            dependencies: vec![],
        }
    }

    datatype_controllers!(ArtifactGraphDtype, ());
}


#[stored_controller(crate::store::Store<ArtifactGraphDtype>)]
pub trait Storage {
    fn list_graphs(&self) -> Vec<Identity>;

    fn create_artifact_graph<'a, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &'a DatatypesRegistry<T>,
        repo: &Repository,
        art_graph: &ArtifactGraph,
    ) -> Result<(), Error> {
        self.write_artifact_graph(repo, art_graph)?;

        for idx in art_graph.artifacts.graph().node_indices() {
            let art = &art_graph[idx];
            let mut meta_controller = dtypes_registry
                .get_model(&art.dtype.name)
                .meta_controller(repo.backend());
            meta_controller.init_artifact(art)?;
        }

        Ok(())
    }

    fn write_artifact_graph(
        &mut self,
        repo: &Repository,
        art_graph: &ArtifactGraph,
    ) -> Result<(), Error>;

    fn get_artifact_graph<'a, T: DatatypeEnum>(
        &self,
        dtypes_registry: &'a DatatypesRegistry<T>,
        repo: &Repository,
        id: &Identity,
    ) -> Result<ArtifactGraph<'a>, Error>;

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


pub type ArtifactGraphDescriptionType =  daggy::Dag<ArtifactDescription, ArtifactRelation>;
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
        self.add_uniform_partitioning(ArtifactDescription{
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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ArtifactDescription {
    pub name: Option<String>,
    pub dtype: String,
    pub self_partitioning: bool,
}
