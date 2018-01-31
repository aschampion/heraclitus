extern crate daggy;
extern crate enum_set;
extern crate petgraph;
extern crate schemer;
extern crate serde;
extern crate serde_json;
extern crate uuid;
extern crate postgres;


use std::collections::{
    BTreeSet,
    BTreeMap,
    HashMap,
};
use std::mem;

use daggy::petgraph::visit::EdgeRef;
use daggy::Walker;
use enum_set::EnumSet;
use uuid::Uuid;

use ::{
    Artifact,
    ArtifactGraph,
    ArtifactGraphIndex,
    ArtifactGraphEdgeIndex,
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
    DatatypesRegistry,
    Description,
    Store,
};
use ::datatype::interface::{
    CustomProductionPolicyController,
    ProductionOutput,
    ProductionStrategies,
    ProductionStrategyID,
};
use ::store::postgres::datatype::artifact_graph::PostgresStore;


#[derive(Default)]
pub struct ArtifactGraphDtype;

impl<T> super::Model<T> for ArtifactGraphDtype {
    fn info(&self) -> Description {
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

    fn meta_controller(&self, store: Store) -> Option<super::StoreMetaController> {
        match store {
            Store::Postgres => Some(super::StoreMetaController::Postgres(Box::new(PostgresStore {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        _store: Store,
        _name: &str,
    ) -> Option<T> {
        None
    }
}

pub fn model_controller(store: Store) -> impl ModelController {
    match store {
        Store::Postgres => PostgresStore {},
        _ => unimplemented!(),
    }
}


/// Defines versions of the relevant producer a production policy requires to be
/// in the version graph.
///
/// Note that later variants are supersets of earlier variants.
#[derive(PartialOrd, PartialEq, Eq, Ord, Clone)]
pub(crate) enum PolicyProducerRequirements {
    /// No requirement.
    None,
    /// Any producer version dependent on parent versions of the new dependency
    /// version.
    DependentOnParentVersions,
    /// All versions of this producer.
    All,
}

/// Defines versions of dependencies of the relevant producer a production
/// policy requires to be in the version graph, in addition to dependencies
/// of producer versions specified by `PolicyProducerRequirements`.
///
/// Note that later variants are supersets of earlier variants.
#[derive(PartialOrd, PartialEq, Eq, Ord, Clone)]
pub(crate) enum PolicyDependencyRequirements {
    /// No requirement.
    None,
    /// Any dependency version on which a producer version (included by
    /// the `PolicyProducerRequirements`) is dependent.
    DependencyOfProducerVersion,
    /// All versions of the producer's dependency artifacts.
    All,
}

/// Defines what dependency and producer versions a production policy requires
/// to be in the version graph.
pub struct ProductionPolicyRequirements {
    pub(crate) producer: PolicyProducerRequirements,
    pub(crate) dependency: PolicyDependencyRequirements,
}

impl Default for ProductionPolicyRequirements {
    fn default() -> Self {
        ProductionPolicyRequirements {
            producer: PolicyProducerRequirements::None,
            dependency: PolicyDependencyRequirements::None,
        }
    }
}

/// Specifies a set of dependency versions for a new producer version.
#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProductionDependencySpec {
    version: VersionGraphIndex,
    relation: ArtifactGraphEdgeIndex,
}

type ProductionDependenciesSpecs = BTreeSet<ProductionDependencySpec>;

/// Specifies sets of dependencies for new producer version, mapped to the
/// parent producer versions for each.
#[derive(Default)]
pub struct ProductionVersionSpecs {
    specs: HashMap<ProductionDependenciesSpecs, BTreeSet<Option<VersionGraphIndex>>>,
}

impl ProductionVersionSpecs {
    pub fn insert(&mut self, spec: ProductionDependenciesSpecs, parent: Option<VersionGraphIndex>) {
        self.specs.entry(spec)
            .or_insert_with(BTreeSet::new)
            .insert(parent);
    }

    pub fn merge(&mut self, other: ProductionVersionSpecs) {
        for (k, mut v) in other.specs {
            self.specs.entry(k)
                .and_modify(|existing| existing.append(&mut v))
                .or_insert(v);
        }
    }

    pub fn retain<F>(&mut self, filter: F)
            where F: FnMut(&ProductionDependenciesSpecs, &mut BTreeSet<Option<VersionGraphIndex>>) -> bool
    {
        self.specs.retain(filter);
    }
}

/// Enacts a policy for what new versions to produce in response to updated
/// dependency versions.
pub trait ProductionPolicy {
    /// Defines what this policy requires to be in the version graph for it
    /// to determine what new production versions should be created.
    fn requirements(&self) -> ProductionPolicyRequirements;
    // TODO: Convert to associated const once that lands.

    /// Given a producer and a new version of one of its dependencies, yield
    /// all sets of dependencies and parent versions for which new production
    /// versions should be created.
    fn new_production_version_specs(
        &self,
        art_graph: &ArtifactGraph,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
    ) -> ProductionVersionSpecs;
}


/// A production policy where existing producer versions are updated to track
/// new dependency versions.
pub struct ExtantProductionPolicy;

impl ProductionPolicy for ExtantProductionPolicy {
    fn requirements(&self) -> ProductionPolicyRequirements {
        ProductionPolicyRequirements {
            producer: PolicyProducerRequirements::DependentOnParentVersions,
            dependency: PolicyDependencyRequirements::None,
        }
    }

    fn new_production_version_specs(
        &self,
        art_graph: &ArtifactGraph,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
    ) -> ProductionVersionSpecs {
        let mut specs = ProductionVersionSpecs::default();

        // The dependency version must have parents, and all its parents must
        // have related dependent versions of this producer.
        // TODO: version artifact and producer artifact could share multiple
        // relationships. This is not yet handled.
        let v_parents: BTreeSet<VersionGraphIndex> = ver_graph.get_parents(v_idx)
            .iter().cloned().collect();
        if v_parents.is_empty() {
            return specs
        }

        let dep_art_idx = art_graph.get_by_id(&ver_graph[v_idx].artifact.id).unwrap().0;
        // TODO: Petgraph doesn't allow multiedges?
        let ver_rels = [
            VersionRelation::Dependence(
                &art_graph[art_graph.artifacts.graph()
                    .find_edge(dep_art_idx, p_art_idx).expect("TODO")]),
        ];

        // TODO: a mess that could be written much more concisely.
        for parent_v_idx in &v_parents {
            for ver_rel in &ver_rels {
                let prod_vers = ver_graph.get_related_versions(
                    *parent_v_idx,
                    ver_rel,
                    petgraph::Direction::Outgoing);

                for prod_ver in &prod_vers {
                    let mut dependencies = ProductionDependenciesSpecs::new();

                    for (e_idx, d_idx) in ver_graph.versions.parents(*prod_ver).iter(&ver_graph.versions) {
                        if let VersionRelation::Dependence(art_rel) = ver_graph[e_idx] {
                            let new_dep_vers = if v_parents.contains(&d_idx) {
                                v_idx
                            } else {
                                d_idx
                            };

                            // TODO: stupid. stupid. stupid.
                            let e_art_idx = art_graph.artifacts.graph()
                                .edges_directed(
                                    p_art_idx,
                                    petgraph::Direction::Incoming)
                                .filter(|e| e.weight() == art_rel)
                                .map(|e| e.id())
                                .nth(0).expect("TODO");

                            dependencies.insert(ProductionDependencySpec {
                                version: new_dep_vers,
                                relation: e_art_idx});
                        }
                    }

                    specs.insert(dependencies, Some(*prod_ver));
                }
            }
        }

        specs
    }
}


/// A production policy where iff there exist only and exactly one single leaf
/// version for all dependencies, a new producer version should be created
/// for these.
pub struct LeafBootstrapProductionPolicy;

impl ProductionPolicy for LeafBootstrapProductionPolicy {
    fn requirements(&self) -> ProductionPolicyRequirements {
        ProductionPolicyRequirements {
            producer: PolicyProducerRequirements::None,
            dependency: PolicyDependencyRequirements::All,
        }
    }

    fn new_production_version_specs(
        &self,
        art_graph: &ArtifactGraph,
        ver_graph: &VersionGraph,
        _: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
    ) -> ProductionVersionSpecs {
        let mut specs = ProductionVersionSpecs::default();
        let prod_art = art_graph.artifacts.node_weight(p_art_idx).expect("Non-existent producer");

        // Any version of this producer already exists.
        if !ver_graph.artifact_versions(prod_art).is_empty() {
            return specs
        }

        let mut dependencies = ProductionDependenciesSpecs::new();
        for (e_idx, d_idx) in art_graph.artifacts.parents(p_art_idx).iter(&art_graph.artifacts) {
            let dependency = &art_graph[d_idx];
            let dep_vers = ver_graph.artifact_versions(dependency);

            if dep_vers.len() != 1 {
                return specs;
            } else {
                dependencies.insert(ProductionDependencySpec {version: dep_vers[0], relation: e_idx});
            }
        }
        specs.insert(dependencies, None);

        specs
    }
}


#[derive(Clone, Copy, Debug, ToSql, FromSql)]
#[repr(u32)]
#[postgres(name = "production_policy")]
pub enum ProductionPolicies {
    #[postgres(name = "extant")]
    Extant,
    #[postgres(name = "leaf_bootstrap")]
    LeafBootstrap,
    #[postgres(name = "custom")]
    Custom,
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for ProductionPolicies {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> ProductionPolicies {
        mem::transmute(v)
    }
}


/// Enacts a policy for what production strategy (from the set of those of
/// which the producer is capable) to use for a particular version.
///
/// Currently this only involves the representation kinds of inputs and outputs
/// the producer supports.
///
/// Note that unlike `ProductionPolicy`, no requirements for related versions
/// are necessary, because this policy by construction only depends on
/// dependency versions of the relevant producer version, which are always in
/// the version graph when producer versions are created and notified.
trait ProductionStrategyPolicy {
    fn select_representation(
        &self,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        strategies: &ProductionStrategies,
    ) -> Option<ProductionStrategyID>;
}

/// A production strategy policy selecting for the strategy with the most
/// parsimonious output representations.
pub struct ParsimoniousRepresentationProductionStrategyPolicy;

impl ProductionStrategyPolicy for ParsimoniousRepresentationProductionStrategyPolicy {
    fn select_representation(
        &self,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        strategies: &ProductionStrategies,
    ) -> Option<ProductionStrategyID> {
        // Collect current version inputs.
        let inputs = ver_graph.versions.graph().edges_directed(v_idx, petgraph::Direction::Incoming)
            .filter_map(|edgeref| match *edgeref.weight() {
                VersionRelation::Dependence(relation) => {
                    match *relation {
                        ArtifactRelation::DtypeDepends(ref dtype_relation) =>
                            Some((dtype_relation.name.as_str(),
                                  ver_graph[edgeref.source()].representation)),
                        _ => None,
                    }
                }
                VersionRelation::Parent => None,
            })
            .collect::<Vec<_>>();

        strategies.iter()
            // Filter strategies by those applicable to current version inputs.
            .filter(|&(_, capability)| capability.matches_inputs(&inputs))
            // From remaining strategies, select that with minimal sum minimum
            // representation kind weighting.
            .map(|(id, capability)|
                (id, capability.outputs().values()
                    .map(|reps| {
                        reps.iter().map(|r| match r {
                            RepresentationKind::State => 3usize,
                            RepresentationKind::CumulativeDelta => 2,
                            RepresentationKind::Delta => 1,
                        })
                        .min()
                        .unwrap_or(0)
                    })
                    .sum::<usize>()
            ))
            .min_by_key(|&(_, score)| score)
            .map(|(id, _)| id.clone())
    }
}


/// Specifies the production strategy to use for a particular producer version.
pub struct ProductionStrategySpecs {
    pub(crate) representation: ProductionStrategyID,
    // TODO: there may be other categories capabilities, strategies and
    // policies in addition representation.
}


pub trait ModelController {
    fn list_graphs(&self) -> Vec<Identity>;

    fn create_artifact_graph(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &ArtifactGraph,
    ) -> Result<(), Error>;

    fn get_artifact_graph<'a, T: DatatypeEnum>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        dtypes_registry: &'a DatatypesRegistry<T>,
        id: &Identity,
    ) -> Result<ArtifactGraph<'a>, Error>;

    fn create_staging_version(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
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
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>
        where Box<::datatype::interface::ProducerController>:
        From<<T as DatatypeEnum>::InterfaceControllerType>,
        Box<CustomProductionPolicyController>:
        From<<T as DatatypeEnum>::InterfaceControllerType>;
    // TODO: many args to avoid reloading state. A 2nd-level API should just take an ID.

    fn cascade_notify_producers<'a, 'b, T:DatatypeEnum> (
        &mut self,
        dtypes_registry: &DatatypesRegistry<T>,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        seed_v_idx: VersionGraphIndex,
    ) -> Result<HashMap<Identity, ProductionOutput>, Error>
            where Box<::datatype::interface::ProducerController>:
            From<<T as DatatypeEnum>::InterfaceControllerType>,
            Box<CustomProductionPolicyController>:
            From<<T as DatatypeEnum>::InterfaceControllerType> {
        let outputs = self.notify_producers(
            dtypes_registry,
            repo_control,
            art_graph,
            ver_graph,
            seed_v_idx)?;

        for output in outputs.values() {
            if let ProductionOutput::Synchronous(ref v_idxs) = *output {
                for v_idx in v_idxs {
                    self.commit_version(
                        dtypes_registry,
                        repo_control,
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
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<HashMap<Identity, ProductionOutput>, Error>
            where Box<::datatype::interface::ProducerController>:
            From<<T as DatatypeEnum>::InterfaceControllerType>,
            Box<CustomProductionPolicyController>:
            From<<T as DatatypeEnum>::InterfaceControllerType> {

        let default_production_policies: Vec<Box<ProductionPolicy>> = vec![
            Box::new(ExtantProductionPolicy),
            Box::new(LeafBootstrapProductionPolicy),
        ];

        // TODO: should be configurable per-production artifact.
        let production_strategy_policy: Box<ProductionStrategyPolicy> =
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

            if let Some(producer_interface) = dtypes_registry.get_model(&dtype.name)
                    .interface_controller(repo_control.store(), "Producer") {
                let producer_controller: Box<::datatype::interface::ProducerController> =
                    producer_interface.into();

                let production_policies: Option<Vec<Box<ProductionPolicy>>> =
                    self.get_production_policies(repo_control, dependent)?
                    .map(|policies| policies.iter().filter_map(|p| match p {
                        ProductionPolicies::Extant =>
                            Some(Box::new(ExtantProductionPolicy) as Box<ProductionPolicy>),
                        ProductionPolicies::LeafBootstrap =>
                            Some(Box::new(LeafBootstrapProductionPolicy) as Box<ProductionPolicy>),
                        ProductionPolicies::Custom => {
                            if let Some(custom_policy_interface) = dtypes_registry.get_model(&dtype.name)
                                    .interface_controller(repo_control.store(), "CustomProductionPolicy") {
                                let custom_policy_controller: Box<CustomProductionPolicyController> =
                                    custom_policy_interface.into();
                                Some(custom_policy_controller.get_custom_production_policy(
                                    repo_control,
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
                    repo_control,
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
                        repo_control,
                        ver_graph,
                        new_prod_ver_idx)?;

                    self.write_production_specs(
                        repo_control,
                        &ver_graph[new_prod_ver_idx],
                        strategy_specs)?;

                    let output = producer_controller.notify_new_version(
                        repo_control,
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
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
        requirements: &ProductionPolicyRequirements,
    ) -> Result<(), Error>;

    fn get_version<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'a, 'b>), Error>;

    fn get_version_graph<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
    ) -> Result<VersionGraph<'a, 'b>, Error>;

    fn create_hunk(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<(), Error>;

    /// Get hunks directly associated with a version.
    ///
    /// # Arguments
    ///
    /// - `partitions` - Partitions indices for which to return hunks. If
    ///                  `None`, return hunks for all partitions.
    fn get_hunks<'a, 'b, 'c, 'd>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &'d Version<'a, 'b>,
        partitioning: &'c Version<'a, 'b>,
        partitions: Option<&BTreeSet<PartitionIndex>>,
    ) -> Result<Vec<Hunk<'a, 'b, 'c, 'd>>, Error>;

    /// Get hunk sets sufficient to reconstruct composite states for a set of
    /// partitions.
    fn get_composition_map<'a: 'b, 'b: 'r, 'c, 'd, 'r: 'c + 'd>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        ver_graph: &'r VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
        partitions: BTreeSet<PartitionIndex>,
    ) -> Result<CompositionMap<'a, 'b, 'c, 'd>, Error>  {
        // TODO: assumes whole version graph is loaded.
        // TODO: not store-specific, but could be optimized to be so.
        let ancestors = ::util::petgraph::induced_stream_toposort(
            ver_graph.versions.graph(),
            &[v_idx],
            petgraph::Direction::Incoming,
            |e: &VersionRelation| match *e {
                VersionRelation::Parent => true,
                _ => false
            })?;

        let mut map = CompositionMap::new();
        // Partition indices that have not yet been resolved.
        let mut unresolved: BTreeSet<PartitionIndex> = partitions.clone();
        // Partition indices that are locked from composition changes because
        // they have received a hunk from an unreached version.
        let mut locked: BTreeMap<Uuid, BTreeSet<PartitionIndex>> = BTreeMap::new();

        for n_idx in ancestors {
            let version = &ver_graph[n_idx];

            if let Some(mut part_idxs) = locked.remove(&version.id.uuid) {
                unresolved.append(&mut part_idxs);
            }

            let hunks = self.get_hunks(
                repo_control,
                version,
                &ver_graph[ver_graph.get_partitioning(n_idx).expect("TODO: comp map part").0],
                Some(&unresolved))?;

            for hunk in hunks {
                let part_idx = hunk.partition.index;

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

        assert!(
            unresolved.is_empty() && locked.is_empty(),
            "Composition map was unfulfilled!");
        Ok(map)
    }

    fn write_production_policies<'a>(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact<'a>,
        policies: EnumSet<ProductionPolicies>,
    ) -> Result<(), Error>;

    fn get_production_policies<'a>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        artifact: &Artifact<'a>,
    ) -> Result<Option<EnumSet<ProductionPolicies>>, Error>;

    fn write_production_specs<'a, 'b>(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &Version<'a, 'b>,
        specs: ProductionStrategySpecs,
    ) -> Result<(), Error>;

    fn get_production_specs<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &Version<'a, 'b>,
    ) -> Result<ProductionStrategySpecs, Error>;
}


type ArtifactGraphDescriptionType =  daggy::Dag<ArtifactDescription, ArtifactRelation>;
pub struct ArtifactGraphDescription {
    pub artifacts: ArtifactGraphDescriptionType,
}

impl ArtifactGraphDescription {
    pub fn new() -> Self {
        Self {
            artifacts: ArtifactGraphDescriptionType::new(),
        }
    }

    pub fn add_unary_partitioning(&mut self) {
        self.add_uniform_partitioning(ArtifactDescription{
                    name: Some("Unary Partitioning Singleton".into()),
                    dtype: "UnaryPartitioning".into(),
                });
    }

    pub fn add_uniform_partitioning(&mut self, partitioning: ArtifactDescription) {
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
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ArtifactDescription {
    pub name: Option<String>,
    pub dtype: String,
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::iter::FromIterator;

    use uuid::Uuid;

    use ::{
        Context,
        Partition,
        PartCompletion,
    };
    use ::datatype::ModelController as DatatypeModelController;
    use datatype::partitioning::arbitrary::ModelController as ArbitraryPartitioningModelController;
    use ::datatype::producer::tests::NegateBlobProducer;


    datatype_enum!(TestDatatypes, ::datatype::DefaultInterfaceController, (
        (ArtifactGraph, ::datatype::artifact_graph::ArtifactGraphDtype),
        (Ref, ::datatype::reference::Ref),
        (UnaryPartitioning, ::datatype::partitioning::UnaryPartitioning),
        (ArbitraryPartitioning, ::datatype::partitioning::arbitrary::ArbitraryPartitioning),
        (Blob, ::datatype::blob::Blob),
        (NoopProducer, ::datatype::producer::NoopProducer),
        (NegateBlobProducer, NegateBlobProducer),
        (TrackingBranchProducer, ::datatype::tracking_branch_producer::TrackingBranchProducer),
    ));

    /// Create a simple artifact chain of
    /// Blob -> Producer -> Blob -> Producer -> Blob.
    fn simple_blob_prod_ag_fixture<'a, T: DatatypeEnum>(
        dtypes_registry: &'a DatatypesRegistry<T>,
        partitioning: Option<ArtifactDescription>,
    ) -> (ArtifactGraph<'a>, [ArtifactGraphIndex; 7]) {
        let mut artifacts = ArtifactGraphDescriptionType::new();

        // Blob 1
        let blob1_node = ArtifactDescription {
            name: Some("Test Blob 1".into()),
            dtype: "Blob".into()
        };
        let blob1_node_idx = artifacts.add_node(blob1_node);
        // Prod 1
        let prod1_node = ArtifactDescription {
            name: Some("Test Producer 1".into()),
            dtype: "NegateBlobProducer".into(),
        };
        let prod1_node_idx = artifacts.add_node(prod1_node);
        artifacts.add_edge(
            blob1_node_idx,
            prod1_node_idx,
            ArtifactRelation::ProducedFrom("input".into())).unwrap();
        // Blob 2
        let blob2_node = ArtifactDescription {
            name: Some("Test Blob 2".into()),
            dtype: "Blob".into()
        };
        let blob2_node_idx = artifacts.add_node(blob2_node);
        artifacts.add_edge(
            prod1_node_idx,
            blob2_node_idx,
            ArtifactRelation::ProducedFrom("output".into())).unwrap();
        // Prod 2
        let prod2_node = ArtifactDescription {
            name: Some("Test Producer 2".into()),
            dtype: "NegateBlobProducer".into(),
        };
        let prod2_node_idx = artifacts.add_node(prod2_node);
        artifacts.add_edge(
            blob2_node_idx,
            prod2_node_idx,
            ArtifactRelation::ProducedFrom("input".into())).unwrap();
        // Blob 3
        let blob3_node = ArtifactDescription {
            name: Some("Test Blob 3".into()),
            dtype: "Blob".into()
        };
        let blob3_node_idx = artifacts.add_node(blob3_node);
        artifacts.add_edge(
            prod2_node_idx,
            blob3_node_idx,
            ArtifactRelation::ProducedFrom("output".into())).unwrap();

        let mut ag_desc = ArtifactGraphDescription {
            artifacts: artifacts,
        };

        match partitioning {
            Some(part_desc) => ag_desc.add_uniform_partitioning(part_desc),
            None => ag_desc.add_unary_partitioning(),
        }

        // Do not set up partitioning for these.
        // Tracking Branch Producer
        let tbp_node = ArtifactDescription {
            name: Some("TBP".into()),
            dtype: "TrackingBranchProducer".into(),
        };
        let tbp_node_idx = ag_desc.artifacts.add_node(tbp_node);
        let tracked_arts = [blob1_node_idx, blob2_node_idx, blob3_node_idx];
        for &tracked_idx in &tracked_arts {
            ag_desc.artifacts.add_edge(
                tracked_idx,
                tbp_node_idx,
                ArtifactRelation::ProducedFrom("tracked".into())).unwrap();
        }
        // Tracking ref
        let ref_node = ArtifactDescription {
            name: Some("blobs".into()),
            dtype: "Ref".into(),
        };
        let ref_node_idx = ag_desc.artifacts.add_node(ref_node);
        ag_desc.artifacts.add_edge(
            tbp_node_idx,
            ref_node_idx,
            ArtifactRelation::ProducedFrom("output".into())).unwrap();
        for &tracked_idx in &tracked_arts {
            ag_desc.artifacts.add_edge(
                tracked_idx,
                ref_node_idx,
                ArtifactRelation::DtypeDepends(DatatypeRelation {
                    name: "ref".into()
                })).unwrap();
        }

        let (ag, idx_map) = ArtifactGraph::from_description(&ag_desc, dtypes_registry);

        let idxs = [
            *idx_map.get(&blob1_node_idx).unwrap(),
            *idx_map.get(&prod1_node_idx).unwrap(),
            *idx_map.get(&blob2_node_idx).unwrap(),
            *idx_map.get(&prod2_node_idx).unwrap(),
            *idx_map.get(&blob3_node_idx).unwrap(),
            *idx_map.get(&tbp_node_idx).unwrap(),
            *idx_map.get(&ref_node_idx).unwrap(),
        ];

        (ag, idxs)
    }

    #[test]
    fn test_postgres_create_get_artifact_graph() {

        let store = Store::Postgres;

        let dtypes_registry = ::datatype::tests::init_dtypes_registry::<TestDatatypes>();
        let repo_control = ::repo::tests::init_repo(store, &dtypes_registry);

        let mut context = Context {dtypes_registry, repo_control};

        let (ag, _) = simple_blob_prod_ag_fixture(&context.dtypes_registry, None);

        // let model = context.dtypes_registry.types.get("ArtifactGraph").expect()
        let mut model_ctrl = model_controller(store);

        model_ctrl.create_artifact_graph(&mut context.repo_control, &ag).unwrap();

        let ag2 = model_ctrl.get_artifact_graph(&mut context.repo_control, &context.dtypes_registry, &ag.id)
                            .unwrap();
        assert!(ag2.verify_hash());
        assert_eq!(ag.id.hash, ag2.id.hash);
    }

    #[test]
    fn test_postgres_create_get_version_graph() {

        let store = Store::Postgres;

        let dtypes_registry = ::datatype::tests::init_dtypes_registry::<TestDatatypes>();
        let repo_control = ::repo::tests::init_repo(store, &dtypes_registry);

        let mut context = Context {dtypes_registry, repo_control};

        let (ag, idxs) = simple_blob_prod_ag_fixture(&context.dtypes_registry, None);

        // let model = context.dtypes_registry.types.get("ArtifactGraph").expect()
        let mut model_ctrl = model_controller(store);

        model_ctrl.create_artifact_graph(&mut context.repo_control, &ag).unwrap();

        let mut ver_graph = VersionGraph::new_from_source_artifacts(&ag);

        // TODO: most of this test should eventually fail because no versions
        // are being committed.
        for node_idx in ver_graph.versions.graph().node_indices() {
            model_ctrl.create_staging_version(
                &mut context.repo_control,
                &ver_graph,
                node_idx.clone()).unwrap();
        }

        let up_idx = ver_graph.versions.graph().node_indices().next().unwrap();
        let (up_art_idx, _) = ag.get_by_id(&ver_graph[up_idx].artifact.id).unwrap();

        let blob1_art_idx = idxs[0];
        let blob1_art = &ag[blob1_art_idx];
        let blob1_ver = Version::new(blob1_art, RepresentationKind::State);
        let blob1_ver_idx = ver_graph.versions.add_node(blob1_ver);
        ver_graph.versions.add_edge(up_idx, blob1_ver_idx,
            VersionRelation::Dependence(
                &ag[ag.artifacts.find_edge(up_art_idx, blob1_art_idx).unwrap()])).unwrap();

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            blob1_ver_idx.clone()).unwrap();

        let (_, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx)
            .expect("Partitioning version missing");
        let ver_part_control: Box<::datatype::interface::PartitioningController> =
                context.dtypes_registry
                                      .get_model(&ver_partitioning.artifact.dtype.name)
                                      .interface_controller(store, "Partitioning")
                                      .expect("Partitioning must have controller for store")
                                      .into();

        let mut blob_control = ::datatype::blob::model_controller(store);
        let ver_blob_real = &ver_graph[blob1_ver_idx];
        let fake_blob = ::datatype::Payload::State(vec![0, 1, 2, 3, 4, 5, 6]);
        let ver_hunks = ver_part_control
                .get_partition_ids(&mut context.repo_control, ver_partitioning)
                .iter()
                .map(|partition_id| Hunk {
                    id: Identity {
                        uuid: Uuid::new_v4(),
                        hash: blob_control.hash_payload(&fake_blob),
                    },
                    version: ver_blob_real,
                    partition: Partition {
                        partitioning: ver_partitioning,
                        index: partition_id.to_owned(),
                    },
                    representation: RepresentationKind::State,
                    completion: PartCompletion::Complete,
                    precedence: None,
                }).collect::<Vec<_>>();

        // Can't do this in an iterator because of borrow conflict on context?
        for hunk in &ver_hunks {
            model_ctrl.create_hunk(
                &mut context.repo_control,
                &hunk).unwrap();
            blob_control.write_hunk(
                &mut context.repo_control,
                &hunk,
                &fake_blob).unwrap();
        }

        for hunk in &ver_hunks {
            let blob = blob_control.read_hunk(&mut context.repo_control, &hunk).unwrap();
            assert_eq!(blob, fake_blob);
        }

        let (_, ver_graph2) = model_ctrl.get_version(
            &mut context.repo_control,
            &ag,
            &ver_blob_real.id).unwrap();

        assert!(petgraph::algo::is_isomorphic_matching(
            &ver_graph.versions.graph(),
            &ver_graph2.versions.graph(),
            |a, b| a.id == b.id,
            |_, _| true));
    }

    #[test]
    fn test_postgres_production() {

        let store = Store::Postgres;

        let dtypes_registry = ::datatype::tests::init_dtypes_registry::<TestDatatypes>();
        let repo_control = ::repo::tests::init_repo(store, &dtypes_registry);

        let mut context = Context {dtypes_registry, repo_control};

        let partitioning = ArtifactDescription {
            name: Some("Arbitrary Partitioning".into()),
            dtype: "ArbitraryPartitioning".into(),
        };
        let (ag, idxs) = simple_blob_prod_ag_fixture(&context.dtypes_registry, Some(partitioning));

        let mut model_ctrl = model_controller(store);

        model_ctrl.create_artifact_graph(&mut context.repo_control, &ag).unwrap();
        model_ctrl.write_production_policies(
            &mut context.repo_control,
            &ag[idxs[5]],
            EnumSet::from_iter(
                vec![ProductionPolicies::LeafBootstrap, ProductionPolicies::Custom]
                .into_iter())).unwrap();

        let mut ver_graph = VersionGraph::new_from_source_artifacts(&ag);

        let part_idx = ver_graph.versions.graph().node_indices().next().unwrap();
        let (part_art_idx, _) = ag.get_by_id(&ver_graph[part_idx].artifact.id).unwrap();

        // Create arbitrary partitions.
        {
            let mut part_control = ::datatype::partitioning::arbitrary::model_controller(store);

            model_ctrl.create_staging_version(
                &mut context.repo_control,
                &ver_graph,
                part_idx).expect("TODO");
            part_control.write(
                &mut context.repo_control,
                &ver_graph[part_idx],
                &[0, 1]).expect("TODO");
            model_ctrl.commit_version(
                &context.dtypes_registry,
                &mut context.repo_control,
                &ag,
                &mut ver_graph,
                part_idx).expect("TODO");
        }

        let blob1_art_idx = idxs[0];
        let blob1_art = &ag[blob1_art_idx];
        let blob1_ver = Version::new(blob1_art, RepresentationKind::State);
        let blob1_ver_idx = ver_graph.versions.add_node(blob1_ver);
        ver_graph.versions.add_edge(part_idx, blob1_ver_idx,
            VersionRelation::Dependence(
                &ag[ag.artifacts.find_edge(part_art_idx, blob1_art_idx).unwrap()])).unwrap();

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            blob1_ver_idx.clone()).unwrap();

        let ver_hash = {
            let (_, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx).unwrap();
            let ver_part_control: Box<::datatype::interface::PartitioningController> =
                    context.dtypes_registry
                                          .get_model(&ver_partitioning.artifact.dtype.name)
                                          .interface_controller(store, "Partitioning")
                                          .expect("Partitioning must have controller for store")
                                          .into();

            let mut blob_control = ::datatype::blob::model_controller(store);
            let ver_blob_real = &ver_graph[blob1_ver_idx];
            let fake_blob = ::datatype::Payload::State(vec![0, 1, 2, 3, 4, 5, 6]);
            let ver_hunks = ver_part_control
                    // Note that this is in ascending order, so version hash
                    // is correct.
                    .get_partition_ids(&mut context.repo_control, ver_partitioning)
                    .iter()
                    .map(|partition_id| Hunk {
                        id: Identity {
                            uuid: Uuid::new_v4(),
                            hash: blob_control.hash_payload(&fake_blob),
                        },
                        version: ver_blob_real,
                        partition: Partition {
                            partitioning: ver_partitioning,
                            index: partition_id.to_owned(),
                        },
                        representation: RepresentationKind::State,
                        completion: PartCompletion::Complete,
                        precedence: None,
                    }).collect::<Vec<_>>();
            let ver_hash = ver_hunks.iter()
                .fold(
                    DefaultHasher::new(),
                    |mut s, hunk| {hunk.id.hash.hash(&mut s); s})
                .finish();

            // Can't do this in an iterator because of borrow conflict on context?
            for hunk in &ver_hunks {
                model_ctrl.create_hunk(
                    &mut context.repo_control,
                    &hunk).unwrap();
                blob_control.write_hunk(
                    &mut context.repo_control,
                    &hunk,
                    &fake_blob).unwrap();
            }

            ver_hash
        };

        ver_graph[blob1_ver_idx].id.hash = ver_hash;

        model_ctrl.commit_version(
            &context.dtypes_registry,
            &mut context.repo_control,
            &ag,
            &mut ver_graph,
            blob1_ver_idx).expect("Commit blob failed");

        let vg2 = model_ctrl.get_version_graph(
            &mut context.repo_control,
            &ag).unwrap();

        println!("{:?}", petgraph::dot::Dot::new(&vg2.versions.graph()));

        let blob1_vg2_idxs = vg2.artifact_versions(&ag[idxs[0]]);
        let blob2_vg2_idxs = vg2.artifact_versions(&ag[idxs[2]]);
        let blob3_vg2_idxs = vg2.artifact_versions(&ag[idxs[4]]);

        assert_eq!(blob2_vg2_idxs.len(), 1);
        assert_eq!(blob3_vg2_idxs.len(), 1);

        assert_eq!(
            vg2[blob1_vg2_idxs[0]].id.hash,
            vg2[blob3_vg2_idxs[0]].id.hash,
            "Version hashes for original and double-negated blob should match.",
            );

        // Test delta state updates.
        let blob1_ver2 = Version::new(blob1_art, RepresentationKind::Delta);
        let blob1_ver2_idx = ver_graph.versions.add_node(blob1_ver2);
        ver_graph.versions.add_edge(part_idx, blob1_ver2_idx,
            VersionRelation::Dependence(
                &ag[ag.artifacts.find_edge(part_art_idx, blob1_art_idx).unwrap()])).unwrap();
        ver_graph.versions.add_edge(blob1_ver_idx, blob1_ver2_idx, VersionRelation::Parent).unwrap();

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            blob1_ver2_idx.clone()).unwrap();

        let ver2_hash = {
            let (_, ver_partitioning) = ver_graph.get_partitioning(blob1_ver2_idx).unwrap();
            let ver_part_control: Box<::datatype::interface::PartitioningController> =
                    context.dtypes_registry
                                          .get_model(&ver_partitioning.artifact.dtype.name)
                                          .interface_controller(store, "Partitioning")
                                          .expect("Partitioning must have controller for store")
                                          .into();

            let mut blob_control = ::datatype::blob::model_controller(store);
            let ver_blob_real = &ver_graph[blob1_ver2_idx];
            let fake_blob = ::datatype::Payload::Delta((vec![1, 6], vec![7, 8]));
            let ver_hunks = ver_part_control
                    // Note that this is in ascending order, so version hash
                    // is correct.
                    .get_partition_ids(&mut context.repo_control, ver_partitioning)
                    .iter()
                    .take(1)
                    .map(|partition_id| Hunk {
                        id: Identity {
                            uuid: Uuid::new_v4(),
                            hash: blob_control.hash_payload(&fake_blob),
                        },
                        version: ver_blob_real,
                        partition: Partition {
                            partitioning: ver_partitioning,
                            index: partition_id.to_owned(),
                        },
                        representation: RepresentationKind::Delta,
                        completion: PartCompletion::Complete,
                        precedence: None,
                    }).collect::<Vec<_>>();
            let ver_hash = ver_hunks.iter()
                .fold(
                    DefaultHasher::new(),
                    |mut s, hunk| {hunk.id.hash.hash(&mut s); s})
                .finish();

            for hunk in &ver_hunks {
                model_ctrl.create_hunk(
                    &mut context.repo_control,
                    &hunk).unwrap();
                blob_control.write_hunk(
                    &mut context.repo_control,
                    &hunk,
                    &fake_blob).unwrap();
            }

            ver_hash
        };

        ver_graph[blob1_ver2_idx].id.hash = ver2_hash;

        model_ctrl.commit_version(
            &context.dtypes_registry,
            &mut context.repo_control,
            &ag,
            &mut ver_graph,
            blob1_ver2_idx).expect("Commit blob delta failed");

        let vg3 = model_ctrl.get_version_graph(
            &mut context.repo_control,
            &ag).unwrap();

        println!("{:?}", petgraph::dot::Dot::new(&vg3.versions.graph()));

        let blob1_vg3_idxs = vg3.artifact_versions(&ag[idxs[0]]);
        let blob2_vg3_idxs = vg3.artifact_versions(&ag[idxs[2]]);
        let blob3_vg3_idxs = vg3.artifact_versions(&ag[idxs[4]]);

        assert_eq!(blob2_vg3_idxs.len(), 2);
        assert_eq!(blob3_vg3_idxs.len(), 2);

        assert_eq!(
            vg3[blob1_vg3_idxs[1]].id.hash,
            vg3[blob3_vg3_idxs[1]].id.hash,
            "Version hashes for original and double-negated blob should match.",
            );

        {
            use datatype::interface::PartitioningController;
            let part_control = ::datatype::partitioning::arbitrary::model_controller(store);
            let (_, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx).unwrap();
            let part_ids = part_control.get_partition_ids(&mut context.repo_control, ver_partitioning);

            let map1 = model_ctrl.get_composition_map(
                &mut context.repo_control,
                &vg3,
                blob1_vg3_idxs[1],
                part_ids.clone(),
            ).unwrap();
            let map3 = model_ctrl.get_composition_map(
                &mut context.repo_control,
                &vg3,
                blob3_vg3_idxs[1],
                part_ids,
            ).unwrap();
            let blob_control = ::datatype::blob::model_controller(store);

            for (p_id, blob1_comp) in &map1 {
                let blob3_comp = &map3[p_id];

                let blob1_state = blob_control.get_composite_state(
                    &mut context.repo_control,
                    blob1_comp).unwrap();
                let blob3_state = blob_control.get_composite_state(
                    &mut context.repo_control,
                    blob3_comp).unwrap();

                assert_eq!(blob1_state, blob3_state, "Blob states do not match");
            }
        }

        {
            use std::str::FromStr;
            use datatype::reference::VersionSpecifier;
            use datatype::reference::ModelController as RefModelController;
            let ref_control = ::datatype::reference::model_controller(context.repo_control.store());
            assert_eq!(
                vg3[blob3_vg3_idxs[1]].id,
                ref_control.get_version_id(
                    &mut context.repo_control,
                    &VersionSpecifier::from_str("blobs/master/Test Blob 3").unwrap()).unwrap(),
                "Tracking branch has wrong version for Blob 3.");
        }
    }

    #[test]
    fn test_production_version_specs() {
      let a = ProductionDependencySpec {
          version: VersionGraphIndex::new(0),
          relation: ArtifactGraphEdgeIndex::new(0)
      };
      let b = ProductionDependencySpec {
          version: VersionGraphIndex::new(1),
          relation: ArtifactGraphEdgeIndex::new(1)
      };
      let c = ProductionDependencySpec {
          version: VersionGraphIndex::new(2),
          relation: ArtifactGraphEdgeIndex::new(0)
      };

      let mut specs_a = ProductionVersionSpecs::default();
      specs_a.insert(btreeset![a.clone(), b.clone()], Some(VersionGraphIndex::new(0)));
      specs_a.insert(btreeset![a.clone(), b.clone()], None);
      specs_a.insert(btreeset![c.clone(), b.clone()], Some(VersionGraphIndex::new(1)));

      assert!(specs_a.specs[&btreeset![a.clone(), b.clone()]].contains(&None));
      assert!(specs_a.specs[&btreeset![a.clone(), b.clone()]]
        .contains(&Some(VersionGraphIndex::new(0))));

      let mut specs_b = ProductionVersionSpecs::default();
      specs_b.insert(btreeset![c.clone(), b.clone()], Some(VersionGraphIndex::new(2)));

      specs_a.merge(specs_b);

      assert!(specs_a.specs[&btreeset![c.clone(), b.clone()]]
        .contains(&Some(VersionGraphIndex::new(1))));
      assert!(specs_a.specs[&btreeset![c.clone(), b.clone()]]
        .contains(&Some(VersionGraphIndex::new(2))));
    }
}
