extern crate daggy;
extern crate petgraph;
extern crate schemer;
extern crate serde;
extern crate serde_json;
extern crate uuid;
extern crate postgres;


use std::collections::{BTreeMap, BTreeSet, HashMap};

use daggy::petgraph::visit::EdgeRef;
use daggy::Walker;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;

use ::{
    Artifact, ArtifactGraph, ArtifactGraphIndex, ArtifactGraphEdgeIndex,
    ArtifactRelation,
    DatatypeRelation, RepresentationKind, Error, Hunk, Identity,
    IdentifiableGraph,
    PartCompletion, Partition,
    Version, VersionGraph, VersionGraphIndex,
    VersionRelation, VersionStatus};
use super::{
    DatatypeEnum, DatatypesRegistry,
    Description, Store};
use ::datatype::interface::ProductionOutput;
use ::repo::{PostgresMigratable};


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
enum PolicyProducerRequirements {
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
enum PolicyDependencyRequirements {
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
    producer: PolicyProducerRequirements,
    dependency: PolicyDependencyRequirements,
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
pub struct ProductionVersionSpecs {
    specs: HashMap<ProductionDependenciesSpecs, BTreeSet<Option<VersionGraphIndex>>>,
}

impl ProductionVersionSpecs {
    pub fn new() -> Self {
        ProductionVersionSpecs {
            specs: HashMap::new(),
        }
    }

    pub fn insert(&mut self, spec: ProductionDependenciesSpecs, parent: Option<VersionGraphIndex>) {
        self.specs.entry(spec)
            .or_insert_with(|| BTreeSet::new())
            .insert(parent);
    }

    pub fn merge(&mut self, other: ProductionVersionSpecs) {
        for (k, mut v) in other.specs {
            self.specs.entry(k)
                .and_modify(|existing| existing.append(&mut v))
                .or_insert(v);
        }
    }
}

/// Enacts a policy for what new versions to produce in response to updated
/// dependency versions.
trait ProductionPolicy {
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
        let new_ver_node = ver_graph.versions.node_weight(v_idx)
                                             .expect("Non-existent version");
        let mut specs = ProductionVersionSpecs::new();

        // TODO

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
        let mut specs = ProductionVersionSpecs::new();
        let prod_art = art_graph.artifacts.node_weight(p_art_idx).expect("Non-existent producer");

        // Any version of this producer already exists.
        if !ver_graph.artifact_versions(prod_art).is_empty() {
            return specs
        }

        let mut dependencies = ProductionDependenciesSpecs::new();
        for (e_idx, d_idx) in art_graph.artifacts.parents(p_art_idx).iter(&art_graph.artifacts) {
            let dependency = art_graph.artifacts.node_weight(d_idx)
                .expect("Impossible: indices from this graph");
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
            From<<T as DatatypeEnum>::InterfaceControllerType> {
        let outputs = self.notify_producers(
            dtypes_registry,
            repo_control,
            art_graph,
            ver_graph,
            seed_v_idx)?;

        for (_, output) in &outputs {
            if let &ProductionOutput::Synchronous(ref v_idxs) = output {
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
            From<<T as DatatypeEnum>::InterfaceControllerType> {
        let production_policies: Vec<Box<ProductionPolicy>> = vec![
            Box::new(ExtantProductionPolicy),
            Box::new(LeafBootstrapProductionPolicy),
        ];

        let production_policy_reqs = production_policies
            .iter()
            .map(|policy| policy.requirements())
            .fold(
                ProductionPolicyRequirements::default(),
                |mut max, ref p| {
                    max.producer = max.producer.max(p.producer.clone());
                    max.dependency = max.dependency.max(p.dependency.clone());
                    max
                });

        let (ver_art_idx, _) = {
            let new_ver = ver_graph.versions.node_weight(v_idx).expect("TODO");
            art_graph.find_by_id(&new_ver.artifact.id)
                .expect("TODO: Unknown artifact")
        };

        let dependent_arts = art_graph.artifacts.children(ver_art_idx).iter(&art_graph.artifacts);

        let mut new_prod_vers = HashMap::new();

        for (e_idx, dep_art_idx) in dependent_arts {
            let dependent = art_graph.artifacts.node_weight(dep_art_idx)
                                               .expect("Impossible: indices from this graph");
            let dtype = dependent.dtype;
            if let Some(producer_interface) = dtypes_registry.models
                    .get(&dtype.name)
                    .expect("Datatype must be known")
                    .as_model()
                    .interface_controller(repo_control.store(), "Producer") {
                let producer_controller: Box<::datatype::interface::ProducerController> =
                    producer_interface.into();

                self.fulfill_policy_requirements(
                    repo_control,
                    art_graph,
                    ver_graph,
                    v_idx,
                    dep_art_idx,
                    &production_policy_reqs)?;

                let prod_specs = production_policies
                    .iter()
                    .map(|policy| policy.new_production_version_specs(
                        art_graph,
                        ver_graph,
                        v_idx,
                        dep_art_idx))
                    .fold(
                        ProductionVersionSpecs::new(),
                        |mut specs, x| {specs.merge(x); specs});

                for (specs, parent_prod_vers) in &prod_specs.specs {
                    let new_prod_ver = Version {
                        id: Identity {uuid: Uuid::new_v4(), hash: 0},
                        artifact: dependent,
                        status: VersionStatus::Staging,
                        representation: RepresentationKind::State,
                    };
                    let new_prod_ver_id = new_prod_ver.id.clone();
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
                        if let &Some(ref idx) = parent_ver {
                            ver_graph.versions.add_edge(
                                *idx,
                                new_prod_ver_idx,
                                VersionRelation::Parent)?;
                        }
                    }

                    self.create_staging_version(
                        repo_control,
                        ver_graph,
                        new_prod_ver_idx.clone())?;

                    let output = producer_controller.notify_new_version(
                        repo_control,
                        art_graph,
                        ver_graph,
                        new_prod_ver_idx.clone())?;

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

    fn get_hunks<'a, 'b, 'c, 'd>(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &'d Version<'a, 'b>,
        partitioning: &'c Version<'a, 'b>
    ) -> Result<Vec<Hunk<'a, 'b, 'c, 'd>>, Error>;
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
                .expect("TODO: not a DAG")
                .into_iter() {
            if node_idx == part_idx {
                continue;
            }
            let has_partitioning = self.artifacts.parents(node_idx).iter(&self.artifacts)
                .fold(false, |hp, (e_idx, p_idx)| {
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


struct PostgresStore {}

impl PostgresStore {
    /// Load version rows from a query result into a version graph.
    fn load_version_rows<'a, 'b>(
        &self,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        rows: &postgres::rows::Rows,
    ) -> Result<BTreeMap<i64, VersionGraphIndex>, Error> {
        // TODO: not using hash. See other comments.
        let mut idx_map = BTreeMap::new();

        enum VerNodeRow {
            ID = 0,
            UUID,
            Hash,
            Status,
            Representation,
            ArtifactUUID,
            ArtifactHash,
        };

        for ver_node_row in rows {
            let ver_node_id: i64 = ver_node_row.get(VerNodeRow::ID as usize);
            let an_id = Identity {
                uuid: ver_node_row.get(VerNodeRow::ArtifactUUID as usize),
                hash: ver_node_row.get::<_, i64>(VerNodeRow::ArtifactHash as usize) as u64,
            };
            let (art_idx, art) = art_graph.find_by_id(&an_id).expect("Version references unkown artifact");

            let ver_id = Identity {
                uuid: ver_node_row.get(VerNodeRow::UUID as usize),
                hash: ver_node_row.get::<_, i64>(VerNodeRow::Hash as usize) as u64,
            };

            let ver_node_idx = ver_graph.emplace(
                &ver_id,
                || Version {
                    id: ver_id,
                    artifact: art,
                    status: ver_node_row.get(VerNodeRow::Status as usize),
                    representation: ver_node_row.get(VerNodeRow::Representation as usize),
                });
            idx_map.insert(ver_node_id, ver_node_idx);
        }

        Ok(idx_map)
    }

    /// Postgres-specific method for adding version relations for a set of
    /// database IDs to a version graph.
    fn get_version_relations<'a, 'b>(
        &self,
        trans: &Transaction,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_db_ids: &Vec<i64>,
        idx_map: &mut BTreeMap<i64, VersionGraphIndex>,
        ancestry_direction: Option<petgraph::Direction>,
        dependence_direction: Option<petgraph::Direction>,
    ) -> Result<(), Error> {

        enum AncNodeRow {
            ID = 0,
            UUID,
            Hash,
            Status,
            Representation,
            ArtifactUUID,
            ArtifactHash,
            ParentID,
            ChildID,
        };
        let ancestry_join = match ancestry_direction {
            Some(petgraph::Direction::Incoming) =>
                "vp.child_id = ANY($1::bigint[]) AND v.id = vp.parent_id",
            Some(petgraph::Direction::Outgoing) =>
                "vp.parent_id = ANY($1::bigint[]) AND v.id = vp.child_id",
            None =>
                "(vp.child_id = ANY($1::bigint[]) AND v.id = vp.parent_id) \
                OR (vp.parent_id = ANY($1::bigint[]) AND v.id = vp.child_id)"
        };
        let ancestry_node_rows = trans.query(
            &format!(r#"
                SELECT
                  v.id, v.uuid_, v.hash, v.status, v.representation,
                  a.uuid_, a.hash, vp.parent_id, vp.child_id
                FROM version_parent vp
                JOIN version v
                  ON ({})
                JOIN artifact a ON a.id = v.artifact_id;
            "#, ancestry_join),
            &[v_db_ids])?;
        for row in &ancestry_node_rows {
            let db_id = row.get::<_, i64>(AncNodeRow::ID as usize);
            let an_id = Identity {
                uuid: row.get(AncNodeRow::ArtifactUUID as usize),
                hash: row.get::<_, i64>(AncNodeRow::ArtifactHash as usize) as u64,
            };

            if !idx_map.contains_key(&db_id) {
                let v_id = Identity {
                    uuid: row.get(AncNodeRow::UUID as usize),
                    hash: row.get::<_, i64>(AncNodeRow::Hash as usize) as u64,
                };

                let v_idx = ver_graph.emplace(
                    &v_id,
                    || Version {
                        id: v_id,
                        artifact: art_graph.find_by_id(&an_id).expect("Version references unkown artifact").1,
                        status: row.get(AncNodeRow::Status as usize),
                        representation: row.get(AncNodeRow::Representation as usize),
                    });

                idx_map.insert(db_id, v_idx);
            }

            let edge = VersionRelation::Parent;
            let parent_idx = idx_map.get(&row.get(AncNodeRow::ParentID as usize)).expect("Graph is malformed.");
            let child_idx = idx_map.get(&row.get(AncNodeRow::ChildID as usize)).expect("Graph is malformed.");
            ver_graph.versions.add_edge(*parent_idx, *child_idx, edge)?;
        }

        enum DepNodeRow {
            ID = 0,
            UUID,
            Hash,
            Status,
            Representation,
            ArtifactUUID,
            ArtifactHash,
            SourceID,
            DependentID,
        };
        let dependence_join = match dependence_direction {
            Some(petgraph::Direction::Incoming) =>
                "vr.dependent_version_id = ANY($1::bigint[]) AND v.id = vr.source_version_id",
            Some(petgraph::Direction::Outgoing) =>
                "vr.source_version_id = ANY($1::bigint[]) AND v.id = vr.dependent_version_id",
            None =>
                "(vr.dependent_version_id = ANY($1::bigint[]) AND v.id = vr.source_version_id) \
                OR (vr.source_version_id = ANY($1::bigint[]) AND v.id = vr.dependent_version_id)"
        };
        let dependence_node_rows = trans.query(
            &format!(r#"
                SELECT
                  v.id, v.uuid_, v.hash, v.status, v.representation,
                  a.uuid_, a.hash,
                  vr.source_version_id, vr.dependent_version_id
                FROM version_relation vr
                JOIN version v
                  ON ({})
                JOIN artifact a ON a.id = v.artifact_id;
            "#, dependence_join),
            &[v_db_ids])?;
        for row in &dependence_node_rows {
            let db_id = row.get::<_, i64>(DepNodeRow::ID as usize);
            let an_id = Identity {
                uuid: row.get(DepNodeRow::ArtifactUUID as usize),
                hash: row.get::<_, i64>(DepNodeRow::ArtifactHash as usize) as u64,
            };
            let (an_idx, an) = art_graph.find_by_id(&an_id).expect("Version references unkown artifact");

            let v_idx = *idx_map.entry(db_id)
                .or_insert_with(|| {
                    let v_id = Identity {
                        uuid: row.get(DepNodeRow::UUID as usize),
                        hash: row.get::<_, i64>(DepNodeRow::Hash as usize) as u64,
                    };

                    ver_graph.emplace(
                        &v_id,
                        || Version {
                            id: v_id,
                            artifact: an,
                            status: row.get(DepNodeRow::Status as usize),
                            representation: row.get(DepNodeRow::Representation as usize),
                        })
                });

            let inbound_existing = row.get::<_, i64>(DepNodeRow::SourceID as usize) == db_id;
            let other_v_db_id = if inbound_existing
                {row.get(DepNodeRow::DependentID as usize)}
                else {row.get(DepNodeRow::SourceID as usize)};
            let other_v_idx = *idx_map.get(&other_v_db_id).expect("Relation with version not in graph");
            let other_art = ver_graph.versions.node_weight(other_v_idx).expect("Impossible").artifact;
            let other_art_idx = art_graph.find_by_id(&other_art.id)
                .expect("Unknown artifact").0;

            let art_rel_idx = if inbound_existing {
                art_graph.artifacts.find_edge(an_idx, other_art_idx)
            } else {
                art_graph.artifacts.find_edge(other_art_idx, an_idx)
            }.expect("Version graph references unknown artifact relation");

            let art_rel = art_graph.artifacts.edge_weight(art_rel_idx).expect("Graph is malformed");
            let edge = VersionRelation::Dependence(art_rel);
            let (parent_idx, child_idx) = if inbound_existing {
                (v_idx, other_v_idx)
            } else {
                (other_v_idx, v_idx)
            };
            ver_graph.versions.add_edge(parent_idx, child_idx, edge)?;
        }

        Ok(())
    }
}

struct PGMigrationArtifactGraphs;
migration!(
    PGMigrationArtifactGraphs,
    "7d1fb6d1-a1b0-4bd4-aa6d-e3ee71c4353b",
    ["acda147a-552f-42a5-bb2b-1ba05d41ec03",],
    "create artifact graph table");

impl PostgresMigration for PGMigrationArtifactGraphs {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/artifact_graph_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/artifact_graph_0001.down.sql"))
    }
}


impl super::MetaController for PostgresStore {
    // fn register_with_repo(&self, repo_controller: &mut PostgresRepoController) {
    //     repo_controller.register_postgres_migratable(Box::new(*self));
    // }
}

impl PostgresMigratable for PostgresStore {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![
            Box::new(PGMigrationArtifactGraphs),
        ]
    }
}

impl super::PostgresMetaController for PostgresStore {}

impl ModelController for PostgresStore {
    fn list_graphs(&self) -> Vec<Identity> {
        unimplemented!()
    }

    fn create_artifact_graph(
            &mut self,
            repo_control: &mut ::repo::StoreRepoController,
            art_graph: &ArtifactGraph) -> Result<(), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let ag_id_row = trans.query(r#"
                INSERT INTO artifact_graph (uuid_, hash)
                VALUES ($1, $2) RETURNING id;
            "#, &[&art_graph.id.uuid, &(art_graph.id.hash as i64)])?;
        // let ag_id = ag_id_row.into_iter().nth(0).ok_or(Error::Store("Insert failed.".into()))?;
        let ag_id: i64 = ag_id_row.get(0).get(0);

        let mut id_map = HashMap::new();
        let insert_artifact = trans.prepare(r#"
                INSERT INTO artifact (uuid_, hash, artifact_graph_id, name, datatype_id)
                SELECT r.uuid_, r.hash, r.ag_id, r.name, d.id
                FROM (VALUES ($1::uuid, $2::bigint, $3::bigint, $4::text))
                  AS r (uuid_, hash, ag_id, name)
                JOIN datatype d ON d.name = $5
                RETURNING id;
            "#)?;

        for idx in art_graph.artifacts.graph().node_indices() {
            let art = art_graph.artifacts.node_weight(idx).unwrap();
            let node_id_row = insert_artifact.query(&[
                        &art.id.uuid, &(art.id.hash as i64), &ag_id,
                        &art.name, &art.dtype.name])?;
            let node_id: i64 = node_id_row.get(0).get(0);

            id_map.insert(idx, node_id);
        }

        let art_prod_edge = trans.prepare(r#"
                INSERT INTO artifact_edge (source_id, dependent_id, name, edge_type)
                VALUES ($1, $2, $3, 'producer');
            "#)?;
        let art_dtype_edge = trans.prepare(r#"
                INSERT INTO artifact_edge (source_id, dependent_id, name, edge_type)
                VALUES ($1, $2, $3, 'dtype');
            "#)?;

        for e in art_graph.artifacts.graph().edge_references() {
            let source_id = id_map.get(&e.source()).expect("Graph is malformed.");
            let dependent_id = id_map.get(&e.target()).expect("Graph is malformed.");
            match e.weight() {
                &ArtifactRelation::DtypeDepends(ref dtype_rel) =>
                    art_dtype_edge.execute(&[&source_id, &dependent_id, &dtype_rel.name])?,
                &ArtifactRelation::ProducedFrom(ref name) =>
                    art_prod_edge.execute(&[&source_id, &dependent_id, name])?,
            };
        }

        trans.set_commit();
        Ok(())
    }

    fn get_artifact_graph<'a, T: DatatypeEnum>(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            dtypes_registry: &'a DatatypesRegistry<T>,
            id: &Identity) -> Result<ArtifactGraph<'a>, Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        // TODO: not using the identity hash. Requires some decisions about how
        // to handle get-by-UUID vs. get-with-verified-hash.
        let ag_rows = trans.query(r#"
                SELECT id, uuid_, hash
                FROM artifact_graph
                WHERE uuid_ = $1::uuid
            "#, &[&id.uuid])?;
        let ag_row = ag_rows.get(0);
        let ag_id = Identity {
            uuid: ag_row.get(1),
            hash: ag_row.get::<_, i64>(2) as u64,
        };

        enum NodeRow {
            ID = 0,
            UUID,
            Hash,
            ArtifactName,
            DatatypeName,
        };
        let nodes = trans.query(r#"
                SELECT
                    a.id,
                    a.uuid_,
                    a.hash,
                    a.name,
                    d.name
                FROM artifact a
                JOIN datatype d ON a.datatype_id = d.id
                WHERE a.artifact_graph_id = $1;
            "#, &[&ag_row.get::<_, i64>(0)])?;

        let mut artifacts = ::ArtifactGraphType::new();
        let mut idx_map = HashMap::new();
        let mut up_idx = None;

        for row in &nodes {
            let db_id = row.get::<_, i64>(NodeRow::ID as usize);
            let id = Identity {
                uuid: row.get(NodeRow::UUID as usize),
                hash: row.get::<_, i64>(NodeRow::Hash as usize) as u64,
            };
            let dtype_name = &row.get::<_, String>(NodeRow::DatatypeName as usize);
            let node = Artifact {
                id: id,
                name: row.get(NodeRow::ArtifactName as usize),
                dtype: dtypes_registry.get_datatype(dtype_name)
                                      .expect("Unknown datatype."),
            };

            let node_idx = artifacts.add_node(node);
            idx_map.insert(db_id, node_idx);
            if dtype_name == "UnaryPartitioning" {
                up_idx = Some(node_idx);
            }
        }

        enum EdgeRow {
            SourceID = 0,
            DependentID,
            Name,
            EdgeType,
        };
        let edges = trans.query(r#"
                SELECT
                    ae.source_id,
                    ae.dependent_id,
                    ae.name,
                    ae.edge_type::text
                FROM artifact_edge ae
                WHERE ae.source_id = ANY($1::bigint[]);
            "#, &[&idx_map.keys().collect::<Vec<_>>()])?;

        for e in &edges {
            let relation = match e.get::<_, String>(EdgeRow::EdgeType as usize).as_ref() {
                "producer" => ArtifactRelation::ProducedFrom(e.get(EdgeRow::Name as usize)),
                "dtype" => ArtifactRelation::DtypeDepends(DatatypeRelation {
                    name: e.get(EdgeRow::Name as usize),
                }),
                _ => return Err(Error::Store("Unknown artifact graph edge reltype.".into())),
            };

            let source_idx = idx_map.get(&e.get(EdgeRow::SourceID as usize)).expect("Graph is malformed.");
            let dependent_idx = idx_map.get(&e.get(EdgeRow::DependentID as usize)).expect("Graph is malformed.");
            artifacts.add_edge(*source_idx, *dependent_idx, relation)?;

        }

        Ok(ArtifactGraph {
            id: ag_id,
            artifacts: artifacts,
        })
    }

    fn create_staging_version(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let ver = ver_graph.versions.node_weight(v_idx).expect("Index is not in version graph");
        // TODO: should we check that hash is nil here?
        // TODO: should check that if a root version, must be State and not Delta.

        let ver_id_row = trans.query(r#"
                INSERT INTO version (uuid_, hash, artifact_id, status, representation)
                SELECT r.uuid_, r.hash, a.id, r.status, r.representation
                FROM (VALUES ($1::uuid, $2::bigint, $3::uuid,
                        $4::version_status, $5::representation_kind))
                AS r (uuid_, hash, a_uuid, status, representation)
                JOIN artifact a ON a.uuid_ = r.a_uuid
                RETURNING id;
            "#, &[&ver.id.uuid, &(ver.id.hash as i64), &ver.artifact.id.uuid,
                  &ver.status, &ver.representation])?;
        let ver_id: i64 = ver_id_row.get(0).get(0);

        let insert_parent = trans.prepare(r#"
                INSERT INTO version_parent (parent_id, child_id)
                SELECT v.id, r.child_id
                FROM (VALUES ($1::uuid, $2::bigint))
                AS r (parent_uuid, child_id)
                JOIN version v ON v.uuid_ = r.parent_uuid;
            "#)?;
        let insert_relation = trans.prepare(r#"
                INSERT INTO version_relation
                  (source_version_id, dependent_version_id, source_id, dependent_id)
                SELECT vp.id, r.child_id, vp.artifact_id, vc.artifact_id
                FROM (VALUES ($1::uuid, $2::bigint))
                AS r (parent_uuid, child_id)
                JOIN version vp ON vp.uuid_ = r.parent_uuid
                JOIN version vc ON vc.id = r.child_id;
            "#)?;

        for (e_idx, p_idx) in ver_graph.versions.parents(v_idx).iter(&ver_graph.versions) {
            let edge = ver_graph.versions.edge_weight(e_idx).expect("Graph is malformed.");
            let parent = ver_graph.versions.node_weight(p_idx).expect("Graph is malformed");
            match *edge {
                VersionRelation::Dependence(ref art_rel) => &insert_relation,
                VersionRelation::Parent => &insert_parent,
            }.execute(&[&parent.id.uuid, &ver_id])?;
        }

        trans.set_commit();
        Ok(())
    }

    fn commit_version<'a, 'b, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &DatatypesRegistry<T>,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>
            where Box<::datatype::interface::ProducerController>:
            From<<T as DatatypeEnum>::InterfaceControllerType> {
        {
            let rc = match *repo_control {
                ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
                _ => panic!("PostgresStore received a non-Postgres context")
            };

            let conn = rc.conn()?;
            let trans = conn.transaction()?;

                let ver = ver_graph.versions.node_weight_mut(v_idx).expect("TODO");
                // TODO: check status? here or from DB?
                ver.status = VersionStatus::Committed;
                let id = ver.id;

            trans.query(r#"
                    UPDATE version
                    SET hash = $2, status = $3
                    WHERE uuid_ = $1;
                "#, &[&id.uuid, &(id.hash as i64), &VersionStatus::Committed])?;
            trans.commit()?;
        }

        self.cascade_notify_producers(
            dtypes_registry,
            repo_control,
            art_graph,
            ver_graph,
            v_idx)?;

        Ok(())
    }

    fn fulfill_policy_requirements<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
        requirements: &ProductionPolicyRequirements,
    ) -> Result<(), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let mut idx_map = BTreeMap::new();
        let mut prod_ver_db_ids = Vec::new();
        let p_art = art_graph.artifacts.node_weight(p_art_idx).expect("TODO");

        // Parent versions of the triggering new dependency version.
        let ver_parent_uuids: Vec<Uuid> = ver_graph.versions.parents(v_idx).iter(&ver_graph.versions)
            .filter_map(|(e_idx, parent_idx)| {
                let relation = ver_graph.versions.edge_weight(e_idx)
                    .expect("Impossible: indices from this graph");
                match relation {
                    &VersionRelation::Dependence(_) => None,
                    &VersionRelation::Parent => {
                        let parent = ver_graph.versions.node_weight(parent_idx)
                            .expect("Impossible: indices from this graph");
                        Some(parent.id.uuid.clone())
                    },
                }
            })
            .collect();

        // Load versions of the producer artifact.
        match requirements.producer {
            PolicyProducerRequirements::None => {},
            PolicyProducerRequirements::DependentOnParentVersions |
            PolicyProducerRequirements::All => {
                let ver_rows = match requirements.producer {
                    PolicyProducerRequirements::None => unreachable!(),
                    // Any producer version dependent on parent versions of the
                    // new dependency version.
                    PolicyProducerRequirements::DependentOnParentVersions => trans.query(r#"
                            SELECT v.id, v.uuid_, v.hash, v.status, v.representation, a.uuid_, a.hash
                            FROM version v
                            JOIN artifact a ON a.id = v.artifact_id
                            JOIN version_parent vp ON v.id = vp.child_id
                            JOIN version vpn ON vp.parent_id = vpn.id
                            WHERE vpn.uuid_ = ANY($1::uuid[]);
                        "#, &[&ver_parent_uuids])?,
                    // All versions of this producer.
                    PolicyProducerRequirements::All => trans.query(r#"
                            SELECT v.id, v.uuid_, v.hash, v.status, a.uuid_, a.hash
                            FROM version v
                            JOIN artifact a ON a.id = v.artifact_id
                            WHERE a.uuid_ = $1
                        "#, &[&p_art.id.uuid])?,
                };

                let prod_ver_idx_map = self.load_version_rows(
                    art_graph,
                    ver_graph,
                    &ver_rows,
                )?;
                prod_ver_db_ids.extend(prod_ver_idx_map.keys().cloned());
                idx_map.extend(prod_ver_idx_map.into_iter());

                self.get_version_relations(
                    &trans,
                    &art_graph,
                    ver_graph,
                    &prod_ver_db_ids,
                    &mut idx_map,
                    // TODO: Possible to be more parsimonious about what
                    // version ancestry to load, but need to think through.
                    None,
                    // Only care about dependencies, not dependents that cannot
                    // affect the policy.
                    Some(petgraph::Direction::Incoming),
                )?;
            }
        }

        match requirements.dependency {
            PolicyDependencyRequirements::None => {},
            PolicyDependencyRequirements::DependencyOfProducerVersion |
            PolicyDependencyRequirements::All => {
                let ver_rows = match requirements.dependency {
                    PolicyDependencyRequirements::None => unreachable!(),
                    PolicyDependencyRequirements::DependencyOfProducerVersion => trans.query(r#"
                            SELECT v.id, v.uuid_, v.hash, v.status, v.representation, a.uuid_, a.hash
                            FROM version v
                            JOIN artifact a ON a.id = v.artifact_id
                            JOIN version_relation vr ON vr.source_version_id = v.id
                            WHERE vr.dependent_version_id = ANY($1::bigint[]);
                        "#, &[&prod_ver_db_ids])?,
                    PolicyDependencyRequirements::All => {
                        let dep_art_uuids: Vec<Uuid> =
                            art_graph.artifacts.parents(v_idx)
                            .iter(&art_graph.artifacts)
                            .filter_map(|(e_idx, dependency_idx)| {
                                // TODO: Not using relation because not clear variants are
                                // distinct after changing producers to datatypes.
                                let dependency = art_graph.artifacts.node_weight(dependency_idx)
                                    .expect("Impossible: indices from this graph");
                                Some(dependency.id.uuid.clone())
                            })
                            .collect();

                        trans.query(r#"
                            SELECT v.id, v.uuid_, v.hash, v.status, v.representation, a.uuid_, a.hash
                            FROM version v
                            JOIN artifact a ON a.id = v.artifact_id
                            WHERE a.uuid_ = ANY($1::uuid[]);
                        "#, &[&dep_art_uuids])?
                    }
                };

                let dep_ver_idx_map = self.load_version_rows(
                    art_graph,
                    ver_graph,
                    &ver_rows,
                )?;
                let dep_ver_db_ids = dep_ver_idx_map.keys().cloned().collect();
                idx_map.extend(dep_ver_idx_map.into_iter());

                self.get_version_relations(
                    &trans,
                    &art_graph,
                    ver_graph,
                    &dep_ver_db_ids,
                    &mut idx_map,
                    // Parent ancestry of dependents cannot affect the policy.
                    Some(petgraph::Direction::Outgoing),
                    // Only care about dependents, not dependencies that cannot
                    // affect the policy.
                    Some(petgraph::Direction::Outgoing),
                )?;
            }
        }

        Ok(())
    }

    fn get_version<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'a, 'b>), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let mut ver_graph = VersionGraph::new();
        let mut idx_map = BTreeMap::new();

        let ver_node_rows = trans.query(r#"
                SELECT v.id, v.uuid_, v.hash, v.status, v.representation, a.uuid_, a.hash
                FROM version v
                JOIN artifact a ON a.id = v.artifact_id
                WHERE v.uuid_ = $1::uuid
            "#, &[&id.uuid])?;

        let ver_idx_map = self.load_version_rows(
            art_graph,
            &mut ver_graph,
            &ver_node_rows,
        )?;
        idx_map.extend(ver_idx_map.into_iter());

        let (ver_node_id, ver_node_idx) = idx_map.iter().next()
            .map(|(db_id, idx)| (*db_id, *idx)).expect("TODO");

        self.get_version_relations(
            &trans,
            &art_graph,
            &mut ver_graph,
            &vec![ver_node_id],
            &mut idx_map,
            None,
            None)?;

        Ok((ver_node_idx, ver_graph))
    }

    fn get_version_graph<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
    ) -> Result<VersionGraph<'a, 'b>, Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let mut ver_graph = VersionGraph::new();
        let mut idx_map = BTreeMap::new();

        let art_uuids: Vec<Uuid> = art_graph.artifacts.raw_nodes().iter()
            .map(|n| n.weight.id.uuid)
            .collect();
        let ver_node_rows = trans.query(r#"
                SELECT v.id, v.uuid_, v.hash, v.status, v.representation, a.uuid_, a.hash
                FROM version v
                JOIN artifact a ON a.id = v.artifact_id
                WHERE a.uuid_ = ANY($1::uuid[]);
            "#, &[&art_uuids])?;

        let ver_idx_map = self.load_version_rows(
            art_graph,
            &mut ver_graph,
            &ver_node_rows,
        )?;
        idx_map.extend(ver_idx_map.into_iter());

        self.get_version_relations(
            &trans,
            &art_graph,
            &mut ver_graph,
            &idx_map.keys().cloned().collect(),
            &mut idx_map,
            // Can use incoming edges only since all nodes are fetched.
            Some(petgraph::Direction::Incoming),
            Some(petgraph::Direction::Incoming))?;

        Ok(ver_graph)
    }

    fn create_hunk(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<(), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        if !hunk.is_valid() {
            return Err(Error::Model("Hunk is invalid.".into()));
        }

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        // TODO should check that version is not committed
        trans.execute(r#"
                INSERT INTO hunk (
                    uuid_, hash,
                    version_id, partition_id,
                    representation, completion)
                SELECT r.uuid_, r.hash, v.id, r.partition_id, r.representation, r.completion
                FROM (VALUES (
                        $1::uuid, $2::bigint,
                        $3::uuid, $4::bigint, $5::bigint,
                        $6::representation_kind, $7::part_completion))
                  AS r (uuid_, hash, v_uuid, v_hash, partition_id, representation, completion)
                JOIN version v
                  ON (v.uuid_ = r.v_uuid AND v.hash = r.v_hash);
            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64),
                  &hunk.version.id.uuid, &(hunk.version.id.hash as i64),
                  &(hunk.partition.index as i64),
                  &hunk.representation, &hunk.completion])?;

        trans.set_commit();
        Ok(())
    }

    fn get_hunks<'a, 'b, 'c, 'd>(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        version: &'d Version<'a, 'b>,
        partitioning: &'c Version<'a, 'b>
    ) -> Result<Vec<Hunk<'a, 'b, 'c, 'd>>, Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        enum HunkRow {
            UUID = 0,
            Hash,
            PartitionID,
            Representation,
            Completion,
        };
        let hunk_rows = trans.query(r#"
                SELECT h.uuid_, h.hash, h.partition_id, h.representation, h.completion
                FROM version v
                JOIN hunk h ON (h.version_id = v.id)
                WHERE v.uuid_ = $1::uuid AND v.hash = $2::bigint;"#,
            &[&version.id.uuid, &(version.id.hash as i64)])?;
        let mut hunks = Vec::new();
        for row in &hunk_rows {
            hunks.push(Hunk {
                id: Identity {
                    uuid: row.get(HunkRow::UUID as usize),
                    hash: row.get::<_, i64>(HunkRow::Hash as usize) as u64,
                },
                version: version,
                partition: Partition {
                    partitioning: partitioning,
                    index: row.get::<_, i64>(HunkRow::PartitionID as usize) as u64,
                },
                representation: row.get(HunkRow::Representation as usize),
                completion: row.get(HunkRow::Completion as usize),
            });
        }

        Ok(hunks)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use ::{Context};
    use ::datatype::blob::ModelController as BlobModelController;
    use datatype::partitioning::arbitrary::ModelController as ArbitraryPartitioningModelController;
    use ::datatype::producer::tests::NegateBlobProducer;


    datatype_enum!(TestDatatypes, ::datatype::DefaultInterfaceController, (
        (ArtifactGraph, ::datatype::artifact_graph::ArtifactGraphDtype),
        (UnaryPartitioning, ::datatype::partitioning::UnaryPartitioning),
        (ArbitraryPartitioning, ::datatype::partitioning::arbitrary::ArbitraryPartitioning),
        (Blob, ::datatype::blob::Blob),
        (NoopProducer, ::datatype::producer::NoopProducer),
        (NegateBlobProducer, NegateBlobProducer),
    ));

    /// Create a simple artifact chain of
    /// Blob -> Producer -> Blob -> Producer -> Blob.
    fn simple_blob_prod_ag_fixture<'a, T: DatatypeEnum>(
        dtypes_registry: &'a DatatypesRegistry<T>,
        partitioning: Option<ArtifactDescription>,
    ) -> (ArtifactGraph<'a>, [ArtifactGraphIndex; 5]) {
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

        let (ag, idx_map) = ArtifactGraph::from_description(&ag_desc, dtypes_registry);

        let idxs = [
            *idx_map.get(&blob1_node_idx).unwrap(),
            *idx_map.get(&prod1_node_idx).unwrap(),
            *idx_map.get(&blob2_node_idx).unwrap(),
            *idx_map.get(&prod2_node_idx).unwrap(),
            *idx_map.get(&blob3_node_idx).unwrap(),
        ];

        (ag, idxs)
    }

    #[test]
    fn test_postgres_create_get_artifact_graph() {

        let store = Store::Postgres;

        let dtypes_registry = ::datatype::tests::init_dtypes_registry::<TestDatatypes>();
        let repo_control = ::repo::tests::init_repo(store, &dtypes_registry);

        let mut context = Context {
            dtypes_registry: dtypes_registry,
            repo_control: repo_control,
        };

        let (ag, idxs) = simple_blob_prod_ag_fixture(&context.dtypes_registry, None);

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

        let mut context = Context {
            dtypes_registry: dtypes_registry,
            repo_control: repo_control,
        };

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
        let (up_art_idx, up_art) = ag.find_by_id(&ver_graph.versions[up_idx].artifact.id).unwrap();

        let blob1_art_idx = idxs[0];
        let blob1_art = &ag.artifacts[blob1_art_idx];
        let blob1_ver = Version {
            id: Identity {uuid: Uuid::new_v4(), hash: 0},
            artifact: blob1_art,
            status: VersionStatus::Staging,
            representation: RepresentationKind::State,
        };
        let blob1_ver_idx = ver_graph.versions.add_node(blob1_ver);
        ver_graph.versions.add_edge(up_idx, blob1_ver_idx,
            VersionRelation::Dependence(
                &ag.artifacts[ag.artifacts.find_edge(up_art_idx, blob1_art_idx).unwrap()])).unwrap();

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            blob1_ver_idx.clone()).unwrap();

        let (_, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx)
            .expect("Partitioning version missing");
        let ver_part_control: Box<::datatype::interface::PartitioningController> =
                context.dtypes_registry.models
                                      .get(&ver_partitioning.artifact.dtype.name)
                                      .expect("Datatype must be known")
                                      .as_model()
                                      .interface_controller(store, "Partitioning")
                                      .expect("Partitioning must have controller for store")
                                      .into();

        let mut blob_control = ::datatype::blob::model_controller(store);
        let ver_blob_real = ver_graph.versions.node_weight(blob1_ver_idx).unwrap();
        let fake_blob = vec![0, 1, 2, 3, 4, 5, 6];
        let ver_hunks = ver_part_control
                .get_partition_ids(&mut context.repo_control, ver_partitioning)
                .iter()
                .map(|partition_id| Hunk {
                    id: Identity {
                        uuid: Uuid::new_v4(),
                        hash: blob_control.hash(&fake_blob),
                    },
                    version: ver_blob_real,
                    partition: Partition {
                        partitioning: ver_partitioning,
                        index: partition_id.to_owned(),
                    },
                    representation: RepresentationKind::State,
                    completion: PartCompletion::Complete,
                }).collect::<Vec<_>>();

        // Can't do this in an iterator because of borrow conflict on context?
        for hunk in &ver_hunks {
            model_ctrl.create_hunk(
                &mut context.repo_control,
                &hunk).unwrap();
            blob_control.write(
                &mut context.repo_control,
                &hunk,
                &fake_blob).unwrap();
        }

        for hunk in &ver_hunks {
            let blob = blob_control.read(&mut context.repo_control, &hunk).unwrap();
            assert_eq!(blob, fake_blob);
        }

        let (ver_blob_idx2, ver_graph2) = model_ctrl.get_version(
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

        let mut context = Context {
            dtypes_registry: dtypes_registry,
            repo_control: repo_control,
        };

        let partitioning = ArtifactDescription {
            name: Some("Arbitrary Partitioning".into()),
            dtype: "ArbitraryPartitioning".into(),
        };
        let (ag, idxs) = simple_blob_prod_ag_fixture(&context.dtypes_registry, Some(partitioning));

        let mut model_ctrl = model_controller(store);

        model_ctrl.create_artifact_graph(&mut context.repo_control, &ag).unwrap();

        let mut ver_graph = VersionGraph::new_from_source_artifacts(&ag);

        let part_idx = ver_graph.versions.graph().node_indices().next().unwrap();
        let (part_art_idx, part_art) = ag.find_by_id(&ver_graph.versions[part_idx].artifact.id).unwrap();

        // Create arbitrary partitions.
        {
            let mut part_control = ::datatype::partitioning::arbitrary::model_controller(store);


            model_ctrl.create_staging_version(
                &mut context.repo_control,
                &ver_graph,
                part_idx).expect("TODO");
            part_control.write(
                &mut context.repo_control,
                &ver_graph.versions[part_idx],
                &[0, 1]).expect("TODO");
            model_ctrl.commit_version(
                &context.dtypes_registry,
                &mut context.repo_control,
                &ag,
                &mut ver_graph,
                part_idx).expect("TODO");
        }

        let blob1_art_idx = idxs[0];
        let blob1_art = &ag.artifacts[blob1_art_idx];
        let blob1_ver = Version {
            id: Identity {uuid: Uuid::new_v4(), hash: 0},
            artifact: blob1_art,
            status: VersionStatus::Staging,
            representation: RepresentationKind::State,
        };
        let blob1_ver_idx = ver_graph.versions.add_node(blob1_ver);
        ver_graph.versions.add_edge(part_idx, blob1_ver_idx,
            VersionRelation::Dependence(
                &ag.artifacts[ag.artifacts.find_edge(part_art_idx, blob1_art_idx).unwrap()])).unwrap();

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            blob1_ver_idx.clone()).unwrap();

        let ver_hash = {
            let (_, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx).unwrap();
            let ver_part_control: Box<::datatype::interface::PartitioningController> =
                    context.dtypes_registry.models
                                          .get(&ver_partitioning.artifact.dtype.name)
                                          .expect("Datatype must be known")
                                          .as_model()
                                          .interface_controller(store, "Partitioning")
                                          .expect("Partitioning must have controller for store")
                                          .into();

            let mut blob_control = ::datatype::blob::model_controller(store);
            let ver_blob_real = ver_graph.versions.node_weight(blob1_ver_idx).unwrap();
            let fake_blob = vec![0, 1, 2, 3, 4, 5, 6];
            let ver_hunks = ver_part_control
                    // Note that this is in ascending order, so version hash
                    // is correct.
                    .get_partition_ids(&mut context.repo_control, ver_partitioning)
                    .iter()
                    .map(|partition_id| Hunk {
                        id: Identity {
                            uuid: Uuid::new_v4(),
                            hash: blob_control.hash(&fake_blob),
                        },
                        version: ver_blob_real,
                        partition: Partition {
                            partitioning: ver_partitioning,
                            index: partition_id.to_owned(),
                        },
                        representation: RepresentationKind::State,
                        completion: PartCompletion::Complete,
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
                blob_control.write(
                    &mut context.repo_control,
                    &hunk,
                    &fake_blob).unwrap();
            }

            ver_hash
        };

        ver_graph.versions[blob1_ver_idx].id.hash = ver_hash;

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

        let blob1_vg2_idxs = vg2.artifact_versions(
            &ag.artifacts[idxs[0]]);

        let blob2_vg2_idxs = vg2.artifact_versions(
            &ag.artifacts[idxs[2]]);

        assert_eq!(blob2_vg2_idxs.len(), 1);

        let blob3_vg2_idxs = vg2.artifact_versions(
            &ag.artifacts[idxs[4]]);

        assert_eq!(blob3_vg2_idxs.len(), 1);

        assert_eq!(
            vg2.versions[blob1_vg2_idxs[0]].id.hash,
            vg2.versions[blob3_vg2_idxs[0]].id.hash,
            "Version hashes for original and double-negated blob should match.",
            );
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

      let mut specs_a = ProductionVersionSpecs::new();
      specs_a.insert(vec![a.clone(), b.clone()].into_iter().collect(),
                     Some(VersionGraphIndex::new(0)));
      specs_a.insert(vec![a.clone(), b.clone()].into_iter().collect(), None);
      specs_a.insert(vec![c.clone(), b.clone()].into_iter().collect(),
                     Some(VersionGraphIndex::new(1)));

      assert!(specs_a.specs.get(&vec![a.clone(), b.clone()].into_iter().collect())
         .unwrap().contains(&None));
      assert!(specs_a.specs.get(&vec![a.clone(), b.clone()].into_iter().collect())
         .unwrap().contains(&Some(VersionGraphIndex::new(0))));

      let mut specs_b = ProductionVersionSpecs::new();
      specs_b.insert(vec![c.clone(), b.clone()].into_iter().collect(),
         Some(VersionGraphIndex::new(2)));

      specs_a.merge(specs_b);

      assert!(specs_a.specs.get(&vec![c.clone(), b.clone()].into_iter().collect())
         .unwrap().contains(&Some(VersionGraphIndex::new(1))));
      assert!(specs_a.specs.get(&vec![c.clone(), b.clone()].into_iter().collect())
         .unwrap().contains(&Some(VersionGraphIndex::new(2))));
    }
}
