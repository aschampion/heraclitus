extern crate daggy;
extern crate petgraph;
extern crate serde;
extern crate serde_json;
extern crate uuid;


use std::collections::{BTreeMap, HashMap};

use daggy::petgraph::visit::EdgeRef;
use daggy::Walker;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use postgres::types::ToSql;
use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;
use url::Url;

use ::{
    Artifact, ArtifactGraph, ArtifactRelation, Context,
    Datatype, DatatypeRelation, DatatypeRepresentationKind, Error, Hunk, Identity,
    PartCompletion, Partition,
    Version, VersionGraph, VersionGraphIndex, VersionRelation, VersionStatus};
use super::{
    Control, DatatypeEnum, DatatypesRegistry, DependencyDescription,
    DependencyStoreRestriction, Description, InterfaceController, Store};
use ::repo::{PostgresRepoController, PostgresMigratable};


#[derive(Default)]
pub struct ArtifactGraphDtype;

impl<T> super::Model<T> for ArtifactGraphDtype {
    fn info(&self) -> Description {
        Description {
            name: "ArtifactGraph".into(),
            version: 1,
            representations: vec![DatatypeRepresentationKind::State]
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
        store: Store,
        name: &str,
    ) -> Option<T> {
        None
    }
}

impl<T, U: ?Sized> Control<T, U> for ArtifactGraphDtype where T: InterfaceController<U> {}

pub fn model_controller(store: Store) -> impl ModelController {
    match store {
        Store::Postgres => PostgresStore {},
        _ => unimplemented!(),
    }
}


pub trait ModelController {
    fn list_graphs(&self) -> Vec<Identity>;

    fn create_graph(
            &mut self,
            repo_control: &mut ::repo::StoreRepoController,
            art_graph: &ArtifactGraph) -> Result<(), Error>;

    fn get_graph<'a, T: DatatypeEnum>(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            dtypes_registry: &'a DatatypesRegistry<T>,
            id: &Identity) -> Result<ArtifactGraph<'a>, Error>;

    fn create_staging_version(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>;

    fn commit_version(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        id: &Identity,
    ) -> Result<(), Error>;

    fn get_version<'a>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'a ArtifactGraph,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'a>), Error>;

    fn create_hunk(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<(), Error>;
}


type ArtifactGraphDescriptionType =  daggy::Dag<ArtifactDescription, ArtifactRelation>;
pub struct ArtifactGraphDescription {
    pub artifacts: ArtifactGraphDescriptionType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ArtifactDescription {
    pub name: Option<String>,
    pub dtype: String,
}


struct PostgresStore {}

struct PGMigrationArtifactGraphs;
migration!(PGMigrationArtifactGraphs, 2, "create artifact graph table");

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
    fn register_migrations(&self, migrator: &mut Migrator<PostgresAdapter>) {
        migrator.register(Box::new(PGMigrationArtifactGraphs));
    }
}

impl super::PostgresMetaController for PostgresStore {}

impl ModelController for PostgresStore {
    fn list_graphs(&self) -> Vec<Identity> {
        unimplemented!()
    }

    fn create_graph(
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

    fn get_graph<'a, T: DatatypeEnum>(
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

        let nodes = trans.query(r#"
                SELECT
                    a.id,
                    a.uuid_,
                    a.hash,
                    a.name,
                    d.name
                FROM artifact a
                JOIN datatype d ON a.datatype_id = d.id
                WHERE artifact_graph_id = $1;
            "#, &[&ag_row.get::<_, i64>(0)])?;

        let mut artifacts = ::ArtifactGraphType::new();
        let mut idx_map = HashMap::new();

        for row in &nodes {
            let db_id = row.get::<_, i64>(0);
            let id = Identity {
                uuid: row.get(1),
                hash: row.get::<_, i64>(2) as u64,
            };
            let node = Artifact {
                id: id,
                name: row.get(3),
                dtype: dtypes_registry.get_datatype(&row.get::<_, String>(4)).expect("Unknown datatype."),
            };

            let node_idx = artifacts.add_node(node);
            idx_map.insert(db_id, node_idx);
        }

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
            let relation = match e.get::<_, String>(3).as_ref() {
                "producer" => ArtifactRelation::ProducedFrom(e.get(2)),
                "dtype" => ArtifactRelation::DtypeDepends(DatatypeRelation {
                    name: e.get(2),
                }),
                _ => return Err(Error::Store("Unknown artifact graph edge reltype.".into())),
            };

            let source_idx = idx_map.get(&e.get(0)).expect("Graph is malformed.");
            let dependent_idx = idx_map.get(&e.get(1)).expect("Graph is malformed.");
            artifacts.add_edge(*source_idx, *dependent_idx, relation);

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
                INSERT INTO version (uuid_, hash, artifact_id)
                SELECT r.uuid_, r.hash, a.id
                FROM (VALUES ($1::uuid, $2::bigint, $3::uuid))
                AS r (uuid_, hash, a_uuid)
                JOIN artifact a ON a.uuid_ = r.a_uuid
                RETURNING id;
            "#, &[&ver.id.uuid, &(ver.id.hash as i64), &ver.artifact.id.uuid])?;
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

    fn commit_version(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        id: &Identity,
    ) -> Result<(), Error> {
        // TODO: implement once PG version has status
        Ok(())
    }

    fn get_version<'a>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'a ArtifactGraph,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'a>), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let mut ver_graph = VersionGraph::new();
        let mut idx_map = BTreeMap::new();

        // TODO: not using hash. See other comments.
        let ver_node_rows = trans.query(r#"
                SELECT v.id, v.uuid_, v.hash, a.uuid_, a.hash
                FROM version v
                JOIN artifact a ON a.id = v.artifact_id
                WHERE v.uuid_ = $1::uuid
            "#, &[&id.uuid])?;
        let ver_node_row = ver_node_rows.get(0);
        let ver_node_id: i64 = ver_node_row.get(0);
        let an_id = Identity {
            uuid: ver_node_row.get(3),
            hash: ver_node_row.get::<_, i64>(4) as u64,
        };
        let (art_idx, art) = art_graph.find_artifact_by_id(&an_id).expect("Version references unkown artifact");
        let ver_node = Version {
            id: Identity {
                uuid: ver_node_row.get(1),
                hash: ver_node_row.get::<_, i64>(2) as u64,
            },
            artifact: art,
            status: VersionStatus::Staging,  // TODO
            representation: DatatypeRepresentationKind::State,  // TODO
        };
        let ver_node_idx = ver_graph.versions.add_node(ver_node);
        idx_map.insert(ver_node_id, ver_node_idx);

        let ancestry_node_rows = trans.query(r#"
                SELECT v.id, v.uuid_, v.hash, a.uuid_, a.hash, vp.parent_id, vp.child_id
                FROM version_parent vp
                JOIN version v
                  ON ((vp.parent_id = $1 AND v.id = vp.child_id)
                    OR (vp.child_id = $1 AND v.id = vp.parent_id))
                JOIN artifact a ON a.id = v.artifact_id;
            "#, &[&ver_node_id])?;
        for row in &ancestry_node_rows {
            let db_id = row.get::<_, i64>(0);
            let an_id = Identity {
                uuid: row.get(3),
                hash: row.get::<_, i64>(4) as u64,
            };
            let v_node = Version {
                id: Identity {
                    uuid: row.get(1),
                    hash: row.get::<_, i64>(2) as u64,
                },
                artifact: art_graph.find_artifact_by_id(&an_id).expect("Version references unkown artifact").1,
                status: VersionStatus::Staging,  // TODO
                representation: DatatypeRepresentationKind::State,  // TODO
            };

            let v_idx = ver_graph.versions.add_node(v_node);
            idx_map.insert(db_id, v_idx);

            let edge = VersionRelation::Parent;
            let parent_idx = idx_map.get(&row.get(5)).expect("Graph is malformed.");
            let child_idx = idx_map.get(&row.get(6)).expect("Graph is malformed.");
            ver_graph.versions.add_edge(*parent_idx, *child_idx, edge);
        }

        let dependence_node_rows = trans.query(r#"
                SELECT
                  v.id, v.uuid_, v.hash,
                  a.uuid_, a.hash,
                  vr.dependent_version_id = $1
                FROM version_relation vr
                JOIN version v
                  ON ((vr.dependent_version_id = $1 AND v.id = vr.source_version_id)
                    OR (vr.source_version_id = $1 AND v.id = vr.dependent_version_id))
                JOIN artifact a ON a.id = v.artifact_id;
            "#, &[&ver_node_id])?;
        for row in &dependence_node_rows {
            let db_id = row.get::<_, i64>(0);
            let an_id = Identity {
                uuid: row.get(3),
                hash: row.get::<_, i64>(4) as u64,
            };
            let (an_idx, an) = art_graph.find_artifact_by_id(&an_id).expect("Version references unkown artifact");
            let v_node = Version {
                id: Identity {
                    uuid: row.get(1),
                    hash: row.get::<_, i64>(2) as u64,
                },
                artifact: an,
                status: VersionStatus::Staging,  // TODO
                representation: DatatypeRepresentationKind::State,  // TODO
            };

            let v_idx = ver_graph.versions.add_node(v_node);

            let inbound = row.get(5);
            let art_rel_idx = if inbound {
                art_graph.artifacts.find_edge(an_idx, art_idx)
            } else {
                art_graph.artifacts.find_edge(art_idx, an_idx)
            }.expect("Version graph references unknown artifact relation");
            let art_rel = art_graph.artifacts.edge_weight(art_rel_idx).expect("Graph is malformed");
            let edge = VersionRelation::Dependence(art_rel);
            let (parent_idx, child_idx) = if inbound {
                (v_idx, ver_node_idx)
            } else {
                (ver_node_idx, v_idx)
            };
            ver_graph.versions.add_edge(parent_idx, child_idx, edge);
        }

        Ok((ver_node_idx, ver_graph))
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

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        trans.execute(r#"
                INSERT INTO hunk (uuid_, hash, version_id, partition_id)
                SELECT r.uuid_, r.hash, v.id, r.partition_id
                FROM (VALUES ($1::uuid, $2::bigint, $3::uuid, $4::bigint, $5::bigint))
                  AS r (uuid_, hash, v_uuid, v_hash, partition_id)
                JOIN version v
                  ON (v.uuid_ = r.v_uuid AND v.hash = r.v_hash);
            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64),
                  &hunk.version.id.uuid, &(hunk.version.id.hash as i64),
                  &(hunk.partition.index as i64)])?;

        trans.set_commit();
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_postgres_create_graph() {
        use super::*;
        use ::datatype::blob::ModelController as BlobModelController;

        let store = Store::Postgres;
        let mut artifacts = ArtifactGraphDescriptionType::new();
        let blob1_node = ArtifactDescription {
            name: Some("Test Blob 1".into()),
            dtype: "Blob".into()
        };
        let blob1_node_idx = artifacts.add_node(blob1_node);
        let prod_node = ArtifactDescription {
            name: Some("Test Producer".into()),
            dtype: "NoopProducer".into(),
        };
        let prod_node_idx = artifacts.add_node(prod_node);
        artifacts.add_edge(
            blob1_node_idx,
            prod_node_idx,
            ArtifactRelation::ProducedFrom("Test Dep 1".into())).unwrap();
        let blob2_node = ArtifactDescription {
            name: Some("Test Blob 2".into()),
            dtype: "Blob".into()
        };
        let blob2_node_idx = artifacts.add_node(blob2_node);
        artifacts.add_edge(
            prod_node_idx,
            blob2_node_idx,
            ArtifactRelation::ProducedFrom("Test Dep 2".into())).unwrap();
        let ag_desc = ArtifactGraphDescription {
            artifacts: artifacts,
        };

        let dtypes_registry = ::datatype::tests::init_default_dtypes_registry();
        let repo_control = ::repo::tests::init_repo(store, &dtypes_registry);

        let mut context = Context {
            dtypes_registry: dtypes_registry,
            repo_control: repo_control,
        };

        let (ag, idx_map) = ArtifactGraph::from_description(&ag_desc, &context.dtypes_registry);
        // let model = context.dtypes_registry.types.get("ArtifactGraph").expect()
        let mut model_ctrl = model_controller(store);

        model_ctrl.create_graph(&mut context.repo_control, &ag).unwrap();

        let ag2 = model_ctrl.get_graph(&mut context.repo_control, &context.dtypes_registry, &ag.id)
                            .unwrap();
        assert!(ag2.verify_hash());
        assert_eq!(ag.id.hash, ag2.id.hash);

        let mut ver_graph = VersionGraph::new();
        let prod_node_idx_real = idx_map.get(&prod_node_idx).expect("Couldn't find producer");
        let ver_prod = Version {
            id: Identity {uuid: Uuid::new_v4(), hash: 0},
            artifact: ag.artifacts.node_weight(*prod_node_idx_real).expect("Couldn't find producer"),
            status: VersionStatus::Staging,
            representation: DatatypeRepresentationKind::State,
        };
        let ver_prod_idx = ver_graph.versions.add_node(ver_prod);

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            ver_prod_idx.clone()).unwrap();
        // model_ctrl.commit_version(
        //     &mut context.repo_control,
        //     &ver_prod.id);

        let ver_node_idx_real = idx_map.get(&blob2_node_idx).expect("Couldn't find blob");
        let ver_blob = Version {
            id: Identity {uuid: Uuid::new_v4(), hash: 0},
            artifact: ag.artifacts.node_weight(*ver_node_idx_real).expect("Couldn't find blob"),
            status: VersionStatus::Staging,
            representation: DatatypeRepresentationKind::State,
        };
        let ver_blob_id = ver_blob.id.clone();
        let ver_blob_idx = ver_graph.versions.add_node(ver_blob);

        let prod_blob_idx_real = ag.artifacts.find_edge(*prod_node_idx_real, *ver_node_idx_real)
                                             .expect("Couldn't find relation");
        let prod_blob_edge_real = ag.artifacts.edge_weight(prod_blob_idx_real).expect("Couldn't find relation");
        ver_graph.versions.add_edge(
            ver_prod_idx,
            ver_blob_idx,
            VersionRelation::Dependence(prod_blob_edge_real)).unwrap();

        model_ctrl.create_staging_version(
            &mut context.repo_control,
            &ver_graph,
            ver_blob_idx.clone()).unwrap();

        // TODO: A mess from de-static-ing UP Singleton.
        let unary_partitioning_art = Artifact {
            id: Identity {uuid: ::datatype::partitioning::UNARY_PARTITIONING_ARTIFACT_UUID.clone(), hash: 0},
            name: None,
            dtype: context.dtypes_registry.get_datatype("UnaryPartitioning")
                                  .expect("Unary partitioning missing from registry"),
        };
        let unary_partitioning_ver = Version {
            id: Identity {
                uuid: ::datatype::partitioning::UNARY_PARTITIONING_VERSION_UUID.clone(),
                hash: 0,
            },
            artifact: &unary_partitioning_art,
            status: ::VersionStatus::Committed,
            representation: ::DatatypeRepresentationKind::State,
        };
        // let (unary_partitioning_ag, unary_partitioning_ver) =
        //     partitioning::UnaryPartitioning::build_singleton_version(&context.dtypes_registry);
        let ver_partitioning = ver_graph.get_partitioning(ver_blob_idx).unwrap_or(&unary_partitioning_ver);
        let ver_part_control: Box<::datatype::interface::PartitioningController> =
                context.dtypes_registry.models
                                      .get(&ver_partitioning.artifact.dtype.name)
                                      .expect("Datatype must be known")
                                      // .as_model()
                                      .interface_controller::<::datatype::interface::PartitioningController>(store) //, "Partitioning")
                                      .expect("Partitioning must have controller for store")
                                      .into();

        let mut blob_control = ::datatype::blob::model_controller(store);
        let ver_blob_real = ver_graph.versions.node_weight(ver_blob_idx).unwrap();
        let ver_hunks = ver_part_control
                .get_partition_ids(&mut context.repo_control, ver_partitioning)
                .iter()
                .map(|partition_id| Hunk {
                    id: Identity {
                        uuid: Uuid::new_v4(),
                        hash: 0,
                    },
                    version: ver_blob_real,
                    partition: Partition {
                        partitioning: ver_partitioning,
                        index: partition_id.to_owned(),
                    },
                    completion: PartCompletion::Complete,
                }).collect::<Vec<_>>();

        // Can't do this in an iterator because of borrow conflict on context?
        let fake_blob = vec![0, 1, 2, 3, 4, 5, 6];
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

        // model_ctrl.commit_version(
        //     &mut context.repo_control,
        //     &ver_blob.id);

        let (ver_blob_idx2, ver_graph2) = model_ctrl.get_version(
            &mut context.repo_control,
            &ag,
            &ver_blob_id).unwrap();

        assert!(petgraph::algo::is_isomorphic_matching(
            &ver_graph.versions.graph(),
            &ver_graph2.versions.graph(),
            |a, b| a.id == b.id,
            |_, _| true));
    }
}
