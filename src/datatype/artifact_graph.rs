extern crate daggy;
extern crate serde;
extern crate serde_json;
extern crate uuid;


use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use daggy::petgraph::visit::EdgeRef;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use postgres::types::ToSql;
use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;
use url::Url;

use ::{Artifact, ArtifactGraph, ArtifactNode, ArtifactRelation, Context, DatatypeRelation, Error, Identity, Producer};
use super::super::{Datatype, DatatypeRepresentationKind};
use super::{DatatypesRegistry, DependencyDescription, DependencyStoreRestriction, Description, Store};
use ::repo::{PostgresRepoController, PostgresMigratable};


pub struct ArtifactGraphDtype;

impl super::Model for ArtifactGraphDtype {
    fn info(&self) -> Description {
        Description {
            datatype: Datatype::new(
                // TODO: Fake UUID.
                // Uuid::new_v4(),
                "ArtifactGraph".into(),
                1,
                vec![DatatypeRepresentationKind::State]
                    .into_iter()
                    .collect(),
            ),
            // TODO: Fake dependency.
            dependencies: vec![],
        }
    }

    fn controller(&self, store: Store) -> Option<super::StoreMetaController> {
        match store {
            Store::Postgres => Some(super::StoreMetaController::Postgres(Box::new(PostgresStore {}))),
            _ => None,
        }
    }
}


pub trait ModelController {
    fn list_graphs(&self) -> Vec<Identity>;

    fn create_graph(
            &mut self,
            repo_control: &mut ::repo::StoreRepoController,
            ag: &ArtifactGraph) -> Result<(), Error>;

    fn get_graph<'a>(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            dtypes_registry: &'a DatatypesRegistry,
            id: &Identity) -> Result<ArtifactGraph<'a>, Error>;
}

pub struct ArtifactGraphDescription {
    pub artifacts: daggy::Dag<ArtifactNodeDescription, ArtifactRelation>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ArtifactNodeDescription {
    Producer(ProducerDescription),
    Artifact(ArtifactDescription),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProducerDescription {
    pub name: String,
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
        transaction.batch_execute(include_str!("artifact_graph_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("DROP TABLE artifact_graph;", &[]).map(|_| ())
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
            ag: &ArtifactGraph) -> Result<(), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let ag_id_row = trans.query(r#"
                INSERT INTO artifact_graph (uuid_, hash)
                VALUES ($1, $2) RETURNING id;
                "#, &[&ag.id.uuid, &(ag.id.hash as i64)])?;
        // let ag_id = ag_id_row.into_iter().nth(0).ok_or(Error::Store("Insert failed.".into()))?;
        let ag_id: i64 = ag_id_row.get(0).get(0);

        let mut id_map = HashMap::new();
        let insert_producer = trans.prepare(r#"
                INSERT INTO producer (uuid_, hash, name, artifact_graph_id)
                VALUES ($1, $2, $3, $4)
                RETURNING id;"#)?;
        let insert_artifact = trans.prepare(r#"
                INSERT INTO artifact (uuid_, hash, name, datatype_id, artifact_graph_id)
                SELECT r.testthis, r.hash, r.name, d.id, r.artifact_graph_id
                FROM (VALUES ($1::uuid, $2::bigint, $3, $4, $5::bigint))
                AS r (testthis, hash, name, datatype_name, artifact_graph_id)
                JOIN datatype d ON d.name = r.datatype_name
                RETURNING id;"#)?;

        for idx in ag.artifacts.graph().node_indices() {
            let node = ag.artifacts.node_weight(idx).unwrap();
            let node_id_row = match node {
                &ArtifactNode::Producer(ref prod) =>
                    insert_producer.query(&[
                        &prod.id.uuid, &(prod.id.hash as i64), &prod.name, &ag_id])?,
                &ArtifactNode::Artifact(ref art) =>
                    insert_artifact.query(&[
                        &art.id.uuid, &(art.id.hash as i64), &art.name,
                        &art.dtype.name, &ag_id])?,
            };
            let node_id: i64 = node_id_row.get(0).get(0);

            id_map.insert(idx, node_id);
        }

        let art_prod_edge = trans.prepare(r#"
                INSERT INTO artifact_producer_edge (source_id, dependent_id, name)
                VALUES ($1, $2, $3);"#)?;
        let art_dtype_edge = trans.prepare(r#"
                INSERT INTO artifact_dtype_edge (source_id, dependent_id, name)
                VALUES ($1, $2, $3);"#)?;

        for e in ag.artifacts.graph().edge_references() {
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

    fn get_graph<'a>(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            dtypes_registry: &'a DatatypesRegistry,
            id: &Identity) -> Result<ArtifactGraph<'a>, Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

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
                    an.id,
                    an.uuid_,
                    an.hash,
                    an.name,
                    d.name
                FROM (
                    SELECT id, uuid_, hash, name, datatype_id
                    FROM artifact
                    WHERE artifact_graph_id = $1

                    UNION

                    SELECT id, uuid_, hash, name, NULL
                    FROM producer
                    WHERE artifact_graph_id = $1)
                    AS an (id, uuid_, hash, name, datatype_id)
                LEFT JOIN datatype d ON an.datatype_id = d.id;
                "#, &[&ag_row.get::<_, i64>(0)])?;

        let mut artifacts: daggy::Dag<ArtifactNode, ArtifactRelation> = daggy::Dag::new();
        let mut idx_map = HashMap::new();

        for row in &nodes {
            let db_id = row.get::<_, i64>(0);
            let id = Identity {
                uuid: row.get(1),
                hash: row.get::<_, i64>(2) as u64,
            };
            let node = match row.get::<_, Option<String>>(4) {
                Some(name) => ArtifactNode::Artifact(Artifact {
                    id: id,
                    name: row.get(3),
                    dtype: dtypes_registry.get_datatype(&*name).expect("Unknown datatype."),
                }),
                None => ArtifactNode::Producer(Producer {
                    id: id,
                    name: row.get(3),
                })
            };

            let node_idx = artifacts.add_node(node);
            idx_map.insert(db_id, node_idx);
        }

        let edges = trans.query(r#"
                SELECT
                    ae.source_id,
                    ae.dependent_id,
                    ae.name,
                    ae.class
                FROM (
                    SELECT pe.source_id, pe.dependent_id, pe.name, pe.tableoid::regclass::text
                    FROM artifact_producer_edge pe
                    WHERE pe.source_id = ANY($1::bigint[])

                    UNION

                    SELECT de.source_id, de.dependent_id, de.name, de.tableoid::regclass::text
                    FROM artifact_producer_edge de
                    WHERE de.source_id = ANY($1::bigint[]))
                    AS ae (source_id, dependent_id, name, class);
                "#, &[&idx_map.keys().collect::<Vec<_>>()])?;

        for e in &edges {
            let relation = match e.get::<_, String>(3).as_ref() {
                "artifact_producer_edge" => ArtifactRelation::ProducedFrom(e.get(2)),
                "artifact_dtype_edge" => ArtifactRelation::DtypeDepends(DatatypeRelation {
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
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_postgres_create_graph() {
        use super::*;

        let mut artifacts: daggy::Dag<ArtifactNodeDescription, ArtifactRelation> = daggy::Dag::new();
        let prod_node = ArtifactNodeDescription::Producer(ProducerDescription {
            name: "Test Producer".into()});
        let prod_node_idx = artifacts.add_node(prod_node);
        let blob_node = ArtifactNodeDescription::Artifact(ArtifactDescription {
            name: Some("Test Blob".into()),
            dtype: "Blob".into() });
        let blob_node_idx = artifacts.add_node(blob_node);
        artifacts.add_edge(prod_node_idx, blob_node_idx, ArtifactRelation::ProducedFrom("Test Dep".into()));
        let ag_desc = ArtifactGraphDescription {
            artifacts: artifacts,
        };

        let mut context = ::repo::tests::init_postgres_repo();

        let ag = ArtifactGraph::from_description(&ag_desc, &context.dtypes_registry);
        // let model = context.dtypes_registry.types.get("ArtifactGraph").expect()
        let mut model_ctrl: Box<ModelController> = Box::new(PostgresStore {});

        model_ctrl.create_graph(&mut context.repo_control, &ag).unwrap();

        let ag2 = model_ctrl.get_graph(&mut context.repo_control, &context.dtypes_registry, &ag.id).unwrap();
        assert!(ag2.verify_hash());
        assert!(ag.id.hash == ag2.id.hash);

        // let serialized = serde_json::to_string(artifacts.graph()).unwrap();
        // println!("serialized = {}", serialized);


        // let data = r#"{
        //               "nodes": [
        //                 {
        //                   "Artifact": {
        //                     "name": "Test Blob",
        //                     "dtype": "blob"
        //                   }
        //                 }
        //               ],
        //               "node_holes": [],
        //               "edge_property": "directed",
        //               "edges": []
        //             }"#;

        // let deserialized: Point = serde_json::from_str(&data).unwrap();
    }
}
