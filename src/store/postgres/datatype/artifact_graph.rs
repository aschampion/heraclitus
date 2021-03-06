use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use heraclitus_core::{
    daggy,
    petgraph,
    postgres,
    schemer,
    schemer_postgres,
    uuid,
};
use daggy::petgraph::visit::EdgeRef;
use daggy::Walker;
use enumset::EnumSet;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::migration;
use schemer_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;

use crate::{
    Artifact,
    ArtifactGraph,
    ArtifactGraphIndex,
    ArtifactRelation,
    DatatypeRelation,
    Error,
    HashType,
    Hunk,
    HunkUuidSpec,
    Identity,
    IdentifiableGraph,
    ModelError,
    PartialIdentity,
    Partition,
    PartitionIndex,
    RepresentationKind,
    Version,
    VersionGraph,
    VersionGraphIndex,
    VersionRelation,
    VersionStatus,
};
use crate::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
    InterfaceController,
    Payload,
};
use crate::datatype::artifact_graph::{
    ArtifactDescription,
    ArtifactGraphDelta,
    ArtifactGraphDescription,
    ArtifactGraphDescriptionType,
    ArtifactGraphDtypeBackend,
    Storage,
    production::{
        PolicyDependencyRequirements,
        PolicyProducerRequirements,
        ProductionPolicies,
        ProductionPolicyRequirements,
        ProductionStrategySpecs,
    },
};
use crate::datatype::interface::{
    CustomProductionPolicyController,
    ProducerController,
};
use crate::repo::Repository;
use crate::store::postgres::{
    PostgresMigratable,
    PostgresRepository,
};


impl ArtifactGraphDtypeBackend<PostgresRepository> {
    /// Load version rows from a query result into a version graph.
    fn load_version_rows<'ag>(
        &self,
        art_graph: &'ag ArtifactGraph,
        ver_graph: &mut VersionGraph<'ag>,
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
                hash: ver_node_row.get::<_, i64>(VerNodeRow::ArtifactHash as usize) as HashType,
            };
            let (_, art) = art_graph.get_by_id(&an_id).expect("Version references unkown artifact");

            let ver_id = Identity {
                uuid: ver_node_row.get(VerNodeRow::UUID as usize),
                hash: ver_node_row.get::<_, i64>(VerNodeRow::Hash as usize) as HashType,
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
    fn get_version_relations<'ag>(
        &self,
        trans: &Transaction,
        art_graph: &'ag ArtifactGraph,
        ver_graph: &mut VersionGraph<'ag>,
        v_db_ids: &[i64],
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
            &[&v_db_ids])?;
        for row in &ancestry_node_rows {
            let db_id = row.get::<_, i64>(AncNodeRow::ID as usize);
            let an_id = Identity {
                uuid: row.get(AncNodeRow::ArtifactUUID as usize),
                hash: row.get::<_, i64>(AncNodeRow::ArtifactHash as usize) as HashType,
            };

            idx_map.entry(db_id).or_insert_with(|| {
                let v_id = Identity {
                    uuid: row.get(AncNodeRow::UUID as usize),
                    hash: row.get::<_, i64>(AncNodeRow::Hash as usize) as HashType,
                };

                ver_graph.emplace(
                    &v_id,
                    || Version {
                        id: v_id,
                        artifact: art_graph.get_by_id(&an_id).expect("Version references unkown artifact").1,
                        status: row.get(AncNodeRow::Status as usize),
                        representation: row.get(AncNodeRow::Representation as usize),
                    })
            });

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
            &[&v_db_ids])?;
        for row in &dependence_node_rows {
            let db_id = row.get::<_, i64>(DepNodeRow::ID as usize);
            let an_id = Identity {
                uuid: row.get(DepNodeRow::ArtifactUUID as usize),
                hash: row.get::<_, i64>(DepNodeRow::ArtifactHash as usize) as HashType,
            };
            let (an_idx, an) = art_graph.get_by_id(&an_id).expect("Version references unkown artifact");

            let v_idx = *idx_map.entry(db_id)
                .or_insert_with(|| {
                    let v_id = Identity {
                        uuid: row.get(DepNodeRow::UUID as usize),
                        hash: row.get::<_, i64>(DepNodeRow::Hash as usize) as HashType,
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
            let other_art = ver_graph[other_v_idx].artifact;
            let other_art_idx = art_graph.get_by_id(&other_art.id)
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
    ["acda147a-552f-42a5-bb2b-1ba05d41ec03",], // Datatype 0001
    "create artifact graph table");

impl PostgresMigration for PGMigrationArtifactGraphs {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/artifact_graph_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/artifact_graph_0001.down.sql"))
    }
}


impl PostgresMigratable for ArtifactGraphDtypeBackend<PostgresRepository> {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![
            Box::new(PGMigrationArtifactGraphs),
        ]
    }
}

impl super::PostgresMetaController for ArtifactGraphDtypeBackend<PostgresRepository> {}

impl crate::datatype::Storage for ArtifactGraphDtypeBackend<PostgresRepository> {

    fn write_hunk(
        &mut self,
        repo: &Repository,
        hunk: &Hunk,
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let db_hunk_id = trans.query(r#"
                SELECT id FROM hunk h
                WHERE (h.uuid_ = $1::uuid AND h.hash = $2::bigint);
            "#,
            &[&hunk.id.uuid, &(hunk.id.hash as i64),])?
        .get(0).get::<_, i64>(0);

        fn insert_graph_description(
            art_graph: &ArtifactGraphDescription,
            db_hunk_id: i64,
            trans: &postgres::transaction::Transaction,
        ) -> Result<(), Error> {
            let mut id_map = HashMap::new();
            let insert_artifact = trans.prepare(r#"
                    INSERT INTO artifact (uuid_, hash, hunk_id, self_partitioning, name, datatype_id)
                    SELECT r.uuid_, r.hash, r.ag_id, r.self_partitioning, r.name, d.id
                    FROM (VALUES ($1::uuid, $2::bigint, $3::bigint, $4::boolean, $5::text))
                      AS r (uuid_, hash, ag_id, self_partitioning, name)
                    JOIN datatype d ON d.name = $6
                    RETURNING id;
                "#)?;
            let query_existing_artifact = trans.prepare(r#"
                    SELECT id
                    FROM artifact a
                    WHERE a.uuid_ = $1::uuid;
                "#)?;

            for idx in art_graph.artifacts.graph().node_indices() {
                let art = &art_graph.artifacts[idx];
                let node_id_row = match art {
                    ArtifactDescription::New {id, name, dtype, self_partitioning} => {
                        let id = id.ok_or_else(||
                            ModelError::Other("Attempt to write artifact without id".into()))?;
                        let hash = id.hash.ok_or_else(||
                            ModelError::Other("Attempt to write artifact without hash".into()))?;
                        insert_artifact.query(&[
                            &id.uuid, &(hash as i64), &db_hunk_id,
                            &self_partitioning, &name, &dtype])?
                    },
                    ArtifactDescription::Existing(uuid) => {
                        query_existing_artifact.query(&[&uuid])?
                    }
                };
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
                match *e.weight() {
                    ArtifactRelation::DtypeDepends(ref dtype_rel) =>
                        art_dtype_edge.execute(&[&source_id, &dependent_id, &dtype_rel.name])?,
                    ArtifactRelation::ProducedFrom(ref name) =>
                        art_prod_edge.execute(&[&source_id, &dependent_id, name])?,
                };
            }

            Ok(())
        }

        match hunk.representation {
            RepresentationKind::State =>
                match *payload {
                    Payload::State(ref art_graph) => {
                        insert_graph_description(art_graph, db_hunk_id, &trans)?;
                    }
                    _ => return Err(Error::Store("Attempt to write state hunk with non-state payload".into())),
                },
            RepresentationKind::Delta =>
                match *payload {
                    Payload::Delta(ref delta) => {
                        insert_graph_description(delta.additions(), db_hunk_id, &trans)?;

                        let insert_artifact_removal = trans.prepare(r#"
                                INSERT INTO artifact_removals (hunk_id, removed_artifact_id)
                                SELECT r.hunk_id, a.id
                                FROM (VALUES ($1::bigint, $2::uuid))
                                  AS r (hunk_id, uuid_)
                                JOIN artifact a ON a.uuid_ = r.uuid_;
                            "#)?;
                        for art_uuid in delta.removals() {
                            insert_artifact_removal.execute(&[&db_hunk_id, art_uuid])?;
                        }
                    }
                    _ => return Err(Error::Store("Attempt to write delta hunk with non-delta payload".into())),
                },
            _ => return Err(Error::Store("Attempt to write a hunk with an unsupported representation".into())),
        }

        trans.set_commit();
        Ok(())
    }

    fn read_hunk(
        &self,
        repo: &Repository,
        hunk: &Hunk,
    ) -> Result<Payload<Self::StateType, Self::DeltaType>, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        // TODO: not using the identity hash. Requires some decisions about how
        // to handle get-by-UUID vs. get-with-verified-hash.
        let hunk_rows = trans.query(r#"
                SELECT id, uuid_, hash
                FROM hunk
                WHERE uuid_ = $1::uuid
            "#, &[&hunk.id.uuid])?;
        let hunk_row = hunk_rows.get(0);
        let db_hunk_id: i64 = hunk_row.get(0);
        let hunk_id = Identity {
            uuid: hunk_row.get(1),
            hash: hunk_row.get::<_, i64>(2) as HashType,
        };

        enum NodeRow {
            ID = 0,
            UUID,
            Hash,
            SelfPartitioning,
            ArtifactName,
            DatatypeName,
        };
        let nodes = trans.query(r#"
                SELECT
                    a.id,
                    a.uuid_,
                    a.hash,
                    a.self_partitioning,
                    a.name,
                    d.name
                FROM artifact a
                JOIN datatype d ON a.datatype_id = d.id
                WHERE a.hunk_id = $1::bigint;
            "#, &[&db_hunk_id])?;

        let mut artifacts = ArtifactGraphDescriptionType::new();
        let mut idx_map = HashMap::new();

        for row in &nodes {
            let db_id = row.get::<_, i64>(NodeRow::ID as usize);
            let id = Some(PartialIdentity {
                uuid: row.get(NodeRow::UUID as usize),
                hash: Some(row.get::<_, i64>(NodeRow::Hash as usize) as HashType),
            });
            let dtype_name = &row.get::<_, String>(NodeRow::DatatypeName as usize);
            let node = ArtifactDescription::New {
                id,
                name: row.get(NodeRow::ArtifactName as usize),
                self_partitioning: row.get(NodeRow::SelfPartitioning as usize),
                dtype: dtype_name.to_owned(),
            };

            let node_idx = artifacts.add_node(node);
            idx_map.insert(db_id, node_idx);
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
                WHERE ae.dependent_id = ANY($1::bigint[]);
            "#, &[&idx_map.keys().collect::<Vec<_>>()])?;

        struct ExternalEdge {
            source_db_id: i64,
            dependent_db_id: i64,
            relation: ArtifactRelation,
        };
        let mut external_edges = vec![];
        let mut external_ids = HashSet::new();

        for e in &edges {
            let relation = match e.get::<_, String>(EdgeRow::EdgeType as usize).as_ref() {
                "producer" => ArtifactRelation::ProducedFrom(e.get(EdgeRow::Name as usize)),
                "dtype" => ArtifactRelation::DtypeDepends(DatatypeRelation {
                    name: e.get(EdgeRow::Name as usize),
                }),
                _ => return Err(Error::Store("Unknown artifact graph edge reltype.".into())),
            };

            let source_db_id = e.get(EdgeRow::SourceID as usize);
            let dependent_db_id = e.get(EdgeRow::DependentID as usize);
            match idx_map.get(&source_db_id) {
                Some(source_idx) => {
                    let dependent_idx = idx_map.get(&dependent_db_id).expect("Graph is malformed.");
                    artifacts.add_edge(*source_idx, *dependent_idx, relation)?;
                },
                None => {
                    external_ids.insert(source_db_id);
                    external_edges.push(ExternalEdge {
                        source_db_id,
                        dependent_db_id,
                        relation,
                    });
                }
            }
        }

        if !external_ids.is_empty() {
            // TODO: should error if this is supposed to be a State hunk.
            let mut external_idx_map = HashMap::<i64, _>::new();
            let external_nodes = trans.query(r#"
                    SELECT id, uuid_ FROM artifact
                    WHERE id = ANY($1::bigint[]);
                "#, &[&external_ids.iter().collect::<Vec<_>>()])?;
            for external_node in &external_nodes {
                let node = ArtifactDescription::Existing(external_node.get(1));

                let node_idx = artifacts.add_node(node);
                external_idx_map.insert(external_node.get(0), node_idx);
            }

            for e in external_edges.into_iter() {
                let source_idx = external_idx_map.get(&e.source_db_id).expect("Graph is malformed.");
                let dependent_idx = idx_map.get(&e.dependent_db_id).expect("Graph is malformed.");
                artifacts.add_edge(*source_idx, *dependent_idx, e.relation)?;
            }
        }

        let ag_desc = ArtifactGraphDescription {
            artifacts,
        };

        match hunk.representation {
            RepresentationKind::State => Ok(Payload::State(ag_desc)),
            RepresentationKind::Delta => {
                // TODO: removals
                Ok(Payload::Delta(ArtifactGraphDelta::new(ag_desc, vec![])))
            }
            _ => unreachable!(), // TODO: check
        }
    }
}

impl Storage for ArtifactGraphDtypeBackend<PostgresRepository> {

    fn read_origin_uuids(
        &self,
        repo: &Repository,
    ) -> Result<Option<HunkUuidSpec>, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;

        let origin_uuid_rows = conn.query(r#"
                SELECT artifact_uuid, version_uuid, hunk_uuid FROM origin LIMIT 1;
            "#, &[])?;
        let origin_uuids = if origin_uuid_rows.is_empty() {
            None
        } else {
            let origin_row = origin_uuid_rows.get(0);
            Some(HunkUuidSpec {
                artifact_uuid: origin_row.get(0),
                version_uuid: origin_row.get(1),
                hunk_uuid: origin_row.get(2),
            })
        };

        Ok(origin_uuids)
    }

    fn bootstrap_origin<T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &DatatypesRegistry<T>,
        repo: &Repository,
        hunk: &Hunk,
        ver_graph: &VersionGraph,
        art_graph: &ArtifactGraph,
    ) -> Result<(), Error> {
        use crate::datatype::Storage;

        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        trans.execute(r#"SET CONSTRAINTS _artifact_hunk_id_fk DEFERRED;"#, &[])?;
        let origin_art = hunk.version.artifact;
        let art_db_id: i64 = trans.query(r#"
                INSERT INTO artifact (uuid_, hash, hunk_id, self_partitioning, name, datatype_id)
                SELECT r.uuid_, r.hash, 0, r.self_partitioning, r.name, d.id
                FROM (VALUES ($1::uuid, $2::bigint, $3::boolean, $4::text))
                  AS r (uuid_, hash, self_partitioning, name)
                JOIN datatype d ON d.uuid_ = $5::uuid
                RETURNING id;
            "#,
            &[&origin_art.id.uuid, &(origin_art.id.hash as i64),
              &origin_art.self_partitioning, &origin_art.name,
              &origin_art.dtype_uuid])?
            .get(0).get(0);
        let ver = hunk.version;
        let ver_db_id: i64 = trans.query(r#"
                INSERT INTO version (uuid_, hash, artifact_id, status, representation)
                VALUES ($1::uuid, $2::bigint, $3::bigint,
                        $4::version_status, $5::representation_kind)
                RETURNING id;
            "#, &[&ver.id.uuid, &(ver.id.hash as i64), &art_db_id,
                  &ver.status, &ver.representation])?
            .get(0).get(0);
        let hunk_db_id: i64 = trans.query(r#"
                INSERT INTO hunk (
                    uuid_, hash,
                    version_id, partition_id,
                    representation, completion)
                VALUES (
                    $1::uuid, $2::bigint,
                    $3::bigint, $4::bigint,
                    $5::representation_kind, $6::part_completion)
                RETURNING id;
            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64),
                  &ver_db_id, &(hunk.partition.index as i64),
                  &hunk.representation, &hunk.completion])?
            .get(0).get(0);
        trans.query(r#"
                UPDATE artifact
                SET hunk_id = $1::bigint
                WHERE id = $2::bigint;
            "#,
            &[&hunk_db_id, &art_db_id])?;

        trans.query(r#"
                INSERT INTO origin (artifact_uuid, version_uuid, hunk_uuid)
                VALUES ($1::uuid, $2::uuid, $3::uuid);
            "#,
            &[&origin_art.id.uuid, &ver.id.uuid, &hunk.id.uuid])?;

        // Adjust AG descrition so that origin artifact is existing reference.
        // HACK: Not only is this delta-like descript graph being used as a
        // state, it is not a valid delta either as the partitioning artifact
        // will have a relation with this existing origin artifact.
        let origin_art_id = hunk.version.artifact.id;
        let mut origin_ag_delta = art_graph.as_description(dtypes_registry);
        let origin_art_idx = origin_ag_delta.get_by_uuid(&origin_art_id.uuid).unwrap().0;
        origin_ag_delta.artifacts[origin_art_idx] = ArtifactDescription::Existing(origin_art_id.uuid);
        let payload = Payload::State(origin_ag_delta);

        trans.set_commit();
        drop(trans);

        // Write AG description hunk.
        // HACK: This is violating many contracts, including that a state
        // hunk has a reference to an existing artifact.
        self.write_hunk(repo, hunk, &payload)?;

        Ok(())
    }

    fn tie_off_origin(
        &self,
        repo: &Repository,
        ver_graph: &VersionGraph,
        origin_v_idx: VersionGraphIndex,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let version = &ver_graph[origin_v_idx];

        let ver_id: i64 = trans.query(r#"
                SELECT id
                FROM version
                WHERE uuid_ = $1::uuid;
            "#, &[&version.id.uuid])?
            .get(0).get(0);
        let insert_relation = trans.prepare(r#"
                INSERT INTO version_relation
                  (source_version_id, dependent_version_id, source_id, dependent_id)
                SELECT vp.id, r.child_id, vp.artifact_id, vc.artifact_id
                FROM (VALUES ($1::uuid, $2::bigint))
                AS r (parent_uuid, child_id)
                JOIN version vp ON vp.uuid_ = r.parent_uuid
                JOIN version vc ON vc.id = r.child_id;
            "#)?;

        for (e_idx, p_idx) in ver_graph.versions.parents(origin_v_idx).iter(&ver_graph.versions) {
            let edge = &ver_graph[e_idx];
            let parent = &ver_graph[p_idx];
            match *edge {
                VersionRelation::Dependence(_) => &insert_relation,
                VersionRelation::Parent =>
                    return Err(Error::Store("Attempt to tie off an origin with a parent version.".into())),
            }.execute(&[&parent.id.uuid, &ver_id])?;
        }

        trans.set_commit();
        Ok(())
    }

    fn create_staging_version(
        &mut self,
        repo: &Repository,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

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
            let edge = &ver_graph[e_idx];
            let parent = &ver_graph[p_idx];
            assert_eq!(match *edge {
                VersionRelation::Dependence(_) => &insert_relation,
                VersionRelation::Parent => &insert_parent,
            }.execute(&[&parent.id.uuid, &ver_id])?, 1, "TODO: relation not inserted!");
        }

        trans.set_commit();
        Ok(())
    }

    fn commit_version<'ag, T: DatatypeEnum>(
        &mut self,
        dtypes_registry: &DatatypesRegistry<T>,
        repo: &Repository,
        art_graph: &'ag ArtifactGraph,
        ver_graph: &mut VersionGraph<'ag>,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>
            where
                <T as DatatypeEnum>::InterfaceControllerType :
                    InterfaceController<ProducerController> +
                    InterfaceController<CustomProductionPolicyController>,
    {
        {
            let rc: &PostgresRepository = repo.borrow();

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
            repo,
            art_graph,
            ver_graph,
            v_idx)?;

        Ok(())
    }

    fn fulfill_policy_requirements<'ag>(
        &self,
        repo: &Repository,
        art_graph: &'ag ArtifactGraph,
        ver_graph: &mut VersionGraph<'ag>,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
        requirements: &ProductionPolicyRequirements,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

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
                match *relation {
                    VersionRelation::Dependence(_) => None,
                    VersionRelation::Parent => Some(ver_graph[parent_idx].id.uuid),
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
                    art_graph,
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
                        let dep_art_uuids: Vec<Uuid> = art_graph.artifacts
                            .parents(p_art_idx)
                            .iter(&art_graph.artifacts)
                            .map(|(_, dependency_idx)|
                                // TODO: Not using relation because not clear variants are
                                // distinct after changing producers to datatypes.
                                art_graph[dependency_idx].id.uuid
                            )
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
                let dep_ver_db_ids = dep_ver_idx_map.keys().cloned().collect::<Vec<_>>();
                idx_map.extend(dep_ver_idx_map.into_iter());

                self.get_version_relations(
                    &trans,
                    art_graph,
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

    fn get_version<'ag>(
        &self,
        repo: &Repository,
        art_graph: &'ag ArtifactGraph,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'ag>), Error> {
        let rc: &PostgresRepository = repo.borrow();

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
            art_graph,
            &mut ver_graph,
            &[ver_node_id],
            &mut idx_map,
            None,
            None)?;

        Ok((ver_node_idx, ver_graph))
    }

    fn get_version_graph<'ag>(
        &self,
        repo: &Repository,
        art_graph: &'ag ArtifactGraph,
    ) -> Result<VersionGraph<'ag>, Error> {
        let rc: &PostgresRepository = repo.borrow();

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
            art_graph,
            &mut ver_graph,
            &idx_map.keys().cloned().collect::<Vec<_>>(),
            &mut idx_map,
            // Can use incoming edges only since all nodes are fetched.
            Some(petgraph::Direction::Incoming),
            Some(petgraph::Direction::Incoming))?;

        Ok(ver_graph)
    }

    fn create_hunks<'ag: 'vg1 + 'vg2, 'vg1, 'vg2, H>(
        &mut self,
        repo: &Repository,
        hunks: &[H],
    ) -> Result<(), Error>
        where H: std::borrow::Borrow<Hunk<'ag, 'vg1, 'vg2>>
    {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let insert_hunk = trans.prepare(r#"
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
                  ON (v.uuid_ = r.v_uuid AND v.hash = r.v_hash)
                RETURNING version_id;
            "#)?;

        let insert_precedence = trans.prepare(r#"
                INSERT INTO hunk_precedence
                    (merge_version_id, partition_id, precedent_version_id)
                SELECT r.merge_version_id, r.partition_id, hpv.id
                FROM (VALUES ($1::bigint, $2::bigint, $3::uuid))
                  AS r (merge_version_id, partition_id, precedent_version_uuid)
                JOIN version hpv ON (hpv.uuid_ = r.precedent_version_uuid);
            "#)?;

        for hunk in hunks {
            let hunk = hunk.borrow();

            if !hunk.is_valid() {
                return Err(ModelError::Other("Hunk is invalid.".into()).into());
            }

            // TODO should check that version is not committed
            let version_id_row = insert_hunk.query(
                    &[&hunk.id.uuid, &(hunk.id.hash as i64),
                      &hunk.version.id.uuid, &(hunk.version.id.hash as i64),
                      &(hunk.partition.index as i64),
                      &hunk.representation, &hunk.completion])?;

            if let Some(ref ver_uuid) = hunk.precedence {
                let version_id: i64 = version_id_row.get(0).get(0);
                insert_precedence.execute(
                    &[&version_id, &(hunk.partition.index as i64), ver_uuid])?;
            }
        }

        trans.set_commit();
        Ok(())
    }

    fn get_hunks<'ag: 'vg1 + 'vg2, 'vg1, 'vg2>(
        &self,
        repo: &Repository,
        version: &'vg2 Version<'ag>,
        partitioning: &'vg1 Version<'ag>,
        partitions: Option<&BTreeSet<PartitionIndex>>,
    ) -> Result<Vec<Hunk<'ag, 'vg1, 'vg2>>, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        enum HunkRow {
            UUID = 0,
            Hash,
            PartitionID,
            Representation,
            Completion,
            PrecedingVersionUuid,
        };
        let hunk_query = r#"
                SELECT
                    h.uuid_, h.hash, h.partition_id, h.representation, h.completion,
                    hpv.uuid_
                FROM version v
                JOIN hunk h ON (h.version_id = v.id)
                LEFT JOIN hunk_precedence hp
                  ON (hp.merge_version_id = v.id AND h.partition_id = hp.partition_id)
                LEFT JOIN version hpv ON (hp.precedent_version_id = v.id)
                WHERE v.uuid_ = $1::uuid AND v.hash = $2::bigint"#;
        let hunk_rows = match partitions {
            Some(part_idxs) => {
                // TODO: annoying vec cast
                let part_idxs_db = part_idxs.iter().map(|i| *i as i64).collect::<Vec<i64>>();
                trans.query(
                    // TODO: can change to concat! or something after const fns land
                    format!("{}{}", &hunk_query, " AND h.partition_id = ANY($3::bigint[])").as_str(),
                    &[&version.id.uuid, &(version.id.hash as i64), &part_idxs_db])?
            },
            None =>
                trans.query(
                    hunk_query,
                    &[&version.id.uuid, &(version.id.hash as i64)])?
        };

        let mut hunks = Vec::new();
        for row in &hunk_rows {
            hunks.push(Hunk {
                id: Identity {
                    uuid: row.get(HunkRow::UUID as usize),
                    hash: row.get::<_, i64>(HunkRow::Hash as usize) as HashType,
                },
                version,
                partition: Partition {
                    partitioning,
                    index: row.get::<_, i64>(HunkRow::PartitionID as usize) as PartitionIndex,
                },
                representation: row.get(HunkRow::Representation as usize),
                completion: row.get(HunkRow::Completion as usize),
                precedence: row.get(HunkRow::PrecedingVersionUuid as usize),
            });
        }

        Ok(hunks)
    }

    fn write_production_policies(
        &mut self,
        repo: &Repository,
        artifact: &Artifact,
        policies: EnumSet<ProductionPolicies>,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        // TODO: ignoring hash here, because semantics of producer versions
        // (esp. uncommitted) are unclear.
        trans.execute(r#"
                INSERT INTO producer_artifact (artifact_id, policies)
                SELECT a.id, r.strategy
                FROM (VALUES ($1::uuid, $2::production_policy[]))
                  AS r (a_uuid, strategy)
                JOIN artifact a
                  ON (a.uuid_ = r.a_uuid)
                ON CONFLICT (artifact_id) DO UPDATE SET policies = EXCLUDED.policies;
            "#, &[&artifact.id.uuid, &policies.iter().collect::<Vec<_>>()])?;

        trans.set_commit();
        Ok(())
    }

    fn get_production_policies(
        &self,
        repo: &Repository,
        artifact: &Artifact,
    ) -> Result<Option<EnumSet<ProductionPolicies>>, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let policies_row = trans.query(r#"
                SELECT pa.policies
                FROM artifact a
                JOIN producer_artifact pa ON (pa.artifact_id = a.id)
                WHERE a.uuid_ = $1::uuid;"#,
            &[&artifact.id.uuid])?;
        Ok(match policies_row.len() {
            0 => None,
            _ => {
                let policies = policies_row.get(0).get::<_, Vec<ProductionPolicies>>(0);
                let policies = policies.into_iter()
                    .fold(EnumSet::new(), |union, p| union | p);
                Some(policies)
            },
        })
    }

    fn write_production_specs<'ag>(
        &mut self,
        repo: &Repository,
        version: &Version<'ag>,
        specs: ProductionStrategySpecs,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        // TODO: ignoring hash here, because semantics of producer versions
        // (esp. uncommitted) are unclear.
        trans.execute(r#"
                INSERT INTO producer_version (version_id, strategy)
                SELECT v.id, r.strategy
                FROM (VALUES ($1::uuid, $2::text))
                  AS r (v_uuid, strategy)
                JOIN version v
                  ON (v.uuid_ = r.v_uuid);
            "#, &[&version.id.uuid, &specs.representation])?;

        trans.set_commit();
        Ok(())
    }

    fn get_production_specs<'ag>(
        &self,
        repo: &Repository,
        version: &Version<'ag>,
    ) -> Result<ProductionStrategySpecs, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        // TODO: ignoring hash here, because semantics of producer versions
        // (esp. uncommitted) are unclear.
        let spec_row = trans.query(r#"
                SELECT pv.strategy
                FROM version v
                JOIN producer_version pv ON (pv.version_id = v.id)
                WHERE v.uuid_ = $1::uuid;"#,
            &[&version.id.uuid])?;
        Ok(ProductionStrategySpecs {
            representation: spec_row.get(0).get(0),
        })
    }
}
