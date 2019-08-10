use std::borrow::Borrow;
use std::collections::HashMap;
use std::str::FromStr;

use heraclitus_core::{
    postgres,
    schemer,
    schemer_postgres,
    uuid,
};
use uuid::Uuid;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::migration;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use crate::{
    Artifact,
    HashType,
    Identity,
    Error,
    Version,
};
use crate::datatype::reference::{
    ArtifactSpecifier,
    BranchRevisionTip,
    Storage,
    RefBackend,
    RevisionPath,
    UuidSpecifier,
    VersionSpecifier,
};
use crate::store::postgres::{PostgresMigratable, PostgresRepository};

use super::PostgresMetaController;


struct PGMigrationRefs;
migration!(
    PGMigrationRefs,
    "56f909ff-056b-4a7e-b589-c43d479cf9d6",
    [
        "7d1fb6d1-a1b0-4bd4-aa6d-e3ee71c4353b", // Artifact graph 0001.
    ],
    "create ref table");

impl PostgresMigration for PGMigrationRefs {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/ref_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/ref_0001.down.sql"))
    }
}

impl PostgresMigratable for RefBackend<PostgresRepository> {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![
            Box::new(PGMigrationRefs),
        ]
    }
}

impl PostgresMetaController for RefBackend<PostgresRepository> {}

impl Storage for RefBackend<PostgresRepository> {
    fn get_branch_revision_tips(
        &self,
        repo: &crate::repo::Repository,
        artifact: &Artifact,
    ) -> Result<HashMap<BranchRevisionTip, Uuid>, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        enum BranchHeadRow {
            BranchName = 0,
            RevisionPathName,
            VersionUUID,
        };
        let branch_head_rows = trans.query(r#"
            SELECT b.name, rp.name, rv.uuid_
            FROM artifact a
            JOIN branch b ON (b.ref_artifact_id = a.id)
            JOIN revision_path rp ON (rp.branch_id = b.id AND rp.name = 'HEAD')
            JOIN version rv ON (rv.id = rp.ref_version_id)
            WHERE (a.uuid_ = $1::uuid AND a.hash = $2::bigint);
        "#, &[&artifact.id.uuid, &(artifact.id.hash as i64)])?;

        let map = branch_head_rows.iter().map(|row| {
            let br_tip = BranchRevisionTip {
                name: row.get(BranchHeadRow::BranchName as usize),
                revision: RevisionPath::from_str(&row.get::<_, String>(BranchHeadRow::RevisionPathName as usize))
                    .expect("TODO"),
            };
            let uuid = row.get(BranchHeadRow::VersionUUID as usize);
            (br_tip, uuid)
        }).collect();

        Ok(map)
    }

    fn set_branch_revision_tips(
        &mut self,
        repo: &crate::repo::Repository,
        artifact: &Artifact,
        tip_versions: &HashMap<BranchRevisionTip, Uuid>,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let mut b_names = vec![];
        let mut rp_names = vec![];
        let mut v_uuids = vec![];

        for (tip, uuid) in tip_versions {
            b_names.push(&tip.name);
            rp_names.push(tip.revision.to_string());
            v_uuids.push(uuid);
        }

        trans.execute(r#"
                INSERT INTO revision_path (branch_id, name, ref_version_id)
                (SELECT b.id, r.rp_name, v.id
                FROM UNNEST($1::text[], $2::text[], $3::uuid[])
                  AS r (b_name, rp_name, v_uuid)
                JOIN branch b ON (
                    b.name = r.b_name AND
                    b.ref_artifact_id = (
                        SELECT id
                        FROM artifact
                        WHERE uuid_ = $4::uuid
                          AND hash = $5::bigint
                    )
                )
                JOIN version v
                  ON (v.uuid_ = r.v_uuid))
                ON CONFLICT (branch_id, name) DO UPDATE SET ref_version_id = EXCLUDED.ref_version_id;
            "#,
            &[&b_names, &rp_names, &v_uuids, &artifact.id.uuid, &(artifact.id.hash as i64)])?;

        Ok(trans.commit()?)
    }

    fn write_message(
        &mut self,
        repo: &crate::repo::Repository,
        version: &Version,
        message: &Option<String>,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        match *message {
            Some(ref t) => {
                let conn = rc.conn()?;
                let trans = conn.transaction()?;

                trans.execute(r#"
                    INSERT INTO ref (version_id, message)
                    SELECT v.id, r.message
                    FROM (VALUES ($1::uuid, $2::bigint, $3::text))
                      AS r (uuid_, hash, message)
                    JOIN version v
                      ON (v.uuid_ = r.uuid_ AND v.hash = r.hash);
                "#, &[&version.id.uuid, &(version.id.hash as i64), t])?;

                trans.set_commit();
                Ok(())
            },
            None => Ok(())
        }
    }

    fn read_message(
        &self,
        repo: &crate::repo::Repository,
        version: &Version,
    ) -> Result<Option<String>, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let message_rows = trans.query(r#"
            SELECT v.message
            FROM version v
            WHERE (v.uuid_ = $1::uuid AND v.hash = $2::bigint);
        "#, &[&version.id.uuid, &(version.id.hash as i64)])?;

        Ok(message_rows.get(0).get(0))
    }

    fn create_branch(
        &mut self,
        repo: &crate::repo::Repository,
        ref_version: &Version,
        name: &str,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        trans.execute(r#"
            WITH insert_branch AS (
                INSERT INTO branch (ref_artifact_id, name)
                SELECT a.id, $3::text
                FROM artifact a WHERE uuid_ = $4::uuid AND hash = $5::bigint
                RETURNING id
            )
            INSERT INTO revision_path (branch_id, name, ref_version_id)
            SELECT
                ib.id,
                'HEAD',
                (SELECT id FROM version
                 WHERE uuid_ = $1::uuid AND hash = $2::bigint)
            FROM insert_branch AS ib (id);
        "#, &[&ref_version.id.uuid, &(ref_version.id.hash as i64), &name,
              &ref_version.artifact.id.uuid, &(ref_version.artifact.id.hash as i64)])?;

        trans.set_commit();
        Ok(())
    }

    fn get_version_uuid(
        &self,
        repo: &crate::repo::Repository,
        specifier: &VersionSpecifier,
    ) -> Result<Uuid, Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let version_rows = match *specifier {
            VersionSpecifier::Uuid(ref us) => {
                match *us {
                    UuidSpecifier::Complete(ref uuid) => {
                        trans.query(r#"
                            SELECT v.uuid_
                            FROM version v
                            WHERE v.uuid_ = $1::uuid;
                        "#, &[uuid])?
                    },
                    UuidSpecifier::Partial(ref prefix) => {
                        trans.query(r#"
                            SELECT v.uuid_
                            FROM version v
                            WHERE v.uuid_::text ILIKE $1::text || '%';
                        "#, &[prefix])?
                    }
                }
            },
            VersionSpecifier::BranchArtifact {
                ref_artifact: ref ref_art,
                branch_revision: ref br,
                artifact: ref art
            } => {
                assert_eq!(br.revision.offset, 0, "Non-tip revisions not yet supported"); // TODO

                let br_rev_path_name = match br.revision.path {
                    RevisionPath::Head => "HEAD",
                    RevisionPath::Named(ref name) => name,
                };

                enum ArtFilterParam<'a> {
                    Uuid(&'a Uuid),
                    Text(&'a str),
                }

                impl<'a> ArtFilterParam<'a> {
                    fn uuid(&self) -> Option<&Uuid> {
                        match *self {
                            ArtFilterParam::Uuid(uuid) => Some(uuid),
                            ArtFilterParam::Text(_) => None,
                        }
                    }

                    fn text(&self) -> Option<&str> {
                        match *self {
                            ArtFilterParam::Uuid(_) => None,
                            ArtFilterParam::Text(s) => Some(s),
                        }
                    }
                }

                let (ref_art_filter, ref_filter_param) = match *ref_art {
                    ArtifactSpecifier::Uuid(ref us) => {
                        match *us {
                            UuidSpecifier::Complete(ref uuid) =>
                                ("$2::text IS NULL AND ra.uuid_ = $1::uuid", ArtFilterParam::Uuid(uuid)),
                            UuidSpecifier::Partial(ref prefix) =>
                                ("$1::uuid IS NULL AND ra.uuid_::text ILIKE $2::text || '%'", ArtFilterParam::Text(prefix)),
                        }
                    },
                    ArtifactSpecifier::Name(ref name) =>
                        ("$1::uuid IS NULL AND ra.name = $2::text", ArtFilterParam::Text(name)),
                };

                let (art_filter, filter_param) = match *art {
                    ArtifactSpecifier::Uuid(ref us) => {
                        match *us {
                            UuidSpecifier::Complete(ref uuid) =>
                                ("$6::text IS NULL AND tva.uuid_ = $5::uuid", ArtFilterParam::Uuid(uuid)),
                            UuidSpecifier::Partial(ref prefix) =>
                                ("$5::uuid IS NULL AND tva.uuid_::text ILIKE $6::text || '%'", ArtFilterParam::Text(prefix)),
                        }
                    },
                    ArtifactSpecifier::Name(ref name) =>
                        ("$5::uuid IS NULL AND tva.name = $6::text", ArtFilterParam::Text(name)),
                };

                trans.query(
                    &format!(r#"
                        SELECT tv.uuid_
                        FROM artifact ra
                        JOIN branch b ON (b.ref_artifact_id = ra.id)
                        JOIN revision_path rp ON (rp.branch_id = b.id)
                        JOIN version_relation rvr ON (rvr.dependent_version_id = rp.ref_version_id)
                        JOIN version tv ON (tv.id = rvr.source_version_id)
                        JOIN artifact tva ON (tva.id = tv.artifact_id)
                        WHERE {}
                          AND b.name = $3::text
                          AND rp.name = $4::text
                          AND {};
                    "#, ref_art_filter, art_filter),
                    &[&ref_filter_param.uuid(), &ref_filter_param.text(),
                      &br.name, &br_rev_path_name,
                      &filter_param.uuid(), &filter_param.text()])?
            },
        };

        match version_rows.len() {
            0 => panic!("TODO: no rows"),
            1 => Ok(version_rows.get(0).get(0)),
            _ => panic!("TODO: too many rows"),
        }
    }
}
