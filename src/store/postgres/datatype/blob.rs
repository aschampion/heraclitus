use std::borrow::Borrow;

use heraclitus_core::{
    postgres,
    schemer,
    schemer_postgres,
};
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::migration;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use crate::{
    RepresentationKind,
    Error,
    Hunk,
};
use crate::datatype::{
    Payload,
};
use crate::datatype::blob::{
    BlobDatatypeBackend,
    Storage,
};
use crate::repo::Repository;
use crate::store::postgres::{PostgresMigratable, PostgresRepository};


struct PGMigrationBlobs;
migration!(
    PGMigrationBlobs,
    "3d314b44-0305-4602-8493-9e42f6864103",
    ["7d1fb6d1-a1b0-4bd4-aa6d-e3ee71c4353b",],
    "create blob table");

impl PostgresMigration for PGMigrationBlobs {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/blob_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/blob_0001.down.sql"))
    }
}


impl PostgresMigratable for BlobDatatypeBackend<PostgresRepository> {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![
            Box::new(PGMigrationBlobs),
        ]
    }
}

impl super::PostgresMetaController for BlobDatatypeBackend<PostgresRepository> {}

impl crate::datatype::Storage for BlobDatatypeBackend<PostgresRepository> {
    blob_common_model_controller_impl!();

    fn write_hunk(
        &mut self,
        repo: &Repository,
        hunk: &Hunk,
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> Result<(), Error> {
        let rc: &PostgresRepository = repo.borrow();

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        match hunk.representation {
            RepresentationKind::State =>
                match *payload {
                    Payload::State(ref blob) => {
                        trans.execute(r#"
                                INSERT INTO blob_dtype_state (hunk_id, blob)
                                SELECT h.id, r.blob
                                FROM (VALUES ($1::uuid, $2::bigint, $3::bytea))
                                  AS r (uuid_, hash, blob)
                                JOIN hunk h
                                  ON (h.uuid_ = r.uuid_ AND h.hash = r.hash);
                            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64), &blob])?;
                    }
                    _ => return Err(Error::Store("Attempt to write state hunk with non-state payload".into())),
                },
            RepresentationKind::Delta =>
                match *payload {
                    Payload::Delta((ref indices_usize, ref bytes)) => {
                        // TODO: Have to copy array here for type coercion.
                        let indices = indices_usize.iter().map(|i| *i as i64).collect::<Vec<i64>>();
                        trans.execute(r#"
                                INSERT INTO blob_dtype_delta (hunk_id, indices, bytes)
                                SELECT h.id, r.indices, r.bytes
                                FROM (VALUES ($1::uuid, $2::bigint, $3::bigint[], $4::bytea))
                                  AS r (uuid_, hash, indices, bytes)
                                JOIN hunk h
                                  ON (h.uuid_ = r.uuid_ AND h.hash = r.hash);
                            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64), &indices, &bytes])?;
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

        let payload = match hunk.representation {
            RepresentationKind::State => {
                let blob_rows = trans.query(r#"
                        SELECT b.blob
                        FROM blob_dtype_state b
                        JOIN hunk h
                          ON (h.id = b.hunk_id)
                        WHERE h.uuid_ = $1::uuid AND h.hash = $2::bigint;
                    "#, &[&hunk.id.uuid, &(hunk.id.hash as i64)])?;
                Payload::State(blob_rows.get(0).get(0))
            },
            RepresentationKind::Delta => {
                let blob_rows = trans.query(r#"
                        SELECT b.indices, b.bytes
                        FROM blob_dtype_delta b
                        JOIN hunk h
                          ON (h.id = b.hunk_id)
                        WHERE h.uuid_ = $1::uuid AND h.hash = $2::bigint;
                    "#, &[&hunk.id.uuid, &(hunk.id.hash as i64)])?;
                let delta_row = blob_rows.get(0);
                Payload::Delta((
                    delta_row.get::<_, Vec<i64>>(0).into_iter().map(|i| i as usize).collect(),
                    delta_row.get(1)))
            },
            _ => return Err(Error::Store("Attempt to read a hunk with an unsupported representation".into())),
        };

        Ok(payload)
    }
}

impl Storage for BlobDatatypeBackend<PostgresRepository> {}
