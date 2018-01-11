extern crate schemer;
extern crate uuid;


use std::borrow::BorrowMut;

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use ::{RepresentationKind, Error, Hunk};
use super::{Description, Payload, Store};
use ::repo::{PostgresMigratable, PostgresRepoController};


#[derive(Default)]
pub struct Blob;

impl<T> super::Model<T> for Blob {
    fn info(&self) -> Description {
        Description {
            name: "Blob".into(),
            version: 1,
            representations: vec![
                        RepresentationKind::State,
                        RepresentationKind::Delta,
                    ]
                    .into_iter()
                    .collect(),
            implements: vec![],
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

// TODO instead have MetaController that can handle stuff hera needs to know,
// like content hashing, but separate dtype-specific controls into separate
// trait that dependent types can concretely call.

// So: matrix of controllers:
// 1. For fns generic to all dtypes: MetaController?
// 2. For fns specific to this dtype: ModelController?
// 3. For fns generic to store: ??? PostgresDatatypeModelController? (Ugh.)
//    (Or MetaController<StoreType>?)
// 4. For fns specific to store's implementation of this dtype: concrete struct impl
//    ^^ SHOULD BE CRATE PRIVATE

// For the enum-based modelcontroller scheme, these would be:
// 1. MetaController
// 2. ModelController
// 3. [Postgres]MetaController
//
// ... and the specific controller returned by `Model.controller` can be a
// compose of these traits, because it need not be the same trait for all
// store backends.
//
// - What facets of this work through trait objects and what through
//   monomorphization?
// - Below we have this ModelController extend a generic trait, in addition to
//   composing with the MetaController trait. Which is preferrable?

type StateType = Vec<u8>;
type DeltaType = (Vec<usize>, Vec<u8>);

macro_rules! common_model_controller_impl {
    () => (
        type StateType = StateType;
        type DeltaType = DeltaType;

        fn compose_state(
            &self,
            state: &mut Self::StateType,
            delta: &Self::DeltaType,
        ) {
            for (&idx, &val) in delta.0.iter().zip(delta.1.iter()) {
                state[idx] = val;
            }
        }
    )
}

pub trait ModelController: super::ModelController<StateType=StateType, DeltaType=DeltaType> {
}


pub struct PostgresStore {}

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


impl super::MetaController for PostgresStore {
    // fn register_with_repo(&self, repo_controller: &mut PostgresRepoController) {
    //     repo_controller.register_postgres_migratable(Box::new(*self));
    // }
}

impl PostgresMigratable for PostgresStore {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![
            Box::new(PGMigrationBlobs),
        ]
    }
}

impl super::PostgresMetaController for PostgresStore {}

impl super::ModelController for PostgresStore {
    common_model_controller_impl!();

    fn write_hunk(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> Result<(), Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

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
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<Payload<Self::StateType, Self::DeltaType>, Error> {
        let rc: &mut PostgresRepoController = repo_control.borrow_mut();

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

impl ModelController for PostgresStore {}
