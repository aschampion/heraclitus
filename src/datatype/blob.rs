extern crate schemer;
extern crate uuid;


use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::Migrator;
use schemer_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;
use url::Url;

use super::super::{Datatype, DatatypeRepresentationKind, Error, Hunk};
use super::{DependencyDescription, DependencyStoreRestriction, Description, InterfaceController, Store};
use ::repo::{PostgresRepoController, PostgresMigratable};


#[derive(Default)]
pub struct Blob;

impl<T> super::Model<T> for Blob {
    fn info(&self) -> Description {
        Description {
            name: "Blob".into(),
            version: 1,
            representations: vec![DatatypeRepresentationKind::State]
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
        store: Store,
        name: &str,
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

pub trait ModelController: super::ModelController {
    fn hash(
        &self,
        blob: &[u8],
    ) -> u64 {
        let mut s = DefaultHasher::new();
        blob.hash(&mut s);
        s.finish()
    }

    fn write(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
        blob: &[u8],
    ) -> Result<(), Error>;

    fn read(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<Vec<u8>, Error>;
}

// // Sketching: could this all be done with monomorph?

// trait SomeStoreType {}

// struct PostgresStoreType {}

// trait Bar {
//     fn baz(&self);
// }

// impl Bar for PostgresStoreType {
//     fn baz(&self) {
//         println!("Is assoc traits vtable accessible for obj in impl scoped by other trait?");
//     }
// }

// struct FilesystemStoreType {}

// impl SomeStoreType for PostgresStoreType {}

// impl SomeStoreType for FilesystemStoreType {}

// trait BlobMC<T> where T: SomeStoreType {
//     fn biff(&self) {
//         println!("non-variant impls?");
//     }
//     fn foo(&self, context: &::Context<T>);
//     fn write(&self, context: &::Context<T>, version: &::Version, blob: &[u8]);
// }

// impl BlobMC<PostgresStoreType> for Blob {
//     fn foo(&self, context: &::Context<PostgresStoreType>) {
//         (self as &BlobMC<PostgresStoreType>).biff();
//         context.store_type.baz();
//         println!("PST!");
//     }
//     fn write(&self, context: &::Context<PostgresStoreType>, version: &::Version, blob: &[u8]) {
//         unimplemented!();
//     }
// }

// impl BlobMC<FilesystemStoreType> for Blob {
//     fn foo(&self, context: &::Context<FilesystemStoreType>) {
//         println!("FST!");
//     }
//     fn write(&self, context: &::Context<FilesystemStoreType>, version: &::Version, blob: &[u8]) {
//         unimplemented!();
//     }
// }
// // Why is above better than below? Callers can call with a Context<T> without
// // needing to name the specialized type/trait/whatever.

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
        transaction.execute("DROP TABLE blob_dtype;", &[]).map(|_| ())
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

impl super::ModelController for PostgresStore {}

impl ModelController for PostgresStore {
    fn write(
        &mut self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
        blob: &[u8],
    ) -> Result<(), Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        trans.execute(r#"
                INSERT INTO blob_dtype (hunk_id, blob)
                SELECT h.id, r.blob
                FROM (VALUES ($1::uuid, $2::bigint, $3::bytea))
                  AS r (uuid_, hash, blob)
                JOIN hunk h
                  ON (h.uuid_ = r.uuid_ AND h.hash = r.hash);
            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64), &blob])?;

        trans.set_commit();
        Ok(())
    }

    fn read(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        hunk: &Hunk,
    ) -> Result<Vec<u8>, Error> {
        let rc = match *repo_control {
            ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("PostgresStore received a non-Postgres context")
        };

        let conn = rc.conn()?;
        let trans = conn.transaction()?;

        let blob_rows = trans.query(r#"
                SELECT b.blob
                FROM blob_dtype b
                JOIN hunk h
                  ON (h.id = b.hunk_id)
                WHERE h.uuid_ = $1::uuid AND h.hash = $2::bigint;
            "#, &[&hunk.id.uuid, &(hunk.id.hash as i64)])?;
        let blob = blob_rows.get(0).get(0);

        Ok(blob)
    }
}


// #[cfg(test)]
// mod tests {
//     #[test]
//     fn test_blob_sketch() {
//         use super::*;

//         let b = Blob {};
//         let pgstore = PostgresStoreType {};

//         let repo = ::Repository {
//             // TODO: fake UUID, version
//             id: ::Identity{uuid: Uuid::new_v4(), hash: 0},
//             name: "Test repo".into(),
//             url: Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap()
//         };
//         let context = ::Context {repo: repo, store_type: pgstore};

//         b.foo(&context);
//     }
// }
