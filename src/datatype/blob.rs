extern crate uuid;


use std::sync::Arc;
use std::sync::Mutex;

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;
use url::Url;

use super::super::{Datatype, DatatypeRepresentationKind};
use super::{DependencyDescription, DependencyStoreRestriction, Description, Store};
use ::repo::{PostgresRepoController, PostgresMigratable};


pub struct Blob;

impl super::Model for Blob {
    fn info(&self) -> Description {
        Description {
            datatype: Datatype::new(
                // TODO: Fake UUID.
                Uuid::new_v4(),
                "Blob".into(),
                1,
                vec![DatatypeRepresentationKind::State]
                    .into_iter()
                    .collect(),
            ),
            // TODO: Fake dependency.
            dependencies: vec![
                // DependencyDescription::new(
                //     "test",
                //     "this",
                //     DependencyStoreRestriction::Stores(vec![Store::Postgres].into_iter().collect()),
                // ),
            ],
        }
    }

    fn controller(&self, store: Store) -> Option<super::StoreMetaController> {
        match store {
            Store::Postgres => Some(super::StoreMetaController::Postgres(Box::new(PostgresStore {}))),
            _ => None,
        }
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
//
// - What facets of this work through trait objects and what through
//   monomorphization?
// - Below we have this ModelController extend a generic trait, in addition to
//   composing with the MetaController trait. Which is preferrable?

trait ModelController: super::ModelController {
    // Does this return a version? No, should be graph controller, right? But
    // then how is this version bootstrapped? What about squashing/staging in
    // existing versions?
    // fn write(&self, context: &::Context, version: &::Version, blob: &[u8]);
    // fn read(&self, context: &::Context, version: &::Version) -> Vec<u8>;
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

struct PostgresStore {}

struct PGMigrationBlobs;
migration!(PGMigrationBlobs, 1, "create blob table");

impl PostgresMigration for PGMigrationBlobs {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("CREATE TABLE blob_dtype (id BIGINT PRIMARY KEY);", &[]).map(|_| ())
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("DROP TABLE blob_dtype;", &[]).map(|_| ())
    }
}


impl super::MetaController<PostgresRepoController> for PostgresStore {
    // fn register_with_repo(&self, repo_controller: &mut PostgresRepoController) {
    //     repo_controller.register_postgres_migratable(Box::new(*self));
    // }
}

impl PostgresMigratable for PostgresStore {
    fn register_migrations(&self, migrator: &mut Migrator<PostgresAdapter>) {
        migrator.register(Box::new(PGMigrationBlobs));
    }
}

impl super::ModelController for PostgresStore {}

impl ModelController for PostgresStore {
//     fn write(&self, context: &::Context, version: &::Version, blob: &[u8]) {
//         unimplemented!();
//     }

//     fn read(&self, context: &::Context, version: &::Version) -> Vec<u8> {
//         // TODO: mocked.
//         return vec![0, 1, 2];
//     }
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
