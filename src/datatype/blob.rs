extern crate uuid;


use std::sync::Arc;
use std::sync::Mutex;

use uuid::Uuid;

use super::super::{Datatype, DatatypeRepresentationKind};
use super::{DependencyDescription, DependencyStoreRestriction, Description, Store};


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
                DependencyDescription::new(
                    "test",
                    "this",
                    DependencyStoreRestriction::Stores(vec![Store::Postgres].into_iter().collect()),
                ),
            ],
        }
    }

    fn controller(&self, store: Store) -> Option<Box<super::MetaController>> {
        match store {
            Store::Postgres => Some(Box::new(PostgresStore {})),
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

trait SomeStoreType {}

struct PostgresStoreType {}

impl SomeStoreType for PostgresStoreType {}

trait BlobMC<T> where T: SomeStoreType {
    fn write(&self, context: &::Context<T>, version: &::Version, blob: &[u8]);
}

impl BlobMC<PostgresStoreType> for Blob {
    fn write(&self, context: &::Context<PostgresStoreType>, version: &::Version, blob: &[u8]) {
        unimplemented!();
    }
}

// // Why is above better than below? Callers can call with a Context<T> without
// // needing to name the specialized type/trait/whatever.

struct PostgresStore {}

impl super::MetaController for PostgresStore {}

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
