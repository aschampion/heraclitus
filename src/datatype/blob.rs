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
                Uuid::parse_str("0").unwrap(),
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

trait ModelController: super::ModelController {
    // Does this return a version? No, should be graph controller, right? But then how is this version bootstrapped? What about squashing/staging in existing versions?
    fn write(&self, context: &::Context, version: &::Version, blob: &[u8]);
    fn read(&self, context: &::Context, version: &::Version) -> Vec<u8>;
}

struct PostgresStore {}

impl super::MetaController for PostgresStore {}

impl super::ModelController for PostgresStore {}

impl ModelController for PostgresStore {
    fn write(&self, context: &::Context, version: &::Version, blob: &[u8]) {
        unimplemented!();
    }

    fn read(&self, context: &::Context, version: &::Version) -> Vec<u8> {
        // TODO: mocked.
        return vec![0, 1, 2];
    }
}
