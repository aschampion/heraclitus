use std::sync::Arc;
use std::sync::Mutex;

pub struct Blob;

impl super::Model for Blob {
    type Controller = ModelController;

    fn info() -> super::Description {
        super::Description {
            name: "Blob",
            version: 1,
            dependencies: vec![super::DependencyDescription::new("test", "this", super::DependencyStoreRestriction::Stores(vec!(super::Store::Postgres).into_iter().collect()))],
        }
    }

    fn controller(store: super::Store) -> Option<Arc<Mutex<Self::Controller>>> {
        match store {
            Postgres => Some(Arc::new(Mutex::new(PostgresStore {}))),
            _ => None,
        }
    }
}

trait ModelController: super::ModelController {
    // Does this return a version? No, should be graph controller, right? But then how is this version bootstrapped? What about squashing/staging in existing versions?
    fn write(&self, context: &::Context, version: &::Version, blob: &[u8]);
    fn read(&self, context: &::Context, version: &::Version) -> Vec<u8>;
}

struct PostgresStore {}

impl super::ModelController for PostgresStore {}

impl ModelController for PostgresStore {
    fn write(&self, context: &::Context, version: &::Version, blob: &[u8]) {
        return;
    }

    fn read(&self, context: &::Context, version: &::Version) -> Vec<u8> {
        return vec![0,1,2];
    }
}