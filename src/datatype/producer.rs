use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};

use ::{DatatypeRepresentationKind};
use ::datatype::{Description, Model};
use ::datatype::interface::PartitioningController;
use ::repo::{PostgresMigratable};
use ::store::Store;


pub struct NoopProducer;

impl<T> Model<T> for NoopProducer {
    fn info(&self) -> Description {
        Description {
            name: "NoopProducer".into(),
            version: 1,
            representations: vec![DatatypeRepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec!["Producer"],
            dependencies: vec![],
        }
    }

    fn meta_controller(&self, store: Store) -> Option<super::StoreMetaController> {
        match store {
            Store::Postgres => Some(super::StoreMetaController::Postgres(
                Box::new(NoopProducerController {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        store: Store,
        name: &str
    ) -> Option<T> {
        None
    }
}

pub struct NoopProducerController;

impl super::MetaController for NoopProducerController {}

impl PostgresMigratable for NoopProducerController {
    fn register_migrations(&self, migrator: &mut Migrator<PostgresAdapter>) {}
}

impl super::PostgresMetaController for NoopProducerController {}
