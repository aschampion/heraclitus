use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};

use ::{DatatypeRepresentationKind};
use ::datatype::{Control, Description, InterfaceController, Model};
use ::datatype::interface::ProducerController;
use ::repo::{PostgresMigratable};
use ::store::Store;


#[derive(Default)]
pub struct NoopProducer;

impl<T: InterfaceController<ProducerController>> Model<T> for NoopProducer {
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
        name: &str,
    ) -> Option<T> {
        match name {
            "Producer" => {
                let control: Box<ProducerController> = Box::new(NoopProducerController {});
                Some(T::from(control))
            },
            _ => None,
        }
    }
}

impl<T, U: ?Sized> Control<T, U> for NoopProducer where T: InterfaceController<U> {}

impl<T> Control<T, ProducerController> for NoopProducer
        where T: InterfaceController<ProducerController>{
    fn interface_controller(&self, store: Store) -> Option<T> {
        let control: Box<ProducerController> = Box::new(NoopProducerController {});
        Some(T::from(control))
    }
}

pub struct NoopProducerController;

impl super::MetaController for NoopProducerController {}

impl PostgresMigratable for NoopProducerController {
    fn register_migrations(&self, migrator: &mut Migrator<PostgresAdapter>) {}
}

impl super::PostgresMetaController for NoopProducerController {}

impl ProducerController for NoopProducerController {}
