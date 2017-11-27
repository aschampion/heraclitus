extern crate schemer;

use schemer::Migrator;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use ::{DatatypeRepresentationKind};
use ::datatype::{
    Description, InterfaceController, MetaController, Model,
    PostgresMetaController, StoreMetaController};
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

    fn meta_controller(&self, store: Store) -> Option<StoreMetaController> {
        match store {
            Store::Postgres => Some(StoreMetaController::Postgres(
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

pub struct NoopProducerController;

impl MetaController for NoopProducerController {}

impl PostgresMigratable for NoopProducerController {}

impl PostgresMetaController for NoopProducerController {}

impl ProducerController for NoopProducerController {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[derive(Default)]
    pub struct AddOneToBlobProducer;

    impl<T: InterfaceController<ProducerController>> Model<T> for AddOneToBlobProducer {
        fn info(&self) -> Description {
            Description {
                name: "AddOneToBlobProducer".into(),
                version: 1,
                representations: vec![DatatypeRepresentationKind::State]
                        .into_iter()
                        .collect(),
                implements: vec!["Producer"],
                dependencies: vec![],
            }
        }

        fn meta_controller(&self, store: Store) -> Option<StoreMetaController> {
            match store {
                Store::Postgres => Some(StoreMetaController::Postgres(
                    Box::new(AddOneToBlobProducerController {}))),
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
                    let control: Box<ProducerController> = Box::new(AddOneToBlobProducerController {});
                    Some(T::from(control))
                },
                _ => None,
            }
        }
    }

    pub struct AddOneToBlobProducerController;

    impl MetaController for AddOneToBlobProducerController {}

    impl PostgresMigratable for AddOneToBlobProducerController {}

    impl PostgresMetaController for AddOneToBlobProducerController {}

    impl ProducerController for AddOneToBlobProducerController {}
}
