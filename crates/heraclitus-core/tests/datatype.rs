//! This tests the minimal, empty datatype should compile.
//!
//! This does not test datatype enums or any calling of datatype traits.

// #![feature(const_fn)]
// #![feature(const_constructor)]

use heraclitus_core as heraclitus;
use heraclitus::{
    datatype::{
        Description,
        InterfaceControllerEnum,
        Model,
    },
    datatype_controllers,
    RepresentationKind,
};
use heraclitus_macros::{
    DatatypeMarker,
    stored_datatype_controller,
};

// A compilation test for `DatatypeMarker` and its derive macros.
#[derive(DatatypeMarker)]
pub struct TestDatatype;

impl<T: InterfaceControllerEnum> Model<T> for TestDatatype {
    fn info(&self) -> Description<T> {
        Description {
            name: "Test".into(),
            version: 1,
            representations: vec![
                        RepresentationKind::State,
                    ]
                    .into_iter()
                    .collect(),
            implements: vec![],
            dependencies: vec![],
        }
    }

    datatype_controllers!(TestDatatype, ());
}

#[stored_datatype_controller(TestDatatype)]
trait TestDatatypeStorage {}

mod store {
    use super::*;

    mod postgres {
        use heraclitus_core::store::postgres::{
            datatype::PostgresMetaController,
            PostgresMigratable,
            PostgresRepository,
        };
        use super::*;

        impl PostgresMigratable for TestDatatypeBackend<PostgresRepository> {}

        impl PostgresMetaController for TestDatatypeBackend<PostgresRepository> {}
    }
}
