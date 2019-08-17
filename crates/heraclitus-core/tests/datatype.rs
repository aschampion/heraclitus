//! This tests the minimal, empty datatype should compile.
//!
//! This does not test datatype enums or any calling of datatype traits.

// #![feature(const_fn)]
// #![feature(const_constructor)]

use heraclitus_core as heraclitus;
use heraclitus::{
    datatype::{
        DatatypeMeta,
        InterfaceControllerEnum,
        Model,
        Reflection,
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

impl DatatypeMeta for TestDatatype {
    const NAME: &'static str = "Test";
    const VERSION: u64 = 1;
}

impl<T: InterfaceControllerEnum> Model<T> for TestDatatype {
    fn reflection(&self) -> Reflection<T> {
        Reflection {
            representations: enumset::enum_set![
                        RepresentationKind::State |
                    ],
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

    #[cfg(feature="backend-debug-filesystem")]
    mod debug_filesystem {
        use heraclitus_core::store::debug_filesystem::{
            datatype::DebugFilesystemMetaController,
            DebugFilesystemRepository,
        };
        use super::*;

        impl DebugFilesystemMetaController for TestDatatypeBackend<DebugFilesystemRepository> {}
    }

    #[cfg(feature="backend-postgres")]
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
