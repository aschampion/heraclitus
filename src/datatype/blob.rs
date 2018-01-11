extern crate uuid;


use ::RepresentationKind;
use super::{
    Description,
    Store,
};
use ::store::postgres::datatype::blob::PostgresStore;


#[derive(Default)]
pub struct Blob;

impl<T> super::Model<T> for Blob {
    fn info(&self) -> Description {
        Description {
            name: "Blob".into(),
            version: 1,
            representations: vec![
                        RepresentationKind::State,
                        RepresentationKind::Delta,
                    ]
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
        _store: Store,
        _name: &str,
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

pub(crate) type StateType = Vec<u8>;
pub(crate) type DeltaType = (Vec<usize>, Vec<u8>);

macro_rules! blob_common_model_controller_impl {
    () => (
        type StateType = ::datatype::blob::StateType;
        type DeltaType = ::datatype::blob::DeltaType;

        fn compose_state(
            &self,
            state: &mut Self::StateType,
            delta: &Self::DeltaType,
        ) {
            for (&idx, &val) in delta.0.iter().zip(delta.1.iter()) {
                state[idx] = val;
            }
        }
    )
}

pub trait ModelController: super::ModelController<StateType=StateType, DeltaType=DeltaType> {
}
