use heraclitus_macros::stored_controller;

use ::RepresentationKind;
use super::{
    DatatypeMarker,
    Description,
    InterfaceControllerEnum,
    StoreMetaController,
};
use ::repo::StoreRepoController;

#[derive(Default)]
pub struct BlobDatatype;

impl DatatypeMarker for BlobDatatype {}

impl<T: InterfaceControllerEnum> super::Model<T> for BlobDatatype {
    fn info(&self) -> Description<T> {
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

    datatype_controllers!(BlobDatatype, ());
}

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

#[stored_controller(<'store> ::store::Store<'store, BlobDatatype>)]
pub trait ModelController: super::ModelController<StateType=StateType, DeltaType=DeltaType> {
}
