use heraclitus_macros::{
    DatatypeMarker,
    stored_datatype_controller,
};

use crate::RepresentationKind;
use super::{
    Description,
    InterfaceControllerEnum,
};


#[derive(Default, DatatypeMarker)]
pub struct BlobDatatype;

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
        type StateType = crate::datatype::blob::StateType;
        type DeltaType = crate::datatype::blob::DeltaType;

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

#[stored_datatype_controller(BlobDatatype)]
pub trait Storage: super::Storage<StateType=StateType, DeltaType=DeltaType> {
}
