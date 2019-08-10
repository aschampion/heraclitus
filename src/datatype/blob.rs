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
            representations: enumset::enum_set!(
                        RepresentationKind::State |
                        RepresentationKind::Delta |
                    ),
            implements: vec![],
            dependencies: vec![],
        }
    }

    datatype_controllers!(BlobDatatype, ());
}

pub(crate) type StateType = Vec<u8>;
pub(crate) type DeltaType = (Vec<usize>, Vec<u8>);

impl crate::datatype::ComposableState for BlobDatatype {
    type StateType = crate::datatype::blob::StateType;
    type DeltaType = crate::datatype::blob::DeltaType;

    fn compose_state(
        state: &mut Self::StateType,
        delta: &Self::DeltaType,
    ) {
        for (&idx, &val) in delta.0.iter().zip(delta.1.iter()) {
            state[idx] = val;
        }
    }
}

#[stored_datatype_controller(BlobDatatype)]
pub trait Storage: super::Storage {}
