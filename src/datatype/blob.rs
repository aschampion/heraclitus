use heraclitus_macros::{
    DatatypeMarker,
    stored_datatype_controller,
};

use crate::RepresentationKind;
use super::{
    DatatypeMeta,
    InterfaceControllerEnum,
    Reflection,
};


#[derive(Default, DatatypeMarker)]
pub struct BlobDatatype;

impl DatatypeMeta for BlobDatatype {
    const NAME: &'static str = "Blob";
    const VERSION: u64 = 1;
}

impl<T: InterfaceControllerEnum> super::Model<T> for BlobDatatype {
    fn reflection(&self) -> Reflection<T> {
        Reflection {
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
