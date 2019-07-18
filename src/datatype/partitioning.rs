use std::collections::{BTreeSet};

use maplit::btreeset;

use heraclitus_macros::{
    DatatypeMarker,
};

use crate::{
    RepresentationKind,
    Error,
    PartitionIndex,
    VersionGraph,
    VersionGraphIndex,
};
use super::{
    Description,
    InterfaceController,
    Model,
};
use crate::datatype::interface::PartitioningController;
use crate::repo::Repository;


pub trait Partitioning {
    fn get_partition_ids(
        &self,
    ) -> BTreeSet<PartitionIndex>;
}

state_interface!(PartitioningState, Partitioning);

#[derive(Default, DatatypeMarker)]
pub struct UnaryPartitioning;

impl<T: InterfaceController<PartitioningState>> Model<T> for UnaryPartitioning {
    fn info(&self) -> Description<T> {
        Description {
            name: "UnaryPartitioning".into(),
            version: 1,
            representations: vec![RepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec![
                <T as InterfaceController<PartitioningState>>::VARIANT,
            ],
            dependencies: vec![],
        }
    }

    datatype_controllers!(UnaryPartitioning, (PartitioningState));
}


/// Index of the unary partition. Unary partitioning is special, common
/// enough case that this is public for convenience.
pub const UNARY_PARTITION_INDEX: PartitionIndex = 0;

impl<RC: crate::repo::RepoController> PartitioningController for UnaryPartitioningBackend<RC> {
    fn get_partition_ids(
        &self,
        _repo: &mut Repository,
        _ver_graph: &VersionGraph,
        _v_idx: VersionGraphIndex,
    ) -> BTreeSet<PartitionIndex> {
        btreeset![UNARY_PARTITION_INDEX]
    }
}

#[derive(Debug, Hash, PartialEq)]
pub struct UnaryPartitioningState;

impl Partitioning for UnaryPartitioningState {
    fn get_partition_ids(
        &self,
    ) -> BTreeSet<PartitionIndex> {
        btreeset![UNARY_PARTITION_INDEX]
    }
}

// impl<RC: crate::repo::RepoController> super::MetaController for StoreRepoBackend<RC, UnaryPartitioning> {}

impl<RC: crate::repo::RepoController> super::Storage for UnaryPartitioningBackend<RC> {
    type StateType = UnaryPartitioningState;
    type DeltaType = super::UnrepresentableType;

    fn read_hunk(
        &self,
        _repo: &Repository,
        _hunk: &crate::Hunk,
    ) -> Result<super::Payload<Self::StateType, Self::DeltaType>, Error> {
        Ok(super::Payload::State(UnaryPartitioningState))
    }

    fn write_hunk(
        &mut self,
        _repo: &Repository,
        _hunk: &crate::Hunk,
        _payload: &super::Payload<Self::StateType, Self::DeltaType>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn compose_state(
        &self,
        _state: &mut Self::StateType,
        _delta: &Self::DeltaType,
    ) {
        unimplemented!()
    }
}


pub mod arbitrary {
    use super::*;

    use heraclitus_macros::stored_datatype_controller;


    #[derive(Default, DatatypeMarker)]
    pub struct ArbitraryPartitioning;

    impl<T: InterfaceController<PartitioningState>> Model<T> for ArbitraryPartitioning {
        fn info(&self) -> Description<T> {
            Description {
                name: "ArbitraryPartitioning".into(),
                version: 1,
                representations: vec![RepresentationKind::State]
                        .into_iter()
                        .collect(),
                implements: vec![
                    <T as InterfaceController<PartitioningState>>::VARIANT,
                ],
                dependencies: vec![],
            }
        }

        datatype_controllers!(ArbitraryPartitioning, (PartitioningState));
    }

    #[derive(Debug, Hash, PartialEq)]
    pub struct ArbitraryPartitioningState {
        pub partition_ids: BTreeSet<PartitionIndex>,
    }

    impl Partitioning for ArbitraryPartitioningState {
        fn get_partition_ids(&self) -> BTreeSet<PartitionIndex> {
            self.partition_ids.clone()
        }
    }

    #[stored_datatype_controller(ArbitraryPartitioning)]
    pub trait Storage:
        crate::datatype::Storage<StateType = ArbitraryPartitioningState,
                                    DeltaType = crate::datatype::UnrepresentableType> {}
}
