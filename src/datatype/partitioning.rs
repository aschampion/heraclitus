use std::collections::{BTreeSet};

use ::{
    RepresentationKind,
    Error,
    PartitionIndex,
    VersionGraph,
    VersionGraphIndex,
};
use super::{
    DatatypeMarker,
    Description,
    InterfaceController,
    Model,
    StoreMetaController,
};
use ::datatype::interface::PartitioningController;
use ::repo::Repository;
use ::store::StoreRepoBackend;


pub trait Partitioning {
    fn get_partition_ids(
        &self,
    ) -> BTreeSet<PartitionIndex>;
}

state_interface!(PartitioningState, Partitioning);

#[derive(Default)]
pub struct UnaryPartitioning;

impl DatatypeMarker for UnaryPartitioning {}

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

impl<RC: ::repo::RepoController> PartitioningController for StoreRepoBackend<RC, UnaryPartitioning> {
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

impl<RC: ::repo::RepoController> super::MetaController for StoreRepoBackend<RC, UnaryPartitioning> {}

impl<RC: ::repo::RepoController> super::Storage for StoreRepoBackend<RC, UnaryPartitioning> {
    type StateType = UnaryPartitioningState;
    type DeltaType = super::UnrepresentableType;

    fn read_hunk(
        &self,
        _repo: &Repository,
        _hunk: &::Hunk,
    ) -> Result<super::Payload<Self::StateType, Self::DeltaType>, Error> {
        Ok(super::Payload::State(UnaryPartitioningState))
    }

    fn write_hunk(
        &mut self,
        _repo: &Repository,
        _hunk: &::Hunk,
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


    #[derive(Default)]
    pub struct ArbitraryPartitioning;

    impl DatatypeMarker for ArbitraryPartitioning {}

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

    pub trait Storage:
        ::datatype::Storage<StateType = ArbitraryPartitioningState,
                                    DeltaType = ::datatype::UnrepresentableType> {}
}
