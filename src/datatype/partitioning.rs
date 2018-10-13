use std::collections::{BTreeSet};

use ::{
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
    Store,
    StoreMetaController,
};
use ::datatype::interface::PartitioningController;


pub trait Partitioning {
    fn get_partition_ids(
        &self,
    ) -> BTreeSet<PartitionIndex>;
}

state_interface!(PartitioningState, Partitioning);

#[derive(Default)]
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

    fn meta_controller(&self, store: Store) -> Option<StoreMetaController> {
        match store {
            Store::Postgres => Some(StoreMetaController::Postgres(
                Box::new(UnaryPartitioningController {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        _store: Store,
        iface: T
    ) -> Option<T> {
        if iface == <T as InterfaceController<PartitioningState>>::VARIANT {
            let control: Box<PartitioningState> = Box::new(UnaryPartitioningController {});
            Some(T::from(control))
        } else {
            None
        }
    }
}


/// Index of the unary partition. Unary partitioning is special, common
/// enough case that this is public for convenience.
pub const UNARY_PARTITION_INDEX: PartitionIndex = 0;
pub struct UnaryPartitioningController;

impl PartitioningController for UnaryPartitioningController {
    fn get_partition_ids(
        &self,
        _repo_control: &mut ::repo::StoreRepoController,
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

impl super::MetaController for UnaryPartitioningController {}

impl super::ModelController for UnaryPartitioningController {
    type StateType = UnaryPartitioningState;
    type DeltaType = super::UnrepresentableType;

    fn read_hunk(
        &self,
        _repo_control: &mut ::repo::StoreRepoController,
        _hunk: &::Hunk,
    ) -> Result<super::Payload<Self::StateType, Self::DeltaType>, Error> {
        Ok(super::Payload::State(UnaryPartitioningState))
    }

    fn write_hunk(
        &mut self,
        _repo_control: &mut ::repo::StoreRepoController,
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

    use ::store::postgres::datatype::partitioning::arbitrary::PostgresStore;


    #[derive(Default)]
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

        fn meta_controller(&self, store: Store) -> Option<super::StoreMetaController> {
            match store {
                Store::Postgres => Some(StoreMetaController::Postgres(
                    Box::new(PostgresStore {}))),
                _ => None,
            }
        }

        fn interface_controller(
            &self,
            store: Store,
            iface: T,
        ) -> Option<T> {
            if iface == <T as InterfaceController<PartitioningState>>::VARIANT {
                match store {
                    Store::Postgres => {
                        let control: Box<PartitioningState> = Box::new(PostgresStore {});
                        Some(T::from(control))
                    }
                    _ => unimplemented!()
                }
            } else {
                None
            }
        }
    }

    pub fn model_controller(store: Store) -> impl ModelController {
        match store {
            Store::Postgres => PostgresStore {},
            _ => unimplemented!(),
        }
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

    pub trait ModelController:
        ::datatype::ModelController<StateType = ArbitraryPartitioningState,
                                    DeltaType = ::datatype::UnrepresentableType> {}
}
