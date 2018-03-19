use std::collections::{BTreeSet};

use ::{
    RepresentationKind,
    Error,
    PartitionIndex,
    Version,
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


#[derive(Default)]
pub struct UnaryPartitioning;

impl<T: InterfaceController<PartitioningController>> Model<T> for UnaryPartitioning {
    fn info(&self) -> Description<T> {
        Description {
            name: "UnaryPartitioning".into(),
            version: 1,
            representations: vec![RepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec![
                <T as InterfaceController<PartitioningController>>::VARIANT,
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
        if iface == <T as InterfaceController<PartitioningController>>::VARIANT {
            let control: Box<PartitioningController> = Box::new(UnaryPartitioningController {});
            Some(T::from(control))
        } else {
            None
        }
    }
}


const UNARY_PARTITION_INDEX: PartitionIndex = 0;
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


impl super::MetaController for UnaryPartitioningController {}


pub mod arbitrary {
    use super::*;

    use ::store::postgres::datatype::partitioning::arbitrary::PostgresStore;


    #[derive(Default)]
    pub struct ArbitraryPartitioning;

    impl<T: InterfaceController<PartitioningController>> Model<T> for ArbitraryPartitioning {
        fn info(&self) -> Description<T> {
            Description {
                name: "ArbitraryPartitioning".into(),
                version: 1,
                representations: vec![RepresentationKind::State]
                        .into_iter()
                        .collect(),
                implements: vec![
                    <T as InterfaceController<PartitioningController>>::VARIANT,
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
            if iface == <T as InterfaceController<PartitioningController>>::VARIANT {
                match store {
                    Store::Postgres => {
                        let control: Box<PartitioningController> = Box::new(PostgresStore {});
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

    pub trait ModelController: PartitioningController {
        // TODO: this should not allow versions with parents, but no current
        // mechanism exists to enforce this.
        fn write(
            &mut self,
            repo_control: &mut ::repo::StoreRepoController,
            version: &Version,
            partition_ids: &[PartitionIndex],
        ) -> Result<(), Error>;

        fn read(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            version: &Version
        ) -> Result<BTreeSet<PartitionIndex>, Error>;
    }
}
