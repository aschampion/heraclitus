extern crate daggy;
extern crate petgraph;
extern crate schemer;
extern crate serde;
extern crate serde_json;
extern crate uuid;


use std::collections::{BTreeSet};
use std::iter::FromIterator;

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use ::{
    DatatypeRepresentationKind, Error,
    PartitionIndex,
    Version};
use super::{
    Description, InterfaceController, MetaController, Model,
    PostgresMetaController, Store, StoreMetaController};
use ::datatype::interface::PartitioningController;
use ::repo::{PostgresMigratable};


#[derive(Default)]
pub struct UnaryPartitioning;

impl<T: InterfaceController<PartitioningController>> Model<T> for UnaryPartitioning {
    fn info(&self) -> Description {
        Description {
            name: "UnaryPartitioning".into(),
            version: 1,
            representations: vec![DatatypeRepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec!["Partitioning"],
            dependencies: vec![],
        }
    }

    fn meta_controller(&self, store: Store) -> Option<super::StoreMetaController> {
        match store {
            Store::Postgres => Some(StoreMetaController::Postgres(
                Box::new(UnaryPartitioningController {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        store: Store,
        name: &str
    ) -> Option<T> {
        match name {
            "Partitioning" => {
                let control: Box<PartitioningController> = Box::new(UnaryPartitioningController {});
                Some(T::from(control))
            },
            _ => None,
        }
    }
}


const UNARY_PARTITION_INDEX: PartitionIndex = 0;
pub struct UnaryPartitioningController;

impl PartitioningController for UnaryPartitioningController {
    fn get_partition_ids(
        &self,
        _repo_control: &mut ::repo::StoreRepoController,
        _partitioning: &Version,
    ) -> BTreeSet<PartitionIndex> {
        BTreeSet::from_iter(vec![UNARY_PARTITION_INDEX])
    }
}


impl super::MetaController for UnaryPartitioningController {
}


impl PostgresMigratable for UnaryPartitioningController {}

impl super::PostgresMetaController for UnaryPartitioningController {}


pub mod arbitrary {
    use super::*;

    // use std::collections::Vec;


    #[derive(Default)]
    pub struct ArbitraryPartitioning;

    impl<T: InterfaceController<PartitioningController>> Model<T> for ArbitraryPartitioning {
        fn info(&self) -> Description {
            Description {
                name: "ArbitraryPartitioning".into(),
                version: 1,
                representations: vec![DatatypeRepresentationKind::State]
                        .into_iter()
                        .collect(),
                implements: vec!["Partitioning"],
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
            name: &str
        ) -> Option<T> {
            match name {
                "Partitioning" => {
                    match store {
                        Store::Postgres => {
                            let control: Box<PartitioningController> = Box::new(PostgresStore {});
                            Some(T::from(control))
                        }
                        _ => unimplemented!()
                    }
                },
                _ => None,
            }
        }
    }

    pub fn model_controller(store: Store) -> impl ModelController {
        match store {
            Store::Postgres => PostgresStore {},
            _ => unimplemented!(),
        }
    }

    pub trait ModelController {
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

    pub struct PostgresStore;

    struct PGMigrationArbitraryPartitioning;
    migration!(
        PGMigrationArbitraryPartitioning,
        "bfef8343-453c-463f-a3c6-f3b957e28292",
        ["7d1fb6d1-a1b0-4bd4-aa6d-e3ee71c4353b",],
        "create arbitrary_partitioning table");

    impl PostgresMigration for PGMigrationArbitraryPartitioning {
        fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
            transaction.batch_execute(include_str!("sql/arbitrary_partitioning_0001.up.sql"))
        }

        fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
            transaction.execute("DROP TABLE arbitrary_partitioning;", &[]).map(|_| ())
        }
    }

    impl PartitioningController for PostgresStore {
        fn get_partition_ids(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            partitioning: &Version,
        ) -> BTreeSet<PartitionIndex> {
            self.read(repo_control, partitioning).expect("TODO")
        }
    }

    impl MetaController for PostgresStore {}

    impl PostgresMigratable for PostgresStore {
        fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
            vec![
                Box::new(PGMigrationArbitraryPartitioning),
            ]
        }
    }

    impl PostgresMetaController for PostgresStore {}

    impl ModelController for PostgresStore {
        fn write(
            &mut self,
            repo_control: &mut ::repo::StoreRepoController,
            version: &Version,
            partition_ids: &[PartitionIndex],
        ) -> Result<(), Error> {
            let rc = match *repo_control {
                ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
                _ => panic!("PostgresStore received a non-Postgres context")
            };

            let conn = rc.conn()?;
            let trans = conn.transaction()?;

            // TODO: Have to construct new array to get Rust to allow this cast.
            let db_partition_ids = partition_ids.iter().map(|p| *p as i64).collect::<Vec<i64>>();

            let nrows = trans.execute(r#"
                    INSERT INTO arbitrary_partitioning (version_id, partition_ids)
                    SELECT v.id, r.partitioning_ids
                    FROM (VALUES ($2::bigint[]))
                      AS r (partitioning_ids)
                    JOIN version v
                      ON (v.uuid_ = $1::uuid);
                "#, &[&version.id.uuid, &db_partition_ids])?;

            trans.set_commit();
            Ok(())
        }

        fn read(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            version: &Version
        ) -> Result<BTreeSet<PartitionIndex>, Error> {
            let rc = match *repo_control {
                ::repo::StoreRepoController::Postgres(ref mut rc) => rc,
                _ => panic!("PostgresStore received a non-Postgres context")
            };

            let conn = rc.conn()?;
            let trans = conn.transaction()?;

            let partition_ids_row = trans.query(r#"
                    SELECT partition_ids
                    FROM arbitrary_partitioning
                    JOIN version ON id = version_id
                    WHERE uuid_ = $1::uuid;
                "#, &[&version.id.uuid])?;
            let partition_ids = partition_ids_row.get(0).get::<_, Vec<i64>>(0)
                .into_iter()
                .map(|p| p as PartitionIndex)
                .collect();

            Ok(partition_ids)
        }

    }
}
