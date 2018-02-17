extern crate daggy;
extern crate petgraph;
extern crate schemer;
extern crate serde;
extern crate serde_json;
extern crate uuid;


use std::collections::{BTreeSet};

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use ::{
    Error,
    PartitionIndex,
    Version,
    VersionGraph,
    VersionGraphIndex,
};
use ::datatype::{
    MetaController,
};
use ::datatype::interface::PartitioningController;
use ::datatype::partitioning::UnaryPartitioningController;
use ::store::postgres::{PostgresMigratable, PostgresRepoController};

use super::PostgresMetaController;


impl PostgresMigratable for UnaryPartitioningController {}

impl super::PostgresMetaController for UnaryPartitioningController {}


pub mod arbitrary {
    use super::*;

    use std::borrow::BorrowMut;

    use ::datatype::partitioning::arbitrary::ModelController;


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
            transaction.batch_execute(include_str!("sql/arbitrary_partitioning_0001.down.sql"))
        }
    }

    impl PartitioningController for PostgresStore {
        fn get_partition_ids(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            ver_graph: &VersionGraph,
            v_idx: VersionGraphIndex,
        ) -> BTreeSet<PartitionIndex> {
            self.read(repo_control, &ver_graph[v_idx]).expect("TODO")
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
            let rc: &mut PostgresRepoController = repo_control.borrow_mut();

            let conn = rc.conn()?;
            let trans = conn.transaction()?;

            // TODO: Have to construct new array to get Rust to allow this cast.
            let db_partition_ids = partition_ids.iter().map(|p| *p as i64).collect::<Vec<i64>>();

            trans.execute(r#"
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
            let rc: &mut PostgresRepoController = repo_control.borrow_mut();

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
