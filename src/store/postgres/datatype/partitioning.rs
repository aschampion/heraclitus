use heraclitus_core::{
    postgres,
    schemer,
    schemer_postgres,
};
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::migration;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use crate::{
    Error,
    PartitionIndex,
};
use crate::datatype::partitioning::{
    UnaryPartitioningBackend,
};
use crate::repo::Repository;
use crate::store::postgres::{PostgresMigratable, PostgresRepository};

use super::PostgresMetaController;


impl PostgresMigratable for UnaryPartitioningBackend<PostgresRepository> {}

impl super::PostgresMetaController for UnaryPartitioningBackend<PostgresRepository> {}


pub mod arbitrary {
    use super::*;

    use std::borrow::Borrow;

    use crate::{
        Hunk,
        RepresentationKind,
    };
    use crate::datatype::{
        Payload,
    };
    use crate::datatype::partitioning::arbitrary::{
        ArbitraryPartitioningBackend,
        ArbitraryPartitioningState,
        Storage,
    };


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


    impl PostgresMigratable for ArbitraryPartitioningBackend<PostgresRepository> {
        fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
            vec![
                Box::new(PGMigrationArbitraryPartitioning),
            ]
        }
    }

    impl PostgresMetaController for ArbitraryPartitioningBackend<PostgresRepository> {}

    impl crate::datatype::Storage for ArbitraryPartitioningBackend<PostgresRepository> {
        fn write_hunk(
            &mut self,
            repo: &Repository,
            hunk: &Hunk,
            payload: &Payload<Self::StateType, Self::DeltaType>,
        ) -> Result<(), Error> {
            let rc: &PostgresRepository = repo.borrow();

            let conn = rc.conn()?;
            let trans = conn.transaction()?;

            match hunk.representation {
            RepresentationKind::State =>
                match *payload {
                    Payload::State(ArbitraryPartitioningState {ref partition_ids}) => {

                        // TODO: Have to construct new array to get Rust to allow this cast.
                        let db_partition_ids = partition_ids.iter().map(|p| *p as i64).collect::<Vec<i64>>();

                        trans.execute(r#"
                                INSERT INTO arbitrary_partitioning (version_id, partition_ids)
                                SELECT v.id, r.partitioning_ids
                                FROM (VALUES ($2::bigint[]))
                                  AS r (partitioning_ids)
                                JOIN version v
                                  ON (v.uuid_ = $1::uuid);
                            "#, &[&hunk.version.id.uuid, &db_partition_ids])?;
                    }
                    _ => return Err(Error::Store("Attempt to write state hunk with non-state payload".into())),
                },
            _ => return Err(Error::Store("Attempt to write a hunk with an unsupported representation".into())),
        }

            trans.set_commit();
            Ok(())
        }

        fn read_hunk(
            &self,
            repo: &Repository,
            hunk: &Hunk,
        ) -> Result<Payload<Self::StateType, Self::DeltaType>, Error> {
            let rc: &PostgresRepository = repo.borrow();

            let conn = rc.conn()?;
            let trans = conn.transaction()?;

            let partition_ids_row = trans.query(r#"
                    SELECT partition_ids
                    FROM arbitrary_partitioning
                    JOIN version ON id = version_id
                    WHERE uuid_ = $1::uuid;
                "#, &[&hunk.version.id.uuid])?;
            let partition_ids = partition_ids_row.get(0).get::<_, Vec<i64>>(0)
                .into_iter()
                .map(|p| p as PartitionIndex)
                .collect();

            Ok(Payload::State(ArbitraryPartitioningState {partition_ids}))
        }
    }

    impl Storage for ArbitraryPartitioningBackend<PostgresRepository> {}
}
