extern crate postgres;
extern crate uuid;

use std::error::Error;
use std::io;

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use url::Url;
use uuid::Uuid;

use ::datatype::DatatypesRegistry;

pub trait RepoController {
    fn init(&self, dtypes_registry: &DatatypesRegistry) -> Result<(), String>;
}

fn get_repo_controller(repo: &super::Repository) -> Box<RepoController> {
    match repo.url.scheme() {
        "postgres" | "postgresql" => Box::new(PostgresRepoController::new(repo)),
        _ => unimplemented!()
    }
}

pub struct FakeRepoController {}

impl RepoController for FakeRepoController {
    fn init(&self, dtypes_registry: &DatatypesRegistry) -> Result<(), String> {
        Ok(())
    }
}

pub trait PostgresMigratable {
    fn register_migrations(&self, migrator: &mut Migrator<PostgresAdapter>);
}

pub struct PostgresRepoController {
    url: Url,
    migratables: Vec<Box<PostgresMigratable>>,
}

impl PostgresRepoController {
    fn new(repo: &super::Repository) -> PostgresRepoController {
        PostgresRepoController {
            url: repo.url.clone(),
            migratables: Vec::new(),
        }
    }

    pub fn register_postgres_migratable(&mut self, migratable: Box<PostgresMigratable>) {
        self.migratables.push(migratable);
    }
}

struct PGMigrationDatatypes;
migration!(PGMigrationDatatypes, 10, "create datatypes table");

impl PostgresMigration for PGMigrationDatatypes {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute(include_str!("datatype_0001.up.sql"), &[]).map(|_| ())
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("DROP TABLE datatype;", &[]).map(|_| ())
    }
}

impl RepoController for PostgresRepoController {
    fn init(&self, dtypes_registry: &DatatypesRegistry) -> Result<(), String> {
        let connection = postgres::Connection::connect(self.url.as_str(), postgres::TlsMode::None)
                .map_err(|e| e.to_string())?;
        let adapter = PostgresAdapter::new(&connection);
        try!(adapter.setup_schema().map_err(|e| e.to_string()));

        let mut migrator = Migrator::new(adapter);

        migrator.register(Box::new(PGMigrationDatatypes));

        for model in dtypes_registry.types.values() {
            let smc: Box<PostgresMigratable> = model.controller(::store::Store::Postgres)
                .expect("Model does not have a Postgres controller.")
                .into();
            smc.register_migrations(&mut migrator);
        }

        self.migratables.iter().for_each(|m| m.register_migrations(&mut migrator));

        migrator.up(None).map_err(|e| e.to_string())
    }
}

// pub fn register_postgres_migrations<T: ::Datatype::Model>(migration: &mut Migrator);



#[cfg(test)]
mod tests {
    #[test]
    fn test_postgres_repo_init() {
        use super::*;

        let mut dtypes_registry = DatatypesRegistry::new();
        dtypes_registry.register_datatype_models(::datatype::build_module_datatype_models());

        let repo = ::Repository {
            // TODO: fake UUID, version
            id: ::Identity{uuid: Uuid::new_v4(), hash: 0},
            name: "Test repo".into(),
            url: Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap()
        };
        let repo_cntrlr = get_repo_controller(&repo);
        repo_cntrlr.init(&dtypes_registry).unwrap()
    }
}
