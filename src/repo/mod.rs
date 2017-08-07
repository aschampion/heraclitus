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


pub trait RepoController {
    fn init(&self) -> Result<(), String>;
}

fn get_repo_controller(repo: &super::Repository) -> Box<RepoController> {
    match repo.url.scheme() {
        "postgres" | "postgresql" => Box::new(PostgresRepoController::new(repo)),
        _ => unimplemented!()
    }
}

pub struct PostgresRepoController {
    url: Url,
}

impl PostgresRepoController {
    fn new(repo: &super::Repository) -> PostgresRepoController {
        PostgresRepoController {
            url: repo.url.clone(),
        }
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
    fn init(&self) -> Result<(), String> {
        let connection = postgres::Connection::connect(self.url.as_str(), postgres::TlsMode::None)
                .map_err(|e| e.to_string())?;
        let adapter = PostgresAdapter::new(&connection);
        try!(adapter.setup_schema().map_err(|e| e.to_string()));

        let mut migrator = Migrator::new(adapter);

        migrator.register(Box::new(PGMigrationDatatypes));

        // Execute migrations all the way upwards:
        migrator.up(None).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_postgres_repo_init() {
        use super::*;

        let repo = ::Repository {
            id: ::Identity{uuid: Uuid::new_v4(), hash: 0},
            name: "Test repo".into(),
            url: Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap()
        };
        let repo_cntrlr = get_repo_controller(&repo);
        repo_cntrlr.init().unwrap()
    }
}
