extern crate postgres;
extern crate uuid;

// use std::borrow::{Borrow, BorrowMut};
use std::convert::From;
use std::io;
use std::option::Option;

use postgres::error::ConnectError;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemamama::Error as SchemamamaError;
use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use url::Url;
use uuid::Uuid;

use ::{Context, Error};
use ::datatype::DatatypesRegistry;
use ::store::Stored;

// pub type StoreRepoController = Stored<Box<RepoController>>;
pub enum StoreRepoController {
    Postgres(PostgresRepoController),
}

// impl<'a> Borrow<RepoController + 'a> for StoreRepoController {
//     fn borrow(&self) -> &(RepoController + 'a) {
//         use self::StoreRepoController::*;

//         println!("borrow");
//         match *self {
//             Postgres(ref rc) => rc as &RepoController
//         }
//     }
// }

// impl<'a> BorrowMut<RepoController + 'a> for StoreRepoController {
//     fn borrow_mut(&mut self) -> &mut (RepoController + 'a) {
//         use self::StoreRepoController::*;

//         println!("borrow_mut");
//         match *self {
//             Postgres(ref mut rc) => rc as &mut RepoController
//         }
//     }
// }

impl RepoController for StoreRepoController {
    fn init(&mut self, dtypes_registry: &DatatypesRegistry) -> Result<(), Error> {
        use self::StoreRepoController::*;

        match *self {
            Postgres(ref mut rc) => rc.init(dtypes_registry)
        }
    }
}



impl From<PostgresError> for Error {
    fn from(e: PostgresError) -> Self {
        Error::Store(e.to_string())
    }
}

impl From<ConnectError> for Error {
    fn from(e: ConnectError) -> Self {
        Error::Store(e.to_string())
    }
}

use std::string::ToString;
impl<T> From<SchemamamaError<T>> for Error where SchemamamaError<T>: ToString {
    fn from(e: SchemamamaError<T>) -> Self {
        Error::Store(e.to_string())
    }
}

pub trait RepoController {
    fn init(&mut self, dtypes_registry: &DatatypesRegistry) -> Result<(), Error>;
}

fn get_repo_controller(repo: &super::Repository) -> StoreRepoController {
    use self::StoreRepoController::*;
    match repo.url.scheme() {
        "postgres" | "postgresql" => Postgres(PostgresRepoController::new(repo)),
        _ => unimplemented!()
    }
}

pub struct FakeRepoController {}

impl RepoController for FakeRepoController {
    fn init(&mut self, dtypes_registry: &DatatypesRegistry) -> Result<(), Error> {
        Ok(())
    }
}

pub trait PostgresMigratable {
    fn register_migrations(&self, migrator: &mut Migrator<PostgresAdapter>);
}

pub struct PostgresRepoController {
    url: Url,
    connection: Option<postgres::Connection>,
}

impl PostgresRepoController {
    fn new(repo: &super::Repository) -> PostgresRepoController {
        PostgresRepoController {
            url: repo.url.clone(),
            connection: None,
        }
    }

    // TODO: should have methods for getting RW or R-only transactions
    pub fn conn(&mut self) -> Result<&mut postgres::Connection, Error> {
        match self.connection {
            Some(ref mut c) => Ok(c),
            None => {
                self.connection = Some(
                        postgres::Connection::connect(
                            self.url.as_str(),
                            postgres::TlsMode::None)?);
                self.conn()
            }
        }
    }
}

struct PGMigrationDatatypes;
migration!(PGMigrationDatatypes, 1, "create datatypes table");

impl PostgresMigration for PGMigrationDatatypes {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("datatype_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("DROP TABLE datatype;", &[]).map(|_| ())
    }
}

impl RepoController for PostgresRepoController {
    fn init(&mut self, dtypes_registry: &DatatypesRegistry) -> Result<(), Error> {
        let connection = self.conn()?;
        let adapter = PostgresAdapter::new(connection);
        adapter.setup_schema()?;

        let mut migrator = Migrator::new(adapter);

        migrator.register(Box::new(PGMigrationDatatypes));

        for model in dtypes_registry.models.values() {
            let smc: Box<PostgresMigratable> = model.controller(::store::Store::Postgres)
                .expect("Model does not have a Postgres controller.")
                .into();
            smc.register_migrations(&mut migrator);
        }

        migrator.up(None)?;

        let trans = connection.transaction()?;
        let stmt = trans.prepare("INSERT INTO datatype (version, name) VALUES ($1, $2)")?;
        for model in dtypes_registry.models.values() {
            let descr = model.info();
            stmt.execute(&[&(descr.datatype.version as i64), &descr.datatype.name])?;
        }

        Ok(trans.commit()?)
    }
}

// pub fn register_postgres_migrations<T: ::Datatype::Model>(migration: &mut Migrator);



#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn init_postgres_repo() -> Context {
        let mut dtypes_registry = DatatypesRegistry::new();
        dtypes_registry.register_datatype_models(::datatype::build_module_datatype_models());

        let repo = ::Repository {
            // TODO: fake UUID, version
            id: ::Identity{uuid: Uuid::new_v4(), hash: 0},
            name: "Test repo".into(),
            // url: Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap()
            url: Url::parse("postgresql://postgres@localhost/?search_path=pg_temp").unwrap()
        };
        let mut repo_cntrlr = get_repo_controller(&repo);
        repo_cntrlr.init(&dtypes_registry).unwrap();

        Context {
            dtypes_registry: dtypes_registry,
            repo_control: repo_cntrlr,
        }
    }

    #[test]
    fn test_postgres_repo_init() {
        init_postgres_repo();
    }
}
