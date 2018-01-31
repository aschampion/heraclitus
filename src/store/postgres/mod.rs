extern crate daggy;
extern crate postgres;
extern crate schemer;


use std::borrow::{Borrow, BorrowMut};
use std::convert::From;
use std::fmt::Debug;
use std::option::Option;

use failure::Fail;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::{
    Migrator,
    MigratorError,
};
use schemer_postgres::{
    PostgresAdapter,
    PostgresMigration,
};
use url::Url;

use ::{
    Error,
    Repository,
};
use ::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use ::repo::{
    RepoController,
    StoreRepoController,
};

use self::datatype::PostgresMetaController;

pub mod datatype;


impl Borrow<PostgresRepoController> for StoreRepoController {
    fn borrow(&self) -> &PostgresRepoController {
        match *self {
            StoreRepoController::Postgres(ref rc) => rc,
            _ => panic!("Attempt to borrow PostgresStore from a non-Postgres repo")
        }
    }
}

impl BorrowMut<PostgresRepoController> for StoreRepoController {
    fn borrow_mut(&mut self) -> &mut PostgresRepoController {
        match *self {
            StoreRepoController::Postgres(ref mut rc) => rc,
            _ => panic!("Attempt to borrow PostgresStore from a non-Postgres repo")
        }
    }
}


impl From<PostgresError> for Error {
    fn from(e: PostgresError) -> Self {
        Error::Store(e.to_string())
    }
}

use std::string::ToString;
impl<T: Debug + Fail> From<MigratorError<T>> for Error
        where MigratorError<T>: ToString {
    fn from(e: MigratorError<T>) -> Self {
        Error::Store(e.to_string())
    }
}


pub trait PostgresMigratable {
    fn migrations(&self) -> Vec<Box<<PostgresAdapter as schemer::Adapter>::MigrationType>> {
        vec![]
    }
}

pub struct PostgresRepoController {
    url: Url,
    connection: Option<postgres::Connection>,
}

impl PostgresRepoController {
    pub(crate) fn new(repo: &Repository) -> PostgresRepoController {
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
migration!(
    PGMigrationDatatypes,
    "acda147a-552f-42a5-bb2b-1ba05d41ec03",
    [],
    "create datatypes table");

impl PostgresMigration for PGMigrationDatatypes {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/datatype_0001.up.sql"))
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.batch_execute(include_str!("sql/datatype_0001.down.sql"))
    }
}

impl RepoController for PostgresRepoController {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error> {
        let connection = self.conn()?;
        let adapter = PostgresAdapter::new(connection, None);
        adapter.init()?;

        let mut migrator = Migrator::new(adapter);

        migrator.register(Box::new(PGMigrationDatatypes))?;

        let migrations = dtypes_registry.iter_dtypes()
            .flat_map(|dtype| {
                let model = dtypes_registry.get_model(&dtype.name);
                let smc: Box<PostgresMetaController> = model
                    .meta_controller(::store::Store::Postgres)
                    .expect("Model does not have a Postgres controller.")
                    .into();
                smc.migrations()
            })
            .collect();

        migrator.register_multiple(migrations)?;
        migrator.up(None)?;

        let trans = connection.transaction()?;
        let stmt = trans.prepare("INSERT INTO datatype (version, name) VALUES ($1, $2)")?;
        for dtype in dtypes_registry.iter_dtypes() {
            stmt.execute(&[&(dtype.version as i64), &dtype.name])?;
        }

        Ok(trans.commit()?)
    }
}
