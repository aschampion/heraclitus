use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::convert::From;
use std::fmt::Debug;
use std::option::Option;

use failure::Fail;
use postgres;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemer::{
    self,
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
    RepositoryLocation,
};
use ::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use ::repo::{
    RepoController,
    Repository,
};

use self::datatype::PostgresMetaController;

pub mod datatype;


impl Borrow<PostgresRepository> for Repository {
    fn borrow(&self) -> &PostgresRepository {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match *self {
            Repository::Postgres(ref rc) => rc,
            _ => panic!("Attempt to borrow PostgresStore from a non-Postgres repo")
        }
    }
}

impl BorrowMut<PostgresRepository> for Repository {
    fn borrow_mut(&mut self) -> &mut PostgresRepository {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match *self {
            Repository::Postgres(ref mut rc) => rc,
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

pub struct PostgresRepository {
    url: Url,
    connection: RefCell<Option<postgres::Connection>>,
}

impl PostgresRepository {
    pub(crate) fn new(repo: &RepositoryLocation) -> PostgresRepository {
        PostgresRepository {
            url: repo.url.clone(),
            connection: RefCell::new(None),
        }
    }

    // TODO: should have methods for getting RW or R-only transactions
    pub fn conn(&self) -> Result<impl std::ops::Deref<Target = postgres::Connection> + '_, Error> {
        {
            let borrow = self.connection.borrow();
            if borrow.is_some() {
                return Ok(std::cell::Ref::map(borrow, |b| b.as_ref().unwrap()));
            }
        }

        self.connection.replace(Some(
                    postgres::Connection::connect(
                        self.url.as_str(),
                        postgres::TlsMode::None)?));
        Ok(std::cell::Ref::map(self.connection.borrow(), |b| b.as_ref().unwrap()))
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

impl RepoController for PostgresRepository {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error> {
        let connection = self.conn()?;
        let adapter = PostgresAdapter::new(&connection, None);
        adapter.init()?;

        let mut migrator = Migrator::new(adapter);

        migrator.register(Box::new(PGMigrationDatatypes))?;

        let migrations = dtypes_registry.iter_dtypes()
            .flat_map(|dtype| {
                let model = dtypes_registry.get_model(&dtype.name);
                let smc: Box<dyn PostgresMetaController> = model
                    .meta_controller(self.backend())
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

    fn backend(&self) -> ::store::Backend {
        ::store::Backend::Postgres
    }
}
