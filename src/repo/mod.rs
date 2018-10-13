use ::Error;
use ::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use ::store::Store;
use ::store::postgres::PostgresRepoController;


// pub type StoreRepoController = Stored<Box<RepoController>>;
pub enum StoreRepoController {
    Postgres(PostgresRepoController),
}

impl StoreRepoController {
    fn new(repo: &super::Repository) -> StoreRepoController {
        use self::StoreRepoController::*;
        match repo.url.scheme() {
            "postgres" | "postgresql" => Postgres(PostgresRepoController::new(repo)),
            _ => unimplemented!()
        }
    }

    pub fn store(&self) -> Store {
        match *self {
            StoreRepoController::Postgres(_) => Store::Postgres,
        }
    }
}

impl RepoController for StoreRepoController {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error> {
        use self::StoreRepoController::*;

        match *self {
            Postgres(ref mut rc) => rc.init(dtypes_registry)
        }
    }
}

pub trait RepoController {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error>;
}


/// Testing utilities.
///
/// This module is public so dependent libraries can reuse these utilities to
/// test custom datatypes.
pub mod testing {
    use super::*;

    use url::Url;

    use ::{Context};

    pub fn init_repo<T: DatatypeEnum>(
            store: Store,
            dtypes_registry: &DatatypesRegistry<T>,
        ) -> StoreRepoController {

        let url = match store {
            Store::Postgres =>
                // Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap(),
                Url::parse("postgresql://postgres@localhost/?search_path=pg_temp").unwrap(),
            _ => unimplemented!()
        };

        let repo = ::Repository {
            url,
        };
        let mut repo_control = StoreRepoController::new(&repo);
        repo_control.init(&dtypes_registry).unwrap();

        repo_control
    }

    pub fn init_default_context(store: Store) -> Context<::datatype::DefaultDatatypes> {
        let dtypes_registry = ::datatype::testing::init_default_dtypes_registry();
        let repo_control = init_repo(store, &dtypes_registry);

        Context {
            dtypes_registry,
            repo_control,
        }
    }

    #[test]
    fn test_postgres_repo_init() {
        let dtypes_registry = ::datatype::testing::init_default_dtypes_registry();
        init_repo(Store::Postgres, &dtypes_registry);
    }
}
