use heraclitus_macros::stored_controller;

use ::Error;
use ::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use ::store::Backend;
use ::store::Store;
use ::store::postgres::PostgresRepoController;


pub enum StoreRepo {
    Postgres(PostgresRepoController),
}

impl StoreRepo {
    fn new(repo: &super::Repository) -> StoreRepo {
        use self::StoreRepo::*;
        match repo.url.scheme() {
            "postgres" | "postgresql" => Postgres(PostgresRepoController::new(repo)),
            _ => unimplemented!()
        }
    }

    fn controller(&self) -> StoreRepoController {
        match self {
            StoreRepo::Postgres(ref c) => StoreRepoController::Postgres(c),
        }
    }
}

pub enum StoreRepoController<'store> {
    Postgres(&'store PostgresRepoController),
}

#[stored_controller(<'store> StoreRepoController<'store>)]
#[stored_controller(<'store, D: ::datatype::DatatypeMarker> Store<'store, D>)]
pub trait RepoController {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error>;

    fn backend(&self) -> Backend;

    // TODO: seems this could be avoid with better handling of backend value/types.
    fn stored(&self) -> StoreRepoController;
}


/// Testing utilities.
///
/// This module is public so dependent libraries can reuse these utilities to
/// test custom datatypes.
pub mod testing {
    use super::*;

    use url::Url;

    pub fn init_repo<T: DatatypeEnum>(
            backend: Backend,
            dtypes_registry: &DatatypesRegistry<T>,
        ) -> StoreRepoController {

        let url = match backend {
            Backend::Postgres =>
                // Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap(),
                Url::parse("postgresql://postgres@localhost/?search_path=pg_temp").unwrap(),
            _ => unimplemented!()
        };

        let repo = ::Repository {
            url,
        };
        let mut repo_control = StoreRepo::new(&repo).controller();
        repo_control.init(&dtypes_registry).unwrap();

        repo_control
    }

    // pub fn init_default_context(backend: Backend) -> Context<::datatype::DefaultDatatypes> {
    //     let dtypes_registry = ::datatype::testing::init_default_dtypes_registry();
    //     let repo_control = init_repo(backend, &dtypes_registry);

    //     Context {
    //         dtypes_registry,
    //         repo_control,
    //     }
    // }

    #[test]
    fn test_postgres_repo_init() {
        let dtypes_registry = ::datatype::testing::init_default_dtypes_registry();
        init_repo(Backend::Postgres, &dtypes_registry);
    }
}
