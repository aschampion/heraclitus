use heraclitus_macros::stored_controller;

use crate::Error;
use crate::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use crate::store::Backend;
use crate::store::postgres::PostgresRepository;


pub enum Repository {
    Postgres(PostgresRepository),
}

impl Repository {
    fn new(repo: &super::RepositoryLocation) -> Repository {
        use self::Repository::*;
        match repo.url.scheme() {
            "postgres" | "postgresql" => Postgres(PostgresRepository::new(repo)),
            _ => unimplemented!()
        }
    }

    // fn controller(&self) -> Repository {
    //     match self {
    //         StoreRepo::Postgres(ref c) => Repository::Postgres(c),
    //     }
    // }
}

// pub enum Repository {
//     Postgres(&'store PostgresRepository),
// }

#[stored_controller( Repository)]
// #[stored_controller(< D: ::datatype::DatatypeMarker> Store< D>)]
pub trait RepoController {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error>;

    fn backend(&self) -> Backend;

    // TODO: seems this could be avoid with better handling of backend value/types.
    // fn stored(&self) -> Repository;
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
        ) -> Repository {

        let url = match backend {
            Backend::Postgres =>
                // Url::parse("postgresql://hera_test:hera_test@localhost/hera_test").unwrap(),
                Url::parse("postgresql://postgres@localhost/?search_path=pg_temp").unwrap(),
            _ => unimplemented!()
        };

        let repo = crate::RepositoryLocation {
            url,
        };
        let mut repo = Repository::new(&repo);
        repo.init(&dtypes_registry).unwrap();

        repo
    }

    // pub fn init_default_context(backend: Backend) -> Context<::datatype::DefaultDatatypes> {
    //     let dtypes_registry = ::datatype::testing::init_default_dtypes_registry();
    //     let repo = init_repo(backend, &dtypes_registry);

    //     Context {
    //         dtypes_registry,
    //         repo,
    //     }
    // }

    #[test]
    fn test_postgres_repo_init() {
        let dtypes_registry = crate::datatype::testing::init_default_dtypes_registry();
        init_repo(Backend::Postgres, &dtypes_registry);
    }
}
