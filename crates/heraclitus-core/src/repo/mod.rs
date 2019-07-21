use heraclitus_macros::stored_controller;

use crate::Error;
use crate::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use crate::store::Backend;
#[cfg(feature="backend-postgres")]
use crate::store::postgres::PostgresRepository;


pub enum Repository {
    #[cfg(feature="backend-postgres")]
    Postgres(PostgresRepository),
}

impl Repository {
    pub fn new(repo: &super::RepositoryLocation) -> Repository {
        #[allow(unused_imports)]
        use self::Repository::*;

        match repo.url.scheme() {
            #[cfg(feature="backend-postgres")]
            "postgres" | "postgresql" => Postgres(PostgresRepository::new(repo)),
            _ => unimplemented!()
        }
    }
}

#[stored_controller(Repository)]
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

    #[cfg(feature="backend-postgres")]
    use url::Url;

    pub fn init_repo<T: DatatypeEnum>(
            backend: Backend,
            dtypes_registry: &DatatypesRegistry<T>,
        ) -> Repository {

        let url = match backend {
            #[cfg(feature="backend-postgres")]
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

    #[cfg(feature="backend-postgres")]
    #[test]
    fn test_postgres_repo_init() {
        let dtypes_registry = crate::datatype::testing::init_empty_dtypes_registry();
        init_repo(Backend::Postgres, &dtypes_registry);
    }
}
