use crate::store::StoreRepoBackend;
use crate::store::postgres::PostgresRepository;
use crate::datatype::producer::NoopProducer;
use crate::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for StoreRepoBackend< PostgresRepository, NoopProducer> {}

impl PostgresMetaController for StoreRepoBackend< PostgresRepository, NoopProducer> {}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::datatype::producer::tests::NegateBlobProducer;

    impl PostgresMigratable for StoreRepoBackend< PostgresRepository, NegateBlobProducer> {}

    impl PostgresMetaController for StoreRepoBackend< PostgresRepository, NegateBlobProducer> {}
}
