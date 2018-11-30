use store::StoreRepoBackend;
use store::postgres::PostgresRepository;
use ::datatype::producer::NoopProducer;
use ::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for StoreRepoBackend< PostgresRepository, NoopProducer> {}

impl PostgresMetaController for StoreRepoBackend< PostgresRepository, NoopProducer> {}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use ::datatype::producer::tests::NegateBlobProducer;

    impl PostgresMigratable for StoreRepoBackend< PostgresRepository, NegateBlobProducer> {}

    impl PostgresMetaController for StoreRepoBackend< PostgresRepository, NegateBlobProducer> {}
}
