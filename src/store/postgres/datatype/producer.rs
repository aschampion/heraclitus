use store::StoreRepoBackend;
use store::postgres::PostgresRepoController;
use ::datatype::producer::NoopProducer;
use ::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl<'repo> PostgresMigratable for StoreRepoBackend<'repo, PostgresRepoController, NoopProducer> {}

impl<'repo> PostgresMetaController for StoreRepoBackend<'repo, PostgresRepoController, NoopProducer> {}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use ::datatype::producer::tests::NegateBlobProducer;

    impl<'repo> PostgresMigratable for StoreRepoBackend<'repo, PostgresRepoController, NegateBlobProducer> {}

    impl<'repo> PostgresMetaController for StoreRepoBackend<'repo, PostgresRepoController, NegateBlobProducer> {}
}
