use crate::store::postgres::PostgresRepository;
use crate::datatype::producer::NoopProducerBackend;
use crate::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for NoopProducerBackend<PostgresRepository> {}

impl PostgresMetaController for NoopProducerBackend<PostgresRepository> {}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::datatype::producer::tests::NegateBlobProducerBackend;

    impl PostgresMigratable for NegateBlobProducerBackend<PostgresRepository> {}

    impl PostgresMetaController for NegateBlobProducerBackend<PostgresRepository> {}
}
