use ::datatype::producer::NoopProducerController;
use ::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for NoopProducerController {}

impl PostgresMetaController for NoopProducerController {}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use ::datatype::producer::tests::NegateBlobProducerController;


    impl PostgresMigratable for NegateBlobProducerController {}

    impl PostgresMetaController for NegateBlobProducerController {}
}
