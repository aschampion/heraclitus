use crate::datatype::StoreMetaController;


pub trait PostgresMetaController: crate::store::postgres::PostgresMigratable {}

impl Into<Box<dyn PostgresMetaController>> for StoreMetaController {
    fn into(self) -> Box<dyn PostgresMetaController> {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match self {
            StoreMetaController::Postgres(smc) => smc,
            _ => panic!("Wrong store type."),
        }
    }
}
