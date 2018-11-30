use ::datatype::{
    MetaController,
    StoreMetaController,
};


pub mod artifact_graph;
pub mod blob;
pub mod partitioning;
pub mod producer;
pub mod reference;
pub mod tracking_branch_producer;


pub trait PostgresMetaController: MetaController + ::store::postgres::PostgresMigratable {}

impl<'a> Into<Box<PostgresMetaController + 'a>> for StoreMetaController<'a> {
    fn into(self) -> Box<PostgresMetaController + 'a> {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match self {
            StoreMetaController::Postgres(smc) => smc,
            _ => panic!("Wrong store type."),
        }
    }
}
