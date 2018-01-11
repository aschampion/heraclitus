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

impl Into<Box<PostgresMetaController>> for StoreMetaController {
    fn into(self) -> Box<PostgresMetaController> {
        match self {
            StoreMetaController::Postgres(smc) => smc,
            _ => panic!("Wrong store type."),
        }
    }
}
