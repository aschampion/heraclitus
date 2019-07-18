use crate::datatype::tracking_branch_producer::TrackingBranchProducerBackend;
use crate::store::postgres::PostgresRepository;
use crate::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for TrackingBranchProducerBackend<PostgresRepository> {}

impl PostgresMetaController for TrackingBranchProducerBackend<PostgresRepository> {}
