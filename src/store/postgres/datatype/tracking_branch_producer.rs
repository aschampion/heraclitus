use crate::datatype::tracking_branch_producer::TrackingBranchProducer;
use crate::store::postgres::PostgresRepository;
use crate::store::StoreRepoBackend;
use crate::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for StoreRepoBackend< PostgresRepository, TrackingBranchProducer> {}

impl PostgresMetaController for StoreRepoBackend< PostgresRepository, TrackingBranchProducer> {}
