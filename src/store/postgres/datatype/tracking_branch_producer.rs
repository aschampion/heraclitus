use ::datatype::tracking_branch_producer::TrackingBranchProducer;
use ::store::postgres::PostgresRepository;
use ::store::StoreRepoBackend;
use ::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for StoreRepoBackend< PostgresRepository, TrackingBranchProducer> {}

impl PostgresMetaController for StoreRepoBackend< PostgresRepository, TrackingBranchProducer> {}
