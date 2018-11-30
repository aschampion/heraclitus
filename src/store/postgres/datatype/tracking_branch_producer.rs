use ::datatype::tracking_branch_producer::TrackingBranchProducer;
use ::store::postgres::PostgresRepoController;
use ::store::StoreRepoBackend;
use ::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl<'repo> PostgresMigratable for StoreRepoBackend<'repo, PostgresRepoController, TrackingBranchProducer> {}

impl<'repo> PostgresMetaController for StoreRepoBackend<'repo, PostgresRepoController, TrackingBranchProducer> {}
