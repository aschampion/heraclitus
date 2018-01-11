use ::datatype::tracking_branch_producer::TrackingBranchProducerController;
use ::store::postgres::PostgresMigratable;

use super::PostgresMetaController;


impl PostgresMigratable for TrackingBranchProducerController {}

impl PostgresMetaController for TrackingBranchProducerController {}
