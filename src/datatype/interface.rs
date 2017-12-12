use std::collections::{BTreeSet, HashSet};

use ::{
    ArtifactGraph, Error, Identity, Interface, PartitionIndex, Version,
    VersionGraph, VersionGraphIndex};
use ::datatype::{DependencyDescription, InterfaceDescription};


lazy_static! {
    pub static ref INTERFACE_PARTITIONING_DESC: InterfaceDescription = InterfaceDescription {
        interface: Interface {
            name: "Partitioning",
        },
        extends: HashSet::new(),
    };

    pub static ref PARTITIONING_RELATION_NAME: String = "Partitioning".into();

    pub static ref INTERFACE_PRODUCER_DESC: InterfaceDescription = InterfaceDescription {
        interface: Interface {
            name: "Producer",
        },
        extends: HashSet::new(),
    };
}


pub trait PartitioningController {
    fn get_partition_ids(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        partitioning: &Version,
    ) -> BTreeSet<PartitionIndex>;
}


// TODO: this is a temporary workaround in the absence of actual server loop/
// commit queue and tokio/futures.
pub enum ProductionOutput {
    Asynchronous,
    /// Staged version nodes ready to be committed (typically including the
    /// producer version itself).
    Synchronous(Vec<VersionGraphIndex>),
}


pub trait ProducerController {
    fn output_descriptions(&self) -> Vec<DependencyDescription>;

    fn notify_new_version<'a, 'b>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<ProductionOutput, Error>;
}
