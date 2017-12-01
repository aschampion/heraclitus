use std::collections::{BTreeSet, HashSet};

use ::{
    ArtifactGraph, Identity, Interface, PartitionIndex, Version,
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


pub trait ProducerController {
    fn output_descriptions(&self) -> Vec<DependencyDescription>;

    fn notify_new_version<'a>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'a ArtifactGraph,
        ver_graph: &mut VersionGraph<'a>,
        v_idx: VersionGraphIndex,
    );
}
