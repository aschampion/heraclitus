use std::collections::{BTreeSet, HashSet};

use ::{Interface, PartitionIndex, Version};
use ::datatype::InterfaceDescription;


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
    // TODO
}
