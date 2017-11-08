use std::collections::BTreeSet;
use std::iter::FromIterator;

use ::{PartitionIndex, UNARY_PARTITION_INDEX, Version};

// Need to:
//
// - [ ] Be able to get a set of partition IDs (given a partitioning version)
//

pub trait PartitioningController {
    fn get_partition_ids(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        partitioning: &Version,
    ) -> BTreeSet<PartitionIndex>;
}

pub struct UnaryPartitioning {}

impl PartitioningController for UnaryPartitioning {
    fn get_partition_ids(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        partitioning: &Version,
    ) -> BTreeSet<PartitionIndex> {
        BTreeSet::from_iter(vec![UNARY_PARTITION_INDEX])
    }
}
