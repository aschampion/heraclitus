use std::collections::{BTreeSet, HashMap, HashSet};

use heraclitus_core::enum_set;
use enum_set::EnumSet;
use heraclitus_macros::{interface, stored_interface_controller};

use crate::{
    ArtifactGraph,
    ArtifactGraphIndex,
    Error,
    Interface,
    PartitionIndex,
    RepresentationKind,
    VersionGraph,
    VersionGraphIndex,
};
use crate::datatype::{DependencyDescription, InterfaceDescription};
use crate::datatype::artifact_graph::production::ProductionPolicy;


pub use heraclitus_core::datatype::interface::*;


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

    pub static ref INTERFACE_CUSTOM_PRODUCTION_POLICY_DESC: InterfaceDescription = InterfaceDescription {
        interface: Interface {
            name: "CustomProductionPolicy",
        },
        extends: HashSet::new(),
    };
}


#[interface]
pub trait PartitioningController {
    fn get_partition_ids(
        &self,
        repo: &mut crate::repo::Repository,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> BTreeSet<PartitionIndex>;
}


pub type ProductionStrategyID = String;

pub type ProductionStrategies = HashMap<ProductionStrategyID, ProductionRepresentationCapability>;

/// Specifies sets of representation kinds for all inputs and outputs which
/// a producer supports.
pub struct ProductionRepresentationCapability {
    inputs: HashMap<&'static str, EnumSet<RepresentationKind>>,
    // TODO: should this also specify anything about the extant (parent) output
    // representations?
    outputs: HashMap<&'static str, EnumSet<RepresentationKind>>,
}

impl ProductionRepresentationCapability {
    pub fn new(
        inputs: HashMap<&'static str, EnumSet<RepresentationKind>>,
        outputs: HashMap<&'static str, EnumSet<RepresentationKind>>,
    ) -> Self {
        ProductionRepresentationCapability {inputs, outputs}
    }

    pub fn matches_inputs(
        &self,
        inputs: &[(&str, RepresentationKind)]
    ) -> bool {
        for &(input, rep) in inputs {
            if let Some(representations) = self.inputs.get(input) {
                if !representations.contains(&rep) {
                    return false;
                }
            }
            // If the input is not known to the representation capability,
            // assume it is satisfactory because artifact graphs may have
            // arbitrary additional relationships beyond the dependency
            // requirements.
        }

        true
    }

    pub fn outputs(&self) -> &HashMap<&'static str, EnumSet<RepresentationKind>> {
        &self.outputs
    }
}

// TODO: this is a temporary workaround in the absence of actual server loop/
// commit queue and tokio/futures.
pub enum ProductionOutput {
    Asynchronous,
    /// Staged version nodes ready to be committed (typically including the
    /// producer version itself).
    Synchronous(Vec<VersionGraphIndex>),
}


#[interface]
#[stored_interface_controller]
pub trait ProducerController {
    fn production_strategies(&self) -> ProductionStrategies;

    fn output_descriptions(&self) -> Vec<DependencyDescription>;

    fn notify_new_version<'a, 'b>(
        &self,
        repo: &crate::repo::Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<ProductionOutput, Error>;
}


#[interface]
#[stored_interface_controller]
pub trait CustomProductionPolicyController {
    fn get_custom_production_policy(
        &self,
        repo: &crate::repo::Repository,
        art_graph: &ArtifactGraph,
        prod_a_idx: ArtifactGraphIndex,
    ) -> Result<Box<dyn ProductionPolicy>, Error>;
}


#[cfg(test)]
mod tests {
    use super::*;

    use maplit::hashmap;

    #[test]
    fn test_production_representation_capability_matching() {
        let mut state = EnumSet::new();
        state.insert(RepresentationKind::State);

        let mut delta = EnumSet::new();
        delta.insert(RepresentationKind::Delta);

        let mut state_delta = EnumSet::new();
        state_delta.insert(RepresentationKind::State);
        state_delta.insert(RepresentationKind::Delta);

        let capability = ProductionRepresentationCapability::new(
            hashmap!{"a" => state, "b" => state_delta},
            HashMap::new(),
        );

        let compat = vec![
            ("a", RepresentationKind::State),
            ("b", RepresentationKind::State),
            ("a", RepresentationKind::State),
            ("b", RepresentationKind::Delta),
            ("c", RepresentationKind::CumulativeDelta),
        ];
        assert!(capability.matches_inputs(&compat));

        let incompat = vec![
            ("a", RepresentationKind::State),
            ("b", RepresentationKind::State),
            ("a", RepresentationKind::Delta),
            ("b", RepresentationKind::Delta),
            ("c", RepresentationKind::CumulativeDelta),
        ];
        assert!(!capability.matches_inputs(&incompat));
    }
}
