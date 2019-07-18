use std::collections::{
    BTreeSet,
    HashMap,
};

use heraclitus_core::{
    daggy,
    enum_set,
    petgraph,
};
use daggy::{
    Walker,
};
use daggy::petgraph::visit::EdgeRef;
#[cfg(feature="backend-postgres")]
use heraclitus_core::postgres;
#[cfg(feature="backend-postgres")]
use postgres::to_sql_checked;

use crate::{
    ArtifactGraph,
    ArtifactGraphIndex,
    ArtifactGraphEdgeIndex,
    ArtifactRelation,
    RepresentationKind,
    IdentifiableGraph,
    VersionGraph,
    VersionGraphIndex,
    VersionRelation,
};
use crate::datatype::interface::{
    ProductionStrategies,
    ProductionStrategyID,
};


/// Defines versions of the relevant producer a production policy requires to be
/// in the version graph.
///
/// Note that later variants are supersets of earlier variants.
#[derive(PartialOrd, PartialEq, Eq, Ord, Clone)]
pub(crate) enum PolicyProducerRequirements {
    /// No requirement.
    None,
    /// Any producer version dependent on parent versions of the new dependency
    /// version.
    DependentOnParentVersions,
    /// All versions of this producer.
    All,
}

/// Defines versions of dependencies of the relevant producer a production
/// policy requires to be in the version graph, in addition to dependencies
/// of producer versions specified by `PolicyProducerRequirements`.
///
/// Note that later variants are supersets of earlier variants.
#[derive(PartialOrd, PartialEq, Eq, Ord, Clone)]
pub(crate) enum PolicyDependencyRequirements {
    /// No requirement.
    None,
    /// Any dependency version on which a producer version (included by
    /// the `PolicyProducerRequirements`) is dependent.
    DependencyOfProducerVersion,
    /// All versions of the producer's dependency artifacts.
    All,
}

/// Defines what dependency and producer versions a production policy requires
/// to be in the version graph.
pub struct ProductionPolicyRequirements {
    pub(crate) producer: PolicyProducerRequirements,
    pub(crate) dependency: PolicyDependencyRequirements,
}

impl Default for ProductionPolicyRequirements {
    fn default() -> Self {
        ProductionPolicyRequirements {
            producer: PolicyProducerRequirements::None,
            dependency: PolicyDependencyRequirements::None,
        }
    }
}

/// Specifies a set of dependency versions for a new producer version.
#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProductionDependencySpec {
    pub(super) version: VersionGraphIndex,
    pub(super) relation: ArtifactGraphEdgeIndex,
}

type ProductionDependenciesSpecs = BTreeSet<ProductionDependencySpec>;

/// Specifies sets of dependencies for new producer version, mapped to the
/// parent producer versions for each.
#[derive(Default)]
pub struct ProductionVersionSpecs {
    pub(super) specs: HashMap<ProductionDependenciesSpecs, BTreeSet<Option<VersionGraphIndex>>>,
}

impl ProductionVersionSpecs {
    pub fn insert(&mut self, spec: ProductionDependenciesSpecs, parent: Option<VersionGraphIndex>) {
        self.specs.entry(spec)
            .or_insert_with(BTreeSet::new)
            .insert(parent);
    }

    pub fn merge(&mut self, other: ProductionVersionSpecs) {
        for (k, mut v) in other.specs {
            self.specs.entry(k)
                .and_modify(|existing| existing.append(&mut v))
                .or_insert(v);
        }
    }

    pub fn retain<F>(&mut self, filter: F)
            where F: FnMut(&ProductionDependenciesSpecs, &mut BTreeSet<Option<VersionGraphIndex>>) -> bool
    {
        self.specs.retain(filter);
    }
}

/// Enacts a policy for what new versions to produce in response to updated
/// dependency versions.
pub trait ProductionPolicy {
    /// Defines what this policy requires to be in the version graph for it
    /// to determine what new production versions should be created.
    fn requirements(&self) -> ProductionPolicyRequirements;
    // TODO: Convert to associated const once that lands.

    /// Given a producer and a new version of one of its dependencies, yield
    /// all sets of dependencies and parent versions for which new production
    /// versions should be created.
    fn new_production_version_specs(
        &self,
        art_graph: &ArtifactGraph,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
    ) -> ProductionVersionSpecs;
}


/// A production policy where existing producer versions are updated to track
/// new dependency versions.
pub struct ExtantProductionPolicy;

impl ProductionPolicy for ExtantProductionPolicy {
    fn requirements(&self) -> ProductionPolicyRequirements {
        ProductionPolicyRequirements {
            producer: PolicyProducerRequirements::DependentOnParentVersions,
            dependency: PolicyDependencyRequirements::None,
        }
    }

    fn new_production_version_specs(
        &self,
        art_graph: &ArtifactGraph,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
    ) -> ProductionVersionSpecs {
        let mut specs = ProductionVersionSpecs::default();

        // The dependency version must have parents, and all its parents must
        // have related dependent versions of this producer.
        // TODO: version artifact and producer artifact could share multiple
        // relationships. This is not yet handled.
        let v_parents: BTreeSet<VersionGraphIndex> = ver_graph.get_parents(v_idx)
            .iter().cloned().collect();
        if v_parents.is_empty() {
            return specs
        }

        let dep_art_idx = art_graph.get_by_id(&ver_graph[v_idx].artifact.id).unwrap().0;
        // TODO: Petgraph doesn't allow multiedges?
        let ver_rels = [
            VersionRelation::Dependence(
                &art_graph[art_graph.artifacts.graph()
                    .find_edge(dep_art_idx, p_art_idx).expect("TODO")]),
        ];

        // TODO: a mess that could be written much more concisely.
        for parent_v_idx in &v_parents {
            for ver_rel in &ver_rels {
                let prod_vers = ver_graph.get_related_versions(
                    *parent_v_idx,
                    ver_rel,
                    petgraph::Direction::Outgoing);

                for prod_ver in &prod_vers {
                    let mut dependencies = ProductionDependenciesSpecs::new();

                    for (e_idx, d_idx) in ver_graph.versions.parents(*prod_ver).iter(&ver_graph.versions) {
                        if let VersionRelation::Dependence(art_rel) = ver_graph[e_idx] {
                            let new_dep_vers = if v_parents.contains(&d_idx) {
                                v_idx
                            } else {
                                d_idx
                            };

                            // TODO: stupid. stupid. stupid.
                            let e_art_idx = art_graph.artifacts.graph()
                                .edges_directed(
                                    p_art_idx,
                                    petgraph::Direction::Incoming)
                                .filter(|e| e.weight() == art_rel)
                                .map(|e| e.id())
                                .nth(0).expect("TODO");

                            dependencies.insert(ProductionDependencySpec {
                                version: new_dep_vers,
                                relation: e_art_idx});
                        }
                    }

                    specs.insert(dependencies, Some(*prod_ver));
                }
            }
        }

        specs
    }
}


/// A production policy where iff there exist only and exactly one single leaf
/// version for all dependencies, a new producer version should be created
/// for these.
pub struct LeafBootstrapProductionPolicy;

impl ProductionPolicy for LeafBootstrapProductionPolicy {
    fn requirements(&self) -> ProductionPolicyRequirements {
        ProductionPolicyRequirements {
            producer: PolicyProducerRequirements::None,
            dependency: PolicyDependencyRequirements::All,
        }
    }

    fn new_production_version_specs(
        &self,
        art_graph: &ArtifactGraph,
        ver_graph: &VersionGraph,
        _: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
    ) -> ProductionVersionSpecs {
        let mut specs = ProductionVersionSpecs::default();
        let prod_art = art_graph.artifacts.node_weight(p_art_idx).expect("Non-existent producer");

        // Any version of this producer already exists.
        if !ver_graph.artifact_versions(prod_art).is_empty() {
            return specs
        }

        let mut dependencies = ProductionDependenciesSpecs::new();
        for (e_idx, d_idx) in art_graph.artifacts.parents(p_art_idx).iter(&art_graph.artifacts) {
            let dependency = &art_graph[d_idx];
            let dep_vers = ver_graph.artifact_versions(dependency);

            if dep_vers.len() != 1 {
                return specs;
            } else {
                dependencies.insert(ProductionDependencySpec {version: dep_vers[0], relation: e_idx});
            }
        }
        specs.insert(dependencies, None);

        specs
    }
}


#[derive(Clone, Copy, Debug)]
#[repr(u32)]
#[cfg_attr(feature="backend-postgres", derive(ToSql, FromSql))]
#[cfg_attr(feature="backend-postgres", postgres(name = "production_policy"))]
pub enum ProductionPolicies {
    #[cfg_attr(feature="backend-postgres", postgres(name = "extant"))]
    Extant,
    #[cfg_attr(feature="backend-postgres", postgres(name = "leaf_bootstrap"))]
    LeafBootstrap,
    #[cfg_attr(feature="backend-postgres", postgres(name = "custom"))]
    Custom,
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for ProductionPolicies {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> ProductionPolicies {
        std::mem::transmute(v)
    }
}


/// Enacts a policy for what production strategy (from the set of those of
/// which the producer is capable) to use for a particular version.
///
/// Currently this only involves the representation kinds of inputs and outputs
/// the producer supports.
///
/// Note that unlike `ProductionPolicy`, no requirements for related versions
/// are necessary, because this policy by construction only depends on
/// dependency versions of the relevant producer version, which are always in
/// the version graph when producer versions are created and notified.
pub(super) trait ProductionStrategyPolicy {
    fn select_representation(
        &self,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        strategies: &ProductionStrategies,
    ) -> Option<ProductionStrategyID>;
}

/// A production strategy policy selecting for the strategy with the most
/// parsimonious output representations.
pub struct ParsimoniousRepresentationProductionStrategyPolicy;

impl ProductionStrategyPolicy for ParsimoniousRepresentationProductionStrategyPolicy {
    fn select_representation(
        &self,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
        strategies: &ProductionStrategies,
    ) -> Option<ProductionStrategyID> {
        // Collect current version inputs.
        let inputs = ver_graph.versions.graph().edges_directed(v_idx, petgraph::Direction::Incoming)
            .filter_map(|edgeref| match *edgeref.weight() {
                VersionRelation::Dependence(relation) => {
                    match *relation {
                        ArtifactRelation::DtypeDepends(ref dtype_relation) =>
                            Some((dtype_relation.name.as_str(),
                                  ver_graph[edgeref.source()].representation)),
                        _ => None,
                    }
                }
                VersionRelation::Parent => None,
            })
            .collect::<Vec<_>>();

        strategies.iter()
            // Filter strategies by those applicable to current version inputs.
            .filter(|&(_, capability)| capability.matches_inputs(&inputs))
            // From remaining strategies, select that with minimal sum minimum
            // representation kind weighting.
            .map(|(id, capability)|
                (id, capability.outputs().values()
                    .map(|reps| {
                        reps.iter().map(|r| match r {
                            RepresentationKind::State => 3usize,
                            RepresentationKind::CumulativeDelta => 2,
                            RepresentationKind::Delta => 1,
                        })
                        .min()
                        .unwrap_or(0)
                    })
                    .sum::<usize>()
            ))
            .min_by_key(|&(_, score)| score)
            .map(|(id, _)| id.clone())
    }
}


/// Specifies the production strategy to use for a particular producer version.
pub struct ProductionStrategySpecs {
    pub(crate) representation: ProductionStrategyID,
    // TODO: there may be other categories capabilities, strategies and
    // policies in addition representation.
}


#[cfg(test)]
mod tests {
    use super::*;

    use maplit::btreeset;

    #[test]
    fn test_production_version_specs() {
      let a = ProductionDependencySpec {
          version: VersionGraphIndex::new(0),
          relation: ArtifactGraphEdgeIndex::new(0)
      };
      let b = ProductionDependencySpec {
          version: VersionGraphIndex::new(1),
          relation: ArtifactGraphEdgeIndex::new(1)
      };
      let c = ProductionDependencySpec {
          version: VersionGraphIndex::new(2),
          relation: ArtifactGraphEdgeIndex::new(0)
      };

      let mut specs_a = ProductionVersionSpecs::default();
      specs_a.insert(btreeset![a.clone(), b.clone()], Some(VersionGraphIndex::new(0)));
      specs_a.insert(btreeset![a.clone(), b.clone()], None);
      specs_a.insert(btreeset![c.clone(), b.clone()], Some(VersionGraphIndex::new(1)));

      assert!(specs_a.specs[&btreeset![a.clone(), b.clone()]].contains(&None));
      assert!(specs_a.specs[&btreeset![a.clone(), b.clone()]]
        .contains(&Some(VersionGraphIndex::new(0))));

      let mut specs_b = ProductionVersionSpecs::default();
      specs_b.insert(btreeset![c.clone(), b.clone()], Some(VersionGraphIndex::new(2)));

      specs_a.merge(specs_b);

      assert!(specs_a.specs[&btreeset![c.clone(), b.clone()]]
        .contains(&Some(VersionGraphIndex::new(1))));
      assert!(specs_a.specs[&btreeset![c.clone(), b.clone()]]
        .contains(&Some(VersionGraphIndex::new(2))));
    }
}
