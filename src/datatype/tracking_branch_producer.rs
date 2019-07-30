use std::collections::{BTreeSet, HashMap, HashSet};

use heraclitus_core::{
    enum_set,
    petgraph,
};
use heraclitus_macros::{
    DatatypeMarker,
};
use enum_set::EnumSet;
use maplit::{hashmap, hashset};
use petgraph::Direction;
use petgraph::visit::EdgeRef;

use crate::{
    ArtifactGraph,
    ArtifactGraphIndex,
    ArtifactRelation,
    Error,
    Identity,
    IdentifiableGraph,
    ModelError,
    RepresentationKind,
    Version,
    VersionGraph,
    VersionGraphIndex,
    VersionRelation,
};
use crate::datatype::{
    DatatypeMarker,
    DependencyDescription,
    DependencyCardinalityRestriction,
    DependencyStoreRestriction,
    DependencyTypeRestriction,
    Description,
    InterfaceController,
    Model,
};
use crate::datatype::artifact_graph::production::{
    ExtantProductionPolicy,
    PolicyDependencyRequirements,
    PolicyProducerRequirements,
    ProductionPolicy,
    ProductionPolicyRequirements,
    ProductionVersionSpecs,
};
use crate::datatype::artifact_graph::Storage as ArtifactGraphStorage;
use crate::datatype::interface::{
    CustomProductionPolicyController,
    ProducerController,
    ProductionOutput,
    ProductionRepresentationCapability,
    ProductionStrategies,
};
use crate::datatype::reference::{
    Storage as ReferenceStorage,
    Ref,
};
use crate::repo::RepoController;


#[derive(Default, DatatypeMarker)]
pub struct TrackingBranchProducer;

impl<T> Model<T> for TrackingBranchProducer
        where T: InterfaceController<ProducerController> +
                 InterfaceController<CustomProductionPolicyController> {
    fn info(&self) -> Description<T> {
        Description {
            name: "TrackingBranchProducer".into(),
            version: 1,
            representations: vec![RepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec![
                <T as InterfaceController<ProducerController>>::VARIANT,
                <T as InterfaceController<CustomProductionPolicyController>>::VARIANT,
            ],
            dependencies: vec![
                DependencyDescription::new(
                    "tracked",
                    DependencyTypeRestriction::Any,
                    DependencyCardinalityRestriction::Unbounded,
                    DependencyStoreRestriction::Same,
                ),
            ],
        }
    }

    datatype_controllers!(TrackingBranchProducer, (ProducerController, CustomProductionPolicyController));
}


struct TrackingBranchProductionPolicy {
    /// Version IDs for branch revision tips of the output ref.
    tips: HashSet<Identity>,
}

impl ProductionPolicy for TrackingBranchProductionPolicy {
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
        // Find if any version in `tips` is dep on parent version of `v_idx`
        // (since not all rels of refs may be loaded, use prod version instead
        // since has same relations).
        let extant_policy = ExtantProductionPolicy;
        let mut extant_specs = extant_policy.new_production_version_specs(
            art_graph,
            ver_graph,
            v_idx,
            p_art_idx);

        let tip_prod_ver_idxs: BTreeSet<Option<VersionGraphIndex>> = self.tips
            .iter()
            .map(|t| ver_graph.get_by_id(t).expect("TODO").0)
            .map(|ref_v_idx| ver_graph.get_related_versions(
                ref_v_idx,
                &VersionRelation::Dependence(&ArtifactRelation::ProducedFrom("output".into())),
                Direction::Incoming))
            .flat_map(|s| s.into_iter().map(Some))
            .collect();

        extant_specs.retain(|_, ref mut v| !v.is_disjoint(&tip_prod_ver_idxs));

        extant_specs
    }
}


impl<RC: RepoController> CustomProductionPolicyController for TrackingBranchProducerBackend<RC> {
    fn get_custom_production_policy(
        &self,
        repo: &crate::repo::Repository,
        art_graph: &ArtifactGraph,
        prod_a_idx: ArtifactGraphIndex,
    ) -> Result<Box<dyn ProductionPolicy>, Error> {
        // Get output ref artifact.
        let ref_art_idx = art_graph.get_related_artifacts(
            prod_a_idx,
            &ArtifactRelation::ProducedFrom("output".into()),
            Direction::Outgoing)[0];
        let ref_art = &art_graph[ref_art_idx];

        // Get ref model controller.
        let ref_control = Ref::store(repo);

        // Get branch heads from model controller.
        let tips = ref_control.get_branch_revision_tips(repo, ref_art)?.values().cloned().collect();
        Ok(Box::new(TrackingBranchProductionPolicy {tips}))
    }
}

impl<RC: RepoController> ProducerController for TrackingBranchProducerBackend<RC> {
    fn production_strategies(&self) -> ProductionStrategies {
        let mut rep = EnumSet::new();
        rep.insert(RepresentationKind::State);
        rep.insert(RepresentationKind::Delta);

        hashmap!{
            "normal".into() => ProductionRepresentationCapability::new(
                hashmap!{"input" => rep},
                hashmap!{"output" => rep},
            )
        }
    }

    fn output_descriptions(&self) -> Vec<DependencyDescription> {
        vec![
            DependencyDescription::new(
                "output",
                DependencyTypeRestriction::Datatype(hashset!["Ref"]),
                DependencyCardinalityRestriction::Exact(1),
                DependencyStoreRestriction::Same,
            ),
        ]
    }

    fn notify_new_version<'a, 'b>(
        &self,
        repo: &crate::repo::Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<ProductionOutput, Error> {
        let prod_a_idx = art_graph.get_by_id(&ver_graph[v_idx].artifact.id).expect("TODO").0;

        // Find output relation and artifact.
        let ref_art_relation_needle = ArtifactRelation::ProducedFrom("output".into());
        let (ref_art_relation, ref_art_idx) = art_graph.artifacts.graph()
            .edges_directed(prod_a_idx, Direction::Outgoing)
            .find(|e| e.weight() == &ref_art_relation_needle)
            .map(|e| (e.weight(), e.target()))
            .expect("TODO3");
        let ref_art = &art_graph[ref_art_idx];

        // Create output ref version, which should have same dependencies as
        // this producer version.
        let ref_ver = Version::new(
            ref_art,
            RepresentationKind::State);
        let ref_ver_idx = ver_graph.versions.add_node(ref_ver);
        ver_graph.versions.add_edge(
            v_idx,
            ref_ver_idx,
            VersionRelation::Dependence(ref_art_relation))?;

        // Add output parent relations to all outputs of this
        // producer's parents.
        let parent_prod_vers = ver_graph.get_related_versions(
            v_idx,
            &VersionRelation::Parent,
            Direction::Incoming);
        let mut parent_ref_ver_idxs = BTreeSet::new();
        for parent_ver_idx in parent_prod_vers {
            let parent_ref_idx = *ver_graph.get_related_versions(
                parent_ver_idx,
                &VersionRelation::Dependence(ref_art_relation),
                Direction::Outgoing).get(0).expect("TODO: parent should have output");
            ver_graph.versions.add_edge(parent_ref_idx, ref_ver_idx,
                VersionRelation::Parent)?;
            parent_ref_ver_idxs.insert(parent_ref_idx);
        }

        // Add dependence relation to all tracked versions.
        let tracked_art_relation_needle = ArtifactRelation::ProducedFrom("tracked".into());
        let tracked_vers = ver_graph.get_related_versions(
            v_idx,
            &VersionRelation::Dependence(&tracked_art_relation_needle),
            Direction::Incoming);
        for tracked_ver_idx in tracked_vers {
            let tracked_ver_art_idx = art_graph.get_by_id(&ver_graph[tracked_ver_idx].artifact.id)
                .expect("TODO: unable to find tracked ver art").0;
            let tracked_ref_rel = &art_graph[
                art_graph.artifacts.find_edge(tracked_ver_art_idx, ref_art_idx)
                    .expect("TODO")
            ];
            ver_graph.versions.add_edge(tracked_ver_idx, ref_ver_idx,
                VersionRelation::Dependence(tracked_ref_rel))?;
        }

        let mut ag_control = crate::datatype::artifact_graph::ArtifactGraphDtype::store(repo);
        ag_control.create_staging_version(
            repo,
            ver_graph,
            ref_ver_idx).unwrap();

        // TODO: ref hash

        // Get ref model controller.
        let mut ref_control = Ref::store(repo);

        // Get branch heads from model controller.
        let old_tips = ref_control.get_branch_revision_tips(repo, ref_art)?;

        if old_tips.is_empty() {
            // Leaf bootstrapping.
            ref_control.create_branch(repo, &ver_graph[ref_ver_idx], "master")?;
        } else {
            let new_tips: HashMap<_, _> = old_tips.into_iter().filter_map(|(bprt, id)| {
                match ver_graph.get_by_id(&id) {
                    Some((idx, _)) => {
                        if parent_ref_ver_idxs.contains(&idx) {
                            Some((bprt, ver_graph[ref_ver_idx].id))
                        } else {
                            None
                        }
                    },
                    None => None,
                }
            }).collect();

            if !new_tips.is_empty() {
                // Normal tracking update.
                ref_control.set_branch_revision_tips(
                    repo,
                    ref_art,
                    &new_tips)?;
            } else {
                return Err(ModelError::Other("Attempt to create tracking version for non-tip".into()).into())
            }
        }

        Ok(ProductionOutput::Synchronous(vec![v_idx, ref_ver_idx]))
    }
}
