use std::collections::HashMap;

use maplit::hashmap;

use heraclitus_macros::DatatypeMarker;

use crate::{
    ArtifactGraph,
    RepresentationKind,
    Error,
    VersionGraph,
    VersionGraphIndex,
};
use crate::datatype::{
    DatatypeMeta,
    DependencyDescription,
    DependencyTypeRestriction,
    DependencyCardinalityRestriction,
    DependencyStoreRestriction,
    InterfaceController,
    Model,
    Reflection,
};
use crate::datatype::interface::{
    ProducerController,
    ProductionOutput,
    ProductionRepresentationCapability,
    ProductionStrategies,
};
use crate::repo::{
    RepoController,
    Repository,
};


#[derive(Default, DatatypeMarker)]
pub struct NoopProducer;

impl DatatypeMeta for NoopProducer {
    const NAME: &'static str = "NoopProducer";
    const VERSION: u64 = 1;
}

impl<T: InterfaceController<ProducerController>> Model<T> for NoopProducer {
    fn reflection(&self) -> Reflection<T> {
        Reflection {
            representations:  enumset::enum_set!(
                    RepresentationKind::State |
                ),
            implements: vec![
                <T as InterfaceController<ProducerController>>::VARIANT,
            ],
            dependencies: vec![
                DependencyDescription::new(
                    "input",
                    DependencyTypeRestriction::Any,
                    DependencyCardinalityRestriction::Unbounded,
                    DependencyStoreRestriction::Same,
                ),
            ],
        }
    }

    datatype_controllers!(NoopProducer, (ProducerController));
}

// impl<RC: RepoController> MetaController for StoreRepoBackend<RC, NoopProducer> {}

impl<RC: RepoController> ProducerController for NoopProducerBackend<RC> {
    fn production_strategies(&self) -> ProductionStrategies {
    // fn representation_capabilities(&self) -> Vec<ProductionRepresentationCapability> {
        hashmap!{
            "only".into() => ProductionRepresentationCapability::new(
                hashmap!{"input" => RepresentationKind::all()},
                HashMap::new(),
            )
        }
    }

    fn output_descriptions(&self) -> Vec<DependencyDescription> {
        vec![]
    }

    fn notify_new_version<'ag>(
        &self,
        _repo: &Repository,
        _art_graph: &'ag ArtifactGraph,
        _ver_graph: &mut VersionGraph<'ag>,
        v_idx: VersionGraphIndex,
    ) -> Result<ProductionOutput, Error> {
        Ok(ProductionOutput::Synchronous(vec![v_idx]))
    }
}


#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use heraclitus_core::{
        petgraph,
        uuid,
    };
    use enumset::enum_set;
    use maplit::hashset;
    use petgraph::Direction;
    use petgraph::visit::EdgeRef;
    use uuid::Uuid;

    use crate::{
        ArtifactRelation, Hunk, Identity, IdentifiableGraph,
        PartCompletion, Version, VersionRelation};
    use crate::datatype::{
        ComposableState,
        DatatypeMarker,
        Payload,
        Storage as DatatypeStorage,
    };
    use crate::datatype::artifact_graph::Storage as ArtifactGraphStorage;
    use crate::datatype::blob::BlobDatatype;


    #[derive(Default, DatatypeMarker)]
    pub struct NegateBlobProducer;

    impl DatatypeMeta for NegateBlobProducer {
        const NAME: &'static str = "NegateBlobProducer";
        const VERSION: u64 = 1;
    }

    impl<T: InterfaceController<ProducerController>> Model<T> for NegateBlobProducer {
        fn reflection(&self) -> Reflection<T> {
            Reflection {
                representations: enum_set!(
                        RepresentationKind::State |
                    ),
                implements: vec![
                    <T as InterfaceController<ProducerController>>::VARIANT,
                ],
                dependencies: vec![
                    DependencyDescription::new(
                        "input",
                        DependencyTypeRestriction::Datatype(hashset!["Blob"]),
                        DependencyCardinalityRestriction::Exact(1),
                        DependencyStoreRestriction::Same,
                    ),
                ],
            }
        }

        datatype_controllers!(NegateBlobProducer, (ProducerController));
    }

    // impl<RC: RepoController> MetaController for StoreRepoBackend<RC, NegateBlobProducer> {}

    impl<RC: RepoController> ProducerController for NegateBlobProducerBackend<RC> {
        fn production_strategies(&self) -> ProductionStrategies {
            let rep = enum_set!(
                    RepresentationKind::State |
                    RepresentationKind::Delta |
                );

            hashmap!{
                "normal".into() => ProductionRepresentationCapability::new(
                    hashmap!{"input" => rep.clone()},
                    hashmap!{"output" => rep},
                )
            }
        }

        fn output_descriptions(&self) -> Vec<DependencyDescription> {
            vec![
                DependencyDescription::new(
                    "output",
                    DependencyTypeRestriction::Datatype(hashset!["Blob"]),
                    DependencyCardinalityRestriction::Exact(1),
                    DependencyStoreRestriction::Same,
                ),
            ]
        }

        fn notify_new_version<'ag>(
            &self,
            repo: &Repository,
            art_graph: &'ag ArtifactGraph,
            ver_graph: &mut VersionGraph<'ag>,
            v_idx: VersionGraphIndex,
        ) -> Result<ProductionOutput, Error> {
            // Find input relation, artifact, and versions.
            let input_art_relation = ArtifactRelation::ProducedFrom("input".into());
            let input_relation = VersionRelation::Dependence(&input_art_relation);
            let input_ver = *ver_graph.get_related_versions(
                v_idx,
                &input_relation,
                Direction::Incoming).get(0).expect("TODO");

            // Set own hash to input version.
            // TODO: not yet clear what producer version hash should be.
            ver_graph[v_idx].id.hash = ver_graph[input_ver].id.hash;

            let (art_idx, _) = art_graph.get_by_id(&ver_graph[v_idx].artifact.id)
                .expect("TODO2");

            // Find output relation and artifact.
            let output_art_relation_needle = ArtifactRelation::ProducedFrom("output".into());
            let (output_art_relation, output_art_idx) = art_graph.artifacts.graph()
                .edges_directed(art_idx, Direction::Outgoing)
                .find(|e| e.weight() == &output_art_relation_needle)
                .map(|e| (e.weight(), e.target()))
                .expect("TODO3");
            let output_art = &art_graph[output_art_idx];

            // Create output version.
            let ver_blob = Version::new(
                output_art,
                ver_graph[input_ver].representation);
            let ver_blob_idx = ver_graph.versions.add_node(ver_blob);
            ver_graph.versions.add_edge(
                v_idx,
                ver_blob_idx,
                VersionRelation::Dependence(output_art_relation))?;

            // This producer requires that the output use the same partitioning
            // as the input.
            // TODO: How should such constraints be formalized?
            let (input_ver_part_idx, _) = ver_graph.get_partitioning(input_ver).unwrap();
            let (input_art_part_idx, _) = art_graph.get_by_id(&ver_graph[input_ver_part_idx].artifact.id).expect("TODO");
            // TODO: should check that this is the same the producer's partitioning.
            let output_part_art_rel_idx = art_graph.artifacts.find_edge(input_art_part_idx, output_art_idx)
                .expect("TODO");
            let output_part_art_rel = &art_graph[output_part_art_rel_idx];
            // TODO: check this is actually a partitioning rel.
            ver_graph.versions.add_edge(input_ver_part_idx, ver_blob_idx,
                VersionRelation::Dependence(output_part_art_rel))?;

            // Add output parent relations to all outputs of this
            // producer's parents.
            let parent_prod_vers = ver_graph.get_related_versions(
                v_idx,
                &VersionRelation::Parent,
                Direction::Incoming);
            for parent_ver_idx in parent_prod_vers {
                let parent_output_idx = *ver_graph.get_related_versions(
                    parent_ver_idx,
                    &VersionRelation::Dependence(output_art_relation),
                    Direction::Outgoing).get(0).expect("TODO: parent should have output");
                ver_graph.versions.add_edge(parent_output_idx, ver_blob_idx,
                    VersionRelation::Parent)?;
            }

            let mut ag_control = crate::datatype::artifact_graph::ArtifactGraphDtype::store(repo);

            let _production_specs = ag_control.get_production_specs(
                repo,
                &ver_graph[v_idx])?;

            ag_control.create_staging_version(
                repo,
                ver_graph,
                ver_blob_idx.clone()).unwrap();

            let mut ver_hash = DefaultHasher::new();
            // Get input hunks.
            // TODO: For now this assumes the hunks are associated directly
            // with the input version.
            {
                let input_hunks = ag_control.get_hunks(
                    repo,
                    &ver_graph[input_ver],
                    &ver_graph[input_ver_part_idx],
                    None).expect("TODO");

                // Create output hunks computed from input hunks.
                let mut blob_control = BlobDatatype::store(repo);
                for input_hunk in &input_hunks {
                    let input_blob = blob_control.read_hunk(repo, input_hunk).expect("TODO");
                    let output_blob = match input_blob {
                        Payload::State(ref blob) =>
                            Payload::State(blob.iter().cloned().map(|b| !b).collect::<Vec<u8>>()),
                        Payload::Delta((ref indices, ref bytes)) =>
                            Payload::Delta((
                                indices.clone(),
                                bytes.iter().clone().map(|b| !b).collect::<Vec<u8>>(),
                            )),
                    };
                    let output_hunk = Hunk {
                        id: BlobDatatype::hash_payload(&output_blob).into(),
                        version: &ver_graph[ver_blob_idx],
                        representation: input_hunk.representation,
                        partition: input_hunk.partition.clone(),
                        completion: PartCompletion::Complete,
                        precedence: None, // TODO
                    };
                    output_hunk.id.hash.hash(&mut ver_hash);

                    ag_control.create_hunk(repo, &output_hunk).expect("TODO");
                    blob_control.write_hunk(
                        repo,
                        &output_hunk,
                        &output_blob).expect("TODO");
                }
            }

            ver_graph[ver_blob_idx].id.hash = ver_hash.finish();

            // TODO commit version
            // TODO can't do this because can't have generic type in fn sig
            // (prevents boxing) necessary for have dtypes reg for committing.
            // TODO When is producer version committed? Must be before this.
            // These TODOs are worked around by making AG's cascade_notify_producers.

            Ok(ProductionOutput::Synchronous(vec![v_idx, ver_blob_idx]))
        }
    }
}
