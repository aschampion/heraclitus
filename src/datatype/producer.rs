extern crate postgres;
extern crate schemer;


use ::{
    ArtifactGraph,
    DatatypeRepresentationKind, Error,
    VersionGraph, VersionGraphIndex,};
use ::datatype::{
    Description, DependencyDescription,
    InterfaceController, MetaController,
    Model, PostgresMetaController, StoreMetaController};
use ::datatype::interface::{ProducerController, ProductionOutput};
use ::repo::{PostgresMigratable};
use ::store::Store;


#[derive(Default)]
pub struct NoopProducer;

impl<T: InterfaceController<ProducerController>> Model<T> for NoopProducer {
    fn info(&self) -> Description {
        Description {
            name: "NoopProducer".into(),
            version: 1,
            representations: vec![DatatypeRepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec!["Producer"],
            dependencies: vec![],
        }
    }

    fn meta_controller(&self, store: Store) -> Option<StoreMetaController> {
        match store {
            Store::Postgres => Some(StoreMetaController::Postgres(
                Box::new(NoopProducerController {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        _store: Store,
        name: &str,
    ) -> Option<T> {
        match name {
            "Producer" => {
                let control: Box<ProducerController> = Box::new(NoopProducerController {});
                Some(T::from(control))
            },
            _ => None,
        }
    }
}

pub struct NoopProducerController;

impl MetaController for NoopProducerController {}

impl PostgresMigratable for NoopProducerController {}

impl PostgresMetaController for NoopProducerController {}

impl ProducerController for NoopProducerController {
    fn output_descriptions(&self) -> Vec<DependencyDescription> {
        vec![]
    }

    fn notify_new_version<'a, 'b>(
        &self,
        _repo_control: &mut ::repo::StoreRepoController,
        _art_graph: &'b ArtifactGraph<'a>,
        _ver_graph: &mut VersionGraph<'a, 'b>,
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

    use petgraph::Direction;
    use petgraph::visit::EdgeRef;
    use uuid::Uuid;

    use ::{
        ArtifactRelation, Hunk, Identity, IdentifiableGraph,
        PartCompletion, Version, VersionRelation};
    use datatype::{
        DependencyStoreRestriction,
    };
    use datatype::artifact_graph::ModelController as ArtifactGraphModelController;
    use datatype::blob::ModelController as BlobModelController;


    #[derive(Default)]
    pub struct NegateBlobProducer;

    impl<T: InterfaceController<ProducerController>> Model<T> for NegateBlobProducer {
        fn info(&self) -> Description {
            Description {
                name: "NegateBlobProducer".into(),
                version: 1,
                representations: vec![DatatypeRepresentationKind::State]
                        .into_iter()
                        .collect(),
                implements: vec!["Producer"],
                dependencies: vec![
                    DependencyDescription::new(
                        "input",
                        "Blob",
                        DependencyStoreRestriction::Same,
                    ),
                ],
            }
        }

        fn meta_controller(&self, store: Store) -> Option<StoreMetaController> {
            match store {
                Store::Postgres => Some(StoreMetaController::Postgres(
                    Box::new(NegateBlobProducerController {}))),
                _ => None,
            }
        }

        fn interface_controller(
            &self,
            _store: Store,
            name: &str,
        ) -> Option<T> {
            match name {
                "Producer" => {
                    let control: Box<ProducerController> = Box::new(NegateBlobProducerController {});
                    Some(T::from(control))
                },
                _ => None,
            }
        }
    }

    pub struct NegateBlobProducerController;

    impl MetaController for NegateBlobProducerController {}

    impl PostgresMigratable for NegateBlobProducerController {}

    impl PostgresMetaController for NegateBlobProducerController {}

    impl ProducerController for NegateBlobProducerController {
        fn output_descriptions(&self) -> Vec<DependencyDescription> {
            vec![
                DependencyDescription::new(
                    "output",
                    "Blob",
                    DependencyStoreRestriction::Same,
                ),
            ]
        }

        fn notify_new_version<'a, 'b>(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            art_graph: &'b ArtifactGraph<'a>,
            ver_graph: &mut VersionGraph<'a, 'b>,
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
            ver_graph.versions[v_idx].id.hash = ver_graph.versions[input_ver].id.hash;

            let (art_idx, art) = art_graph.find_by_id(&ver_graph.versions[v_idx].artifact.id)
                .expect("TODO2");

            // Find output relation and artifact.
            let output_art_relation_needle = ArtifactRelation::ProducedFrom("output".into());
            let (output_art_relation, output_art_idx) = art_graph.artifacts.graph()
                .edges_directed(art_idx, Direction::Outgoing)
                .find(|e| e.weight() == &output_art_relation_needle)
                .map(|e| (e.weight(), e.target()))
                .expect("TODO3");
            let output_art = &art_graph.artifacts[output_art_idx];

            // Create output version.
            let ver_blob = Version::new(output_art, DatatypeRepresentationKind::State);
            let ver_blob_idx = ver_graph.versions.add_node(ver_blob);
            ver_graph.versions.add_edge(
                v_idx,
                ver_blob_idx,
                VersionRelation::Dependence(output_art_relation))?;

            // This producer requires that the output use the same partitioning
            // as the input.
            // TODO: How should such constraints be formalized?
            let (input_ver_part_idx, _) = ver_graph.get_partitioning(input_ver).unwrap();
            let (input_art_part_idx, _) = art_graph.find_by_id(&ver_graph.versions[input_ver_part_idx].artifact.id).expect("TODO");
            // TODO: should check that this is the same the producer's partitioning.
            let output_part_art_rel_idx = art_graph.artifacts.find_edge(input_art_part_idx, output_art_idx)
                .expect("TODO");
            let output_part_art_rel = &art_graph.artifacts[output_part_art_rel_idx];
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

            let mut ag_control = ::datatype::artifact_graph::model_controller(repo_control.store());

            ag_control.create_staging_version(
                repo_control,
                ver_graph,
                ver_blob_idx.clone()).unwrap();

            let mut ver_hash = DefaultHasher::new();
            // Get input hunks.
            // TODO: For now this assumes the hunks are associated directly
            // with the input version. Does not account for partial partioning
            // versions.
            {
                let input_hunks = ag_control.get_hunks(
                    repo_control,
                    &ver_graph.versions[input_ver],
                    &ver_graph.versions[input_ver_part_idx]).expect("TODO");

                // Create output hunks computed from input hunks.
                let mut blob_control = ::datatype::blob::model_controller(repo_control.store());
                for input_hunk in &input_hunks {
                    let input_blob = blob_control.read(repo_control, input_hunk).expect("TODO");
                    let output_blob = input_blob.iter().cloned().map(|b| !b).collect::<Vec<u8>>();
                    let output_hunk = Hunk {
                        id: Identity {
                            uuid: Uuid::new_v4(),
                            hash: blob_control.hash(&output_blob),
                        },
                        version: &ver_graph.versions[ver_blob_idx],
                        partition: input_hunk.partition.clone(),
                        completion: PartCompletion::Complete,
                    };
                    output_hunk.id.hash.hash(&mut ver_hash);

                    ag_control.create_hunk(
                        repo_control,
                        &output_hunk).expect("TODO");
                    blob_control.write(
                        repo_control,
                        &output_hunk,
                        &output_blob).expect("TODO");
                }
            }

            ver_graph.versions[ver_blob_idx].id.hash = ver_hash.finish();

            // TODO commit version
            // TODO can't do this because can't have generic type in fn sig
            // (prevents boxing) necessary for have dtypes reg for committing.
            // TODO When is producer version committed? Must be before this.
            // These TODOs are worked around by making AG's cascade_notify_producers.

            Ok(ProductionOutput::Synchronous(vec![v_idx, ver_blob_idx]))
        }
    }
}
