extern crate postgres;
extern crate schemer;

use petgraph::Direction;
use petgraph::visit::EdgeRef;
use schemer::Migrator;
use schemer_postgres::{PostgresAdapter, PostgresMigration};

use ::{
    ArtifactGraph, ArtifactRelation,
    DatatypeRepresentationKind, Identity, IdentifiableGraph,
    Version, VersionGraph, VersionGraphIndex, VersionRelation, VersionStatus};
use ::datatype::{
    Description, DependencyDescription, DependencyStoreRestriction,
    InterfaceController, MetaController,
    Model, PostgresMetaController, StoreMetaController};
use ::datatype::artifact_graph::ModelController as ArtifactGraphModelController;
use ::datatype::interface::ProducerController;
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
        store: Store,
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

    fn notify_new_version<'a>(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        art_graph: &'a ArtifactGraph,
        ver_graph: &mut VersionGraph<'a>,
        v_idx: VersionGraphIndex,
    ) {}
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

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
            store: Store,
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

        fn notify_new_version<'a>(
            &self,
            repo_control: &mut ::repo::StoreRepoController,
            art_graph: &'a ArtifactGraph,
            ver_graph: &mut VersionGraph<'a>,
            v_idx: VersionGraphIndex,
        ) {
            let input_art_relation = ArtifactRelation::ProducedFrom("input".into());
            let input_relation = VersionRelation::Dependence(&input_art_relation);
            let input_ver = ver_graph.get_related_version(
                v_idx,
                &input_relation,
                Direction::Incoming).expect("TODO1");

            let (art_idx, art) = art_graph.find_by_id(&ver_graph.versions[v_idx].artifact.id)
                .expect("TODO2");

            let output_art_relation_needle = ArtifactRelation::ProducedFrom("output".into());
            let (output_art_relation, output_art_idx) = art_graph.artifacts.graph()
                .edges_directed(art_idx, Direction::Outgoing)
                .find(|e| e.weight() == &output_art_relation_needle)
                .map(|e| (e.weight(), e.target()))
                .expect("TODO3");
            let output_art = &art_graph.artifacts[output_art_idx];

            let ver_blob = Version::new(output_art, DatatypeRepresentationKind::State);
            let ver_blob_idx = ver_graph.versions.add_node(ver_blob);
            ver_graph.versions.add_edge(
                v_idx,
                ver_blob_idx,
                VersionRelation::Dependence(output_art_relation));

            let mut model_ctrl = ::datatype::artifact_graph::model_controller(repo_control.store());

            model_ctrl.create_staging_version(
                repo_control,
                ver_graph,
                ver_blob_idx.clone()).unwrap();

            // unimplemented!();
        }
    }
}
