extern crate daggy;
extern crate petgraph;
extern crate schemer;
extern crate serde;
extern crate serde_json;
extern crate uuid;


use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

use daggy::petgraph::visit::EdgeRef;
use daggy::Walker;
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use postgres::types::ToSql;
use schemer::Migrator;
use schemer_postgres::{PostgresAdapter, PostgresMigration};
use uuid::Uuid;
use url::Url;

use ::{
    Artifact, ArtifactGraph, ArtifactRelation, Context,
    Datatype, DatatypeRelation, DatatypeRepresentationKind, Error, Identity,
    PartCompletion, PartitionIndex,
    Version, VersionGraph, VersionGraphIndex, VersionRelation, VersionStatus};
// use ::SingletonVersion;
// use ::singleton::SingletonVersion;
use super::{
    DatatypeEnum, DatatypesRegistry, DependencyDescription, DependencyStoreRestriction,
    Description, InterfaceController, Model, Store};
use ::datatype::interface::PartitioningController;
use ::repo::{PostgresRepoController, PostgresMigratable};


#[derive(Default)]
pub struct UnaryPartitioning;

impl<T: InterfaceController<PartitioningController>> Model<T> for UnaryPartitioning {
    fn info(&self) -> Description {
        Description {
            name: "UnaryPartitioning".into(),
            version: 1,
            representations: vec![DatatypeRepresentationKind::State]
                    .into_iter()
                    .collect(),
            implements: vec!["Partitioning"],
            dependencies: vec![],
        }
    }

    fn meta_controller(&self, store: Store) -> Option<super::StoreMetaController> {
        match store {
            Store::Postgres => Some(super::StoreMetaController::Postgres(
                Box::new(UnaryPartitioningController {}))),
            _ => None,
        }
    }

    fn interface_controller(
        &self,
        store: Store,
        name: &str
    ) -> Option<T> {
        match name {
            // "Partitioning" => T::from_box::<PartitioningController>(
            //     name,
            //     Box::new(UnaryPartitioningController {})
            // ),
            "Partitioning" => {
                let control: Box<PartitioningController> = Box::new(UnaryPartitioningController {});
                Some(T::from(control))
            },
            _ => None,
        }
    }
}

// // TODO: Code smell.
// pub struct SingletonVersion<'a> {
//     artifact: Artifact<'a>,
//     version: Version<'a>,
// }

// impl UnaryPartitioning {
//     pub fn build_singleton_version<'a, T: DatatypeEnum>(
//         dtypes_registry: &'a DatatypesRegistry<T>,
//     ) -> SingletonVersion<'a> {
//         ::new_singleton_version(
//             dtypes_registry,
//             Identity {uuid: UNARY_PARTITIONING_ARTIFACT_UUID.clone(), hash: 0},
//             Identity {uuid: UNARY_PARTITIONING_VERSION_UUID.clone(), hash: 0},
//             "UnaryPartitioning")
//         // let art = Artifact {
//         //     id: Identity {uuid: UNARY_PARTITIONING_ARTIFACT_UUID.clone(), hash: 0},
//         //     name: None,
//         //     dtype: dtypes_registry.get_datatype("UnaryPartitioning")
//         //                           .expect("Unary partitioning missing from registry"),
//         // };
//         // let art_graph = ArtifactGraph::new_singleton(
//         //     dtypes_registry.get_datatype("UnaryPartitioning")
//         //                           .expect("Unary partitioning missing from registry"),
//         //     UNARY_PARTITIONING_ART_GRAPH_UUID.clone(),
//         //     UNARY_PARTITIONING_ARTIFACT_UUID.clone(),
//         // );

//         // let up_an = art_graph
//         //     .find_artifact_by_uuid(&UNARY_PARTITIONING_ARTIFACT_UUID)
//         //     .expect("Impossible for unary partitioning to be missing from own singleton graph.").1;


//         // let mut sv = SingletonVersion {artifact: art, version: Version {
//         //         id: Identity {
//         //             uuid: UNARY_PARTITIONING_VERSION_UUID.clone(),
//         //             hash: 0,
//         //         },
//         //         artifact: &art,
//         //         status: ::VersionStatus::Committed,
//         //         representation: ::DatatypeRepresentationKind::State,
//         //     }};
//         // sv.version.artifact = &sv.artifact;
//         // return sv;

//         // (
//         //     Version {
//         //         id: Identity {
//         //             uuid: UNARY_PARTITIONING_VERSION_UUID.clone(),
//         //             hash: 0,
//         //         },
//         //         artifact: &art,
//         //         status: ::VersionStatus::Committed,
//         //         representation: ::DatatypeRepresentationKind::State,
//         //     },
//         //     art,
//         // )
//     }
// }

// impl PartitioningControllerFactor for UnaryPartitioning {
//     fn meta_controller(&self, store: Store) -> Box<PartitioningController> {
//         // Always return the same controller, since no backend is necessary.
//         Box::new(UnaryPartitioning)
//     }
// }

// TODO: The necessity of this singleton smells, but alternatives seem to
// converge back to it.
lazy_static! {
    pub static ref UNARY_PARTITIONING_ARTIFACT_UUID: Uuid =
        Uuid::parse_str("07659fa1-15a1-4e0d-a2b0-fb7b47685890").unwrap();

    pub static ref UNARY_PARTITIONING_ART_GRAPH_UUID: Uuid =
        Uuid::parse_str("0c1fac94-b785-42cf-b155-6869c116e036").unwrap();

//     static ref UNARY_PARTITIONING_DTYPE: Datatype = {
//         let model = UnaryPartitioning {};
//         model.info().datatype
//     };

//     static ref UNARY_PARTITIONING_SINGLETON_ART_GRAPH: ArtifactGraph<'static> = {
//         let mut art_graph = ArtifactGraph {
//             id: Identity {
//                 uuid: UNARY_PARTITIONING_ART_GRAPH_UUID.clone(),
//                 hash: 0,
//             },
//             artifacts: daggy::Dag::new(),
//         };
//         let mut s = DefaultHasher::new();
//         let mut ag_hash = DefaultHasher::new();
//         let mut art = Artifact {
//             id: Identity {uuid: UNARY_PARTITIONING_ARTIFACT_UUID.clone(), hash: 0},
//             name: Some("Unary Partitioning".into()),
//             dtype: &UNARY_PARTITIONING_DTYPE,
//         };
//         art.hash(&mut s);
//         art.id.hash = s.finish();
//         art.id.hash.hash(&mut ag_hash);
//         let art_node = ArtifactNode::Artifact(art);
//         art_graph.artifacts.add_node(art_node);
//         art_graph.id.hash = ag_hash.finish();
//         art_graph
//     };

    pub static ref UNARY_PARTITIONING_VERSION_UUID: Uuid =
        Uuid::parse_str("d1addd57-7846-48c5-bd09-565790c7ce29").unwrap();

//     pub static ref UNARY_PARTITIONING_VERSION: Version<'static> = {
//         let (_, up_an) = UNARY_PARTITIONING_SINGLETON_ART_GRAPH
//             .find_artifact_by_uuid(&UNARY_PARTITIONING_ARTIFACT_UUID)
//             .expect("Impossible for unary partitioning to be missing from own singleton graph.");
//         Version {
//             id: Identity {
//                 uuid: ,
//                 hash: 0,
//             },
//             artifact: up_an,
//             status: ::VersionStatus::Committed,
//             representation: ::DatatypeRepresentationKind::State,
//         }
//     };
}

const UNARY_PARTITION_INDEX: PartitionIndex = 0;
pub struct UnaryPartitioningController;

impl PartitioningController for UnaryPartitioningController {
    fn get_partition_ids(
        &self,
        repo_control: &mut ::repo::StoreRepoController,
        partitioning: &Version,
    ) -> BTreeSet<PartitionIndex> {
        BTreeSet::from_iter(vec![UNARY_PARTITION_INDEX])
    }
}


impl super::MetaController for UnaryPartitioningController {
}


impl PostgresMigratable for UnaryPartitioningController {}

impl super::PostgresMetaController for UnaryPartitioningController {}
