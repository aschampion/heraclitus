use super::*;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

use maplit::{
    btreeset,
    hashmap,
};
use uuid::Uuid;

use crate::{
    Partition,
    PartCompletion,
};
use crate::datatype::{
    ComposableState,
    DatatypeMarker,
};
use crate::datatype::blob::BlobDatatype;
use crate::datatype::partitioning::{
    Partitioning,
    PartitioningState,
    UNARY_PARTITION_INDEX,
};
use crate::datatype::Storage as DatatypeStorage;
use crate::datatype::partitioning::arbitrary::{
    ArbitraryPartitioning,
    ArbitraryPartitioningState,
};
use crate::datatype::producer::tests::NegateBlobProducer;

use crate::store::{Backend};


datatype_enum!(TestDatatypes, crate::datatype::DefaultInterfaceController, (
    (ArtifactGraph, crate::datatype::artifact_graph::ArtifactGraphDtype),
    (Ref, crate::datatype::reference::Ref),
    (UnaryPartitioning, crate::datatype::partitioning::UnaryPartitioning),
    (ArbitraryPartitioning, crate::datatype::partitioning::arbitrary::ArbitraryPartitioning),
    (Blob, crate::datatype::blob::BlobDatatype),
    (NoopProducer, crate::datatype::producer::NoopProducer),
    (NegateBlobProducer, NegateBlobProducer),
    (TrackingBranchProducer, crate::datatype::tracking_branch_producer::TrackingBranchProducer),
));

/// Create a simple artifact chain of
/// Blob -> Producer -> Blob -> Producer -> Blob.
fn simple_blob_prod_ag_fixture<'a, T: DatatypeEnum>(
    dtypes_registry: &'a DatatypesRegistry<T>,
    partitioning: Option<ArtifactDescription>,
) -> (ArtifactGraph<'a>, HashMap<&str, ArtifactGraphIndex>) {
    let mut artifacts = ArtifactGraphDescriptionType::new();

    // Blob 1
    let blob1_node = ArtifactDescription {
        id: None,
        name: Some("Test Blob 1".into()),
        dtype: "Blob".into(),
        self_partitioning: false,
    };
    let blob1_node_idx = artifacts.add_node(blob1_node);
    // Prod 1
    let prod1_node = ArtifactDescription {
        id: None,
        name: Some("Test Producer 1".into()),
        dtype: "NegateBlobProducer".into(),
        self_partitioning: false,
    };
    let prod1_node_idx = artifacts.add_node(prod1_node);
    artifacts.add_edge(
        blob1_node_idx,
        prod1_node_idx,
        ArtifactRelation::ProducedFrom("input".into())).unwrap();
    // Blob 2
    let blob2_node = ArtifactDescription {
        id: None,
        name: Some("Test Blob 2".into()),
        dtype: "Blob".into(),
        self_partitioning: false,
    };
    let blob2_node_idx = artifacts.add_node(blob2_node);
    artifacts.add_edge(
        prod1_node_idx,
        blob2_node_idx,
        ArtifactRelation::ProducedFrom("output".into())).unwrap();
    // Prod 2
    let prod2_node = ArtifactDescription {
        id: None,
        name: Some("Test Producer 2".into()),
        dtype: "NegateBlobProducer".into(),
        self_partitioning: false,
    };
    let prod2_node_idx = artifacts.add_node(prod2_node);
    artifacts.add_edge(
        blob2_node_idx,
        prod2_node_idx,
        ArtifactRelation::ProducedFrom("input".into())).unwrap();
    // Blob 3
    let blob3_node = ArtifactDescription {
        id: None,
        name: Some("Test Blob 3".into()),
        dtype: "Blob".into(),
        self_partitioning: false,
    };
    let blob3_node_idx = artifacts.add_node(blob3_node);
    artifacts.add_edge(
        prod2_node_idx,
        blob3_node_idx,
        ArtifactRelation::ProducedFrom("output".into())).unwrap();

    let mut ag_desc = ArtifactGraphDescription {
        artifacts,
    };

    let part_idx = partitioning.map(|part_desc| ag_desc.add_uniform_partitioning(part_desc));
    let up_idx = ag_desc.add_unary_partitioning();

    // Do not set up partitioning for these.
    // Tracking Branch Producer
    let tbp_node = ArtifactDescription {
        id: None,
        name: Some("TBP".into()),
        dtype: "TrackingBranchProducer".into(),
        self_partitioning: false,
    };
    let tbp_node_idx = ag_desc.artifacts.add_node(tbp_node);
    let tracked_arts = [blob1_node_idx, blob2_node_idx, blob3_node_idx];
    for &tracked_idx in &tracked_arts {
        ag_desc.artifacts.add_edge(
            tracked_idx,
            tbp_node_idx,
            ArtifactRelation::ProducedFrom("tracked".into())).unwrap();
    }
    // Tracking ref
    let ref_node = ArtifactDescription {
        id: None,
        name: Some("blobs".into()),
        dtype: "Ref".into(),
        self_partitioning: false,
    };
    let ref_node_idx = ag_desc.artifacts.add_node(ref_node);
    ag_desc.artifacts.add_edge(
        tbp_node_idx,
        ref_node_idx,
        ArtifactRelation::ProducedFrom("output".into())).unwrap();
    for &tracked_idx in &tracked_arts {
        ag_desc.artifacts.add_edge(
            tracked_idx,
            ref_node_idx,
            ArtifactRelation::DtypeDepends(DatatypeRelation {
                name: "ref".into()
            })).unwrap();
    }

    let (ag, idx_map) = ArtifactGraph::from_description(&ag_desc, dtypes_registry);

    let mut idxs = hashmap![
        "UP"                => idx_map[&up_idx],
        "Test Blob 1"       => idx_map[&blob1_node_idx],
        "Test Producer 1"   => idx_map[&prod1_node_idx],
        "Test Blob 2"       => idx_map[&blob2_node_idx],
        "Test Producer 2"   => idx_map[&prod2_node_idx],
        "Test Blob 3"       => idx_map[&blob3_node_idx],
        "TBP"               => idx_map[&tbp_node_idx],
        "blobs"             => idx_map[&ref_node_idx],
    ];
    if let Some(ref idx) = part_idx {
        idxs.insert("Partitioning", idx_map[idx]);
    }

    (ag, idxs)
}

fn test_create_get_artifact_graph(backend: Backend) {

    let dtypes_registry = crate::datatype::testing::init_dtypes_registry::<TestDatatypes>();
    let repo = crate::repo::testing::init_repo(backend, &dtypes_registry);

    let (ag, _) = simple_blob_prod_ag_fixture(&dtypes_registry, None);

    let mut model_ctrl = ArtifactGraphDtype::store(&repo);

    model_ctrl.create_artifact_graph(&dtypes_registry, &repo, &ag).unwrap();

    let ag2 = model_ctrl.get_artifact_graph(&dtypes_registry, &repo, &ag.id).unwrap();
    assert!(ag2.verify_hash());
    assert_eq!(ag.id.hash, ag2.id.hash);
}

fn test_create_get_version_graph(backend: Backend) {

    let dtypes_registry = crate::datatype::testing::init_dtypes_registry::<TestDatatypes>();
    let repo = crate::repo::testing::init_repo(backend, &dtypes_registry);

    let (ag, idxs) = simple_blob_prod_ag_fixture(&dtypes_registry, None);

    let mut model_ctrl = ArtifactGraphDtype::store(&repo);

    model_ctrl.create_artifact_graph(&dtypes_registry, &repo, &ag).unwrap();

    let mut ver_graph = VersionGraph::new_from_source_artifacts(&ag);

    // TODO: most of this test should eventually fail because no versions
    // are being committed.
    for node_idx in ver_graph.versions.graph().node_indices() {
        model_ctrl.create_staging_version(
            &repo,
            &ver_graph,
            node_idx.clone()).unwrap();
    }

    let up_art_idx = idxs["UP"];
    let up_idx = ver_graph.artifact_versions(&ag[up_art_idx])[0];
    // Create meaningless unary partitioning hunk (necessary for getting its
    // composition map).
    for part_id in crate::datatype::partitioning::UnaryPartitioningState.get_partition_ids() {
        let hunk = Hunk {
            id: 0.into(),
            version: &ver_graph[up_idx],
            partition: Partition {
                partitioning: &ver_graph[up_idx],
                index: part_id,
            },
            representation: RepresentationKind::State,
            completion: PartCompletion::Complete,
            precedence: None,
        };
        model_ctrl.create_hunk(&repo, &hunk).unwrap();
    }

    let blob1_art_idx = idxs["Test Blob 1"];
    let blob1_art = &ag[blob1_art_idx];
    let blob1_ver = Version::new(blob1_art, RepresentationKind::State);
    let blob1_ver_idx = ver_graph.versions.add_node(blob1_ver);
    ver_graph.versions.add_edge(up_idx, blob1_ver_idx,
        VersionRelation::Dependence(
            &ag[ag.artifacts.find_edge(up_art_idx, blob1_art_idx).unwrap()])).unwrap();

    model_ctrl.create_staging_version(
        &repo,
        &ver_graph,
        blob1_ver_idx.clone()).unwrap();

    let (ver_part_idx, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx)
        .expect("Partitioning version missing");
    let ver_part_comp = model_ctrl.get_composition_map(
        &repo,
        &ver_graph,
        ver_part_idx,
        crate::datatype::partitioning::UnaryPartitioningState.get_partition_ids())
        .expect("UP comp map").into_iter().last().expect("UP state hunk").1;
    let ver_part_control: Box<dyn PartitioningState> =
            dtypes_registry
                .get_model_interface::<dyn PartitioningState>(&ver_partitioning.artifact.dtype.name)
                .map(|gen| gen(&repo))
                .expect("Partitioning must have controller for backend");

    let mut blob_control = BlobDatatype::store(&repo);
    let ver_blob_real = &ver_graph[blob1_ver_idx];
    let fake_blob = crate::datatype::Payload::State(vec![0, 1, 2, 3, 4, 5, 6]);
    let ver_hunks = ver_part_control
            .get_composite_interface(&repo, &ver_part_comp).unwrap()
            .get_partition_ids()
            .iter()
            .map(|partition_id| Hunk {
                id: BlobDatatype::hash_payload(&fake_blob).into(),
                version: ver_blob_real,
                partition: Partition {
                    partitioning: ver_partitioning,
                    index: partition_id.to_owned(),
                },
                representation: RepresentationKind::State,
                completion: PartCompletion::Complete,
                precedence: None,
            }).collect::<Vec<_>>();

    // Can't do this in an iterator because of borrow conflict on context?
    for hunk in &ver_hunks {
        model_ctrl.create_hunk(&repo, &hunk).unwrap();
        blob_control.write_hunk(&repo, &hunk, &fake_blob).unwrap();
    }

    for hunk in &ver_hunks {
        let blob = blob_control.read_hunk(&repo, &hunk).unwrap();
        assert_eq!(blob, fake_blob);
    }

    let (_, ver_graph2) = model_ctrl.get_version(
        &repo,
        &ag,
        &ver_blob_real.id).unwrap();

    assert!(petgraph::algo::is_isomorphic_matching(
        &ver_graph.versions.graph(),
        &ver_graph2.versions.graph(),
        |a, b| a.id == b.id,
        |_, _| true));
}

fn test_production(backend: Backend) {

    let dtypes_registry = crate::datatype::testing::init_dtypes_registry::<TestDatatypes>();
    let mut repo = crate::repo::testing::init_repo(backend, &dtypes_registry);

    let partitioning = ArtifactDescription {
        id: None,
        name: Some("Arbitrary Partitioning".into()),
        dtype: "ArbitraryPartitioning".into(),
        self_partitioning: false,
    };
    let (ag, idxs) = simple_blob_prod_ag_fixture(&dtypes_registry, Some(partitioning));

    let mut model_ctrl = ArtifactGraphDtype::store(&repo);


    model_ctrl.create_artifact_graph(&dtypes_registry, &repo, &ag).unwrap();
    model_ctrl.write_production_policies(
        &repo,
        &ag[idxs["TBP"]],
        EnumSet::from_iter(
            vec![ProductionPolicies::LeafBootstrap, ProductionPolicies::Custom]
            .into_iter())).unwrap();

    let mut ver_graph = VersionGraph::new_from_source_artifacts(&ag);

    let up_idx = ver_graph.artifact_versions(&ag[idxs["UP"]])[0];
    let part_art_idx = idxs["Partitioning"];
    let part_idx = ver_graph.versions.add_node(
        Version::new(&ag[part_art_idx], RepresentationKind::State));
    let up_part_rel = ag.artifacts.find_edge(idxs["UP"], part_art_idx).unwrap();
    ver_graph.versions.add_edge(
        up_idx,
        part_idx,
        VersionRelation::Dependence(&ag[up_part_rel])).unwrap();

    // Create arbitrary partitions.
    {
        let mut part_control = ArbitraryPartitioning::store(&repo);

        model_ctrl.create_staging_version(
            &repo,
            &ver_graph,
            part_idx).expect("TODO");
        let part_state = crate::datatype::Payload::State(
            ArbitraryPartitioningState { partition_ids: btreeset![0, 1] });
        let hunk = Hunk {
            id: ArbitraryPartitioning::hash_payload(&part_state).into(),
            version: &ver_graph[part_idx],
            partition: Partition {
                partitioning: &ver_graph[up_idx],
                index: UNARY_PARTITION_INDEX,
            },
            representation: RepresentationKind::State,
            completion: PartCompletion::Complete,
            precedence: None,
        };
        model_ctrl.create_hunk(&repo, &hunk).unwrap();
        part_control.write_hunk(&repo, &hunk, &part_state).expect("TODO");
    }
    model_ctrl.commit_version(
        &dtypes_registry,
        &repo,
        &ag,
        &mut ver_graph,
        part_idx).expect("TODO");

    let blob1_art_idx = idxs["Test Blob 1"];
    let blob1_art = &ag[blob1_art_idx];
    let blob1_ver = Version::new(blob1_art, RepresentationKind::State);
    let blob1_ver_idx = ver_graph.versions.add_node(blob1_ver);
    ver_graph.versions.add_edge(part_idx, blob1_ver_idx,
        VersionRelation::Dependence(
            &ag[ag.artifacts.find_edge(part_art_idx, blob1_art_idx).unwrap()])).unwrap();

    model_ctrl.create_staging_version(
        &repo,
        &ver_graph,
        blob1_ver_idx.clone()).unwrap();

    let ver_hash = {
        let (ver_part_idx, ver_partitioning) = ver_graph.get_partitioning(blob1_ver_idx).unwrap();
        let ver_part_comp = model_ctrl.get_composition_map(
            &repo,
            &ver_graph,
            ver_part_idx,
            crate::datatype::partitioning::UnaryPartitioningState.get_partition_ids())
            .expect("Composition map failed").into_iter().last().expect("No maps").1;
        let ver_part_control: Box<dyn PartitioningState> =
                dtypes_registry
                    .get_model_interface::<dyn PartitioningState>(&ver_partitioning.artifact.dtype.name)
                    .map(|gen| gen(&repo))
                    .expect("Partitioning must have controller for backend");

        let mut blob_control = BlobDatatype::store(&repo);
        let ver_blob_real = &ver_graph[blob1_ver_idx];
        let fake_blob = crate::datatype::Payload::State(vec![0, 1, 2, 3, 4, 5, 6]);
        let ver_hunks = ver_part_control
                .get_composite_interface(&repo, &ver_part_comp).unwrap()
                // Note that this is in ascending order, so version hash
                // is correct.
                .get_partition_ids()
                .iter()
                .map(|partition_id| Hunk {
                    id: BlobDatatype::hash_payload(&fake_blob).into(),
                    version: ver_blob_real,
                    partition: Partition {
                        partitioning: ver_partitioning,
                        index: partition_id.to_owned(),
                    },
                    representation: RepresentationKind::State,
                    completion: PartCompletion::Complete,
                    precedence: None,
                }).collect::<Vec<_>>();
        let ver_hash = ver_hunks.iter()
            .fold(
                DefaultHasher::new(),
                |mut s, hunk| {hunk.id.hash.hash(&mut s); s})
            .finish();

        // Can't do this in an iterator because of borrow conflict on context?
        for hunk in &ver_hunks {
            model_ctrl.create_hunk(&repo, &hunk).unwrap();
            blob_control.write_hunk(&repo, &hunk, &fake_blob).unwrap();
        }

        ver_hash
    };

    ver_graph[blob1_ver_idx].id.hash = ver_hash;

    model_ctrl.commit_version(
        &dtypes_registry,
        &repo,
        &ag,
        &mut ver_graph,
        blob1_ver_idx).expect("Commit blob failed");

    let vg2 = model_ctrl.get_version_graph(&repo, &ag).unwrap();

    println!("{:?}", petgraph::dot::Dot::new(&vg2.versions.graph()));

    let blob1_vg2_idxs = vg2.artifact_versions(&ag[idxs["Test Blob 1"]]);
    let blob2_vg2_idxs = vg2.artifact_versions(&ag[idxs["Test Blob 2"]]);
    let blob3_vg2_idxs = vg2.artifact_versions(&ag[idxs["Test Blob 3"]]);

    assert_eq!(blob2_vg2_idxs.len(), 1);
    assert_eq!(blob3_vg2_idxs.len(), 1);

    assert_eq!(
        vg2[blob1_vg2_idxs[0]].id.hash,
        vg2[blob3_vg2_idxs[0]].id.hash,
        "Version hashes for original and double-negated blob should match.",
        );

    // Test delta state updates.
    let blob1_ver2 = Version::new(blob1_art, RepresentationKind::Delta);
    let blob1_ver2_idx = ver_graph.versions.add_node(blob1_ver2);
    ver_graph.versions.add_edge(part_idx, blob1_ver2_idx,
        VersionRelation::Dependence(
            &ag[ag.artifacts.find_edge(part_art_idx, blob1_art_idx).unwrap()])).unwrap();
    ver_graph.versions.add_edge(blob1_ver_idx, blob1_ver2_idx, VersionRelation::Parent).unwrap();

    model_ctrl.create_staging_version(
        &repo,
        &ver_graph,
        blob1_ver2_idx.clone()).unwrap();

    let ver2_hash = {
        let (ver_part_idx, ver_partitioning) = ver_graph.get_partitioning(blob1_ver2_idx).unwrap();
        let ver_part_comp = model_ctrl.get_composition_map(
            &repo,
            &ver_graph,
            ver_part_idx,
            crate::datatype::partitioning::UnaryPartitioningState.get_partition_ids())
            .unwrap().into_iter().last().unwrap().1;
        let ver_part_control: Box<dyn PartitioningState> =
                dtypes_registry
                    .get_model_interface::<dyn PartitioningState>(&ver_partitioning.artifact.dtype.name)
                    .map(|gen| gen(&repo))
                    .expect("Partitioning must have controller for backend");

        let mut blob_control = BlobDatatype::store(&repo);
        let ver_blob_real = &ver_graph[blob1_ver2_idx];
        let fake_blob = crate::datatype::Payload::Delta((vec![1, 6], vec![7, 8]));
        let ver_hunks = ver_part_control
                .get_composite_interface(&repo, &ver_part_comp).unwrap()
                // Note that this is in ascending order, so version hash
                // is correct.
                .get_partition_ids()
                .iter()
                .take(1)
                .map(|partition_id| Hunk {
                    id: BlobDatatype::hash_payload(&fake_blob).into(),
                    version: ver_blob_real,
                    partition: Partition {
                        partitioning: ver_partitioning,
                        index: partition_id.to_owned(),
                    },
                    representation: RepresentationKind::Delta,
                    completion: PartCompletion::Complete,
                    precedence: None,
                }).collect::<Vec<_>>();
        let ver_hash = ver_hunks.iter()
            .fold(
                DefaultHasher::new(),
                |mut s, hunk| {hunk.id.hash.hash(&mut s); s})
            .finish();

        for hunk in &ver_hunks {
            model_ctrl.create_hunk(&repo, &hunk).unwrap();
            blob_control.write_hunk(&repo, &hunk, &fake_blob).unwrap();
        }

        ver_hash
    };

    ver_graph[blob1_ver2_idx].id.hash = ver2_hash;

    model_ctrl.commit_version(
        &dtypes_registry,
        &repo,
        &ag,
        &mut ver_graph,
        blob1_ver2_idx).expect("Commit blob delta failed");

    let vg3 = model_ctrl.get_version_graph(&repo, &ag).unwrap();

    println!("{:?}", petgraph::dot::Dot::new(&vg3.versions.graph()));

    let blob1_vg3_idxs = vg3.artifact_versions(&ag[idxs["Test Blob 1"]]);
    let blob2_vg3_idxs = vg3.artifact_versions(&ag[idxs["Test Blob 2"]]);
    let blob3_vg3_idxs = vg3.artifact_versions(&ag[idxs["Test Blob 3"]]);

    assert_eq!(blob2_vg3_idxs.len(), 2);
    assert_eq!(blob3_vg3_idxs.len(), 2);

    assert_eq!(
        vg3[blob1_vg3_idxs[1]].id.hash,
        vg3[blob3_vg3_idxs[1]].id.hash,
        "Version hashes for original and double-negated blob should match.",
        );

    {
        let part_control = crate::datatype::partitioning::arbitrary::ArbitraryPartitioning::store(&repo);
        let (ver_part_idx, _) = ver_graph.get_partitioning(blob1_ver_idx).unwrap();
        let ver_part_comp = model_ctrl.get_composition_map(
            &repo,
            &ver_graph,
            ver_part_idx,
            crate::datatype::partitioning::UnaryPartitioningState.get_partition_ids())
            .unwrap().into_iter().last().unwrap().1;
        let part_ids = part_control
                .get_composite_interface(&repo, &ver_part_comp).unwrap()
                .get_partition_ids();

        let map1 = model_ctrl.get_composition_map(
            &repo,
            &vg3,
            blob1_vg3_idxs[1],
            part_ids.clone(),
        ).unwrap();
        let map3 = model_ctrl.get_composition_map(
            &repo,
            &vg3,
            blob3_vg3_idxs[1],
            part_ids,
        ).unwrap();
        let blob_control = crate::datatype::blob::BlobDatatype::store(&repo);

        for (p_id, blob1_comp) in &map1 {
            let blob3_comp = &map3[p_id];

            let blob1_state = blob_control.get_composite_state(&repo, blob1_comp).unwrap();
            let blob3_state = blob_control.get_composite_state(&repo, blob3_comp).unwrap();

            assert_eq!(blob1_state, blob3_state, "Blob states do not match");
        }
    }

    {
        use std::str::FromStr;
        use crate::datatype::reference::VersionSpecifier;
        use crate::datatype::reference::Storage as RefStorage;
        let ref_control = crate::datatype::reference::Ref::store(&repo);
        assert_eq!(
            vg3[blob3_vg3_idxs[1]].id,
            ref_control.get_version_id(
                &mut repo,
                &VersionSpecifier::from_str("blobs/master/Test Blob 3").unwrap()).unwrap(),
            "Tracking branch has wrong version for Blob 3.");
    }
}

macro_rules! backend_test_suite {
    ( $backend_name:ident, $backend:path ) => {
        mod $backend_name {
            use super::*;

            #[test]
            fn test_create_get_artifact_graph() {
                super::test_create_get_artifact_graph($backend);
            }

            #[test]
            fn test_create_get_version_graph() {
                super::test_create_get_version_graph($backend);
            }

            #[test]
            fn test_production() {
                super::test_production($backend);
            }
        }
    }
}

#[cfg(feature="backend-postgres")]
backend_test_suite!(postgres, Backend::Postgres);
