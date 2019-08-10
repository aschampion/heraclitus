use std::collections::{
    BTreeSet,
};
use std::fs::File;
use std::path::PathBuf;
use std::io::{
    BufReader,
};

use heraclitus_core::{
    petgraph,
    uuid,
};
use enumset::EnumSet;
use petgraph::visit::EdgeRef;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{
    Artifact,
    ArtifactGraph,
    ArtifactGraphIndex,
    Error,
    Hunk,
    HunkUuidSpec,
    IdentifiableGraph,
    Identity,
    PartCompletion,
    Partition,
    PartitionIndex,
    repo::Repository,
    RepresentationKind,
    Version,
    VersionGraph,
    VersionGraphIndex,
    VersionRelation,
    VersionStatus,
};
use crate::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
    InterfaceController,
};
use crate::datatype::artifact_graph::{
    ArtifactGraphDtypeBackend,
    production::{
        ProductionPolicies,
        ProductionPolicyRequirements,
        ProductionStrategySpecs,
    },
    Storage,
};
use crate::datatype::interface::{
    CustomProductionPolicyController,
    ProducerController,
};
use crate::default_debug_filesystem_store_backend;
use crate::store::debug_filesystem::{
    artifact_path,
    DebugFilesystemRepository,
    hunk_path,
    read_json,
    read_optional_json,
    version_path,
    write_json,
};


const HUNK_FILE: &'static str = "hunk.json";
const ORIGIN_FILE: &'static str = "origin.json";
const PRODUCTION_POLICIES_FILE: &'static str = "production_policies.json";
const PRODUCTION_SPECS_FILE: &'static str = "production_specs.json";
const VERSION_FILE: &'static str = "version.json";
const VERSION_DEPENDENCIES_FILE: &'static str = "version_dependencies.json";
const VERSION_PARENTS_FILE: &'static str = "version_parents.json";


default_debug_filesystem_store_backend!(ArtifactGraphDtypeBackend);

// The filesystem is structured as:
//
// ```
// /
// origin.json
// [Artifact UUID]/
//      production_policies.json
//      [Version UUID]/
//          production_specs.json
//          version.json
//          version_depdencies.json
//          version_parents.json
//          [Hunk UUID]/
//              hunk.json
//              payload.json (from datatype's storage)
// ```

impl ArtifactGraphDtypeBackend<DebugFilesystemRepository> {
    fn find_version_by_uuid(
        &self,
        rc: &DebugFilesystemRepository,
        version_uuid: Uuid,
    ) -> Result<(Uuid, PathBuf), Error> {
        let version_dir = WalkDir::new(rc.path())
            .min_depth(2).max_depth(2)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_dir())
            .find(|e| e.file_name().to_string_lossy() == version_uuid.to_string().as_ref()).unwrap();
        let artifact_dir = version_dir.path().parent().unwrap();
        let artifact_uuid = Uuid::parse_str(&artifact_dir.file_name().unwrap().to_string_lossy()).unwrap();

        Ok((artifact_uuid, version_dir.into_path()))
    }

    fn load_version_by_uuid<'a, 'b>(
        &self,
        rc: &DebugFilesystemRepository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        version_uuid: Uuid,
    ) -> Result<VersionGraphIndex, Error> {
        let (artifact_uuid, mut path) = self.find_version_by_uuid(rc, version_uuid)?;
        let (_, artifact) = art_graph.get_by_uuid(&artifact_uuid).unwrap();

        path.push(VERSION_FILE);
        let partial: VersionPartial = read_json(path)?;
        let version = partial.to_version(artifact);

        let v_idx = ver_graph.emplace(&version.id.clone(), || version);

        Ok(v_idx)
    }

    fn get_version_relations<'a, 'b>(
        &self,
        rc: &DebugFilesystemRepository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idxs: &[VersionGraphIndex],
        ancestry_direction: Option<petgraph::Direction>,
        dependence_direction: Option<petgraph::Direction>,
    ) -> Result<(), Error> {
        // Collect the set of artifacts for v_idxs.

        // Ancestry:
        // If the direction is incoming or None.
        if ancestry_direction.is_none() ||
           ancestry_direction == Some(petgraph::Direction::Incoming) {
            // For the set of artifacts, get the relations from their directories.
            for v_idx in v_idxs {
                let mut path = version_path(rc, &ver_graph[*v_idx]);
                path.push(VERSION_PARENTS_FILE);
                let parent_uuids: Vec<Uuid> = read_json(path)?;

                for uuid in parent_uuids {
                    let (parent_idx, _) = ver_graph.get_by_uuid(&uuid)
                        .expect("Relation with version not in graph");
                    ver_graph.versions.add_edge(parent_idx, *v_idx, VersionRelation::Parent)?;
                }
            }
        }
        // If the direction is outgoing or None.
        if ancestry_direction.is_none() ||
           ancestry_direction == Some(petgraph::Direction::Outgoing) {
            // For all other versions of each artifact in v_idxs that are not
            // the version in v_idx.
            // Get the relations from their directories, and add any that point
            // to v_idxs.
            unimplemented!()
        }

        // Dependence:
        // If the direction is incoming or None.
        if dependence_direction.is_none() ||
           dependence_direction == Some(petgraph::Direction::Incoming) {
            // For the set of artifacts, get the relations from their directories.
            for v_idx in v_idxs {
                let mut path = version_path(rc, &ver_graph[*v_idx]);
                path.push(VERSION_DEPENDENCIES_FILE);
                let dep_uuids: Vec<Uuid> = read_json(path)?;

                for uuid in dep_uuids {
                    let dep_idx = self.load_version_by_uuid(rc, art_graph, ver_graph, uuid)?;
                    let art_idx = art_graph.get_by_id(&ver_graph[*v_idx].artifact.id).unwrap().0;
                    let dep_art_idx = art_graph.get_by_id(&ver_graph[dep_idx].artifact.id).unwrap().0;
                    let art_rel_idx = art_graph.artifacts.find_edge(dep_art_idx, art_idx)
                        .expect("Version graph references unknown artifact relation");
                    let art_rel = art_graph.artifacts.edge_weight(art_rel_idx).expect("Graph is malformed");
                    let edge = VersionRelation::Dependence(art_rel);
                    ver_graph.versions.add_edge(dep_idx, *v_idx, edge)?;
                }
            }
        }
        // If the direction is outgoing or None.
        if dependence_direction.is_none() ||
           dependence_direction == Some(petgraph::Direction::Outgoing) {
            // Collect the set of artifacts outgoing from the v_idxs' artifacts.
            // let arts = v_idxs.iter()
            //     .map(|v| ver_graph[*v].artifact);
            // For this new set of artifacts, get the relatinos from their directories.
            unimplemented!()
        }

        Ok(())
    }
}

impl Storage for ArtifactGraphDtypeBackend<DebugFilesystemRepository> {

    fn read_origin_uuids(
        &self,
        repo: &Repository,
    ) -> Result<Option<HunkUuidSpec>, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = rc.path();
        path.push(ORIGIN_FILE);
        read_optional_json(path)
    }

    fn bootstrap_origin(
        &mut self,
        repo: &Repository,
        hunk: &Hunk,
        ver_graph: &VersionGraph,
        art_graph: &ArtifactGraph,
    ) -> Result<(), Error> {
        use crate::datatype::Storage;

        self.create_hunk(repo, hunk)?;

        let rc: &DebugFilesystemRepository = repo.borrow();

        let v_idx = ver_graph.get_by_id(&hunk.version.id).unwrap().0;
        self.create_staging_version(repo, ver_graph, v_idx)?;

        self.create_hunk(repo, hunk)?;

        let mut path = rc.path();
        path.push(ORIGIN_FILE);
        write_json(path, &hunk.uuid_spec())?;

        let payload = crate::datatype::Payload::State(art_graph.as_description());
        self.write_hunk(repo, hunk, &payload)
    }

    fn tie_off_origin(
        &self,
        _repo: &Repository,
        _ver_graph: &VersionGraph,
        _origin_v_idx: VersionGraphIndex,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn create_staging_version(
        &mut self,
        repo: &Repository,
        ver_graph: &VersionGraph,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let version = &ver_graph[v_idx];
        let mut path = version_path(rc, version);
        path.push(VERSION_FILE);
        let partial = VersionPartial::from_version(version);
        write_json(path, &partial)?;

        let parent_uuids: Vec<Uuid> = ver_graph.get_parents(v_idx)
            .into_iter()
            .map(|p_idx| ver_graph[p_idx].id.uuid.clone())
            .collect();

        let mut path = version_path(rc, version);
        path.push(VERSION_PARENTS_FILE);
        write_json(path, &parent_uuids)?;

        let dependency_uuids: Vec<Uuid> = ver_graph.versions.graph()
            .edges_directed(v_idx, petgraph::Direction::Incoming)
            .filter(|e| match e.weight() {
                VersionRelation::Dependence(_) => true,
                _ => false,
            })
            .map(|e| ver_graph[e.source()].id.uuid.clone())
            .collect();

        let mut path = version_path(rc, version);
        path.push(VERSION_DEPENDENCIES_FILE);
        write_json(path, &dependency_uuids)
    }

    fn commit_version<'a, 'b, T: DatatypeEnum>(
        &mut self,
        // TODO: dirty hack to work around mut/immut refs to context. Either
        // look at other Rust workarounds, or better yet finally design a way
        // to get model directly from datatypes.
        dtypes_registry: &DatatypesRegistry<T>,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
    ) -> Result<(), Error>
            where
                <T as DatatypeEnum>::InterfaceControllerType :
                    InterfaceController<ProducerController> +
                    InterfaceController<CustomProductionPolicyController> {

        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut version = &mut ver_graph[v_idx];
        let mut path = version_path(rc, version);
        path.push(VERSION_FILE);
        version.status = VersionStatus::Committed;
        let partial = VersionPartial::from_version(version);
        write_json(path, &partial)?;

        self.cascade_notify_producers(
            dtypes_registry,
            repo,
            art_graph,
            ver_graph,
            v_idx)?;

        Ok(())
    }

    fn fulfill_policy_requirements<'a, 'b>(
        &self,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        ver_graph: &mut VersionGraph<'a, 'b>,
        v_idx: VersionGraphIndex,
        p_art_idx: ArtifactGraphIndex,
        requirements: &ProductionPolicyRequirements,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn get_version<'a, 'b>(
        &self,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
        id: &Identity,
    ) -> Result<(VersionGraphIndex, VersionGraph<'a, 'b>), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut ver_graph = VersionGraph::new();
        let v_idx = self.load_version_by_uuid(rc, art_graph, &mut ver_graph, id.uuid)?;

        self.get_version_relations(
            rc,
            art_graph,
            &mut ver_graph,
            &[v_idx],
            None,
            None)?;

        Ok((v_idx, ver_graph))
    }

    fn get_version_graph<'a, 'b>(
        &self,
        repo: &Repository,
        art_graph: &'b ArtifactGraph<'a>,
    ) -> Result<VersionGraph<'a, 'b>, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let art_uuids = art_graph.artifacts.raw_nodes().iter()
            .map(|n| n.weight.id.uuid);

        let ver_uuids = art_uuids
            .map(|a| {
                let mut path = rc.path();
                path.push(a.to_string());
                WalkDir::new(path)
                    .min_depth(1).max_depth(1)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().is_dir())
                    .map(|e| Uuid::parse_str(&e.path().file_name().unwrap().to_string_lossy()).unwrap())
                    .collect::<Vec<Uuid>>()
            })
            .flatten();

        let mut ver_graph = VersionGraph::new();
        // TODO: since art is known, can be more efficient here by not finding
        // the version UUID again.
        let v_idxs = ver_uuids
            .map(|v| self.load_version_by_uuid(rc, art_graph, &mut ver_graph, v))
            .collect::<Result<Vec<VersionGraphIndex>, Error>>()?;

        self.get_version_relations(
            rc,
            art_graph,
            &mut ver_graph,
            &v_idxs,
            // Can use incoming edges only since all nodes are fetched.
            Some(petgraph::Direction::Incoming),
            Some(petgraph::Direction::Incoming))?;

        Ok(ver_graph)
    }

    fn create_hunk(
        &mut self,
        repo: &Repository,
        hunk: &Hunk,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = hunk_path(rc, hunk);
        path.push(HUNK_FILE);
        write_json(path, hunk)
    }

    fn get_hunks<'a, 'b, 'c, 'd>(
        &self,
        repo: &Repository,
        version: &'d Version<'a, 'b>,
        partitioning: &'c Version<'a, 'b>,
        partitions: Option<&BTreeSet<PartitionIndex>>,
    ) -> Result<Vec<Hunk<'a, 'b, 'c, 'd>>, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let ver_path = version_path(rc, version);
        let hunks = WalkDir::new(ver_path)
            .min_depth(1).max_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_dir())
            .map(|e| {
                let mut path = e.into_path();
                path.push(HUNK_FILE);
                File::open(path).expect("TODO")
            })
            .filter_map(|f| {
                let reader = BufReader::new(f);
                let hunk: HunkPartial = serde_json::from_reader(reader)
                    .map_err(|e| Error::Store(e.to_string()))
                    .expect("TODO");

                if partitions.map(|p| p.contains(&hunk.partition.index)).unwrap_or(true) {
                    Some(hunk.to_hunk(version, partitioning))
                } else {
                    None
                }
            })
            .collect();

        Ok(hunks)
    }

    fn write_production_policies<'a>(
        &mut self,
        repo: &Repository,
        artifact: &Artifact<'a>,
        policies: EnumSet<ProductionPolicies>,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = artifact_path(rc, artifact);
        path.push(PRODUCTION_POLICIES_FILE);
        write_json(path, &policies)
    }

    fn get_production_policies<'a>(
        &self,
        repo: &Repository,
        artifact: &Artifact<'a>,
    ) -> Result<Option<EnumSet<ProductionPolicies>>, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = artifact_path(rc, artifact);
        path.push(PRODUCTION_POLICIES_FILE);
        read_optional_json(path)
    }

    fn write_production_specs<'a, 'b>(
        &mut self,
        repo: &Repository,
        version: &Version<'a, 'b>,
        specs: ProductionStrategySpecs,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = version_path(rc, version);
        path.push(PRODUCTION_SPECS_FILE);
        write_json(path, &specs)
    }

    fn get_production_specs<'a, 'b>(
        &self,
        repo: &Repository,
        version: &Version<'a, 'b>,
    ) -> Result<ProductionStrategySpecs, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = version_path(rc, version);
        path.push(PRODUCTION_SPECS_FILE);
        read_json(path)
    }
}

#[derive(Deserialize, Serialize)]
struct VersionPartial {
    id: Identity,
    status: VersionStatus,
    representation: RepresentationKind,
}

impl VersionPartial {
    fn from_version(version: &Version) -> Self {
        VersionPartial {
            id: version.id,
            status: version.status.clone(),
            representation: version.representation,
        }
    }

    fn to_version<'a: 'b, 'b>(self, artifact: &'b Artifact<'a>) -> Version<'a, 'b> {
        Version {
            id: self.id,
            artifact,
            status: self.status,
            representation: self.representation,
        }
    }
}

#[derive(Deserialize)]
struct PartitionPartial {
    index: PartitionIndex,
}

#[derive(Deserialize)]
struct HunkPartial {
    id: Identity,
    partition: PartitionPartial,
    representation: RepresentationKind,
    completion: PartCompletion,
    precedence: Option<Uuid>,
}

impl HunkPartial {
    fn to_hunk<'a, 'b, 'c, 'd>(
        self,
        version: &'d Version<'a, 'b>,
        partitioning: &'c Version<'a, 'b>,
    ) -> Hunk<'a, 'b, 'c, 'd> {
        Hunk {
            id: self.id,
            version: version,
            partition: Partition {
                partitioning,
                index: self.partition.index,
            },
            representation: self.representation,
            completion: self.completion,
            precedence: self.precedence,
        }
    }
}
