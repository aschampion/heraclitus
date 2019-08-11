use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{
    BufReader,
    BufWriter,
    Seek,
};
use std::path::Path;
use std::str::FromStr;

use heraclitus_core::{
    uuid,
};
use uuid::Uuid;
use serde_json::{
    json,
    Value,
};
use walkdir::WalkDir;

use crate::{
    Artifact,
    Error,
    Version,
};
use crate::datatype::reference::{
    BranchRevisionTip,
    Storage,
    RefBackend,
    RevisionPath,
    UuidSpecifier,
    VersionSpecifier,
};
use crate::store::debug_filesystem::{
    artifact_path,
    DebugFilesystemRepository,
    read_optional_json,
    version_path,
};

use super::DebugFilesystemMetaController;


const MESSAGE_FILE: &'static str = "message.json";
const REVISION_PATH_FILE: &'static str = "revision_path.json";


impl DebugFilesystemMetaController for RefBackend<DebugFilesystemRepository> {}

// From: https://github.com/serde-rs/json/issues/377
// TODO: Could be much better.
fn merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (&mut Value::Object(ref mut a), &Value::Object(ref b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

impl Storage for RefBackend<DebugFilesystemRepository> {
    fn get_branch_revision_tips(
        &self,
        repo: &crate::repo::Repository,
        artifact: &Artifact,
    ) -> Result<HashMap<BranchRevisionTip, Uuid>, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = artifact_path(rc, artifact);
        path.push(REVISION_PATH_FILE);
        let path = Path::new(&path);

        let mut map = HashMap::new();

        if !path.exists() {
            return Ok(map);
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let paths: HashMap<String, HashMap<String, Uuid>> = serde_json::from_reader(reader)
            .map_err(|e| Error::Store(e.to_string()))?;
        for (branch, path) in paths.into_iter() {
            for (revision_name, version_id) in path {
                if revision_name != "HEAD" { continue; }

                let br_tip = BranchRevisionTip {
                    name: branch.clone(),
                    revision: RevisionPath::from_str(&revision_name).expect("TODO"),
                };
                map.insert(br_tip, version_id);
            }
        }
        Ok(map)
    }

    fn set_branch_revision_tips(
        &mut self,
        repo: &crate::repo::Repository,
        artifact: &Artifact,
        tip_versions: &HashMap<BranchRevisionTip, Uuid>,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = artifact_path(rc, artifact);
        path.push(REVISION_PATH_FILE);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut reader = BufReader::new(file);

        let mut map: HashMap<String, HashMap<String, Uuid>> =
            serde_json::from_reader(&mut reader).unwrap_or_else(|_| HashMap::new());
        tip_versions
            .iter()
            .for_each(|(brt, uuid)| {
                let map_revision = map.entry(brt.name.clone()).or_insert_with(HashMap::new);
                map_revision.insert(brt.revision.to_string(), *uuid);
            });

        let mut file = reader.into_inner();
        file.seek(std::io::SeekFrom::Start(0))?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &map)
            .map_err(|e| Error::Store(e.to_string()))?;

        Ok(())
    }

    fn write_message(
        &mut self,
        repo: &crate::repo::Repository,
        version: &Version,
        message: &Option<String>,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        match *message {
            Some(ref t) => {
                let mut path = version_path(rc, version);
                path.push(MESSAGE_FILE);
                let file = File::create(path)?;
                let writer = BufWriter::new(file);
                serde_json::to_writer_pretty(writer, t)
                    .map_err(|e| Error::Store(e.to_string()))?;
                Ok(())
            },
            None => Ok(())
        }
    }

    fn read_message(
        &self,
        repo: &crate::repo::Repository,
        version: &Version,
    ) -> Result<Option<String>, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = version_path(rc, version);
        path.push(MESSAGE_FILE);
        read_optional_json(path)
    }

    fn create_branch(
        &mut self,
        repo: &crate::repo::Repository,
        ref_version: &Version,
        name: &str,
    ) -> Result<(), Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let mut path = artifact_path(rc, ref_version.artifact);
        path.push(REVISION_PATH_FILE);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut reader = BufReader::new(file);

        let existing = serde_json::from_reader(&mut reader).unwrap_or_else(|_| json!({}));
        let mut file = reader.into_inner();
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut merged = existing.clone();

        let new: Value = json!({name: {"HEAD": ref_version.id.uuid}});

        merge(&mut merged, &new);

        if new != existing {
            let writer = BufWriter::new(file);
            serde_json::to_writer_pretty(writer, &merged)
                .map_err(|e| Error::Store(e.to_string()))?;
        }

        Ok(())
    }

    fn get_version_uuid(
        &self,
        repo: &crate::repo::Repository,
        specifier: &VersionSpecifier,
    ) -> Result<Uuid, Error> {
        let rc: &DebugFilesystemRepository = repo.borrow();

        let uuid = match *specifier {
            VersionSpecifier::Uuid(ref us) => {
                let mut version_dirs = WalkDir::new(rc.path())
                    .min_depth(2).max_depth(2)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().is_dir())
                    .map(|e| e.file_name().to_string_lossy().into_owned());
                Uuid::parse_str(&match *us {
                    UuidSpecifier::Complete(ref uuid) => {
                        version_dirs.find(|e| e == &uuid.to_string()).unwrap()
                    },
                    UuidSpecifier::Partial(ref prefix) => {
                        version_dirs.find(|e| e.starts_with(prefix)).unwrap()
                    }
                }).unwrap()
            },
            VersionSpecifier::BranchArtifact {
                ref_artifact: ref ref_art,
                branch_revision: ref br,
                artifact: ref art
            } => {
                assert_eq!(br.revision.offset, 0, "Non-tip revisions not yet supported"); // TODO

                let br_rev_path_name = match br.revision.path {
                    RevisionPath::Head => "HEAD",
                    RevisionPath::Named(ref name) => name,
                };


                unimplemented!()
            },
        };

        Ok(uuid)
    }
}
