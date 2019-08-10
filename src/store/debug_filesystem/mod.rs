pub use heraclitus_core::store::debug_filesystem::*;

use std::path::{
    Path,
    PathBuf,
};

use serde::{
    de::DeserializeOwned,
    Serialize,
};

use crate::{
    Artifact,
    Error,
    Hunk,
    Version,
};


pub mod datatype;


pub fn hunk_path(repo: &DebugFilesystemRepository, hunk: &Hunk) -> PathBuf {
    let mut path = version_path(repo, hunk.version);
    path.push(hunk.id.uuid.to_string());

    path
}

pub fn version_path(repo: &DebugFilesystemRepository, version: &Version) -> PathBuf {
    let mut path = artifact_path(repo, version.artifact);
    path.push(version.id.uuid.to_string());

    path
}

pub fn artifact_path(repo: &DebugFilesystemRepository, artifact: &Artifact) -> PathBuf {
    let mut path = repo.path().clone();
    path.push(artifact.id.uuid.to_string());

    path
}


pub fn write_json<T: Serialize, P: AsRef<Path>>(path: P, object: &T) -> Result<(), Error> {
    std::fs::create_dir_all(path.as_ref().parent().unwrap())?;
    let file = std::fs::File::create(path)?;
    let writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(writer, object)
        .map_err(|e| heraclitus::Error::Store(e.to_string()))?;
    Ok(())
}

pub fn read_json<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> Result<T, Error> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let payload = serde_json::from_reader(reader)
        .map_err(|e| heraclitus::Error::Store(e.to_string()))?;
    Ok(payload)
}

pub fn read_optional_json<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> Result<Option<T>, Error> {
    if path.as_ref().exists() {
        Ok(Some(read_json(path)?))
    } else {
        Ok(None)
    }
}
