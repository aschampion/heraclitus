use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::convert::From;
use std::io::BufWriter;
use std::fmt::Debug;
use std::fs::File;
use std::option::Option;
use std::path::PathBuf;

use failure::Fail;
use url::Url;

use crate::{
    Error,
    RepositoryLocation,
};
use crate::datatype::{
    DatatypeEnum,
    DatatypesRegistry,
};
use crate::repo::{
    RepoController,
    Repository,
};

use self::datatype::DebugFilesystemMetaController;

pub mod datatype;


impl Borrow<DebugFilesystemRepository> for Repository {
    fn borrow(&self) -> &DebugFilesystemRepository {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match *self {
            Repository::DebugFilesystem(ref rc) => rc,
            _ => panic!("Attempt to borrow DebugFilesystemStore from a non-DebugFilesystem repo")
        }
    }
}

impl BorrowMut<DebugFilesystemRepository> for Repository {
    fn borrow_mut(&mut self) -> &mut DebugFilesystemRepository {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match *self {
            Repository::DebugFilesystem(ref mut rc) => rc,
            _ => panic!("Attempt to borrow DebugFilesystemStore from a non-DebugFilesystem repo")
        }
    }
}


pub struct DebugFilesystemRepository {
    url: Url,
    path: PathBuf,
}

impl DebugFilesystemRepository {
    pub(crate) fn new(repo: &RepositoryLocation) -> DebugFilesystemRepository {
        DebugFilesystemRepository {
            url: repo.url.clone(),
            path: repo.url.to_file_path().expect("TODO"),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

impl RepoController for DebugFilesystemRepository {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error> {
        let dtypes = dtypes_registry.iter_dtypes().cloned().collect::<Vec<_>>();

        let datatypes_path = self.path.join("datatypes.json");
        let file = File::create(datatypes_path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &dtypes)
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(())
    }

    fn backend(&self) -> crate::store::Backend {
        crate::store::Backend::DebugFilesystem
    }
}
