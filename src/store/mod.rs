use std::marker::PhantomData;
use std::mem;

use enum_set;

use datatype::DatatypeEnum;
use datatype::DatatypesRegistry;
use Error;


pub mod postgres;


#[derive(Clone, Copy)]
#[repr(u32)]
pub enum Backend {
    Filesystem,
    Memory,
    Postgres,
}

// Boilerplate necessary for EnumSet compatibility.
impl enum_set::CLike for Backend {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> Backend {
        mem::transmute(v)
    }
}


/// Provides repository-backed storage for a datatype.
///
/// TODO: explain the purpose of enum dispatch, history of design decisions.
pub enum Store<'a, D> {
    Postgres(StoreRepoBackend<'a, ::store::postgres::PostgresRepoController, D>),
}

impl<'a, D> Store<'a, D> {
    pub fn new(repo_control: &::repo::StoreRepoController<'a>) -> Store<'a, D> {
        use ::repo::StoreRepoController;

        match repo_control {
            StoreRepoController::Postgres(ref rc) => Store::Postgres(StoreRepoBackend::new(rc)),
        }
    }
}

/// A backend-specific `Store` internal type. This is public so that other
/// libraries can provide backend implementations for their datatypes.
pub struct StoreRepoBackend<'a, RC: ::repo::RepoController, D> {
    repo_control: &'a RC,
    datatype: PhantomData<D>,
}

impl<'a, RC: ::repo::RepoController, D> StoreRepoBackend<'a, RC, D> {
    pub fn new(repo_control: &'a RC) -> StoreRepoBackend<'a, RC, D> {
        StoreRepoBackend {
            repo_control,
            datatype: PhantomData,
        }
    }

    pub fn dtype_controller<D2>(&self) -> StoreRepoBackend<'a, RC, D2> {
        StoreRepoBackend {
            repo_control: self.repo_control,
            datatype: PhantomData,
        }
    }
}

impl<'a, RC: ::repo::RepoController, D> ::repo::RepoController for StoreRepoBackend<'a, RC, D> {
    fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error> {
        self.repo_control.init(dtypes_registry)
    }

    fn backend(&self) -> Backend {
        self.repo_control.backend()
    }

    fn stored(&self) -> ::repo::StoreRepoController {
        self.repo_control.stored()
    }
}
