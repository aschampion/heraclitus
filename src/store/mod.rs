use std::marker::PhantomData;
use std::mem;

use enum_set;






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
pub enum Store<D> {
    Postgres(StoreRepoBackend<crate::store::postgres::PostgresRepository, D>),
}

impl<D> Store<D> {
    pub fn new(repo: &crate::repo::Repository) -> Store<D> {
        use crate::repo::Repository;

        match repo {
            Repository::Postgres(ref rc) => Store::Postgres(StoreRepoBackend::new(rc)),
        }
    }
}

/// A backend-specific `Store` internal type. This is public so that other
/// libraries can provide backend implementations for their datatypes.
pub struct StoreRepoBackend<RC: crate::repo::RepoController, D> {
    repo: PhantomData<RC>,
    datatype: PhantomData<D>,
}

impl<RC: crate::repo::RepoController, D> StoreRepoBackend<RC, D> {
    pub fn new(_repo: &RC) -> StoreRepoBackend<RC, D> {
        StoreRepoBackend {
            repo: PhantomData,
            datatype: PhantomData,
        }
    }

    pub fn infer() -> StoreRepoBackend<RC, D> {
        StoreRepoBackend {
            repo: PhantomData,
            datatype: PhantomData,
        }
    }

    pub fn dtype_controller<D2>(&self) -> StoreRepoBackend<RC, D2> {
        StoreRepoBackend {
            repo: self.repo,
            datatype: PhantomData,
        }
    }
}

// impl<RC: ::repo::RepoController, D> ::repo::RepoController for StoreRepoBackend<RC, D> {
//     fn init<T: DatatypeEnum>(&mut self, dtypes_registry: &DatatypesRegistry<T>) -> Result<(), Error> {
//         self.init(dtypes_registry)
//     }

//     fn backend(&self) -> Backend {
//         self.backend()
//     }

//     // fn stored(&self) -> ::repo::Repository {
//     //     self.stored()
//     // }
// }
