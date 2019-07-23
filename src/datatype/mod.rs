use std;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use heraclitus_macros::{
    stored_storage_controller,
};

pub use heraclitus_core::datatype::*;

use crate::{Composition, Error, Hunk};
use crate::repo::Repository;
use self::interface::{
    ProducerController,
    CustomProductionPolicyController,
};


#[macro_use]
pub mod macros;
pub mod artifact_graph;
#[macro_use]
pub mod blob;
pub mod interface;
pub mod partitioning;
pub mod producer;
pub mod reference;
pub mod tracking_branch_producer;


#[derive(Debug, Hash, PartialEq)]
pub enum Payload<S, D> {
    State(S),
    Delta(D),
}

pub trait ComposableState {
    type StateType: Debug + Hash + PartialEq;
    type DeltaType: Debug + Hash + PartialEq;

    fn hash_payload(
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> crate::HashType {
        let mut s = DefaultHasher::new();
        payload.hash(&mut s);
        s.finish()
    }

    fn compose_state(
        state: &mut Self::StateType,
        delta: &Self::DeltaType,
    );
}

pub trait StateOnly {
    // Note this is named differently than `ComposableState::StateType` to avoid
    // ambiguous associated type errors requiring verbose trait expansion.
    type StateOnlyType: Debug + Hash + PartialEq;
}

impl<T> ComposableState for T where T: StateOnly {
    type StateType = <Self as StateOnly>::StateOnlyType;
    type DeltaType = UnrepresentableType;

    fn compose_state(
        _state: &mut Self::StateType,
        _delta: &Self::DeltaType,
    ) {
        unimplemented!()
    }
}

/// Common interface to all datatypes that involves state.
#[stored_storage_controller] // This will only compile if some backend is enabled
pub trait Storage: StoreOrBackend<Datatype: ComposableState> {
    // type StateType = <<Self as StoreBackend>::Datatype as ComposableState>::StateType;

    fn write_hunk(
        &mut self,
        repo: &Repository,
        hunk: &Hunk,
        payload: &Payload<Self::StateType, Self::DeltaType>,
    ) -> Result<(), Error> {

        self.write_hunks(repo, &[hunk], &[payload])
    }

    /// Write multiple hunks to this model. All hunks should be from the same
    /// version.
    fn write_hunks<'a: 'b, 'b: 'c + 'd, 'c, 'd, H, P>(
        &mut self,
        repo: &Repository,
        hunks: &[H],
        payloads: &[P],
    ) -> Result<(), Error>
            where H: std::borrow::Borrow<Hunk<'a, 'b, 'c, 'd>>,
                P: std::borrow::Borrow<Payload<Self::StateType, Self::DeltaType>> {

        for (hunk, payload) in hunks.iter().zip(payloads) {
            let hunk: &Hunk = hunk.borrow();
            let payload = payload.borrow();

            self.write_hunk(repo, hunk, payload)?;
        }

        Ok(())
    }

    fn read_hunk(
        &self,
        repo: &Repository,
        hunk: &Hunk,
    ) -> Result<Payload<Self::StateType, Self::DeltaType>, Error>;

    /// Compose state from a composition of sufficient hunks.
    ///
    /// Datatypes' store types may choose to implement this more efficiently.
    fn get_composite_state(
        &self,
        repo: &Repository,
        composition: &Composition,
    ) -> Result<Self::StateType, Error> {
            let mut hunk_iter = composition.iter().rev();

            let mut state = match self.read_hunk(repo, hunk_iter.next().expect("TODO"))? {
                Payload::State(state) => state,
                _ => panic!("Composition rooted in non-state hunk"),
            };

            for hunk in hunk_iter {
                match self.read_hunk(repo, hunk)? {
                    Payload::State(_) => panic!("TODO: shouldn't have non-root state"),
                    Payload::Delta(ref delta) => {
                        Self::Datatype::compose_state(&mut state, delta);
                    }
                }
            }

            Ok(state)
        }
}

/// A type for a representation kind that is not supported by a model. This
/// allows, for example, models to implement `Storage` if they do not
/// support deltas.
///
/// The type is uninstantiable.
#[allow(unreachable_code, unreachable_patterns)]
#[derive(Debug, PartialEq)]
pub struct UnrepresentableType (!);

impl Hash for UnrepresentableType {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unreachable!()
    }
}


interface_controller_enum!(DefaultInterfaceController, (
        (ArtifactMeta, artifact_graph::ArtifactMeta, &*artifact_graph::INTERFACE_ARTIFACT_META_DESC),
        (Partitioning, partitioning::PartitioningState, &*interface::INTERFACE_PARTITIONING_DESC),
        (Producer, ProducerController, &*interface::INTERFACE_PRODUCER_DESC),
        (CustomProductionPolicy, CustomProductionPolicyController, &*interface::INTERFACE_CUSTOM_PRODUCTION_POLICY_DESC)
    ));

datatype_enum!(DefaultDatatypes, DefaultInterfaceController, (
        (ArtifactGraph, artifact_graph::ArtifactGraphDtype),
        (Ref, reference::Ref),
        (UnaryPartitioning, partitioning::UnaryPartitioning),
        (ArbitraryPartitioning, partitioning::arbitrary::ArbitraryPartitioning),
        (Blob, blob::BlobDatatype),
        (NoopProducer, producer::NoopProducer),
        (TrackingBranchProducer, tracking_branch_producer::TrackingBranchProducer),
    ));


/// Testing utilities.
///
/// This module is public so dependent libraries can reuse these utilities to
/// test custom datatypes.
pub mod testing {
    use super::*;

    pub use heraclitus_core::datatype::testing::*;

    pub fn init_default_dtypes_registry() -> DatatypesRegistry<DefaultDatatypes> {
        heraclitus_core::datatype::testing::init_dtypes_registry::<DefaultDatatypes>()
    }
}
