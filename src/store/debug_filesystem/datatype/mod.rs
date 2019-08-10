pub use heraclitus_core::store::debug_filesystem::datatype::*;


pub mod artifact_graph;
// pub mod blob;
pub mod blob {
    use crate::datatype::blob::{
        BlobDatatypeBackend,
        Storage,
    };
    use crate::default_debug_filesystem_store_backend;
    default_debug_filesystem_store_backend!(BlobDatatypeBackend);
    impl Storage for BlobDatatypeBackend<heraclitus::store::debug_filesystem::DebugFilesystemRepository> {}
}
// pub mod partitioning;
pub mod partitioning {
    use crate::datatype::partitioning::UnaryPartitioningBackend;
    use crate::store::debug_filesystem::DebugFilesystemRepository;

    impl super::DebugFilesystemMetaController for UnaryPartitioningBackend<DebugFilesystemRepository> {}

    pub mod arbitrary {
        use crate::datatype::partitioning::arbitrary::{
            ArbitraryPartitioningBackend,
            Storage,
        };
        use crate::default_debug_filesystem_store_backend;
        default_debug_filesystem_store_backend!(ArbitraryPartitioningBackend);
        impl Storage for ArbitraryPartitioningBackend<heraclitus::store::debug_filesystem::DebugFilesystemRepository> {}
    }
}
// pub mod producer;
pub mod producer {
    use crate::datatype::producer::NoopProducerBackend;
    use crate::store::debug_filesystem::DebugFilesystemRepository;
    use super::DebugFilesystemMetaController;

    impl DebugFilesystemMetaController for NoopProducerBackend<DebugFilesystemRepository> {}

    #[cfg(test)]
    pub(crate) mod tests {
        use super::*;

        use crate::datatype::producer::tests::NegateBlobProducerBackend;

        impl DebugFilesystemMetaController for NegateBlobProducerBackend<DebugFilesystemRepository> {}
    }
}
pub mod reference;
// pub mod tracking_branch_producer;
pub mod tracking_branch_producer {
    use crate::datatype::tracking_branch_producer::TrackingBranchProducerBackend;
    use crate::store::debug_filesystem::DebugFilesystemRepository;

    impl super::DebugFilesystemMetaController for TrackingBranchProducerBackend<DebugFilesystemRepository> {}
}


// Note that this cannot be implemented as a default impl without the opt-in
// marker trait because such an impl, like:
// ```rs
// impl<B, S> crate::datatype::Storage for B
// where
//     B: StoreBackend<Base=S>,
//     S: crate::datatype::Store<BackendDebugFilesystem=B>,
// ```
// would conflict with datatype-specific cross-backend blanket impls, like
// those for UnaryPartitioning. Specialization does not help because neither
// impl is a subset of the other (because one is generic across datatypes and
// the other across backends).
//
// Likewise it cannot be implemented as a marker trait with a blanket impl
// for that trait because it conflicts with the stored_storage_controller
// blanket impl for Stores, even though these are actually disjoint, because
// the trait system cannot express negative or exclusive bounds.

#[macro_export]
macro_rules! default_debug_filesystem_store_backend {
    ( $store_backend:ident ) => {
        use std::borrow::Borrow;

        impl heraclitus::store::debug_filesystem::datatype::DebugFilesystemMetaController for
            $store_backend<heraclitus::store::debug_filesystem::DebugFilesystemRepository> {}

        impl heraclitus::datatype::Storage for
            $store_backend<heraclitus::store::debug_filesystem::DebugFilesystemRepository>
        {

            default fn write_hunk(
                &mut self,
                repo: &heraclitus::repo::Repository,
                hunk: &heraclitus::Hunk,
                payload: &heraclitus::datatype::Payload<Self::StateType, Self::DeltaType>,
            ) -> Result<(), heraclitus::Error> {
                let rc: &heraclitus::store::debug_filesystem::DebugFilesystemRepository = repo.borrow();

                let mut path = heraclitus::store::debug_filesystem::hunk_path(rc, hunk);
                path.push("payload.json");
                heraclitus::store::debug_filesystem::write_json(path, payload)
            }

            default fn read_hunk(
                &self,
                repo: &heraclitus::repo::Repository,
                hunk: &heraclitus::Hunk,
            ) -> Result<heraclitus::datatype::Payload<Self::StateType, Self::DeltaType>, heraclitus::Error> {
                let rc: &heraclitus::store::debug_filesystem::DebugFilesystemRepository = repo.borrow();

                let mut path = heraclitus::store::debug_filesystem::hunk_path(rc, hunk);
                path.push("payload.json");
                heraclitus::store::debug_filesystem::read_json(path)
            }
        }
    };
}
