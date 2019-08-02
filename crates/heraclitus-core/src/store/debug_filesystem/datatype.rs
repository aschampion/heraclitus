use crate::datatype::StoreMetaController;


pub trait DebugFilesystemMetaController {}

impl Into<Box<dyn DebugFilesystemMetaController>> for StoreMetaController {
    fn into(self) -> Box<dyn DebugFilesystemMetaController> {
        #[allow(unreachable_patterns)] // Other store types may exist.
        match self {
            StoreMetaController::DebugFilesystem(smc) => smc,
            _ => panic!("Wrong store type."),
        }
    }
}
