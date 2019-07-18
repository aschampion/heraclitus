//! This tests the minimal, empty interface should compile.
//!
//! This does not test interface enums or any interface retrieval features.

use heraclitus_core as heraclitus;
use heraclitus_macros::{
    interface,
    stored_interface_controller,
};

#[interface]
#[stored_interface_controller]
pub trait TestInterface {
    fn foo(&self) -> ();
}
