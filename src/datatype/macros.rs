//! This is in a separate submodule to resolve import order issues (which
//! affect macros).

#[macro_export]
macro_rules! state_interface {
    ( $trait_name:ident, $iface:path ) => {
        #[heraclitus_macros::interface]
        pub trait $trait_name {
            fn get_composite_interface(
                &self,
                repo: &$crate::repo::Repository,
                composition: &$crate::Composition,
            ) -> Result<Box<dyn $iface>, $crate::Error>;
        }

        impl<S: 'static, MC> $trait_name for MC
                where
                    S: $iface + ::std::fmt::Debug + ::std::hash::Hash + PartialEq,
                    MC: $crate::datatype::Storage<StateType = S> {
            fn get_composite_interface(
                &self,
                repo: &$crate::repo::Repository,
                composition: &$crate::Composition,
            ) -> Result<Box<dyn $iface>, $crate::Error> {
                Ok(Box::new(self.get_composite_state(repo, composition)?))
            }
        }
    };
}
