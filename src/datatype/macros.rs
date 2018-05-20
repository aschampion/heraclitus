//! Macros for defining terminal enums of interfaces and datatypes.
//!
//! This is in a separate submodule to resolve import order issues (which
//! affect macros).

#[macro_export]
macro_rules! interface_controller_enum {
    ( $enum_name:ident, ( $( ( $i_name:ident, $i_control:ty, $i_desc:expr ) ),*  $(,)* ) ) => {
        pub enum $enum_name {
            $(
                $i_name(Option<Box<$i_control>>),
            )*
        }

        impl std::cmp::PartialEq for $enum_name {
            fn eq(&self, other: &$enum_name) -> bool {
                match (self, other) {
                    $(
                        (&$enum_name::$i_name(None), &$enum_name::$i_name(None)) => true,
                    )*
                    _ => false,
                }
            }
        }

        impl std::str::FromStr for $enum_name {
            type Err = ();

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $(
                        stringify!($i_name) => Ok($enum_name::$i_name(None)),
                    )*
                    _ => Err(())
                }
            }
        }

        impl std::fmt::Display for $enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", match self {
                    $(
                        &$enum_name::$i_name(_) => stringify!($i_name),
                    )*
                })
            }
        }

        impl $crate::datatype::InterfaceControllerEnum for $enum_name {
            fn all_descriptions() -> Vec<&'static $crate::datatype::InterfaceDescription> {
                vec![
                    $($i_desc,)*
                ]
            }
        }

        $(
            impl $crate::datatype::InterfaceController<$i_control> for $enum_name {
                const VARIANT: $enum_name = $enum_name::$i_name(None);
            }

            impl std::convert::From<Box<$i_control>> for $enum_name {
                fn from(inner: Box<$i_control>) -> $enum_name {
                    $enum_name::$i_name(Some(inner))
                }
            }

            impl std::convert::From<$enum_name> for Box<$i_control> {
                fn from(iface_control: $enum_name) -> Box<$i_control> {
                    match iface_control {
                        $enum_name::$i_name(Some(inner)) => inner,
                        _ => panic!("Attempt to unwrap interface controller into wrong type!"),
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! datatype_enum {
    ( $enum_name:ident, $iface_enum:ty, ( $( ( $d_name:ident, $d_type:ty ) ),* $(,)* ) ) => {
        pub enum $enum_name {
            $(
                $d_name($d_type),
            )*
        }

        impl $crate::datatype::DatatypeEnum for $enum_name {
            type InterfaceControllerType = $iface_enum;

            fn variant_names() -> Vec<&'static str> {
                vec![
                    $(stringify!($d_name),)*
                ]
            }

            fn from_name(name: &str) -> Option<$enum_name> {
                match name {
                    $(
                        stringify!($d_name) => Some($enum_name::$d_name(<$d_type as Default>::default())),
                    )*
                    _ => None,
                }
            }

            fn as_model<'a>(&self) -> &($crate::datatype::Model<Self::InterfaceControllerType> + 'a) {
                match *self {
                    $(
                        $enum_name::$d_name(ref d) => d,
                    )*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! state_interface {
    ( $trait_name:ident, $iface:path ) => {
        pub trait $trait_name {
            fn get_composite_interface(
                &self,
                repo_control: &mut $crate::repo::StoreRepoController,
                composition: &$crate::Composition,
            ) -> Result<Box<$iface>, $crate::Error>;
        }

        impl<S: 'static, MC> $trait_name for MC
                where
                    S: $iface + ::std::fmt::Debug + ::std::hash::Hash + PartialEq,
                    MC: $crate::datatype::ModelController<StateType = S> {
            fn get_composite_interface(
                &self,
                repo_control: &mut $crate::repo::StoreRepoController,
                composition: &$crate::Composition,
            ) -> Result<Box<$iface>, $crate::Error> {
                Ok(Box::new(self.get_composite_state(repo_control, composition)?))
            }
        }
    };
}
