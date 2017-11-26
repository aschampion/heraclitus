//! Macros for defining terminal enums of interfaces and datatypes.
//!
//! This is in a separate submodule to resolve import order issues (which
//! affect macros).

#[macro_export]
macro_rules! interface_controller_enum {
    ( $enum_name:ident, ( $( ( $i_name:ident, $i_control:ident, $i_desc:expr ) ),*  $(,)* ) ) => {
        pub enum $enum_name {
            $(
                $i_name(Box<$i_control>),
            )*
        }

        impl InterfaceControllerEnum for $enum_name {
            fn all_descriptions() -> Vec<&'static $crate::datatype::InterfaceDescription> {
                vec![
                    $($i_desc,)*
                ]
            }
        }

        $(
            impl $crate::datatype::InterfaceController<$i_control> for $enum_name {}

            impl std::convert::From<Box<$i_control>> for $enum_name {
                fn from(inner: Box<$i_control>) -> $enum_name {
                    $enum_name::$i_name(inner)
                }
            }

            impl std::convert::From<$enum_name> for Box<$i_control> {
                fn from(iface_control: $enum_name) -> Box<$i_control> {
                    match iface_control {
                        $enum_name::$i_name(inner) => inner,
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

        impl DatatypeEnum for $enum_name {
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

            fn as_model(&self) -> &$crate::datatype::Model<Self::InterfaceControllerType> {
                match *self {
                    $(
                        $enum_name::$d_name(ref d) => d,
                    )*
                }
            }
        }
    };
}
