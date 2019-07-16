//! Macros for defining terminal enums of interfaces and datatypes.
//!
//! This is in a separate submodule to resolve import order issues (which
//! affect macros).

#[macro_export]
macro_rules! interface_controller_enum {
    ( $enum_name:ident, ( $( ( $i_name:ident, $i_control:path, $i_desc:expr ) ),*  $(,)* ) ) => {
        pub enum $enum_name {
            $(
                $i_name(Option<<$i_control as $crate::datatype::interface::InterfaceMeta>::Generator>),
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
            #[allow(unreachable_code)] // Necessary because of empty interface testing types.
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", match *self {
                    $(
                        $enum_name::$i_name(_) => stringify!($i_name),
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
            impl $crate::datatype::InterfaceController<dyn $i_control> for $enum_name {
                const VARIANT: $enum_name = $enum_name::$i_name(None);

                fn into_controller_generator(self) -> Option<<$i_control as $crate::datatype::interface::InterfaceMeta>::Generator> {
                    match self {
                        $enum_name::$i_name(g) => g,
                        _ => panic!("Attempt to unwrap interface controller into wrong type!"),
                    }
                }
            }

            impl std::convert::From<<$i_control as $crate::datatype::interface::InterfaceMeta>::Generator> for $enum_name {
                fn from(inner: <$i_control as $crate::datatype::interface::InterfaceMeta>::Generator) -> $enum_name {
                    $enum_name::$i_name(Some(inner))
                }
            }

            // impl std::convert::From<$enum_name> for <$i_control as $crate::datatype::interface::InterfaceMeta>::Generator {
            //     fn from(iface_control: $enum_name) -> <$i_control as $crate::datatype::interface::InterfaceMeta>::Generator {
            //         match iface_control {
            //             $enum_name::$i_name(Some(inner)) => inner,
            //             _ => panic!("Attempt to unwrap interface controller into wrong type!"),
            //         }
            //     }
            // }
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

            fn as_model<'a>(&self) -> &(dyn $crate::datatype::Model<Self::InterfaceControllerType> + 'a) {
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
macro_rules! datatype_controllers {
    ( $dtype:ident, ( $( $i_control:ident ),* $(,)* ) ) => {
        fn meta_controller(
            &self,
            backend: $crate::store::Backend,
        ) -> StoreMetaController {
            StoreMetaController::from_backend::<$dtype>(backend)
        }

        fn interface_controller(
            &self,
            iface: T,
        ) -> Option<T> {

            $(
                if iface == <T as InterfaceController<$i_control>>::VARIANT {
                    let closure: <$i_control as $crate::datatype::interface::InterfaceMeta>::Generator =
                        Box::new(|repo| {
                            let store = $crate::store::Store::<$dtype>::new(repo);
                            let control: Box<dyn $i_control> = Box::new(store);
                            control
                        });
                    return Some(T::from(closure));
                }
            )*

            {&iface;} // Suppress unused warnings in datatypes without interfaces.

            None
        }
    }
}
