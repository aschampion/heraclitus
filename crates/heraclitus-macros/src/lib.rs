#![recursion_limit="256"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;


use syn::parse::{
    Parse,
    ParseStream,
    Result,
};

const BACKENDS: &'static [&'static str] = &[
    #[cfg(feature="backend-postgres")]
    "Postgres",
];

struct StoreType {
    impl_generics: syn::Generics,
    self_ty: syn::Type,
    ty_generics: syn::Generics,
    where_clause: Option<syn::WhereClause>,
    trait_items: Vec<syn::ImplItem>,
}

impl Parse for StoreType {

    fn parse(input: ParseStream) -> Result<Self> {
        let fork = input.fork();
        let impl_generics: Result<syn::Generics> = fork.parse();
        let impl_generics: syn::Generics = if impl_generics.is_ok() {
            input.parse()?
        } else {
            syn::Generics::default()
        };

        let self_ty: syn::Type = input.call(syn::Type::parse)?;

        let has_generics = input.peek(syn::Token![<]);
        let ty_generics: syn::Generics = if has_generics {
            input.parse()?
        } else {
            syn::Generics::default()
        };

        let where_clause: Option<syn::WhereClause> = input.parse()?;

        let trait_items = if input.peek(syn::token::Brace) {
            let content;
            let _ = syn::braced!(content in input);
            let mut items = Vec::new();
            while !content.is_empty() {
                items.push(content.parse()?);
            }
            items
        } else {
            vec![]
        };

        Ok(StoreType {
            impl_generics,
            self_ty,
            ty_generics,
            where_clause,
            trait_items,
        })
    }
}

#[proc_macro_attribute]
pub fn slow_stored_controller(attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(item as syn::ItemTrait);
    let etype = syn::parse_macro_input!(attr as StoreType);

    let gen = impl_slow_stored_controller(&ast, &etype);

    gen.into()
}

fn impl_slow_stored_controller(mc: &syn::ItemTrait, etype: &StoreType) -> proc_macro2::TokenStream {
    let name = &mc.ident;
    let methods = mc.items.iter().filter_map(|i| match i {
        syn::TraitItem::Method(m) => Some(&m.sig),
        _ => None,
    });
    let method_calls = methods.clone().map(|m| {
        let args = m.decl.inputs.iter().filter_map(|a| {
            match a {
                syn::FnArg::Captured(c) => Some(&c.pat),
                _ => None,
            }
        });
        let name = &m.ident;
        quote!{
            #name(
                #(
                    #args
                ),*
            )
        }
    });
    let etype_ty = &etype.self_ty;
    let impl_generics = &etype.impl_generics;
    let ty_generics = &etype.ty_generics;
    let where_clause = &etype.where_clause;
    let trait_items = etype.trait_items.iter();
    let backend = std::iter::repeat(BACKENDS.into_iter()
        .map(|b| proc_macro2::Ident::new(b, name.span())));
    let backend_assoc = std::iter::repeat(BACKENDS.into_iter()
        .map(|b| proc_macro2::Ident::new(&format!("Backend{}", b), name.span())));
    let method_calls_rep = method_calls.map(|m| std::iter::repeat(m));

    quote! {
        #mc

        impl #impl_generics #name for #etype_ty #ty_generics #where_clause {
            #(
                #trait_items
            )*

            #(
                #methods {
                    use heraclitus::store::Backend::*;
                    use heraclitus::datatype::StoreBackend;

                    match self.backend() {
                        #(
                            #backend => <Self as heraclitus::datatype::Store>::#backend_assoc::new().#method_calls_rep,
                        )*
                        _ => unimplemented!(),
                    }
                }
            )*
        }
    }
}

#[proc_macro_attribute]
pub fn stored_controller(attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    // Cloning the token stream prevents a problem where associated types are
    // silently removed from the output.
    let fork = item.clone();
    let ast = syn::parse_macro_input!(fork as syn::ItemTrait);
    let etype = syn::parse_macro_input!(attr as StoreType);

    let gen = impl_stored_controller(item.into(), &ast, &etype);

    gen.into()
}

fn impl_stored_controller(item: proc_macro2::TokenStream, mc: &syn::ItemTrait, etype: &StoreType) -> proc_macro2::TokenStream {
    let name = &mc.ident;
    let methods = mc.items.iter().filter_map(|i| match i {
        syn::TraitItem::Method(m) => Some(&m.sig),
        _ => None,
    });
    let method_calls = methods.clone().map(|m| {
        let args = m.decl.inputs.iter().filter_map(|a| {
            match a {
                syn::FnArg::Captured(c) => Some(&c.pat),
                _ => None,
            }
        });
        let name = &m.ident;
        quote!{
            #name(
                #(
                    #args
                ),*
            )
        }
    });
    let etype_ty = &etype.self_ty;
    let impl_generics = &etype.impl_generics;
    let ty_generics = &etype.ty_generics;
    let where_clause = &etype.where_clause;
    let backend = std::iter::repeat(BACKENDS.into_iter()
        .map(|b| proc_macro2::Ident::new(b, name.span())));
    let method_calls_rep = method_calls.map(|m| std::iter::repeat(m));

    quote! {
        #item

        impl #impl_generics #name for #etype_ty #ty_generics #where_clause {
            #(
                #methods {
                    match self {
                        #(
                            Self::#backend(c) => c.#method_calls_rep,
                        )*
                        _ => unreachable!(),
                    }
                }
            )*
        }
    }
}


#[proc_macro_attribute]
pub fn stored_datatype_controller(attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let dtype = syn::parse_macro_input!(attr as syn::Ident);

    let gen = impl_stored_datatype_controller(&dtype, item.into());

    gen.into()
}

fn impl_stored_datatype_controller(dtype: &syn::Ident, item: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    quote! {
        #[heraclitus_macros::stored_controller(<#dtype as heraclitus::datatype::DatatypeMarker>::Store)]
        #item
    }
}


#[proc_macro_attribute]
pub fn interface(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(item as syn::ItemTrait);

    let gen = impl_interface(&ast);

    gen.into()
}

fn impl_interface(mc: &syn::ItemTrait) -> proc_macro2::TokenStream {
    let name = &mc.ident;
    // let gen_name = proc_macro2::Ident::new(&format!("{}Generator", name), name.span());

    quote! {
        #mc

        // pub type #gen_name = Box<dyn Fn(&heraclitus::repo::Repository) -> Box<dyn #name>>;

        impl heraclitus::datatype::interface::InterfaceMeta for #name {
            // type Generator = #gen_name;
            type Generator = fn(&heraclitus::repo::Repository) -> Box<dyn #name>;
        }
    }
}


#[proc_macro_attribute]
pub fn stored_interface_controller(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(item as syn::ItemTrait);

    let gen = impl_stored_interface_controller(&ast);

    gen.into()
}

fn impl_stored_interface_controller(mc: &syn::ItemTrait) -> proc_macro2::TokenStream {
    let name = std::iter::repeat(&mc.ident);
    let backend_assoc = BACKENDS.into_iter()
        .map(|b| proc_macro2::Ident::new(&format!("Backend{}", b), mc.ident.span()));

    quote! {
        #[heraclitus_macros::slow_stored_controller(<S: heraclitus::datatype::Store>
            S
            where #(
                S::#backend_assoc: #name,
            )*
        )]
        #mc
    }
}


/// Special macro just for setting the appropriate `S::Backend*: Storage<...>`
/// constraints for `datatype::Storage`. This could be generalized into
/// `slow_stored_controller`/`stored_controller` with some work.
#[proc_macro_attribute]
pub fn stored_storage_controller(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(item as syn::ItemTrait);

    let gen = impl_stored_storage_controller(&ast);

    gen.into()
}

fn impl_stored_storage_controller(mc: &syn::ItemTrait) -> proc_macro2::TokenStream {
    let name = std::iter::repeat(&mc.ident);
    let backend_assoc = BACKENDS.into_iter()
        .map(|b| proc_macro2::Ident::new(&format!("Backend{}", b), mc.ident.span()));

    quote! {
        #[heraclitus_macros::slow_stored_controller(
            <State, Delta, S> S
            where
                State: Debug + Hash + PartialEq,
                Delta: Debug + Hash + PartialEq,
                S: heraclitus::datatype::Store,
                #(
                    S::#backend_assoc: #name<StateType=State, DeltaType=Delta>,
                )*
            {
                type StateType = State;
                type DeltaType = Delta;
            }
        )]
        #mc
    }
}

#[proc_macro_derive(DatatypeMarker)]
pub fn datatype_marker_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);

    let gen = impl_datatype_store(&ast);

    gen.into()
}

fn impl_datatype_store(ast: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let name = &ast.ident;
    let store_name = proc_macro2::Ident::new(&format!("{}Store", name), name.span());
    let store_backend_name = proc_macro2::Ident::new(&format!("{}Backend", name), store_name.span());

    quote! {
        pub struct #store_backend_name<RC: heraclitus::repo::RepoController> {
            repo: std::marker::PhantomData<RC>,
        }

        impl<RC: heraclitus::repo::RepoController> #store_backend_name<RC> {
            fn new() -> Self {
                Self {
                    repo: std::marker::PhantomData,
                }
            }
        }

        impl<RC: heraclitus::repo::RepoController> heraclitus::datatype::StoreBackend for #store_backend_name<RC> {
            type Datatype = #name;
            type Base = #store_name;

            fn new() -> Self {
                Self::new()
            }
        }

        #[cfg(feature="backend-postgres")]
        impl From<#store_name> for #store_backend_name<heraclitus::store::postgres::PostgresRepository> {
            fn from(store: #store_name) -> Self {
                match store {
                    #store_name::Postgres(c) => c,
                    _ => unreachable!(),
                }
            }
        }

        pub enum #store_name {
            #[cfg(feature="backend-postgres")]
            Postgres(#store_backend_name::<heraclitus::store::postgres::PostgresRepository>),
        }

        // Must do this until GATs are available.
        impl heraclitus::datatype::Store for #store_name {
            #[cfg(feature="backend-postgres")]
            type BackendPostgres = #store_backend_name::<heraclitus::store::postgres::PostgresRepository>;

            fn backend(&self) -> heraclitus::store::Backend {
                use heraclitus::store::Backend::*;

                match *self {
                    #[cfg(feature="backend-postgres")]
                    Self::Postgres(_) => Postgres,
                }
            }

            fn for_backend(backend: heraclitus::store::Backend) -> Self {
                use heraclitus::store::Backend::*;

                match backend {
                    #[cfg(feature="backend-postgres")]
                    Postgres => Self::Postgres(
                        #store_backend_name::<heraclitus::store::postgres::PostgresRepository>::new()),
                    _ => unimplemented!()
                }
            }
        }

        impl Into<heraclitus::datatype::StoreMetaController> for #store_name {
            fn into(self) -> heraclitus::datatype::StoreMetaController {
                match self {
                    #[cfg(feature="backend-postgres")]
                    Self::Postgres(c) => heraclitus::datatype::StoreMetaController::Postgres(Box::new(c)),
                }
            }
        }

        impl heraclitus::datatype::DatatypeMarker for #name {
            type Store = #store_name;
        }
    }
}
