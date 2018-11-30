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

struct StoreType {
    impl_generics: syn::Generics,
    self_ty: syn::Path,
    ty_generics: syn::Generics,
    where_clause: Option<syn::WhereClause>,
}

impl Parse for StoreType {

    fn parse(input: ParseStream) -> Result<Self> {
        let has_generics = input.peek(syn::Token![<]);
        let impl_generics: syn::Generics = if has_generics {
            input.parse()?
        } else {
            syn::Generics::default()
        };

        // let self_ty: syn::TypePath = input.parse()?;
        let self_ty: syn::Path = input.call(syn::Path::parse_mod_style)?;

        let has_generics = input.peek(syn::Token![<]);
        let ty_generics: syn::Generics = if has_generics {
            input.parse()?
        } else {
            syn::Generics::default()
        };

        let where_clause: Option<syn::WhereClause> = input.parse()?;

        Ok(StoreType {
            impl_generics,
            self_ty,
            ty_generics,
            where_clause,
        })
    }
}

#[proc_macro_attribute]
pub fn stored_controller(attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(item as syn::ItemTrait);
    // let etype_lit = syn::parse_macro_input!(attr as syn::LitStr);
    // let etype = etype_lit.parse::<StoreType>().unwrap();
    let etype = syn::parse_macro_input!(attr as StoreType);

    let gen = impl_stored_controller(&ast, &etype);

    gen.into()
}

fn impl_stored_controller(mc: &syn::ItemTrait, etype: &StoreType) -> proc_macro2::TokenStream {
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
    // let etype_ident = &etype_ty.ident;
    let etype_repeat = std::iter::repeat(etype_ty);
    let impl_generics = &etype.impl_generics;
    let ty_generics = &etype.ty_generics;
    let where_clause = &etype.where_clause;

    // let (impl_generics, ty_generics, where_clause) = etype.generics.split_for_impl();

    quote! {
        #mc

        impl #impl_generics #name for #etype_ty #ty_generics #where_clause {
            #(
                #methods {
                    match self {
                        #etype_repeat::Postgres(c) => c.#method_calls,
                    }
                }
            )*
        }
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
    let gen_name = proc_macro2::Ident::new(&format!("{}Generator", name), name.span());

    quote! {
        #mc

        pub type #gen_name = Box<for <'repo> Fn(&StoreRepoController<'repo>) -> Box<dyn #name + 'repo>>;

        impl InterfaceMeta for #name {
            type Generator = #gen_name;
        }
    }
}
