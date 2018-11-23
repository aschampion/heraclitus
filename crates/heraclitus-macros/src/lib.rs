extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;


#[proc_macro_attribute]
pub fn stored_controller(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream)
        -> proc_macro::TokenStream {
    let ast = syn::parse_macro_input!(item as syn::ItemTrait);

    let gen = impl_stored_controller(&ast);

    gen.into()
}

fn impl_stored_controller(mc: &syn::ItemTrait) -> proc_macro2::TokenStream {
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

    quote! {
        #mc

        impl #name for ::repo::StoreRepoController {
            #(
                #methods {
                    match self {
                        ::repo::StoreRepoController::Postgres(c) => c.#method_calls,
                    }
                }
            )*
        }
    }
}
