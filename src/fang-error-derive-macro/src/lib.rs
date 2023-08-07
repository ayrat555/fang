extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

#[proc_macro_derive(ToFangError)]
pub fn implement_fang_error(input: TokenStream) -> TokenStream {
    // returing a simple TokenStream for Struct
    let ast = syn::parse(input).unwrap();

    implement_fang_error_macro(&ast)
}

fn implement_fang_error_macro(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let gen = quote! {
        use fang::FangError;
        impl From<#name> for FangError where #name : Debug {
            fn from(error: #name) ->  FangError {
                FangError { description: format!("{error:?}") }
            }
        }
    };

    gen.into()
}
