use fang::FangError;
use fang_derive_error::ToFangError;
use std::fmt::Debug;
#[derive(Debug, ToFangError)]
pub enum MyAwesomeError {
    MyVariantErrorOne(String),
    MyVariantErrorTwo(u32),
}

fn some_func_error() -> Result<(), FangError> {
    let one = 1;

    if one == 1 {
        Err(MyAwesomeError::MyVariantErrorOne("hello".to_string()))?
    } else {
        Ok(())
    }
}

fn some_other_error() -> Result<(), FangError> {
    let one = 1;

    if one == 1 {
        Err(MyAwesomeError::MyVariantErrorTwo(2u32))?
    } else {
        Ok(())
    }
}

fn main() {
    if let Err(err) = some_func_error() {
        eprintln!("{err:?}");
    }

    if let Err(err) = some_other_error() {
        eprintln!("{err:?}");
    }
}
