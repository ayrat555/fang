use fang::FangError;
use fang_derive_error::ToFangError;
use std::fmt::Debug;
#[derive(Debug, ToFangError)]
pub enum MyAwesomeError {
    MyVariantErrorOne(String),
    MyVariantErrorTwo(u32),
}
#[cfg(test)]
mod tests {
    use crate::MyAwesomeError;
    use fang::FangError;

    #[test]
    fn converts_error_correctly() {
        let error = MyAwesomeError::MyVariantErrorOne("wow".to_string());

        let error_generated = FangError {
            description: format!("{error:?}"),
        };

        let error_converted: FangError = error.into();
        assert_eq!(error_generated.description, error_converted.description);
    }
}
