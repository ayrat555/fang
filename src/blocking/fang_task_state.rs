#[derive(diesel_derive_enum::DbEnum, Debug, Eq, PartialEq, Clone)]
#[DieselTypePath = "crate::schema::sql_types::FangTaskState"]
pub enum FangTaskState {
    New,
    InProgress,
    Failed,
    Finished,
    Retried,
}
