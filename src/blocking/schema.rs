pub mod sql_types {
    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "fang_task_state"))]
    pub struct FangTaskState;
}

#[derive(diesel_derive_enum::DbEnum, Debug, Eq, PartialEq, Clone)]
#[DieselTypePath = "crate::blocking::schema::sql_types::FangTaskState"]
pub enum FangTaskState {
    New,
    InProgress,
    Failed,
    Finished,
}

table! {
    use super::sql_types::FangTaskState;
    use diesel::sql_types::Jsonb;
    use diesel::sql_types::Nullable;
    use diesel::sql_types::Text;
    use diesel::sql_types::Timestamptz;
    use diesel::sql_types::Uuid;
    use diesel::sql_types::Varchar;
    use diesel::pg::sql_types::Bpchar;

    fang_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        error_message -> Nullable<Text>,
        state -> FangTaskState,
        task_type -> Varchar,
        uniq_hash -> Nullable<Bpchar>,
        scheduled_at -> Timestamptz,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
