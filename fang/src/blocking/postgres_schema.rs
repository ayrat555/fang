// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "fang_task_state"))]
    pub struct FangTaskState;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::FangTaskState;

    fang_tasks (id) {
        id -> Bytea,
        metadata -> Jsonb,
        error_message -> Nullable<Text>,
        state -> FangTaskState,
        task_type -> Text,
        uniq_hash -> Nullable<Text>,
        retries -> Int4,
        scheduled_at -> Timestamptz,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
