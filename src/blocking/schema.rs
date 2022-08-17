use diesel_derive_enum::DbEnum;

#[derive(DbEnum, Debug, Eq, PartialEq, Clone)]
pub enum FangTaskState {
    New,
    InProgress,
    Failed,
    Finished,
}

table! {
    use super::FangTaskStateMapping;
    use diesel::sql_types::Jsonb;
    use diesel::sql_types::Nullable;
    use diesel::sql_types::Text;
    use diesel::sql_types::Timestamptz;
    use diesel::sql_types::Uuid;
    use diesel::sql_types::Varchar;


    fang_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        error_message -> Nullable<Text>,
        state -> FangTaskStateMapping,
        task_type -> Varchar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

table! {
    fang_periodic_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        period_in_millis -> Int4,
        scheduled_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
