// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::sql_types::SqlType)]
    #[diesel(mysql_type(name = "Enum"))]
    pub struct FangTasksStateEnum;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::FangTasksStateEnum;

    fang_tasks (id) {
        #[max_length = 36]
        id -> Varchar,
        #[max_length = 2048]
        metadata -> Varchar,
        #[max_length = 2048]
        error_message -> Nullable<Varchar>,
        #[max_length = 11]
        state -> FangTasksStateEnum,
        #[max_length = 255]
        task_type -> Varchar,
        #[max_length = 64]
        uniq_hash -> Nullable<Varchar>,
        retries -> Integer,
        scheduled_at -> Datetime,
        created_at -> Datetime,
        updated_at -> Datetime,
    }
}
