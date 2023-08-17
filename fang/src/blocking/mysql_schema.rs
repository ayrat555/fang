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
        metadata -> Json,
        error_message -> Nullable<Text>,
        #[max_length = 11]
        state -> FangTasksStateEnum,
        #[max_length = 80]
        task_type -> Varchar,
        #[max_length = 64]
        uniq_hash -> Nullable<Char>,
        retries -> Integer,
        scheduled_at -> Datetime,
        created_at -> Datetime,
        updated_at -> Datetime,
    }
}
