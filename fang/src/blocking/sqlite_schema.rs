// @generated automatically by Diesel CLI.

diesel::table! {
    fang_tasks (id) {
        id -> Text,
        metadata -> Text,
        error_message -> Nullable<Text>,
        state -> Text,
        task_type -> Text,
        uniq_hash -> Nullable<Text>,
        retries -> Integer,
        scheduled_at -> Integer,
        created_at -> Integer,
        updated_at -> Integer,
    }
}
