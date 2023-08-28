// @generated automatically by Diesel CLI.

diesel::table! {
    fang_tasks (id) {
        id -> Binary,
        metadata -> Text,
        error_message -> Nullable<Text>,
        state -> Text,
        task_type -> Text,
        uniq_hash -> Nullable<Text>,
        retries -> Integer,
        scheduled_at -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}
