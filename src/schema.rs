table! {
    fang_periodic_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        period_in_seconds -> Int4,
        scheduled_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

table! {
    fang_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        error_message -> Nullable<Text>,
        state -> Fang_task_state,
        task_type -> Varchar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

allow_tables_to_appear_in_same_query!(
    fang_periodic_tasks,
    fang_tasks,
);
