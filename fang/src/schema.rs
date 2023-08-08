table! {
    fang_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        error_message -> Nullable<Text>,
        state -> Fang_task_state,
        task_type -> Varchar,
        uniq_hash -> Nullable<Bpchar>,
        scheduled_at -> Timestamptz,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
