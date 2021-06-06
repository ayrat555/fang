table! {
    fang_tasks (id) {
        id -> Uuid,
        metadata -> Jsonb,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
