table! {
    fang_tasks (id) {
        id -> Int8,
        metadata -> Nullable<Jsonb>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
