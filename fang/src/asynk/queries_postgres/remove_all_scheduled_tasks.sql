DELETE FROM "fang_tasks" WHERE scheduled_at > to_timestamp($1)
