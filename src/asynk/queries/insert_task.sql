INSERT INTO "fang_tasks" ("metadata", "task_type", "scheduled_at") VALUES ($1, $2, $3) RETURNING *
