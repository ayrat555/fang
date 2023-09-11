INSERT INTO "fang_tasks" ("id", "metadata", "task_type", "scheduled_at") VALUES ($1, $2, $3, $4 ) RETURNING *
