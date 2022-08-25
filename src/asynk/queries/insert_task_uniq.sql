INSERT INTO "fang_tasks" ("metadata", "task_type" , "uniq_hash", "scheduled_at") VALUES ($1, $2 , $3, $4) RETURNING *
