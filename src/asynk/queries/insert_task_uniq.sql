INSERT INTO "fang_tasks" ("metadata", "task_type" , "uniq_hash", "period_in_millis") VALUES ($1, $2 , $3, $4) RETURNING *
