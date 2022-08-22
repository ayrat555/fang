INSERT INTO "fang_tasks" ("metadata", "task_type" , "uniq_hash") VALUES ($1, $2 , $3) RETURNING *
