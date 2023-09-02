INSERT INTO "fang_tasks" ( "id" , "metadata", "task_type" , "uniq_hash", "scheduled_at") VALUES ($1, $2 , $3, $4, $5 ) RETURNING *
