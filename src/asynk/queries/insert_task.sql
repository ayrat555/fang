INSERT INTO "fang_tasks" ("metadata", "task_type") VALUES ($1, $2) RETURNING id , state , metadata , error_message , task_type , created_at , updated_at
