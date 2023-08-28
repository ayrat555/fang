UPDATE "fang_tasks" SET "state" = $1 , "updated_at" = $2 WHERE id = $3 RETURNING id , metadata , error_message, state , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at
