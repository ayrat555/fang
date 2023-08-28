INSERT INTO "fang_tasks" ( "id" , "metadata", "task_type" , "uniq_hash", "scheduled_at") VALUES ($1, $2::jsonb , $3, $4, $5::timestamptz ) RETURNING id , metadata::text , error_message, state::text , task_type , uniq_hash, retries , scheduled_at::text , created_at::text , updated_at::text
