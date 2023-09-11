SELECT id::text , metadata::text , error_message, state::text , task_type , uniq_hash, retries , scheduled_at::text , created_at::text , updated_at::text FROM fang_tasks  WHERE id = $1::uuid
