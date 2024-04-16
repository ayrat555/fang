SELECT id , metadata::text , error_message, state::text , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at FROM fang_tasks  WHERE id = $1::uuid
