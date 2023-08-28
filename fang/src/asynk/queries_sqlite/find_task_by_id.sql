SELECT id , metadata , error_message, state , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at FROM fang_tasks  WHERE id = $1
