SELECT id , metadata , error_message, state , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at FROM fang_tasks WHERE uniq_hash = $1 AND state in ('new', 'retried') LIMIT 1
