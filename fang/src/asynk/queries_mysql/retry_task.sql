UPDATE fang_tasks SET state = 'retried' , error_message = $1, retries = $2, scheduled_at = $3, updated_at = $4 WHERE id = $5;

SELECT id , metadata , error_message, state , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at FROM fang_tasks WHERE id = $5 ;