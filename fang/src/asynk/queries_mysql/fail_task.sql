UPDATE fang_tasks SET state = $1 , error_message = $2 , updated_at = $3 WHERE id = $4 ;

SELECT id , metadata , error_message, state , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at FROM fang_tasks WHERE id = $4 ;
