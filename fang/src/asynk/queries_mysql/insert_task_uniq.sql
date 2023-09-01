INSERT INTO fang_tasks ( id , metadata, task_type , uniq_hash, scheduled_at)
VALUES ($1, $2 , $3, $4, $5 ) ;


SELECT id , metadata , error_message, state , task_type , uniq_hash, retries , scheduled_at , created_at , updated_at FROM fang_tasks WHERE id = $1 ;
