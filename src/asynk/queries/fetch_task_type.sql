SELECT * FROM fang_tasks  WHERE task_type = $1 AND state = 'new' AND CURRENT_TIMESTAMP <= scheduled_at  ORDER BY created_at ASC, scheduled_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
