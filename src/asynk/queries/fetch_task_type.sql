SELECT * FROM fang_tasks  WHERE task_type = $1 AND state = 'new' AND $2 >= scheduled_at  ORDER BY created_at ASC, scheduled_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
