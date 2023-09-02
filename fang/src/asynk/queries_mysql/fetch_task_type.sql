SELECT * FROM fang_tasks  WHERE task_type = ? AND state in ('new', 'retried') AND ? >= scheduled_at  ORDER BY created_at ASC, scheduled_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
