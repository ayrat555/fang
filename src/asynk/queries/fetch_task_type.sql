SELECT * FROM fang_tasks  WHERE state = 'new' AND task_type = $1 ORDER BY created_at ASC  LIMIT 1 FOR UPDATE SKIP LOCKED
