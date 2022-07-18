SELECT * FROM (SELECT * FROM fang_tasks  ORDER BY created_at ASC ) ft WHERE ft.state = 'new' AND ft.task_type = $1 LIMIT 1 FOR UPDATE SKIP LOCKED
