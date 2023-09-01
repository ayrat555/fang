BEGIN

INSERT INTO fang_tasks (id, metadata, task_type, scheduled_at) VALUES (?, ?, ?, ?);

SELECT * FROM fang_tasks WHERE id = 'uuid';

END