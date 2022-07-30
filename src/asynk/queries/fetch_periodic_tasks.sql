SELECT * FROM fang_periodic_tasks WHERE scheduled_at BETWEEN $1 AND $2 OR scheduled_at IS NULL
