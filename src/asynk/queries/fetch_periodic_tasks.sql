SELECT * FROM fang_periodic_tasks WHERE schedule_at BETWEEN $1 AND $2 OR schedule_at IS NULL
