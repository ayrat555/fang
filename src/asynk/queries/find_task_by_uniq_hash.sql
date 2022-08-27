SELECT * FROM fang_tasks WHERE uniq_hash = $1 AND state = 'new' LIMIT 1
