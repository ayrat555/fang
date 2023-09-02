SELECT * FROM fang_tasks WHERE uniq_hash = $1 AND state in ('new', 'retried') LIMIT 1
