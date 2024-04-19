SELECT * FROM fang_tasks WHERE uniq_hash = ? AND state in ('new', 'retried') LIMIT 1
