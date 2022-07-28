INSERT INTO "fang_periodic_tasks" ("metadata", "period_in_seconds") VALUES ($1, $2) RETURNING id , metadata , period_in_seconds , scheduled_at , created_at , updated_at
