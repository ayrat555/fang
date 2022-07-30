INSERT INTO "fang_periodic_tasks" ("metadata", "scheduled_at", "period_in_seconds") VALUES ($1, $2, $3) RETURNING id , metadata , period_in_seconds , scheduled_at , created_at , updated_at
