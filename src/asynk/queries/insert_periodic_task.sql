INSERT INTO "fang_periodic_tasks" ("metadata", "scheduled_at", "period_in_millis") VALUES ($1, $2, $3) RETURNING id , metadata , period_in_millis , scheduled_at , created_at , updated_at
