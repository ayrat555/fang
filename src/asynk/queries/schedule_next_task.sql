UPDATE "fang_periodic_tasks" SET "scheduled_at" = $1 , "updated_at" = $2 RETURNING id , metadata , period_in_seconds , scheduled_at , created_at , updated_at
