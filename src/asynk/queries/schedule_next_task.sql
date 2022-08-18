UPDATE "fang_periodic_tasks" SET "scheduled_at" = $1 , "updated_at" = $2 RETURNING id , metadata , period_in_millis , scheduled_at , created_at , updated_at
