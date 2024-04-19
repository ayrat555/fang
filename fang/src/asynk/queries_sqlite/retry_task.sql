UPDATE "fang_tasks" SET "state" = 'retried' , "error_message" = $1, "retries" = $2, scheduled_at = $3, "updated_at" = $4 WHERE id = $5 RETURNING *
