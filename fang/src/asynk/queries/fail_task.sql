UPDATE "fang_tasks" SET "state" = $1 , "error_message" = $2 , "updated_at" = $3 WHERE id = $4 RETURNING *
