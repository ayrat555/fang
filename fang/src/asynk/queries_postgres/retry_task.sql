UPDATE "fang_tasks" SET "state" = 'retried' , "error_message" = $1, "retries" = $2, scheduled_at = $3::timestamptz, "updated_at" = $4::timestamptz WHERE id = $5::uuid RETURNING id::text , metadata::text , error_message, state::text , task_type , uniq_hash, retries , scheduled_at::text , created_at::text , updated_at::text