INSERT INTO "fang_tasks" ("id", "metadata", "task_type", "scheduled_at") VALUES ($1, to_json($2), $3, $4::timestamptz ) RETURNING id , metadata::text , error_message, state::text , task_type , uniq_hash, retries , scheduled_at::text , created_at::text , updated_at::text
