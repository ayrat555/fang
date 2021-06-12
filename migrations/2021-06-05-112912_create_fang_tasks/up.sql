CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE fang_task_state AS ENUM ('new', 'in_progress', 'failed', 'finished');

CREATE TABLE fang_tasks (
     id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
     metadata jsonb NOT NULL,
     error_message TEXT,
     state fang_task_state default 'new' NOT NULL,
     created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX fang_tasks_created_at_index ON fang_tasks(created_at);
