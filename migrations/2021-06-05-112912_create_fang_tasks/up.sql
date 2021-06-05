CREATE TABLE fang_tasks (
     id BIGSERIAL primary key,
     metadata jsonb,
     created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
