CREATE TABLE fang_periodic_tasks (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  metadata jsonb NOT NULL,
  period_in_seconds INTEGER NOT NULL,
  scheduled_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX fang_periodic_tasks_scheduled_at_index ON fang_periodic_tasks(scheduled_at);
CREATE INDEX fang_periodic_tasks_metadata_index ON fang_periodic_tasks(metadata);
