-- Your SQL goes here


-- docker exec -ti mysql mysql -u root -pfang -P 3360 fang -e "$(catn fang/mysql_migrations/migrations/2023-08-17-102017_create_fang_tasks/up.sql)"

CREATE TABLE fang_tasks (
    id VARCHAR(36) DEFAULT (uuid()) PRIMARY KEY,
    metadata JSON NOT NULL,
    error_message TEXT,
    state ENUM('new', 'in_progress', 'failed', 'finished', 'retried') NOT NULL DEFAULT 'new',
    task_type VARCHAR(255) NOT NULL DEFAULT 'common', -- TEXT type can not have default value, stupid MySQL policy
    uniq_hash CHAR(64),
    retries INTEGER NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX fang_tasks_state_index ON fang_tasks(state);
CREATE INDEX fang_tasks_type_index ON fang_tasks(task_type);
CREATE INDEX fang_tasks_scheduled_at_index ON fang_tasks(scheduled_at);
CREATE INDEX fang_tasks_uniq_hash ON fang_tasks(uniq_hash);