-- Your SQL goes here


-- docker exec -ti mysql mysql -u root -pfang -P 3360 fang -e "$(catn fang/mysql_migrations/migrations/2023-08-17-102017_create_fang_tasks/up.sql)"

 /*
        why `metadata` and `error_message` are not a TEXT ?
        MySQL TEXT type, I think it is stored as a BLOB.
        So that breaks FromRow trait, implemented in lib.rs line 183
    */

CREATE TABLE fang_tasks (
    id BINARY(16) PRIMARY KEY,
    metadata VARCHAR(2048) NOT NULL,
    error_message VARCHAR(2048),
    state ENUM('new', 'in_progress', 'failed', 'finished', 'retried') NOT NULL DEFAULT 'new',
    task_type VARCHAR(255) NOT NULL DEFAULT 'common', -- TEXT type can not have default value, stupid MySQL policy
    uniq_hash VARCHAR(64),
    retries INTEGER NOT NULL DEFAULT 0,
    scheduled_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE INDEX fang_tasks_state_index ON fang_tasks(state);
CREATE INDEX fang_tasks_type_index ON fang_tasks(task_type);
CREATE INDEX fang_tasks_scheduled_at_index ON fang_tasks(scheduled_at);
CREATE INDEX fang_tasks_uniq_hash ON fang_tasks(uniq_hash);