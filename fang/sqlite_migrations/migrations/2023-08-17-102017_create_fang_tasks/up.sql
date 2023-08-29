-- Your SQL goes here


-- docker exec -ti mysql mysql -u root -pfang -P 3360 fang -e "$(catn fang/mysql_migrations/migrations/2023-08-17-102017_create_fang_tasks/up.sql)"

CREATE TABLE fang_tasks (
    id TEXT CHECK (LENGTH(id) = 36) NOT NULL PRIMARY KEY, -- UUID generated inside the language
    -- why uuid is a blob ? https://stackoverflow.com/questions/17277735/using-uuids-in-sqlite
    metadata TEXT NOT NULL, 
    -- why metadata is text ? https://stackoverflow.com/questions/16603621/how-to-store-json-object-in-sqlite-database#16603687
    error_message TEXT,
    state TEXT CHECK ( state IN ('new', 'in_progress', 'failed', 'finished', 'retried') ) NOT NULL DEFAULT 'new',
    -- why state is a text ? https://stackoverflow.com/questions/5299267/how-to-create-enum-type-in-sqlite#17203007
    task_type TEXT NOT NULL DEFAULT 'common',
    uniq_hash CHAR(64),
    retries INTEGER NOT NULL DEFAULT 0,
    -- The datetime() function returns the date and time as text in this formats: YYYY-MM-DD HH:MM:SS. 
    -- https://www.sqlite.org/lang_datefunc.html
    scheduled_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP || '.000000+00'),
    -- why timestamps are texts ? https://www.sqlite.org/datatype3.html
    created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP || '.000000+00'),
    updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP || '.000000+00')
);

CREATE INDEX fang_tasks_state_index ON fang_tasks(state);
CREATE INDEX fang_tasks_type_index ON fang_tasks(task_type);
CREATE INDEX fang_tasks_scheduled_at_index ON fang_tasks(scheduled_at);
CREATE INDEX fang_tasks_uniq_hash ON fang_tasks(uniq_hash);