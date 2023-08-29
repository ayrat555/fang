use std::fmt::Debug;

const INSERT_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_POSTGRES: &str = include_str!("queries_postgres/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/update_task_state.sql");
const FAIL_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_POSTGRES: &str = include_str!("queries_postgres/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_POSTGRES: &str =
    include_str!("queries_postgres/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_POSTGRES: &str = include_str!("queries_postgres/find_task_by_id.sql");
const RETRY_TASK_QUERY_POSTGRES: &str = include_str!("queries_postgres/retry_task.sql");

const INSERT_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_SQLITE: &str = include_str!("queries_sqlite/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_SQLITE: &str = include_str!("queries_sqlite/update_task_state.sql");
const FAIL_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE: &str =
    include_str!("queries_sqlite/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_SQLITE: &str =
    include_str!("queries_sqlite/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_SQLITE: &str = include_str!("queries_sqlite/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_SQLITE: &str = include_str!("queries_sqlite/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_SQLITE: &str =
    include_str!("queries_sqlite/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_SQLITE: &str = include_str!("queries_sqlite/find_task_by_id.sql");
const RETRY_TASK_QUERY_SQLITE: &str = include_str!("queries_sqlite/retry_task.sql");

pub trait BackendSqlX: Send + Debug + Clone + Sync {
    fn select_query(&self, query: &str) -> &str;
    fn name(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct BackendSqlXPg {}

impl BackendSqlX for BackendSqlXPg {
    fn select_query(&self, query: &str) -> &str {
        match query {
            "INSERT_TASK_QUERY" => INSERT_TASK_QUERY_POSTGRES,
            "INSERT_TASK_UNIQ_QUERY" => INSERT_TASK_UNIQ_QUERY_POSTGRES,
            "UPDATE_TASK_STATE_QUERY" => UPDATE_TASK_STATE_QUERY_POSTGRES,
            "FAIL_TASK_QUERY" => FAIL_TASK_QUERY_POSTGRES,
            "REMOVE_ALL_TASK_QUERY" => REMOVE_ALL_TASK_QUERY_POSTGRES,
            "REMOVE_ALL_SCHEDULED_TASK_QUERY" => REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES,
            "REMOVE_TASK_QUERY" => REMOVE_TASK_QUERY_POSTGRES,
            "REMOVE_TASK_BY_METADATA_QUERY" => REMOVE_TASK_BY_METADATA_QUERY_POSTGRES,
            "REMOVE_TASKS_TYPE_QUERY" => REMOVE_TASKS_TYPE_QUERY_POSTGRES,
            "FETCH_TASK_TYPE_QUERY" => FETCH_TASK_TYPE_QUERY_POSTGRES,
            "FIND_TASK_BY_UNIQ_HASH_QUERY" => FIND_TASK_BY_UNIQ_HASH_QUERY_POSTGRES,
            "FIND_TASK_BY_ID_QUERY" => FIND_TASK_BY_ID_QUERY_POSTGRES,
            "RETRY_TASK_QUERY" => RETRY_TASK_QUERY_POSTGRES,
            _ => unreachable!(),
        }
    }

    fn name(&self) -> &str {
        "PostgreSQL"
    }
}

#[derive(Debug, Clone)]
pub struct BackendSqlXSQLite {}

impl BackendSqlX for BackendSqlXSQLite {
    fn select_query(&self, query: &str) -> &str {
        match query {
            "INSERT_TASK_QUERY" => INSERT_TASK_QUERY_SQLITE,
            "INSERT_TASK_UNIQ_QUERY" => INSERT_TASK_UNIQ_QUERY_SQLITE,
            "UPDATE_TASK_STATE_QUERY" => UPDATE_TASK_STATE_QUERY_SQLITE,
            "FAIL_TASK_QUERY" => FAIL_TASK_QUERY_SQLITE,
            "REMOVE_ALL_TASK_QUERY" => REMOVE_ALL_TASK_QUERY_SQLITE,
            "REMOVE_ALL_SCHEDULED_TASK_QUERY" => REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE,
            "REMOVE_TASK_QUERY" => REMOVE_TASK_QUERY_SQLITE,
            "REMOVE_TASK_BY_METADATA_QUERY" => REMOVE_TASK_BY_METADATA_QUERY_SQLITE,
            "REMOVE_TASKS_TYPE_QUERY" => REMOVE_TASKS_TYPE_QUERY_SQLITE,
            "FETCH_TASK_TYPE_QUERY" => FETCH_TASK_TYPE_QUERY_SQLITE,
            "FIND_TASK_BY_UNIQ_HASH_QUERY" => FIND_TASK_BY_UNIQ_HASH_QUERY_SQLITE,
            "FIND_TASK_BY_ID_QUERY" => FIND_TASK_BY_ID_QUERY_SQLITE,
            "RETRY_TASK_QUERY" => RETRY_TASK_QUERY_SQLITE,
            _ => unreachable!(),
        }
    }

    fn name(&self) -> &str {
        "SQLite"
    }
}
