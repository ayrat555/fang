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

#[derive(Debug, Clone)]
pub enum BackendSqlX {
    Pg,
    Sqlite,
    Mysql,
    NoBackend,
}

impl BackendSqlX {
    pub fn new_with_name(name: &str) -> BackendSqlX {
        match name {
            "PostgreSQL" => BackendSqlX::Pg,
            "SQLite" => BackendSqlX::Sqlite,
            "MySQL" => BackendSqlX::Mysql,
            _ => unreachable!(),
        }
    }

    pub fn select_query<'a>(&self, query: &'a str) -> &'a str {
        match self {
            BackendSqlX::Pg => BackendSqlXPg::select_query(query),
            BackendSqlX::Sqlite => BackendSqlXSQLite::select_query(query),
            BackendSqlX::Mysql => BackendSqlXMySQL::select_query(query),
            _ => unreachable!(),
        }
    }
    pub fn name(&self) -> &str {
        match self {
            BackendSqlX::Pg => BackendSqlXPg::name(),
            BackendSqlX::Sqlite => BackendSqlXSQLite::name(),
            BackendSqlX::Mysql => BackendSqlXMySQL::name(),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
struct BackendSqlXPg {}

impl BackendSqlXPg {
    fn select_query(query: &str) -> &str {
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

    fn name() -> &'static str {
        "PostgreSQL"
    }
}

#[derive(Debug, Clone)]
struct BackendSqlXSQLite {}

impl BackendSqlXSQLite {
    fn select_query(query: &str) -> &str {
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

    fn name() -> &'static str {
        "SQLite"
    }
}

#[derive(Debug, Clone)]
struct BackendSqlXMySQL {}

impl BackendSqlXMySQL {
    fn select_query(query: &str) -> &str {
        // TODO: MySQL queries
        let _query = match query {
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
        };

        todo!()
    }

    fn name() -> &'static str {
        "MySQL"
    }
}
