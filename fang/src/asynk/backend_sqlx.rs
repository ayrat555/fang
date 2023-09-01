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

const INSERT_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/insert_task.sql");
const INSERT_TASK_UNIQ_QUERY_MYSQL: &str = include_str!("queries_mysql/insert_task_uniq.sql");
const UPDATE_TASK_STATE_QUERY_MYSQL: &str = include_str!("queries_mysql/update_task_state.sql");
const FAIL_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/fail_task.sql");
const REMOVE_ALL_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/remove_all_tasks.sql");
const REMOVE_ALL_SCHEDULED_TASK_QUERY_MYSQL: &str =
    include_str!("queries_mysql/remove_all_scheduled_tasks.sql");
const REMOVE_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/remove_task.sql");
const REMOVE_TASK_BY_METADATA_QUERY_MYSQL: &str =
    include_str!("queries_mysql/remove_task_by_metadata.sql");
const REMOVE_TASKS_TYPE_QUERY_MYSQL: &str = include_str!("queries_mysql/remove_tasks_type.sql");
const FETCH_TASK_TYPE_QUERY_MYSQL: &str = include_str!("queries_mysql/fetch_task_type.sql");
const FIND_TASK_BY_UNIQ_HASH_QUERY_MYSQL: &str =
    include_str!("queries_mysql/find_task_by_uniq_hash.sql");
const FIND_TASK_BY_ID_QUERY_MYSQL: &str = include_str!("queries_mysql/find_task_by_id.sql");
const RETRY_TASK_QUERY_MYSQL: &str = include_str!("queries_mysql/retry_task.sql");

#[derive(Debug, Clone)]
pub(crate) enum BackendSqlX {
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

    pub(crate) fn select_query<'a>(&self, query: SqlXQuery) -> &'a str {
        match self {
            BackendSqlX::Pg => BackendSqlXPg::select_query(query),
            BackendSqlX::Sqlite => BackendSqlXSQLite::select_query(query),
            BackendSqlX::Mysql => BackendSqlXMySQL::select_query(query),
            _ => unreachable!(),
        }
    }

    // I think it is useful to have this method, although it is not used
    pub(crate) fn _name(&self) -> &str {
        match self {
            BackendSqlX::Pg => BackendSqlXPg::_name(),
            BackendSqlX::Sqlite => BackendSqlXSQLite::_name(),
            BackendSqlX::Mysql => BackendSqlXMySQL::_name(),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SqlXQuery {
    InsertTask,
    InsertTaskUniq,
    UpdateTaskState,
    FailTask,
    RemoveAllTask,
    RemoveAllScheduledTask,
    RemoveTask,
    RemoveTaskByMetadata,
    RemoveTaskType,
    FetchTaskType,
    FindTaskByUniqHash,
    FindTaskById,
    RetryTask,
}

#[derive(Debug, Clone)]
struct BackendSqlXPg {}

use SqlXQuery as Q;
impl BackendSqlXPg {
    fn select_query(query: SqlXQuery) -> &'static str {
        match query {
            Q::InsertTask => INSERT_TASK_QUERY_POSTGRES,
            Q::InsertTaskUniq => INSERT_TASK_UNIQ_QUERY_POSTGRES,
            Q::UpdateTaskState => UPDATE_TASK_STATE_QUERY_POSTGRES,
            Q::FailTask => FAIL_TASK_QUERY_POSTGRES,
            Q::RemoveAllTask => REMOVE_ALL_TASK_QUERY_POSTGRES,
            Q::RemoveAllScheduledTask => REMOVE_ALL_SCHEDULED_TASK_QUERY_POSTGRES,
            Q::RemoveTask => REMOVE_TASK_QUERY_POSTGRES,
            Q::RemoveTaskByMetadata => REMOVE_TASK_BY_METADATA_QUERY_POSTGRES,
            Q::RemoveTaskType => REMOVE_TASKS_TYPE_QUERY_POSTGRES,
            Q::FetchTaskType => FETCH_TASK_TYPE_QUERY_POSTGRES,
            Q::FindTaskByUniqHash => FIND_TASK_BY_UNIQ_HASH_QUERY_POSTGRES,
            Q::FindTaskById => FIND_TASK_BY_ID_QUERY_POSTGRES,
            Q::RetryTask => RETRY_TASK_QUERY_POSTGRES,
        }
    }

    fn _name() -> &'static str {
        "PostgreSQL"
    }
}

#[derive(Debug, Clone)]
struct BackendSqlXSQLite {}

impl BackendSqlXSQLite {
    fn select_query(query: SqlXQuery) -> &'static str {
        match query {
            Q::InsertTask => INSERT_TASK_QUERY_SQLITE,
            Q::InsertTaskUniq => INSERT_TASK_UNIQ_QUERY_SQLITE,
            Q::UpdateTaskState => UPDATE_TASK_STATE_QUERY_SQLITE,
            Q::FailTask => FAIL_TASK_QUERY_SQLITE,
            Q::RemoveAllTask => REMOVE_ALL_TASK_QUERY_SQLITE,
            Q::RemoveAllScheduledTask => REMOVE_ALL_SCHEDULED_TASK_QUERY_SQLITE,
            Q::RemoveTask => REMOVE_TASK_QUERY_SQLITE,
            Q::RemoveTaskByMetadata => REMOVE_TASK_BY_METADATA_QUERY_SQLITE,
            Q::RemoveTaskType => REMOVE_TASKS_TYPE_QUERY_SQLITE,
            Q::FetchTaskType => FETCH_TASK_TYPE_QUERY_SQLITE,
            Q::FindTaskByUniqHash => FIND_TASK_BY_UNIQ_HASH_QUERY_SQLITE,
            Q::FindTaskById => FIND_TASK_BY_ID_QUERY_SQLITE,
            Q::RetryTask => RETRY_TASK_QUERY_SQLITE,
        }
    }

    fn _name() -> &'static str {
        "SQLite"
    }
}

#[derive(Debug, Clone)]
struct BackendSqlXMySQL {}

impl BackendSqlXMySQL {
    fn select_query(query: SqlXQuery) -> &'static str {
        match query {
            Q::InsertTask => INSERT_TASK_QUERY_MYSQL,
            Q::InsertTaskUniq => INSERT_TASK_UNIQ_QUERY_MYSQL,
            Q::UpdateTaskState => UPDATE_TASK_STATE_QUERY_MYSQL,
            Q::FailTask => FAIL_TASK_QUERY_MYSQL,
            Q::RemoveAllTask => REMOVE_ALL_TASK_QUERY_MYSQL,
            Q::RemoveAllScheduledTask => REMOVE_ALL_SCHEDULED_TASK_QUERY_MYSQL,
            Q::RemoveTask => REMOVE_TASK_QUERY_MYSQL,
            Q::RemoveTaskByMetadata => REMOVE_TASK_BY_METADATA_QUERY_MYSQL,
            Q::RemoveTaskType => REMOVE_TASKS_TYPE_QUERY_MYSQL,
            Q::FetchTaskType => FETCH_TASK_TYPE_QUERY_MYSQL,
            Q::FindTaskByUniqHash => FIND_TASK_BY_UNIQ_HASH_QUERY_MYSQL,
            Q::FindTaskById => FIND_TASK_BY_ID_QUERY_MYSQL,
            Q::RetryTask => RETRY_TASK_QUERY_MYSQL,
        }
    }

    fn _name() -> &'static str {
        "MySQL"
    }
}
