# Changelog

## Not released

### Added

- Add retries for tasks - [#92](https://github.com/ayrat555/fang/pull/92)

## 0.9.1 (2022-09-14)

### Added

- Add methods to remove a task by uniq hash or id - [#90](https://github.com/ayrat555/fang/pull/90)

## 0.9.0 (2022-09-02)

###  [#69](https://github.com/ayrat555/fang/pull/69)

### Added

- Added cron support for scheduled tasks.

### Improved
- Major refactoring of the blocking module.
- Delete the graceful shutdown in blocking module.
- Simplify database schema.
- Re-write scheduled tasks in asynk module.
- Re-write the errors in both modules.
- Update diesel crate to 2.0 version.
- Update uuid crate to 1.1 version.

## 0.8.0 (2022-08-18)

- Use Duration in SleepParams and schedulers - [#67](https://github.com/ayrat555/fang/pull/67)

## 0.7.2 (2022-08-16)

- Set task_type when starting a worker pool - [#66](https://github.com/ayrat555/fang/pull/66)

## 0.7.1 (2022-08-04)

- Fix a conflict in exports of the `blocking` and the `asynk` features - [#61](https://github.com/ayrat555/fang/pull/61)

## 0.7.0 (2022-08-03)

### [#21](https://github.com/ayrat555/fang/pull/21)

- Create a full functional async processing module for fang which uses `tokio` to summon processes that are more lighter than threads.
- Rename Job starting names into Task [#29](https://github.com/ayrat555/fang/pull/29).
- Export serde correctly - [commit](https://github.com/ayrat555/fang/pull/21/commits/cf2ce19c970e93d71e387526e882c67db53cea18).
- Implement Postgres backend for async.

## 0.6.0 (2022-07-01)

- Rust 1.62 is required for `typetag`

## 0.5.0 (2021-12-05)

- Add graceful shutdown - [#14](https://github.com/ayrat555/fang/pull/14)

## 0.4.2 (2021-11-30)

- Bump deps - [#13](https://github.com/ayrat555/fang/pull/13)

## 0.4.1 (2021-08-22)

- Do not use env-logger - [#11](https://github.com/ayrat555/fang/pull/11)

## 0.4.0 (2021-07-31)

### [#8](https://github.com/ayrat555/fang/pull/8):

- Maintain ConnectionPool for Worker Threads
- Rename Postgres into Queue
- Pass PgConnection into run function
- Add function to remove all tasks of the specified type

## 0.3.1 (2021-07-24)

- Add periodic tasks - [#5](https://github.com/ayrat555/fang/pull/5), [#7](https://github.com/ayrat555/fang/pull/7)

## 0.3.1 (2021-07-11)

- Simplify usage by re-exporting serde traits - [#3](https://github.com/ayrat555/fang/pull/3)

## 0.3.0 (2021-07-04)

- Execute different types of tasks in separate workers - [#1](https://github.com/ayrat555/fang/pull/1)
- Add retention mode for tasks - [#2](https://github.com/ayrat555/fang/pull/2)

## 0.2.0 (2021-06-24)

- The first release on crates.io
