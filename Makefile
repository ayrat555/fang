include .env

BOLD='\033[1m'
END_BOLD='\033[0m'

DB_TARGETS=db_postgres db_mysql db_sqlite
WAIT_TARGETS=wait_for_postgres wait_for_mysql wait_for_sqlite
DIESEL_TARGETS=diesel_postgres diesel_mysql diesel_sqlite
CLEAN_TARGETS=clean_postgres clean_mysql clean_sqlite
STOP_TARGETS=stop_postgres stop_mysql stop_sqlite

.DEFAULT_GOAL=default

default: db tests ignored stop

.PHONY: db $(DB_TARGETS) \
	$(WAIT_TARGETS) \
	diesel $(DIESEL_TARGETS) \
	clean $(CLEAN_TARGETS)
	stop $(STOP_TARGETS) \
	default clippy tests ignored doc .FORCE

.SILENT: $(DB_TARGETS) $(WAIT_TARGETS) $(DIESEL_TARGETS) $(CLEAN_TARGETS) $(STOP_TARGETS)

.NOTPARALLEL: default

db: $(DB_TARGETS)

db_postgres:
	@echo -e $(BOLD)Setting up Postgres database...$(END_BOLD)
	docker run --rm -d --name "$(POSTGRES_CONTAINER)" -p 5432:5432 \
		-e POSTGRES_DB="$(POSTGRES_DB)" \
		-e POSTGRES_USER="$(POSTGRES_USER)" \
		-e POSTGRES_PASSWORD="$(POSTGRES_PASSWORD)" \
		postgres:latest
	$(MAKE) diesel_postgres

db_mysql:
	@echo -e $(BOLD)Setting up MySQL database...$(END_BOLD)
	docker run --rm -d --name "$(MYSQL_CONTAINER)" -p 3306:3306 \
		-e MYSQL_DATABASE="$(MYSQL_DB)" \
		-e MYSQL_ROOT_PASSWORD="$(MYSQL_PASSWORD)" \
		-e TZ=UTC \
		mysql:latest
	$(MAKE) diesel_mysql

db_sqlite:
	@echo -e $(BOLD)Setting up SQLite database...$(END_BOLD)
	sqlite3 "$(SQLITE_FILE)" "VACUUM;"
	$(MAKE) diesel_sqlite

wait_for_postgres:
	@echo -e $(BOLD)Waiting for Postgres server to be up and running...$(END_BOLD)
	while ! docker exec "$(POSTGRES_CONTAINER)" psql -U "$(POSTGRES_USER)" --command='' 2> /dev/null; \
	do \
		sleep 1; \
	done

wait_for_mysql:
	@echo -e $(BOLD)Waiting for MySQL server to be up and running...$(END_BOLD)
	while ! docker exec "$(MYSQL_CONTAINER)" mysql --user="$(MYSQL_USER)" --password="$(MYSQL_PASSWORD)" --execute='' 2> /dev/null; \
	do \
		sleep 1; \
	done

wait_for_sqlite:
	@echo -e $(BOLD)Waiting for SQLite DB file to be created...$(END_BOLD)
	while [ ! -f "$(SQLITE_FILE)" ]; \
	do \
		sleep 1; \
	done

diesel: $(DIESEL_TARGETS)

diesel_postgres: wait_for_postgres
	@echo -e $(BOLD)Running Diesel migrations on Postgres database...$(END_BOLD)
	diesel migration run \
		--database-url "$(POSTGRES_URL)" \
		--migration-dir "$(POSTGRES_MIGRATIONS)" \
		--config-file "$(POSTGRES_CONFIG)"

diesel_mysql: wait_for_mysql
	@echo -e $(BOLD)Running Diesel migrations on MySQL database...$(END_BOLD)
	diesel migration run \
		--database-url "$(MYSQL_URL)" \
		--migration-dir "$(MYSQL_MIGRATIONS)" \
		--config-file "$(MYSQL_CONFIG)"

diesel_sqlite: wait_for_sqlite
	@echo -e $(BOLD)Running Diesel migrations on SQLite database...$(END_BOLD)
	diesel migration run \
		--database-url sqlite://"$(SQLITE_FILE)" \
		--migration-dir "$(SQLITE_MIGRATIONS)" \
		--config-file "$(SQLITE_CONFIG)"

clean: $(CLEAN_TARGETS)

clean_postgres: wait_for_postgres
	@echo -e $(BOLD)Cleaning Postgres database...$(END_BOLD)
	docker exec "$(POSTGRES_CONTAINER)" dropdb -U "$(POSTGRES_USER)" "$(POSTGRES_DB)"
	docker exec "$(POSTGRES_CONTAINER)" psql -U "$(POSTGRES_USER)" --command="CREATE DATABASE $(POSTGRES_DB);"
	$(MAKE) diesel_postgres

clean_mysql: wait_for_mysql
	@echo -e $(BOLD)Cleaning MySQL database...$(END_BOLD)
	docker exec "$(MYSQL_CONTAINER)" mysql \
		--user="$(MYSQL_USER)" \
		--password="$(MYSQL_PASSWORD)" \
		--execute="DROP DATABASE $(MYSQL_DB); CREATE DATABASE $(MYSQL_DB);"
	$(MAKE) diesel_mysql

clean_sqlite: wait_for_sqlite
	@echo -e $(BOLD)Cleaning SQLite database...$(END_BOLD)
	$(MAKE) stop_sqlite db_sqlite

stop: $(STOP_TARGETS)

stop_postgres:
	@echo -e $(BOLD)Stopping Postgres database...$(END_BOLD)
	docker kill "$(POSTGRES_CONTAINER)"

stop_mysql:
	@echo -e $(BOLD)Stopping MySQL database...$(END_BOLD)
	docker kill "$(MYSQL_CONTAINER)"

stop_sqlite:
	@echo -e $(BOLD)Stopping SQLite database...$(END_BOLD)
	rm "$(SQLITE_FILE)"

clippy:
	cargo clippy --verbose --all-targets --all-features -- -D warnings

tests:
	@echo -e $(BOLD)Running tests...$(END_BOLD)
	cargo test --all-features -- --color always --nocapture
	$(MAKE) clean

ignored:
	@echo -e $(BOLD)Running ignored tests...$(END_BOLD)
	cargo test --all-features -- --color always --nocapture --ignored
	$(MAKE) clean

doc:
	cargo doc --package fang --open
