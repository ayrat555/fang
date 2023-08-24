BOLD = '\033[1m'
END_BOLD = '\033[0m'

DB_TARGETS = db_postgres db_mysql db_sqlite
WAIT_TARGETS = wait_for_postgres wait_for_mysql wait_for_sqlite
DIESEL_TARGETS = diesel_postgres diesel_mysql diesel_sqlite
CLEAN_TARGETS = clean_postgres clean_mysql clean_sqlite
STOP_TARGETS = stop_postgres stop_mysql stop_sqlite

.PHONY: db $(DB_TARGETS) \
	$(WAIT_TARGETS) \
	diesel $(DIESEL_TARGETS) \
	clean $(CLEAN_TARGETS)
	stop $(STOP_TARGETS) \
	clippy tests ignored doc

.SILENT: $(DB_TARGETS) $(WAIT_TARGETS) $(DIESEL_TARGETS) $(CLEAN_TARGETS) $(STOP_TARGETS)

db: $(DB_TARGETS)

db_postgres:
	@echo -e $(BOLD)Setting up Postgres database...$(END_BOLD)
	docker run --rm -d --name postgres -p 5432:5432 \
		-e POSTGRES_DB=fang \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		postgres:latest
	$(MAKE) diesel_postgres

db_mysql:
	@echo -e $(BOLD)Setting up MySQL database...$(END_BOLD)
	docker run --rm -d --name mysql -p 3306:3306 \
		-e MYSQL_DATABASE=fang \
		-e MYSQL_ROOT_PASSWORD=mysql \
		-e TZ=UTC \
		mysql:latest
	$(MAKE) diesel_mysql

db_sqlite:
	@echo -e $(BOLD)Setting up SQLite database...$(END_BOLD)
	sqlite3 fang.db "VACUUM;"
	$(MAKE) diesel_sqlite

wait_for_postgres:
	@echo -e $(BOLD)Waiting for Postgres server to be up and running...$(END_BOLD)
	while ! docker exec postgres psql -U postgres --command='' 2> /dev/null; \
	do \
		sleep 1; \
	done

wait_for_mysql:
	@echo -e $(BOLD)Waiting for MySQL server to be up and running...$(END_BOLD)
	while ! docker exec mysql mysql --user=root --password=mysql --execute='' 2> /dev/null; \
	do \
		sleep 1; \
	done

wait_for_sqlite:
	@echo -e $(BOLD)Waiting for SQLite DB file to be created...$(END_BOLD)
	while [ ! -f fang.db ]; \
	do \
		sleep 1; \
	done

diesel: $(DIESEL_TARGETS)

diesel_postgres: wait_for_postgres
	@echo -e $(BOLD)Running Diesel migrations on Postgres database...$(END_BOLD)
	cd fang/postgres_migrations && \
	diesel migration run \
		--database-url postgres://postgres:postgres@127.0.0.1/fang

diesel_mysql: wait_for_mysql
	@echo -e $(BOLD)Running Diesel migrations on MySQL database...$(END_BOLD)
	cd fang/mysql_migrations && \
	diesel migration run \
		--database-url mysql://root:mysql@127.0.0.1/fang

diesel_sqlite: wait_for_sqlite
	@echo -e $(BOLD)Running Diesel migrations on SQLite database...$(END_BOLD)
	cd fang/sqlite_migrations && \
	diesel migration run \
		--database-url sqlite://../../fang.db

clean: $(CLEAN_TARGETS)

clean_postgres: wait_for_postgres
	@echo -e $(BOLD)Cleaning Postgres database...$(END_BOLD)
	docker exec postgres dropdb -U postgres fang
	docker exec postgres psql -U postgres --command="CREATE DATABASE fang;"
	$(MAKE) diesel_postgres

clean_mysql: wait_for_mysql
	@echo -e $(BOLD)Cleaning MySQL database...$(END_BOLD)
	docker exec mysql mysql --user=root --password=mysql --execute="DROP DATABASE fang; CREATE DATABASE fang;"
	$(MAKE) diesel_mysql

clean_sqlite: wait_for_sqlite
	@echo -e $(BOLD)Cleaning SQLite database...$(END_BOLD)
	$(MAKE) stop_sqlite db_sqlite

stop: $(STOP_TARGETS)

stop_postgres:
	@echo -e $(BOLD)Stopping Postgres database...$(END_BOLD)
	docker kill postgres

stop_mysql:
	@echo -e $(BOLD)Stopping MySQL database...$(END_BOLD)
	docker kill mysql

stop_sqlite:
	@echo -e $(BOLD)Stopping SQLite database...$(END_BOLD)
	rm fang.db

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
