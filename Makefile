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
	@echo Setting up Postgres database...
	docker run --rm -d --name postgres -p 5432:5432 \
		-e POSTGRES_DB=fang \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		postgres:latest
	$(MAKE) diesel_postgres

db_mysql:
	@echo Setting up MySQL database...
	docker run --rm -d --name mysql -p 3306:3306 \
		-e MYSQL_DATABASE=fang \
		-e MYSQL_ROOT_PASSWORD=mysql \
		-e TZ=UTC \
		mysql:latest
	$(MAKE) diesel_mysql

db_sqlite:
	@echo Setting up SQLite database...
	sqlite3 fang.db "VACUUM;"
	$(MAKE) diesel_sqlite

wait_for_postgres:
	@echo Waiting for Postgres server to be up and running...
	while ! docker exec postgres psql -U postgres --command='' 2> /dev/null; \
	do \
		sleep 1; \
	done

wait_for_mysql:
	@echo Waiting for MySQL server to be up and running...
	while ! docker exec mysql mysql --user=root --password=mysql --execute='' 2> /dev/null; \
	do \
		sleep 1; \
	done

wait_for_sqlite:
	@echo Waiting for SQLite DB file to be created...
	while [ ! -f fang.db ]; \
	do \
		sleep 1; \
	done

diesel: $(DIESEL_TARGETS)

diesel_postgres: wait_for_postgres
	@echo Running Diesel migrations on Postgres database...
	cd fang/postgres_migrations && \
	diesel migration run \
		--database-url postgres://postgres:postgres@127.0.0.1/fang

diesel_mysql: wait_for_mysql
	@echo Running Diesel migrations on MySQL database...
	cd fang/mysql_migrations && \
	diesel migration run \
		--database-url mysql://root:mysql@127.0.0.1/fang

diesel_sqlite: wait_for_sqlite
	@echo Running Diesel migrations on SQLite database...
	cd fang/sqlite_migrations && \
	diesel migration run \
		--database-url sqlite://../../fang.db

clean: $(CLEAN_TARGETS)

clean_postgres: wait_for_postgres
	@echo Cleaning Postgres database...
	docker exec postgres dropdb -U postgres fang
	docker exec postgres psql -U postgres --command="CREATE DATABASE fang;"
	$(MAKE) diesel_postgres

clean_mysql: wait_for_mysql
	@echo Cleaning MySQL database...
	docker exec mysql mysql --user=root --password=mysql --execute="DROP DATABASE fang; CREATE DATABASE fang;"
	$(MAKE) diesel_mysql

clean_sqlite: wait_for_sqlite
	@echo Cleaning SQLite database...
	$(MAKE) stop_sqlite db_sqlite

stop: $(STOP_TARGETS)

stop_postgres:
	@echo Stopping Postgres database...
	docker kill postgres

stop_mysql:
	@echo Stopping MySQL database...
	docker kill mysql

stop_sqlite:
	@echo Stopping SQLite database...
	rm fang.db

clippy:
	cargo clippy --verbose --all-targets --all-features -- -D warnings

tests:
	cargo test --all-features -- --color always --nocapture
	$(MAKE) clean

ignored:
	cargo test --all-features -- --color always --nocapture --ignored
	$(MAKE) clean

doc:
	cargo doc --open
