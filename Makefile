.PHONY: db db_postgres db_mysql db_sqlite \
	wait_for_postgres wait_for_mysql wait_for_sqlite \
	diesel diesel_postgres diesel_mysql diesel_sqlite \
	stop stop_postgres stop_mysql stop_sqlite \
	clippy tests ignored doc

.SILENT:

db: db_postgres db_mysql db_sqlite

db_postgres:
	docker run --rm -d --name postgres -p 5432:5432 \
		-e POSTGRES_DB=fang \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		postgres:latest
	$(MAKE) diesel_postgres

db_mysql:
	docker run --rm -d --name mysql -p 3306:3306 \
		-e MYSQL_DATABASE=fang \
		-e MYSQL_ROOT_PASSWORD=mysql \
		-e TZ=UTC \
		mysql:latest
	$(MAKE) diesel_mysql

db_sqlite:
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

diesel: diesel_postgres diesel_mysql diesel_sqlite

diesel_postgres: wait_for_postgres
	cd fang/postgres_migrations && \
	diesel migration run \
		--database-url postgres://postgres:postgres@127.0.0.1/fang

diesel_mysql: wait_for_mysql
	cd fang/mysql_migrations && \
	diesel migration run \
		--database-url mysql://root:mysql@127.0.0.1/fang

diesel_sqlite: wait_for_sqlite
	cd fang/sqlite_migrations && \
	diesel migration run \
		--database-url sqlite://../../fang.db

clean: clean_postgres clean_mysql clean_sqlite

clean_postgres: wait_for_postgres
	docker exec postgres dropdb -U postgres fang
	docker exec postgres psql -U postgres --command="CREATE DATABASE fang;"
	$(MAKE) diesel_postgres

clean_mysql: wait_for_mysql
	docker exec mysql mysql --user=root --password=mysql --execute="DROP DATABASE fang; CREATE DATABASE fang;"
	$(MAKE) diesel_mysql

clean_sqlite: wait_for_sqlite stop_sqlite db_sqlite

stop: stop_postgres stop_mysql stop_sqlite

stop_mysql: 
	docker kill mysql

stop_postgres:
	docker kill postgres

stop_sqlite:
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
