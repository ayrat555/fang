.PHONY: db_postgres db_mysql db_sqlite \
	wait_for_postgres \
	diesel_postgres diesel_mysql diesel_sqlite \
	stop_postgres stop_mysql stop_sqlite \
	clippy tests ignored doc

db_postgres:
	docker run --rm -d --name postgres -p 5432:5432 \
		-e POSTGRES_DB=fang \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		postgres:latest

db_mysql:
	docker run --rm -d --name mysql -p 3306:3306 \
		-e MYSQL_DATABASE=fang \
		-e MYSQL_ROOT_PASSWORD=mysql \
		-e TZ=UTC \
		mysql:latest

db_sqlite:
	sqlite3 fang.db "VACUUM;"

wait_for_postgres:
	@echo Waiting for Postgres server to be up and running...
	while ! docker exec postgres psql -U fang --command=';' 2> /dev/null; \
	do \
		sleep 1; \
	done

diesel_postgres: db_postgres wait_for_postgres
	cd fang/postgres_migrations && \
	diesel migration run \
		--database-url postgres://fang:fang@localhost/fang \

diesel_mysql:
	cd fang/mysql_migrations && \
	diesel migration run \
		--database-url mysql://root:fang@127.0.0.1/fang \

diesel_sqlite:
	cd fang/sqlite_migrations && \
	diesel migration run \
		--database-url sqlite://../../fang.db \

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

ignored:
	cargo test --all-features -- --color always --nocapture --ignored

doc:
	cargo doc --open
