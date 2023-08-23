db_postgres:
	docker run --rm -d --name postgres -p 5432:5432 \
		-e POSTGRES_DB=fang \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		postgres:latest

# login is root fang
db_mysql:
	docker run --rm -d --name mysql -p 3306:3306 \
		-e MYSQL_DATABASE=fang \
		-e MYSQL_ROOT_PASSWORD=fang \
		-e TZ=UTC \
		mysql:latest

db_sqlite:
	sqlite3 fang.db "VACUUM;"

clippy:
	cargo clippy --verbose --all-targets --all-features -- -D warnings

diesel_sqlite:
	diesel migration run \
		--database-url sqlite://../../fang.db \
		--migration-dir fang/sqlite_migrations

diesel_postgres:
	diesel migration run \
		--database-url postgres://postgres:postgres@localhost/fang \
		--migration-dir fang/postgres_migrations

diesel_mysql:
	diesel migration run \
		--database-url mysql://root:fang@127.0.0.1/fang \
		--migration-dir fang/mysql_migrations

stop_mysql: 
	docker kill mysql

stop_postgres:
	docker kill postgres

stop_sqlite:
	rm fang.db
tests:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture

ignored:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture --ignored

doc:
	cargo doc --open
