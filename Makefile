db_postgres:
	docker run --rm -d --name postgres -p 5432:5432 \
		-e POSTGRES_DB=fang \
		-e POSTGRES_USER=postgres \
		-e POSTGRES_PASSWORD=postgres \
		postgres:latest


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

diesel_postgres:
	cd fang/postgres_migrations && \
	diesel migration run \
		--database-url postgres://postgres:postgres@localhost/fang \

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

tests:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture

ignored:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture --ignored

doc:
	cargo doc --open
