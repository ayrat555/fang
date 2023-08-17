db_postgres:
	docker run --rm -d --name postgres -p 5432:5432 \
  -e POSTGRES_DB=fang \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest

clippy:
	cargo clippy --verbose --all-targets --all-features -- -D warnings
diesel_postgres:
	cd fang/postgres_migrations && DATABASE_URL=postgres://postgres:postgres@localhost/fang diesel migration run

diesel_mysql:
	cd fang/mysql_migrations && DATABASE_URL=mysql://root:fang@127.0.0.1/fang diesel setup

# login is root fang
db_mysql:
	docker run --network=host --rm -d --name mysql \
  	-e MYSQL_DATABASE=fang \
  	-e MYSQL_ROOT_PASSWORD=fang \
  	mysql:latest

stop_mysql: 
	docker kill mysql

stop_postgres:
	docker kill postgres
tests:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture

ignored:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture --ignored

doc:
	cargo doc --open
