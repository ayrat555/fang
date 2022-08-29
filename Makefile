db:
	docker run --rm -d --name postgres -p 5432:5432 \
  -e POSTGRES_DB=fang \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  postgres:latest
diesel:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang diesel migration run
stop:
	docker kill postgres
tests:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture

ignored:
	DATABASE_URL=postgres://postgres:postgres@localhost/fang cargo test --all-features -- --color always --nocapture --ignored
