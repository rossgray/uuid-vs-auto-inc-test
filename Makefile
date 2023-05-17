.PHONY: run db

db:
	@echo "Starting docker container"
	@docker run --name postgres -e POSTGRES_PASSWORD=topsecret -p 127.0.0.1:5432:5432 -d postgres
	@sleep 10

run: db
	@echo "Running test..."
	@poetry run python -m uuid_vs_auto_inc_test.main
	@echo "Stopping docker container"
	@docker stop postgres
	@docker rm postgres
