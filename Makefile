COMPOSE := docker compose -f deploy/compose.yaml

.PHONY: up scale down test integration-test seed chaos-demo benchmark logs observability

up:
	$(COMPOSE) up -d --build --wait

scale:
	$(COMPOSE) up -d --scale worker=3

down:
	$(COMPOSE) down

test:
	$(COMPOSE) run --rm api-test python -m pytest
	$(COMPOSE) run --rm worker-test go test -race ./...
	$(COMPOSE) run --rm dashboard-test npm test -- --run

integration-test:
	python -m pytest tests/integration tests/concurrency -v

seed:
	python tests/seed_demo.py

chaos-demo:
	python -m pytest tests/chaos -v

benchmark:
	python tests/benchmark/run.py

logs:
	$(COMPOSE) logs -f api scheduler worker dashboard

observability:
	$(COMPOSE) --profile observability up -d --wait
