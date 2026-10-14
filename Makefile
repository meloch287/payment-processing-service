COMPOSE := docker compose

.PHONY: up down logs ps migrate lint fmt typecheck test verify e2e-compose

up: .env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f api consumer

ps:
	$(COMPOSE) ps

migrate:
	$(COMPOSE) exec api alembic upgrade head

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy app tests

test:
	uv run pytest

verify:
	bash scripts/verify.sh

e2e-compose:
	bash scripts/e2e-compose.sh

.env:
	cp .env.example .env
