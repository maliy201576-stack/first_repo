---
inclusion: always
---

# Build, Test & Run Commands

## Install dependencies

```bash
pip install -e ".[dev]"
```

## Run all unit tests

```bash
python -m pytest tests/unit/ -v
```

## Run a specific test file

```bash
python -m pytest tests/unit/test_keyword_filter.py -v
```

## Run only property-based tests (Hypothesis)

```bash
python -m pytest tests/unit/ -v -k "property or hypothesis"
```

## Run tests with short output

```bash
python -m pytest tests/unit/ -q
```

## Run integration tests (requires Docker)

```bash
python -m pytest tests/integration/ -v
```

## Apply database migration

```bash
psql $DATABASE_URL -f migrations/001_init.sql
```

## Start the API server (manual — do not run in agent)

```bash
uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

## Docker Compose

```bash
docker compose up -d          # Start all services
docker compose logs -f        # Follow logs
docker compose down           # Stop and remove containers
docker compose down -v        # Stop and remove containers + volumes
```

Services: postgres (16-alpine), redis (7-alpine), api (:8000), worker_tg, worker_web, notifier.
PostgreSQL data persisted via `pgdata` Docker volume. Migration applied automatically on first start via `docker-entrypoint-initdb.d`.

## Environment

All services read config from `.env` in the project root. See `.env.example` for a full template.
Required variables with no defaults: `DATABASE_URL`, `TG_API_ID`, `TG_API_HASH`, `NOTIFIER_BOT_TOKEN`, `NOTIFIER_CHAT_ID`.
Docker Compose overrides `DATABASE_URL` and `REDIS_URL` to use internal service names.
