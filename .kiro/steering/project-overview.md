---
inclusion: always
---

# Glukhov Sales Engine — Project Overview

## What is this project?

Glukhov Sales Engine (MVP) is an automated lead collection system for GlukhovSystems, a software development studio. It monitors Telegram channels, freelance marketplaces (FL.ru, Habr Freelance), and Russian government procurement portal (zakupki.gov.ru) to discover potential project leads.

## Tech Stack

- Python 3.12
- FastAPI + uvicorn (REST API)
- Telethon (Telegram monitoring via MTProto)
- Playwright + BeautifulSoup 4 (web scraping)
- RapidFuzz (fuzzy string matching for deduplication)
- PostgreSQL 16 + asyncpg + SQLAlchemy 2.0 async
- Redis (hash cache, proxy block list)
- python-telegram-bot (error notifications via Bot API)
- Pydantic v2 + pydantic-settings (models, config)
- Docker Compose (orchestration)

## Project Structure

```
src/
├── common/          # Shared: config, models, enums, db, logging
├── worker_tg/       # Telegram channel monitor (Telethon) — __main__.py entry point
├── worker_web/      # Web scraper (Playwright) + parsers/ — __main__.py entry point
├── dedup/           # Two-level deduplication service (Redis + PostgreSQL)
├── api/             # FastAPI REST API + routes/
└── notifier/        # Telegram Bot API error notifications — __main__.py entry point
config/              # channels.yaml, proxies.txt
migrations/          # SQL DDL migrations
Dockerfile.api       # Multi-stage build for REST API
Dockerfile.worker_tg # Multi-stage build for Telegram worker
Dockerfile.worker_web# Multi-stage build for web scraper (includes Playwright/Chromium)
Dockerfile.notifier  # Multi-stage build for notifier
docker-compose.yml   # Full stack: postgres, redis, api, worker_tg, worker_web, notifier
tests/
├── unit/            # Unit + property-based tests (Hypothesis)
└── integration/     # End-to-end tests (testcontainers)
```

## Key Architectural Decisions

1. Each worker runs in its own Docker container — independent failure domains
2. Two-level deduplication: fast SHA-256 hash check in Redis, then fuzzy title match (RapidFuzz token_sort_ratio ≥ 85%) in PostgreSQL
3. Redis fallback: when Redis is unavailable, hash checks fall back to PostgreSQL
4. Hot-reloadable config: channels.yaml can be updated without restarting Worker_TG
5. Proxy rotation: round-robin with Redis-backed block list (1h TTL)
6. All config via environment variables (.env) loaded through Pydantic Settings

## Data Flow

Source → Worker (TG/Web) → Keyword/Category Filter → DedupService → PostgreSQL
                                                                   ↗ Redis (hash cache)
Errors → Notifier → Telegram Bot API → Admin chat

## Database

PostgreSQL tables: `leads` (main), `lead_hashes` (dedup). See `migrations/001_init.sql`.
Enums: LeadStatus (new/viewed/in_progress/rejected), LeadSource, LeadTag (urgent/normal).

## Testing

- pytest + pytest-asyncio
- Hypothesis for property-based tests (10 correctness properties, ≥100 iterations each)
- httpx for async API testing
- testcontainers for integration tests
- respx for HTTP mocking
