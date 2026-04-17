---
inclusion: always
---

# Coding Standards

## Language & Style

- Python 3.12+ — use modern syntax: `str | None`, `list[str]`, `type[X]`
- All modules start with a module-level docstring
- All public classes and functions have docstrings (Google style with Args/Returns)
- Use `from __future__ import annotations` for forward references
- Type hints on all function signatures (use `# noqa: ANN001` only for Playwright page objects and async callables where typing is impractical)

## Async Patterns

- All I/O operations are async (asyncpg, Redis, Telethon, Playwright)
- Use `async with session_factory() as session:` for database access
- Use `async with session.begin():` when writes are needed
- Graceful shutdown: cancel tasks, await them with `CancelledError` handling, close resources

## Error Handling

- Workers catch exceptions per-item and continue processing the rest
- All `except Exception` blocks log via `logger.exception(...)` before any other action
- Notifier calls are wrapped in their own try/except — notification failure must never crash a worker
- Redis failures fall back gracefully (dedup falls back to PostgreSQL, proxy pool treats all as available)

## Imports

- Group: stdlib → third-party → local (`src.common`, `src.dedup`, etc.)
- Use absolute imports from `src.*`

## Models

- Pydantic v2 BaseModel for all DTOs (LeadCandidate, LeadResponse, etc.)
- SQLAlchemy 2.0 declarative with `Mapped[]` and `mapped_column()` for ORM models
- Enums inherit from `(str, Enum)` for JSON serialization

## Testing

- Property-based tests use Hypothesis with `@settings(max_examples=100)` minimum
- Each property test has a comment: `# Feature: glukhov-sales-engine, Property N: description`
- Unit tests use pytest + pytest-asyncio (asyncio_mode = "auto")
- Mock external services (Telethon, Playwright, Redis, Bot API) — never make real network calls in unit tests
- Use `AsyncMock` for async dependencies, `MagicMock` for sync ones

## Configuration

- All settings via environment variables, loaded through `src/common/config.py` (Pydantic Settings)
- Sensitive values (tokens, DB URLs) go in `.env` (gitignored)
- Non-secret defaults are defined in the Settings class
- Worker-specific config (channels, keywords) in `config/channels.yaml`
