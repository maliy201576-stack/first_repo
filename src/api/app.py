"""FastAPI application with lifespan for database connection management."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.common.config import get_settings
from src.common.db import create_engine, create_session_factory

from src.api.routes.health import router as health_router
from src.api.routes.leads import router as leads_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage async engine lifecycle: create on startup, dispose on shutdown."""
    settings = get_settings()
    if not settings.DATABASE_URL:
        raise RuntimeError("API missing required env var: DATABASE_URL")
    engine = create_engine(settings.DATABASE_URL)
    app.state.async_session_factory = create_session_factory(engine)
    yield
    await engine.dispose()


app = FastAPI(title="Glukhov Sales Engine API", version="0.1.0", lifespan=lifespan)

app.include_router(health_router)
app.include_router(leads_router, prefix="/api/v1")
