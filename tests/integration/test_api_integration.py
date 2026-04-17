"""Integration test: REST API → PostgreSQL with filtering and pagination.

Validates Requirement 5.2: API returns correctly filtered and paginated
results from a real PostgreSQL database (via testcontainers).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.common.db import Lead


@pytest_asyncio.fixture()
async def seeded_db(session_factory):
    """Insert a set of test leads into the database and wire the app."""
    leads_data = [
        dict(
            source="telegram",
            title="Python developer needed",
            matched_keywords=["python"],
            tags=["urgent"],
            status="new",
            discovered_at=datetime(2024, 7, 1, tzinfo=timezone.utc),
        ),
        dict(
            source="telegram",
            title="Rust backend engineer",
            matched_keywords=["rust", "backend"],
            tags=[],
            status="viewed",
            discovered_at=datetime(2024, 7, 2, tzinfo=timezone.utc),
        ),
        dict(
            source="fl.ru",
            title="Web application project",
            matched_keywords=[],
            tags=[],
            status="new",
            discovered_at=datetime(2024, 7, 3, tzinfo=timezone.utc),
        ),
        dict(
            source="fl.ru",
            title="Mobile app development",
            matched_keywords=[],
            tags=["urgent"],
            status="in_progress",
            discovered_at=datetime(2024, 7, 4, tzinfo=timezone.utc),
        ),
        dict(
            source="habr_freelance",
            title="Data pipeline project",
            matched_keywords=["python"],
            tags=[],
            status="rejected",
            discovered_at=datetime(2024, 7, 5, tzinfo=timezone.utc),
        ),
    ]

    async with session_factory() as session:
        async with session.begin():
            for data in leads_data:
                session.add(Lead(**data))

    app.state.async_session_factory = session_factory
    return leads_data


class TestAPIFilteringAndPagination:
    """REST API filtering and pagination against a real PostgreSQL."""

    async def test_list_all_leads(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/leads")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 5
        assert len(body["items"]) == 5

    async def test_filter_by_source(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/leads", params={"source": "telegram"})

        body = resp.json()
        assert body["total"] == 2
        assert all(item["source"] == "telegram" for item in body["items"])

    async def test_filter_by_status(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/leads", params={"status": "new"})

        body = resp.json()
        assert body["total"] == 2
        assert all(item["status"] == "new" for item in body["items"])

    async def test_filter_by_tags(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/leads", params={"tags": "urgent"})

        body = resp.json()
        assert body["total"] == 2
        assert all("urgent" in item["tags"] for item in body["items"])

    async def test_pagination(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/leads", params={"page": 1, "per_page": 2})

        body = resp.json()
        assert body["total"] == 5
        assert body["page"] == 1
        assert body["per_page"] == 2
        assert len(body["items"]) == 2

    async def test_pagination_page_2(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/leads", params={"page": 2, "per_page": 2})

        body = resp.json()
        assert body["page"] == 2
        assert len(body["items"]) == 2

    async def test_get_single_lead(self, seeded_db, session_factory):
        """GET /api/v1/leads/{id} returns the correct lead."""
        from sqlalchemy import select

        async with session_factory() as session:
            lead = (await session.execute(select(Lead).limit(1))).scalar_one()
            lead_id = str(lead.id)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/api/v1/leads/{lead_id}")

        assert resp.status_code == 200
        assert resp.json()["id"] == lead_id

    async def test_update_lead_status(self, seeded_db, session_factory):
        """PATCH /api/v1/leads/{id} updates status in the real database."""
        from sqlalchemy import select

        async with session_factory() as session:
            lead = (
                await session.execute(select(Lead).where(Lead.status == "new").limit(1))
            ).scalar_one()
            lead_id = str(lead.id)

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.patch(
                f"/api/v1/leads/{lead_id}",
                json={"status": "viewed"},
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "viewed"

        # Verify in database
        async with session_factory() as session:
            updated = (
                await session.execute(select(Lead).where(Lead.id == lead_id))
            ).scalar_one()
            assert updated.status == "viewed"

    async def test_health_endpoint(self, seeded_db):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "service_name" in body
