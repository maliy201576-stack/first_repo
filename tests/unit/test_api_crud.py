"""Unit tests for REST API CRUD operations, pagination, and health endpoint.

Validates: Requirements 5.2, 5.3, 7.3

Covers:
- GET /api/v1/leads — list with pagination fields
- GET /api/v1/leads — pagination (page/per_page)
- GET /api/v1/leads — filtering by source and status
- GET /api/v1/leads/{id} — single lead retrieval and 404
- PATCH /api/v1/leads/{id} — status update, 404, and 422
- GET /health — correct HealthResponse format
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.api.app import app
from src.common.enums import LeadStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


def _fake_lead(
    *,
    lead_id: str | None = None,
    source: str = "telegram",
    title: str = "Test lead",
    status: str = "new",
    tags: list[str] | None = None,
) -> MagicMock:
    """Return a MagicMock that behaves like a SQLAlchemy Lead row."""
    lead = MagicMock()
    lead.id = lead_id or str(uuid.uuid4())
    lead.source = source
    lead.title = title
    lead.description = None
    lead.url = None
    lead.budget = None
    lead.category = None
    lead.matched_keywords = []
    lead.tags = tags or []
    lead.status = status
    lead.okpd2_codes = None
    lead.max_contract_price = None
    lead.submission_deadline = None
    lead.discovered_at = _NOW
    lead.created_at = _NOW
    lead.updated_at = _NOW
    return lead


def _mock_session_factory(session: AsyncMock) -> MagicMock:
    """Wrap an AsyncMock session into a factory compatible with app.state."""
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return factory


# ---------------------------------------------------------------------------
# 1. GET /api/v1/leads — returns list with correct pagination fields
# ---------------------------------------------------------------------------

async def test_list_leads_returns_pagination_fields():
    leads = [_fake_lead(title=f"Lead {i}") for i in range(3)]

    async def _execute(query):
        result = MagicMock()
        q = str(query).lower()
        if "count" in q:
            result.scalar_one.return_value = 3
        else:
            scalars = MagicMock()
            scalars.all.return_value = leads
            result.scalars.return_value = scalars
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads")

    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert "total" in body
    assert "page" in body
    assert "per_page" in body
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["per_page"] == 20
    assert len(body["items"]) == 3


# ---------------------------------------------------------------------------
# 2. GET /api/v1/leads — pagination works correctly (page=2, per_page=5)
# ---------------------------------------------------------------------------

async def test_list_leads_pagination():
    """Page 2 with per_page=5 should return the correct subset."""
    all_leads = [_fake_lead(title=f"Lead {i}") for i in range(12)]
    page, per_page = 2, 5
    page_leads = all_leads[5:10]

    async def _execute(query):
        result = MagicMock()
        q = str(query).lower()
        if "count" in q:
            result.scalar_one.return_value = 12
        else:
            scalars = MagicMock()
            scalars.all.return_value = page_leads
            result.scalars.return_value = scalars
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads", params={"page": page, "per_page": per_page})

    assert resp.status_code == 200
    body = resp.json()
    assert body["page"] == page
    assert body["per_page"] == per_page
    assert body["total"] == 12
    assert len(body["items"]) == 5


# ---------------------------------------------------------------------------
# 3. GET /api/v1/leads — filtering by source
# ---------------------------------------------------------------------------

async def test_list_leads_filter_by_source():
    leads = [_fake_lead(source="fl.ru", title="FL lead")]

    async def _execute(query):
        result = MagicMock()
        q = str(query).lower()
        if "count" in q:
            result.scalar_one.return_value = 1
        else:
            scalars = MagicMock()
            scalars.all.return_value = leads
            result.scalars.return_value = scalars
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads", params={"source": "fl.ru"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert all(item["source"] == "fl.ru" for item in body["items"])


# ---------------------------------------------------------------------------
# 4. GET /api/v1/leads — filtering by status
# ---------------------------------------------------------------------------

async def test_list_leads_filter_by_status():
    leads = [_fake_lead(status="viewed")]

    async def _execute(query):
        result = MagicMock()
        q = str(query).lower()
        if "count" in q:
            result.scalar_one.return_value = 1
        else:
            scalars = MagicMock()
            scalars.all.return_value = leads
            result.scalars.return_value = scalars
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads", params={"status": "viewed"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert all(item["status"] == "viewed" for item in body["items"])


# ---------------------------------------------------------------------------
# 5. GET /api/v1/leads/{id} — returns a single lead by UUID
# ---------------------------------------------------------------------------

async def test_get_lead_by_id():
    lead_id = str(uuid.uuid4())
    lead = _fake_lead(lead_id=lead_id, title="Single lead")

    async def _execute(query):
        result = MagicMock()
        result.scalar_one_or_none.return_value = lead
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/api/v1/leads/{lead_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == lead_id
    assert body["title"] == "Single lead"


# ---------------------------------------------------------------------------
# 6. GET /api/v1/leads/{id} — returns 404 for non-existent UUID
# ---------------------------------------------------------------------------

async def test_get_lead_not_found():
    async def _execute(query):
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/api/v1/leads/{uuid.uuid4()}")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lead not found"


# ---------------------------------------------------------------------------
# 7. PATCH /api/v1/leads/{id} — updates lead status successfully
# ---------------------------------------------------------------------------

async def test_update_lead_status():
    lead_id = str(uuid.uuid4())
    lead = _fake_lead(lead_id=lead_id, status="new")

    call_count = 0

    async def _execute(query):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        result.scalar_one_or_none.return_value = lead
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    session.commit = AsyncMock()

    async def _refresh(obj, **kwargs):
        # After commit, the lead status should reflect the update
        obj.status = "viewed"

    session.refresh = AsyncMock(side_effect=_refresh)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.patch(
            f"/api/v1/leads/{lead_id}",
            json={"status": "viewed"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == lead_id
    assert body["status"] == "viewed"
    session.commit.assert_awaited_once()


# ---------------------------------------------------------------------------
# 8. PATCH /api/v1/leads/{id} — returns 404 for non-existent UUID
# ---------------------------------------------------------------------------

async def test_update_lead_not_found():
    async def _execute(query):
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_execute)
    app.state.async_session_factory = _mock_session_factory(session)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.patch(
            f"/api/v1/leads/{uuid.uuid4()}",
            json={"status": "viewed"},
        )

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Lead not found"


# ---------------------------------------------------------------------------
# 9. PATCH /api/v1/leads/{id} — returns 422 for invalid status value
# ---------------------------------------------------------------------------

async def test_update_lead_invalid_status():
    """Sending an invalid status value should trigger Pydantic validation → 422."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.patch(
            f"/api/v1/leads/{uuid.uuid4()}",
            json={"status": "INVALID_STATUS_VALUE"},
        )

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 10. GET /health — returns correct HealthResponse format
# ---------------------------------------------------------------------------

async def test_health_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "service_name" in body
    assert isinstance(body["service_name"], str)
    assert len(body["service_name"]) > 0
    assert "last_successful_cycle" in body
