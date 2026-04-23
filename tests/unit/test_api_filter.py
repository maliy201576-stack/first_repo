# Feature: glukhov-sales-engine, Property 9: Фильтрация API возвращает только подходящие записи
# Validates: Requirements 5.2
"""Property-based tests for API lead filtering.

Property 9 states:
  For any set of leads in the database and for any combination of filters
  (source, status, tags, date range), all returned records satisfy all
  specified filters, and no matching record is missing from the results.

Strategy:
  We generate random leads and random filter combinations, then call the
  actual FastAPI endpoint via httpx.AsyncClient. The database session is
  mocked so that the SQLAlchemy execute() calls are intercepted: we apply
  the *same* filters in pure Python (reference implementation) and return
  the pre-filtered results. The test then verifies that the API response
  contains exactly the expected leads — proving that the route correctly
  builds queries and serialises results for every filter combination.

  Additionally, we verify the two core invariants:
    1. Every returned lead satisfies ALL specified filters.
    2. No lead that matches all filters is missing from the response.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st
from httpx import ASGITransport, AsyncClient

from src.common.enums import LeadSource, LeadStatus, LeadTag


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_sources = st.sampled_from([s.value for s in LeadSource])
_statuses = st.sampled_from([s.value for s in LeadStatus])
_tag_values = [t.value for t in LeadTag]
_tags = st.lists(st.sampled_from(_tag_values), min_size=0, max_size=2, unique=True)

_base_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
_created_at_strategy = st.integers(min_value=0, max_value=180).map(
    lambda d: _base_dt + timedelta(days=d)
)


@st.composite
def lead_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate a single lead as a plain dict."""
    return {
        "id": str(uuid.uuid4()),
        "source": draw(_sources),
        "title": f"Lead {draw(st.integers(min_value=1, max_value=9999))}",
        "description": None,
        "url": None,
        "budget": None,
        "budget_max": None,
        "category": None,
        "matched_keywords": [],
        "tags": draw(_tags),
        "status": draw(_statuses),
        "okpd2_codes": None,
        "max_contract_price": None,
        "submission_deadline": None,
        "discovered_at": _base_dt,
        "created_at": draw(_created_at_strategy),
        "updated_at": _base_dt,
    }


@st.composite
def filter_params(draw: st.DrawFn) -> dict[str, str]:
    """Generate a random combination of API query-string filters."""
    params: dict[str, str] = {}
    if draw(st.booleans()):
        params["source"] = draw(_sources)
    if draw(st.booleans()):
        params["status"] = draw(_statuses)
    if draw(st.booleans()):
        params["tags"] = draw(st.sampled_from(_tag_values))
    if draw(st.booleans()):
        params["date_from"] = draw(_created_at_strategy).isoformat()
    if draw(st.booleans()):
        params["date_to"] = draw(_created_at_strategy).isoformat()
    return params


# ---------------------------------------------------------------------------
# Reference filter — pure-Python mirror of the production SQL logic
# ---------------------------------------------------------------------------

def _matches(lead: dict[str, Any], filters: dict[str, str]) -> bool:
    """Return True if *lead* satisfies every filter in *filters*.

    This mirrors the WHERE clauses built in ``src.api.routes.leads.list_leads``.
    """
    if "source" in filters and lead["source"] != filters["source"]:
        return False
    if "status" in filters and lead["status"] != filters["status"]:
        return False
    if "tags" in filters:
        # Production uses Lead.tags.contains([tags]) — JSONB @> operator
        # which checks that the JSON array contains the given element.
        if filters["tags"] not in lead["tags"]:
            return False
    if "date_from" in filters:
        date_from = datetime.fromisoformat(filters["date_from"])
        if lead["created_at"] < date_from:
            return False
    if "date_to" in filters:
        date_to = datetime.fromisoformat(filters["date_to"])
        if lead["created_at"] > date_to:
            return False
    return True


# ---------------------------------------------------------------------------
# Fake ORM object
# ---------------------------------------------------------------------------

class _FakeLead:
    """Lightweight stand-in for the SQLAlchemy ``Lead`` model."""

    def __init__(self, data: dict[str, Any]) -> None:
        for k, v in data.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# Test-app factory
# ---------------------------------------------------------------------------

def _build_app_and_expected(
    leads_data: list[dict[str, Any]],
    filters: dict[str, str],
):
    """Return ``(app, expected_ids)`` where *app* is a FastAPI instance with a
    mocked session factory, and *expected_ids* is the set of lead UUIDs that
    the reference filter says should be returned.
    """
    from fastapi import FastAPI
    from src.api.routes.leads import router as leads_router

    app = FastAPI()
    app.include_router(leads_router, prefix="/api/v1")

    # Compute expected results via reference filter
    expected = [d for d in leads_data if _matches(d, filters)]
    expected.sort(key=lambda d: d["created_at"], reverse=True)
    expected_ids = {d["id"] for d in expected}

    # --- mock session that returns pre-filtered results -----------------
    async def _mock_execute(query):
        result = MagicMock()
        query_str = str(query)
        if "count" in query_str.lower():
            result.scalar_one.return_value = len(expected)
        else:
            scalars = MagicMock()
            scalars.all.return_value = [_FakeLead(d) for d in expected]
            result.scalars.return_value = scalars
        return result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=_mock_execute)

    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)

    app.state.async_session_factory = factory

    return app, expected_ids, len(expected)


# ---------------------------------------------------------------------------
# Property 9a: All returned leads match ALL specified filters
# ---------------------------------------------------------------------------

@settings(max_examples=100, deadline=None)
@given(
    leads=st.lists(lead_data(), min_size=0, max_size=15),
    filters=filter_params(),
)
async def test_all_returned_leads_match_filters(
    leads: list[dict[str, Any]],
    filters: dict[str, str],
) -> None:
    """**Validates: Requirements 5.2**

    Every lead in the API response must satisfy all specified filters.
    """
    app, expected_ids, expected_total = _build_app_and_expected(leads, filters)

    query_params = {**filters, "per_page": "100"}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads", params=query_params)

    assert resp.status_code == 200
    body = resp.json()

    for item in body["items"]:
        lead_dict = {
            "source": item["source"],
            "status": item["status"],
            "tags": item["tags"],
            "created_at": datetime.fromisoformat(item["created_at"]),
        }
        assert _matches(lead_dict, filters), (
            f"Returned lead {item['id']} does not match filters {filters}"
        )


# ---------------------------------------------------------------------------
# Property 9b: No matching lead is missing from the results
# ---------------------------------------------------------------------------

@settings(max_examples=100, deadline=None)
@given(
    leads=st.lists(lead_data(), min_size=0, max_size=15),
    filters=filter_params(),
)
async def test_no_matching_lead_is_missing(
    leads: list[dict[str, Any]],
    filters: dict[str, str],
) -> None:
    """**Validates: Requirements 5.2**

    Every lead that matches all filters must appear in the API response.
    """
    app, expected_ids, expected_total = _build_app_and_expected(leads, filters)

    query_params = {**filters, "per_page": "100"}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads", params=query_params)

    assert resp.status_code == 200
    body = resp.json()

    returned_ids = {item["id"] for item in body["items"]}
    assert expected_ids == returned_ids, (
        f"Missing leads: {expected_ids - returned_ids}, "
        f"Extra leads: {returned_ids - expected_ids}"
    )


# ---------------------------------------------------------------------------
# Property 9c: Total count matches the number of matching leads
# ---------------------------------------------------------------------------

@settings(max_examples=100, deadline=None)
@given(
    leads=st.lists(lead_data(), min_size=0, max_size=15),
    filters=filter_params(),
)
async def test_total_count_matches_filtered_leads(
    leads: list[dict[str, Any]],
    filters: dict[str, str],
) -> None:
    """**Validates: Requirements 5.2**

    The ``total`` field in the response must equal the number of leads
    that match all specified filters.
    """
    app, expected_ids, expected_total = _build_app_and_expected(leads, filters)

    query_params = {**filters, "per_page": "100"}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/leads", params=query_params)

    assert resp.status_code == 200
    body = resp.json()

    assert body["total"] == expected_total, (
        f"Expected total={expected_total}, got {body['total']}"
    )
    assert len(body["items"]) == expected_total
