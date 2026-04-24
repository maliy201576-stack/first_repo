"""Lead CRUD endpoints with filtering and pagination."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import cast, func, select, String
from sqlalchemy.exc import SQLAlchemyError

from src.common.db import Lead
from src.common.models import LeadListResponse, LeadResponse, LeadUpdateRequest

logger = logging.getLogger(__name__)
router = APIRouter()


def _lead_to_response(lead: Lead) -> LeadResponse:
    """Convert a SQLAlchemy Lead row to a Pydantic LeadResponse."""
    return LeadResponse(
        id=lead.id,
        source=lead.source,
        title=lead.title,
        description=lead.description,
        url=lead.url,
        budget=lead.budget,
        budget_max=lead.budget_max,
        category=lead.category,
        matched_keywords=lead.matched_keywords,
        tags=lead.tags,
        status=lead.status,
        okpd2_codes=lead.okpd2_codes,
        max_contract_price=lead.max_contract_price,
        submission_deadline=lead.submission_deadline,
        discovered_at=lead.discovered_at,
        created_at=lead.created_at,
        updated_at=lead.updated_at,
    )


def _apply_filters(query, *, source, status, tags, category, keyword, okpd2, date_from, date_to):
    """Apply optional WHERE clauses to a query.

    Args:
        query: SQLAlchemy select statement.
        source: Filter by lead source.
        status: Filter by lead status.
        tags: Filter by tag (JSONB contains).
        category: Filter by category (exact match).
        keyword: Filter by matched keyword (JSONB contains).
        okpd2: Filter by OKPD2 code (JSONB text search).
        date_from: Filter leads created on or after this date.
        date_to: Filter leads created on or before this date.

    Returns:
        Modified query with filters applied.
    """
    if source is not None:
        query = query.where(Lead.source == source)
    if status is not None:
        query = query.where(Lead.status == status)
    if tags is not None:
        query = query.where(Lead.tags.contains([tags]))
    if category is not None:
        query = query.where(Lead.category == category)
    if keyword is not None:
        query = query.where(Lead.matched_keywords.contains([keyword]))
    if okpd2 is not None:
        query = query.where(
            cast(Lead.okpd2_codes, String).ilike(f"%{okpd2}%")
        )
    if date_from is not None:
        query = query.where(Lead.created_at >= date_from)
    if date_to is not None:
        query = query.where(Lead.created_at <= date_to)
    return query


@router.get("/leads", response_model=LeadListResponse)
async def list_leads(
    request: Request,
    source: str | None = Query(None),
    status: str | None = Query(None),
    tags: str | None = Query(None),
    category: str | None = Query(None),
    keyword: str | None = Query(None),
    okpd2: str | None = Query(None),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    sort_by: str = Query("created_at"),
    sort_dir: str = Query("desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> LeadListResponse:
    """Return a paginated, filtered list of leads."""
    session_factory = request.app.state.async_session_factory
    try:
        async with session_factory() as session:
            filt = dict(
                source=source, status=status, tags=tags, category=category,
                keyword=keyword, okpd2=okpd2, date_from=date_from, date_to=date_to,
            )
            query = _apply_filters(select(Lead), **filt)
            count_query = _apply_filters(
                select(func.count()).select_from(Lead), **filt
            )

            total = (await session.execute(count_query)).scalar_one()

            # Sorting — NULLs always go last regardless of direction
            _allowed_sort = {
                "created_at": Lead.created_at,
                "discovered_at": Lead.discovered_at,
                "budget": Lead.budget,
                "title": Lead.title,
                "source": Lead.source,
            }
            sort_col = _allowed_sort.get(sort_by, Lead.created_at)
            if sort_dir == "asc":
                order = sort_col.asc().nullslast()
            else:
                order = sort_col.desc().nullslast()

            offset = (page - 1) * per_page
            query = query.order_by(order).offset(offset).limit(per_page)
            result = await session.execute(query)
            leads = result.scalars().all()

            return LeadListResponse(
                items=[_lead_to_response(lead) for lead in leads],
                total=total,
                page=page,
                per_page=per_page,
            )
    except SQLAlchemyError:
        logger.exception("Database error listing leads")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/leads/{lead_id}", response_model=LeadResponse)
async def get_lead(request: Request, lead_id: UUID) -> LeadResponse:
    """Return a single lead by its UUID."""
    session_factory = request.app.state.async_session_factory
    try:
        async with session_factory() as session:
            result = await session.execute(select(Lead).where(Lead.id == str(lead_id)))
            lead = result.scalar_one_or_none()
            if lead is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            return _lead_to_response(lead)
    except HTTPException:
        raise
    except SQLAlchemyError:
        logger.exception("Database error fetching lead %s", lead_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.patch("/leads/{lead_id}", response_model=LeadResponse)
async def update_lead(
    request: Request, lead_id: UUID, body: LeadUpdateRequest
) -> LeadResponse:
    """Update the status of a lead."""
    session_factory = request.app.state.async_session_factory
    try:
        async with session_factory() as session:
            result = await session.execute(select(Lead).where(Lead.id == str(lead_id)))
            lead = result.scalar_one_or_none()
            if lead is None:
                raise HTTPException(status_code=404, detail="Lead not found")
            lead.status = body.status
            lead.updated_at = datetime.now(timezone.utc)
            await session.commit()
            await session.refresh(lead)
            return _lead_to_response(lead)
    except HTTPException:
        raise
    except SQLAlchemyError:
        logger.exception("Database error updating lead %s", lead_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/leads-filter-options")
async def filter_options(request: Request) -> dict:
    """Return distinct values for filter dropdowns in the web UI."""
    session_factory = request.app.state.async_session_factory
    try:
        async with session_factory() as session:
            sources = (
                await session.execute(
                    select(Lead.source).distinct().order_by(Lead.source)
                )
            ).scalars().all()

            statuses = (
                await session.execute(
                    select(Lead.status).distinct().order_by(Lead.status)
                )
            ).scalars().all()

            categories = (
                await session.execute(
                    select(Lead.category)
                    .where(Lead.category.isnot(None))
                    .distinct()
                    .order_by(Lead.category)
                )
            ).scalars().all()

            return {
                "sources": sources,
                "statuses": statuses,
                "categories": categories,
            }
    except SQLAlchemyError:
        logger.exception("Database error fetching filter options")
        raise HTTPException(status_code=500, detail="Internal server error")
