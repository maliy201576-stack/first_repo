"""Lead CRUD endpoints with filtering and pagination."""

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from src.common.db import Lead
from src.common.models import LeadListResponse, LeadResponse, LeadUpdateRequest

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


@router.get("/leads", response_model=LeadListResponse)
async def list_leads(
    request: Request,
    source: str | None = Query(None),
    status: str | None = Query(None),
    tags: str | None = Query(None),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> LeadListResponse:
    """Return a paginated, filtered list of leads."""
    session_factory = request.app.state.async_session_factory
    try:
        async with session_factory() as session:
            query = select(Lead)
            count_query = select(func.count()).select_from(Lead)

            if source is not None:
                query = query.where(Lead.source == source)
                count_query = count_query.where(Lead.source == source)
            if status is not None:
                query = query.where(Lead.status == status)
                count_query = count_query.where(Lead.status == status)
            if tags is not None:
                query = query.where(Lead.tags.contains([tags]))
                count_query = count_query.where(Lead.tags.contains([tags]))
            if date_from is not None:
                query = query.where(Lead.created_at >= date_from)
                count_query = count_query.where(Lead.created_at >= date_from)
            if date_to is not None:
                query = query.where(Lead.created_at <= date_to)
                count_query = count_query.where(Lead.created_at <= date_to)

            total = (await session.execute(count_query)).scalar_one()

            offset = (page - 1) * per_page
            query = query.order_by(Lead.created_at.desc()).offset(offset).limit(per_page)
            result = await session.execute(query)
            leads = result.scalars().all()

            return LeadListResponse(
                items=[_lead_to_response(lead) for lead in leads],
                total=total,
                page=page,
                per_page=per_page,
            )
    except SQLAlchemyError:
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
        raise HTTPException(status_code=500, detail="Internal server error")
