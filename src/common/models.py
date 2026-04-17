"""Pydantic models for lead data transfer objects."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from src.common.enums import LeadStatus


class LeadCandidate(BaseModel):
    """Input model for new leads coming from workers."""

    source: str
    title: str
    description: str | None = None
    url: str | None = None
    budget: Decimal | None = None
    category: str | None = None
    matched_keywords: list[str] = []
    tags: list[str] = []
    okpd2_codes: list[str] | None = None
    max_contract_price: Decimal | None = None
    submission_deadline: datetime | None = None
    discovered_at: datetime
    message_id: int | None = None


class LeadResponse(BaseModel):
    """Full lead output model."""

    id: UUID
    source: str
    title: str
    description: str | None = None
    url: str | None = None
    budget: Decimal | None = None
    category: str | None = None
    matched_keywords: list[str]
    tags: list[str]
    status: LeadStatus
    okpd2_codes: list[str] | None = None
    max_contract_price: Decimal | None = None
    submission_deadline: datetime | None = None
    discovered_at: datetime
    created_at: datetime
    updated_at: datetime


class LeadListResponse(BaseModel):
    """Paginated list of leads."""

    items: list[LeadResponse]
    total: int
    page: int
    per_page: int


class LeadUpdateRequest(BaseModel):
    """Request model for updating lead status."""

    status: LeadStatus


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str
    service_name: str
    last_successful_cycle: datetime | None = None
