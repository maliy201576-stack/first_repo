"""SQLAlchemy 2.0 async models and database engine factory."""

from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from src.common.enums import LeadStatus
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Lead(Base):
    """SQLAlchemy model for the leads table."""

    __tablename__ = "leads"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    budget: Mapped[float | None] = mapped_column(Numeric(15, 2), nullable=True)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    matched_keywords: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    tags: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    status: Mapped[str] = mapped_column(
        Enum(
            LeadStatus,
            name="lead_status",
            create_type=False,
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        server_default=text("'new'"),
    )
    okpd2_codes: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    max_contract_price: Mapped[float | None] = mapped_column(
        Numeric(15, 2), nullable=True
    )
    submission_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("NOW()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("NOW()"),
    )

    __table_args__ = (
        Index("idx_leads_source", "source"),
        Index("idx_leads_status", "status"),
        Index("idx_leads_created_at", "created_at"),
        Index("idx_leads_source_title", "source", "title"),
        Index("idx_leads_source_created", "source", "created_at"),
        Index("idx_leads_tags", "tags", postgresql_using="gin"),
    )


class LeadHash(Base):
    """SQLAlchemy model for the lead_hashes table."""

    __tablename__ = "lead_hashes"

    hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    lead_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("leads.id", ondelete="CASCADE"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("NOW()"),
    )


def create_engine(database_url: str):
    """Create an async SQLAlchemy engine from a database URL.

    Args:
        database_url: PostgreSQL connection string
            (e.g. ``postgresql+asyncpg://user:pass@host/db``).

    Returns:
        An ``AsyncEngine`` instance.
    """
    return create_async_engine(
        database_url,
        echo=False,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
    )


def create_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory bound to the given engine.

    Args:
        engine: An ``AsyncEngine`` returned by :func:`create_engine`.

    Returns:
        An ``async_sessionmaker`` that produces ``AsyncSession`` instances.
    """
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
