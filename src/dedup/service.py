"""Deduplication service — two-level duplicate detection for leads.

Level 1: Exact hash check in PostgreSQL (indexed primary key lookup).
Level 2: Fuzzy title comparison in PostgreSQL via RapidFuzz.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

from rapidfuzz import fuzz
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.common.constants import DEDUP_WINDOW_DAYS
from src.common.db import Lead, LeadHash
from src.common.enums import LeadStatus
from src.common.models import LeadCandidate

logger = logging.getLogger(__name__)


@dataclass
class DeduplicationResult:
    """Outcome of a deduplication check."""

    is_duplicate: bool
    original_lead_id: UUID | None = None
    match_type: str | None = None  # "exact_hash" | "fuzzy_match" | None
    similarity_score: float | None = None


class DedupService:
    """Two-level lead deduplication using PostgreSQL only."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        fuzzy_threshold: int = 85,
    ) -> None:
        self._session_factory = session_factory
        self._fuzzy_threshold = fuzzy_threshold

    # ------------------------------------------------------------------
    # Hashing
    # ------------------------------------------------------------------

    @staticmethod
    def compute_hash(
        source: str,
        url: str | None = None,
        message_id: int | None = None,
    ) -> str:
        """Compute a deterministic SHA-256 hash for a lead candidate.

        The hash is derived from ``source`` combined with either ``url``
        or ``message_id`` (whichever is available, URL takes priority).
        """
        identifier = url if url is not None else str(message_id) if message_id is not None else ""
        raw = f"{source}:{identifier}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # Exact duplicate (PostgreSQL)
    # ------------------------------------------------------------------

    async def check_exact_duplicate(self, hash_value: str) -> bool:
        """Return ``True`` if *hash_value* already exists in PostgreSQL."""
        async with self._session_factory() as session:
            stmt = select(LeadHash.hash).where(LeadHash.hash == hash_value).limit(1)
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

    async def _store_hash(self, hash_value: str, lead_id: UUID, session: AsyncSession) -> None:
        """Persist *hash_value* in PostgreSQL."""
        lead_hash = LeadHash(hash=hash_value, lead_id=str(lead_id))
        session.add(lead_hash)

    # ------------------------------------------------------------------
    # Fuzzy duplicate (PostgreSQL)
    # ------------------------------------------------------------------

    @staticmethod
    def fuzzy_match(title_a: str, title_b: str) -> float:
        """Return the RapidFuzz ``token_sort_ratio`` between two titles."""
        return fuzz.token_sort_ratio(title_a, title_b)

    async def check_fuzzy_duplicate(
        self, source: str, title: str
    ) -> DeduplicationResult:
        """Search for a fuzzy duplicate among recent leads of the same *source*.

        Checks only leads created in the last 30 days to avoid loading
        the entire table into memory.

        Returns a :class:`DeduplicationResult` indicating whether a
        duplicate was found (similarity ≥ threshold).
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=DEDUP_WINDOW_DAYS)
        async with self._session_factory() as session:
            stmt = (
                select(Lead.id, Lead.title)
                .where(Lead.source == source)
                .where(Lead.created_at >= cutoff)
            )
            result = await session.execute(stmt)
            rows = result.all()

        for lead_id, existing_title in rows:
            score = self.fuzzy_match(title, existing_title)
            if score >= self._fuzzy_threshold:
                return DeduplicationResult(
                    is_duplicate=True,
                    original_lead_id=UUID(str(lead_id)),
                    match_type="fuzzy_match",
                    similarity_score=score,
                )

        return DeduplicationResult(is_duplicate=False)

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    async def check_and_store(self, candidate: LeadCandidate) -> DeduplicationResult:
        """Run the full deduplication pipeline and store the lead if unique.

        Steps:
        1. Compute SHA-256 hash from (source, url/message_id).
        2. Check exact duplicate in PostgreSQL.
        3. If new hash — check fuzzy duplicate in PostgreSQL.
        4. If unique — INSERT lead and persist hash.
        """
        hash_value = self.compute_hash(
            source=candidate.source,
            url=candidate.url,
            message_id=candidate.message_id,
        )

        # Step 1 — exact hash check
        if await self.check_exact_duplicate(hash_value):
            logger.info(
                "Exact duplicate rejected: hash=%s source=%s",
                hash_value[:12],
                candidate.source,
            )
            return DeduplicationResult(
                is_duplicate=True,
                match_type="exact_hash",
            )

        # Step 2 — fuzzy check
        fuzzy_result = await self.check_fuzzy_duplicate(candidate.source, candidate.title)
        if fuzzy_result.is_duplicate:
            logger.info(
                "Fuzzy duplicate rejected: source=%s title='%s' original_id=%s score=%.1f",
                candidate.source,
                candidate.title[:80],
                fuzzy_result.original_lead_id,
                fuzzy_result.similarity_score,
            )
            return fuzzy_result

        # Step 3 — store unique lead
        async with self._session_factory() as session:
            async with session.begin():
                lead = Lead(
                    source=candidate.source,
                    title=candidate.title,
                    description=candidate.description,
                    url=candidate.url,
                    budget=candidate.budget,
                    budget_max=candidate.budget_max,
                    category=candidate.category,
                    matched_keywords=candidate.matched_keywords,
                    tags=candidate.tags,
                    status=LeadStatus.NEW,
                    okpd2_codes=candidate.okpd2_codes,
                    max_contract_price=candidate.max_contract_price,
                    submission_deadline=candidate.submission_deadline,
                    discovered_at=candidate.discovered_at,
                )
                session.add(lead)
                await session.flush()  # populate lead.id

                await self._store_hash(hash_value, UUID(str(lead.id)), session)

        logger.info(
            "New lead stored: id=%s source=%s title='%s'",
            lead.id,
            candidate.source,
            candidate.title[:80],
        )
        return DeduplicationResult(is_duplicate=False)
