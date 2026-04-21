"""Lead freshness checker — verifies that leads are still active on source sites.

Periodically checks stored leads by visiting their URLs and marking
expired/completed ones as rejected. This avoids accumulating stale leads
that are no longer available on the source platforms.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.common.db import Lead
from src.common.enums import LeadStatus

logger = logging.getLogger(__name__)

# How often to run the freshness check (seconds)
_CHECK_INTERVAL = 3600 * 6  # every 6 hours

# Max leads to check per cycle (avoid overloading sources)
_BATCH_SIZE = 50

# HTTP status codes that indicate the lead page is gone
_GONE_STATUSES = {404, 410, 301}

# Text patterns on the page that indicate the order is closed/completed
_CLOSED_PATTERNS = [
    "проект закрыт",
    "проект завершён",
    "проект завершен",
    "заказ выполнен",
    "заказ закрыт",
    "задача закрыта",
    "задача выполнена",
    "проект удалён",
    "проект удален",
    "страница не найдена",
    "project is closed",
    "project completed",
    "заказ снят",
    "не найден",
    "удалено",
]


class FreshnessChecker:
    """Checks whether stored leads are still active on their source sites.

    Visits each lead's URL and checks if the page still exists and the
    order is still open. Leads that are gone or completed get marked
    as rejected.

    Args:
        session_factory: Async SQLAlchemy session factory.
        browser_factory: Async callable returning a Playwright Browser.
        interval: Seconds between check cycles.
        batch_size: Max leads to check per cycle.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        browser_factory,  # noqa: ANN001
        interval: int = _CHECK_INTERVAL,
        batch_size: int = _BATCH_SIZE,
    ) -> None:
        self._session_factory = session_factory
        self._browser_factory = browser_factory
        self._interval = interval
        self._batch_size = batch_size
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the periodic freshness check loop."""
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("FreshnessChecker started (interval=%ds)", self._interval)

    async def stop(self) -> None:
        """Stop the freshness check loop."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("FreshnessChecker stopped")

    async def _loop(self) -> None:
        """Run check cycles periodically."""
        while self._running:
            try:
                await self.check_batch()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in freshness check cycle")
            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break

    async def check_batch(self) -> int:
        """Check a batch of leads for freshness.

        Selects the oldest-checked leads with status 'new' or 'viewed'
        that have a URL, visits each URL, and marks expired ones as rejected.

        Returns:
            Number of leads marked as rejected in this batch.
        """
        async with self._session_factory() as session:
            # Get leads that haven't been checked recently, oldest first
            result = await session.execute(
                select(Lead)
                .where(
                    Lead.status.in_([LeadStatus.NEW.value, LeadStatus.VIEWED.value]),
                    Lead.url.isnot(None),
                    Lead.url != "",
                )
                .order_by(Lead.updated_at.asc())
                .limit(self._batch_size)
            )
            leads = result.scalars().all()

        if not leads:
            logger.debug("FreshnessChecker: no leads to check")
            return 0

        browser = await self._browser_factory()
        expired_ids: list[str] = []

        try:
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(15_000)

            for lead in leads:
                try:
                    is_expired = await self._check_lead(page, lead)
                    if is_expired:
                        expired_ids.append(str(lead.id))
                        logger.info(
                            "Lead expired: %s [%s] %s",
                            lead.source, lead.id, lead.title[:60],
                        )
                except Exception:
                    logger.debug(
                        "Could not check lead %s: %s", lead.id, lead.url
                    )

            await page.close()
            await context.close()
        finally:
            try:
                await browser.close()
            except Exception:
                pass

        # Mark expired leads as rejected
        if expired_ids:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(
                        update(Lead)
                        .where(Lead.id.in_(expired_ids))
                        .values(
                            status=LeadStatus.REJECTED.value,
                            updated_at=datetime.now(timezone.utc),
                        )
                    )
            logger.info(
                "FreshnessChecker: marked %d/%d leads as rejected",
                len(expired_ids), len(leads),
            )

        # Touch updated_at on checked leads so they go to the back of the queue
        checked_ids = [str(lead.id) for lead in leads if str(lead.id) not in expired_ids]
        if checked_ids:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(
                        update(Lead)
                        .where(Lead.id.in_(checked_ids))
                        .values(updated_at=datetime.now(timezone.utc))
                    )

        return len(expired_ids)

    async def _check_lead(self, page, lead: Lead) -> bool:  # noqa: ANN001
        """Check if a single lead is still active.

        Args:
            page: Playwright page object.
            lead: The Lead ORM object to check.

        Returns:
            True if the lead appears to be expired/closed.
        """
        response = await page.goto(lead.url, wait_until="domcontentloaded", timeout=15_000)

        # Check HTTP status
        if response and response.status in _GONE_STATUSES:
            return True

        # Wait briefly for JS rendering
        await page.wait_for_timeout(2000)

        # Check page content for closed/completed indicators
        content = await page.content()
        content_lower = content.lower()

        for pattern in _CLOSED_PATTERNS:
            if pattern in content_lower:
                return True

        return False
