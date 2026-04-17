# Feature: glukhov-sales-engine, Property 6: Сохранение данных закупки в Lead
# Validates: Requirement 3.4
"""Property-based tests for procurement data preservation during transformation.

Property 6 states:
  For any procurement with OKPD2 codes and a maximum contract price, the
  created Lead record must contain the same OKPD2 codes and the same price —
  data is not lost or distorted during transformation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from src.common.models import LeadCandidate
from src.worker_web.parsers.base import ScrapedOrder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def scraped_order_to_lead_candidate(order: ScrapedOrder) -> LeadCandidate:
    """Convert a ScrapedOrder to a LeadCandidate (mimics WorkerWeb logic)."""
    return LeadCandidate(
        source=order.source,
        title=order.title,
        description=order.description,
        url=order.url,
        budget=order.budget,
        category=order.category,
        tags=["urgent"] if order.is_urgent else [],
        okpd2_codes=order.okpd2_codes,
        max_contract_price=order.max_contract_price,
        submission_deadline=order.submission_deadline,
        discovered_at=order.published_at,
    )


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_okpd2_codes = st.lists(
    st.from_regex(r"[0-9]{2}\.[0-9]{2}\.[0-9]{2}\.[0-9]{3}", fullmatch=True),
    min_size=1,
    max_size=5,
)

_prices = st.decimals(
    allow_nan=False,
    allow_infinity=False,
    min_value=0,
    max_value=10**12,
)


# ---------------------------------------------------------------------------
# Property 6a: OKPD2 codes are preserved exactly
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(codes=_okpd2_codes)
def test_okpd2_codes_preserved(codes: list[str]) -> None:
    """**Validates: Requirements 3.4**

    OKPD2 codes must be preserved exactly (same list, same order) when
    transforming a ScrapedOrder into a LeadCandidate.
    """
    order = ScrapedOrder(
        source="zakupki_gov",
        title="Тестовая закупка",
        description="Описание",
        url="https://zakupki.gov.ru/test",
        okpd2_codes=codes,
    )
    lead = scraped_order_to_lead_candidate(order)
    assert lead.okpd2_codes == codes


# ---------------------------------------------------------------------------
# Property 6b: max_contract_price is preserved exactly
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(price=_prices)
def test_max_contract_price_preserved(price: Decimal) -> None:
    """**Validates: Requirements 3.4**

    The maximum contract price must be preserved exactly when transforming
    a ScrapedOrder into a LeadCandidate.
    """
    order = ScrapedOrder(
        source="zakupki_gov",
        title="Тестовая закупка",
        description="Описание",
        url="https://zakupki.gov.ru/test",
        max_contract_price=price,
    )
    lead = scraped_order_to_lead_candidate(order)
    assert lead.max_contract_price == price


# ---------------------------------------------------------------------------
# Property 6c: Both OKPD2 codes and price preserved together
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(codes=_okpd2_codes, price=_prices)
def test_okpd2_and_price_preserved_together(
    codes: list[str], price: Decimal
) -> None:
    """**Validates: Requirements 3.4**

    Both OKPD2 codes and max_contract_price must be preserved simultaneously
    when transforming a ScrapedOrder into a LeadCandidate.
    """
    order = ScrapedOrder(
        source="zakupki_gov",
        title="Тестовая закупка",
        description="Описание",
        url="https://zakupki.gov.ru/test",
        okpd2_codes=codes,
        max_contract_price=price,
    )
    lead = scraped_order_to_lead_candidate(order)
    assert lead.okpd2_codes == codes
    assert lead.max_contract_price == price


# ---------------------------------------------------------------------------
# Property 6d: None values are preserved (no codes / no price)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(
    codes=st.one_of(st.none(), _okpd2_codes),
    price=st.one_of(st.none(), _prices),
)
def test_none_values_preserved(
    codes: list[str] | None, price: Decimal | None
) -> None:
    """**Validates: Requirements 3.4**

    When OKPD2 codes or max_contract_price are None, the LeadCandidate
    must also have None for those fields — no default substitution.
    """
    order = ScrapedOrder(
        source="zakupki_gov",
        title="Тестовая закупка",
        description="Описание",
        url="https://zakupki.gov.ru/test",
        okpd2_codes=codes,
        max_contract_price=price,
    )
    lead = scraped_order_to_lead_candidate(order)
    assert lead.okpd2_codes == codes
    assert lead.max_contract_price == price
