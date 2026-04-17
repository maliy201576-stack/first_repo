# Feature: glukhov-sales-engine, Property 8: Детерминированность хеширования
# Validates: Requirement 4.3
"""Property-based tests for DedupService.compute_hash determinism.

Property 8 states:
  For any two identical pairs (source, identifier), compute_hash returns
  the same hash. For any two distinct pairs, compute_hash returns
  distinct hashes.
"""

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from src.dedup.service import DedupService


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Non-empty text for source names
_source = st.text(min_size=1, max_size=200)
_url = st.text(min_size=1, max_size=500)
_message_id = st.integers(min_value=0, max_value=2**63 - 1)


# ---------------------------------------------------------------------------
# Property 8a: Identical inputs → identical hash (determinism)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(source=_source, url=_url)
def test_same_source_and_url_produce_same_hash(source: str, url: str) -> None:
    """compute_hash(source, url) called twice with the same args must return
    the same value."""
    h1 = DedupService.compute_hash(source, url=url)
    h2 = DedupService.compute_hash(source, url=url)
    assert h1 == h2


@settings(max_examples=200)
@given(source=_source, message_id=_message_id)
def test_same_source_and_message_id_produce_same_hash(
    source: str, message_id: int
) -> None:
    """compute_hash(source, message_id) called twice with the same args must
    return the same value."""
    h1 = DedupService.compute_hash(source, message_id=message_id)
    h2 = DedupService.compute_hash(source, message_id=message_id)
    assert h1 == h2


# ---------------------------------------------------------------------------
# Property 8b: Distinct inputs → distinct hashes (collision-free for
#              structurally different inputs)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(source_a=_source, source_b=_source, url=_url)
def test_different_sources_produce_different_hashes(
    source_a: str, source_b: str, url: str
) -> None:
    """Two different sources with the same URL must yield different hashes."""
    assume(source_a != source_b)
    h1 = DedupService.compute_hash(source_a, url=url)
    h2 = DedupService.compute_hash(source_b, url=url)
    assert h1 != h2


@settings(max_examples=200)
@given(source=_source, url_a=_url, url_b=_url)
def test_different_urls_produce_different_hashes(
    source: str, url_a: str, url_b: str
) -> None:
    """Same source with two different URLs must yield different hashes."""
    assume(url_a != url_b)
    h1 = DedupService.compute_hash(source, url=url_a)
    h2 = DedupService.compute_hash(source, url=url_b)
    assert h1 != h2


@settings(max_examples=200)
@given(source=_source, id_a=_message_id, id_b=_message_id)
def test_different_message_ids_produce_different_hashes(
    source: str, id_a: int, id_b: int
) -> None:
    """Same source with two different message IDs must yield different hashes."""
    assume(id_a != id_b)
    h1 = DedupService.compute_hash(source, message_id=id_a)
    h2 = DedupService.compute_hash(source, message_id=id_b)
    assert h1 != h2


# ---------------------------------------------------------------------------
# Property 8c: Hash is a valid 64-char lowercase hex string (SHA-256)
# ---------------------------------------------------------------------------

@settings(max_examples=200)
@given(source=_source, url=st.one_of(st.none(), _url), message_id=st.one_of(st.none(), _message_id))
def test_hash_is_valid_sha256_hex(
    source: str, url: str | None, message_id: int | None
) -> None:
    """compute_hash must always return a 64-character lowercase hex string."""
    h = DedupService.compute_hash(source, url=url, message_id=message_id)
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)
