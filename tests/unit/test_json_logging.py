# Feature: glukhov-sales-engine, Property 10: JSON-логи содержат все обязательные поля
"""Property-based test: JSON logs contain all mandatory fields.

**Validates: Requirements 7.1**

For any log event, the serialized JSON object must contain fields
`timestamp`, `service_name`, `level`, and `message`, and the `timestamp`
value must be a valid ISO 8601 date.
"""

import json
import logging
from datetime import datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from src.common.logging import JSONFormatter

LEVELS = ["INFO", "WARNING", "ERROR"]


@settings(max_examples=100)
@given(
    service_name=st.text(min_size=1),
    level=st.sampled_from(LEVELS),
    message=st.text(min_size=1),
)
def test_json_log_contains_mandatory_fields(
    service_name: str, level: str, message: str
) -> None:
    """Every formatted log record must contain timestamp, service_name, level, message."""
    formatter = JSONFormatter(service_name=service_name)

    record = logging.LogRecord(
        name="test",
        level=getattr(logging, level),
        pathname="",
        lineno=0,
        msg=message,
        args=None,
        exc_info=None,
    )

    output = formatter.format(record)
    parsed = json.loads(output)

    # All four mandatory fields must be present
    assert "timestamp" in parsed, "Missing 'timestamp' field"
    assert "service_name" in parsed, "Missing 'service_name' field"
    assert "level" in parsed, "Missing 'level' field"
    assert "message" in parsed, "Missing 'message' field"

    # timestamp must be valid ISO 8601
    datetime.fromisoformat(parsed["timestamp"])

    # Values must match inputs
    assert parsed["service_name"] == service_name
    assert parsed["level"] == level
    assert parsed["message"] == message
