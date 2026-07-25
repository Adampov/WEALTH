"""Unit tests for canonical event invariants."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from wealth.domain.events import DomainEvent, Environment, EventType

EVENT_ID = UUID("00000000-0000-0000-0000-000000000001")
CORRELATION_ID = UUID("00000000-0000-0000-0000-000000000002")
BASE_TIME = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


def build_event(**overrides: object) -> DomainEvent:
    """Build a valid event and allow one test to override selected fields."""

    values: dict[str, object] = {
        "event_id": EVENT_ID,
        "correlation_id": CORRELATION_ID,
        "event_type": EventType.SYSTEM_HEALTH,
        "source": "tests",
        "environment": Environment.TEST,
        "event_time": BASE_TIME,
        "observed_at": BASE_TIME,
        "processed_at": BASE_TIME,
        "payload": {"status": "ok"},
    }
    values.update(overrides)
    return DomainEvent.model_validate(values)


def test_event_is_immutable_and_forbids_unknown_fields() -> None:
    event = build_event()

    with pytest.raises(ValidationError):
        DomainEvent.model_validate({**event.model_dump(), "unexpected": True})

    with pytest.raises(ValidationError):
        event.source = "changed"


@pytest.mark.parametrize("field_name", ["event_time", "observed_at", "processed_at"])
def test_event_rejects_naive_timestamps(field_name: str) -> None:
    with pytest.raises(ValidationError):
        build_event(**{field_name: datetime(2026, 7, 20, 12, 0)})


def test_event_rejects_string_enums() -> None:
    with pytest.raises(ValidationError):
        build_event(environment="test")


@pytest.mark.parametrize("field_name", ["event_time", "observed_at", "processed_at"])
def test_event_rejects_non_utc_timestamps(field_name: str) -> None:
    non_utc_time = BASE_TIME.astimezone(timezone(timedelta(hours=2)))

    with pytest.raises(ValidationError, match=f"{field_name} must use UTC"):
        build_event(**{field_name: non_utc_time})


@given(
    observed_delay=st.integers(min_value=0, max_value=86_400),
    processed_delay=st.integers(min_value=0, max_value=86_400),
)
def test_event_accepts_monotonic_timestamps(
    observed_delay: int,
    processed_delay: int,
) -> None:
    observed_at = BASE_TIME + timedelta(seconds=observed_delay)
    processed_at = observed_at + timedelta(seconds=processed_delay)

    event = build_event(observed_at=observed_at, processed_at=processed_at)

    assert event.event_time <= event.observed_at <= event.processed_at


def test_event_rejects_non_monotonic_timestamps() -> None:
    with pytest.raises(ValidationError, match="event_time must not be after observed_at"):
        build_event(observed_at=BASE_TIME - timedelta(microseconds=1))

    with pytest.raises(ValidationError, match="observed_at must not be after processed_at"):
        build_event(processed_at=BASE_TIME - timedelta(microseconds=1))
