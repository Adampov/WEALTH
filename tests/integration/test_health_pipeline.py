"""Integration test for validation, storage, and structured logging."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from uuid import UUID

import pytest
from pydantic import JsonValue

from wealth.adapters.foundation import InMemoryEventStore
from wealth.application.health import HealthCheckService
from wealth.domain.events import Environment, EventType
from wealth.observability.logging import StandardLibraryEventLogger, configure_json_logger


@dataclass(frozen=True, slots=True)
class FixedClock:
    """Test clock that never reads wall-clock time."""

    value: datetime

    def now(self) -> datetime:
        return self.value


class SequenceIdGenerator:
    """Deterministic ID generator for replayable tests."""

    def __init__(self, values: tuple[UUID, ...]) -> None:
        self._values = iter(values)
        self.calls = 0

    def new(self) -> UUID:
        self.calls += 1
        return next(self._values)


def test_health_pipeline_validates_stores_and_logs_one_event() -> None:
    event_id = UUID("00000000-0000-0000-0000-000000000011")
    correlation_id = UUID("00000000-0000-0000-0000-000000000012")
    now = datetime(2026, 7, 20, 14, 30, tzinfo=UTC)
    output = StringIO()
    store = InMemoryEventStore()
    logger = configure_json_logger(name="wealth.test.health", stream=output)
    service = HealthCheckService(
        clock=FixedClock(now),
        ids=SequenceIdGenerator((event_id, correlation_id)),
        store=store,
        logger=StandardLibraryEventLogger(logger),
    )

    event = service.run(Environment.TEST)

    assert store.all() == (event,)
    assert event.event_type is EventType.SYSTEM_HEALTH
    assert event.event_id == event_id
    assert event.correlation_id == correlation_id

    log_record: dict[str, JsonValue] = json.loads(output.getvalue())
    assert log_record["level"] == "INFO"
    assert log_record["message"] == "domain_event_recorded"
    logged_event = log_record["event"]
    assert isinstance(logged_event, dict)
    assert logged_event["event_id"] == str(event_id)
    assert logged_event["environment"] == "test"
    assert logged_event["payload"] == {"status": "ok"}


def test_invalid_health_clock_fails_before_ids_storage_or_logging(
    invalid_clock_value: datetime,
) -> None:
    output = StringIO()
    ids = SequenceIdGenerator((UUID(int=11), UUID(int=12)))
    store = InMemoryEventStore()
    service = HealthCheckService(
        clock=FixedClock(invalid_clock_value),
        ids=ids,
        store=store,
        logger=StandardLibraryEventLogger(
            configure_json_logger(name="wealth.test.invalid-health-clock", stream=output)
        ),
    )

    with pytest.raises(ValueError):
        service.run(Environment.TEST)

    assert ids.calls == 0
    assert store.all() == ()
    assert output.getvalue() == ""
