"""Unit tests for the canonical injected-clock contract."""

from datetime import UTC, datetime, timedelta, timezone, tzinfo

import pytest

from wealth.adapters.foundation import SystemClock
from wealth.ports.foundation import ClockContractError, require_utc_clock


class FoldCapableZeroOffset(tzinfo):
    """Expose a zero-offset fold whose zone identity is intentionally not UTC."""

    def utcoffset(self, value: datetime | None) -> timedelta:
        if value is not None and value.fold:
            return timedelta(hours=1)
        return timedelta(0)

    def dst(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        del value
        return "fold-capable-zero"


class ShiftedUtcDatetime(datetime):
    """Prove zone identity alone cannot establish built-in UTC semantics."""

    def utcoffset(self) -> timedelta:
        return timedelta(hours=1)


def test_require_utc_clock_returns_the_existing_fixed_utc_value() -> None:
    value = datetime(2026, 7, 25, 14, 30, 15, 123456, tzinfo=UTC)

    assert require_utc_clock(value) is value


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime(2026, 7, 25, 14, 30), id="naive"),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))),
            id="positive-offset",
        ),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(-timedelta(hours=4))),
            id="negative-offset",
        ),
        pytest.param(
            datetime(
                2026,
                7,
                25,
                14,
                30,
                tzinfo=timezone(timedelta(0), "named-zero"),
            ),
            id="named-zero-offset",
        ),
        pytest.param(
            datetime(2026, 1, 1, 14, 30, tzinfo=FoldCapableZeroOffset(), fold=0),
            id="fold-capable-zero-offset",
        ),
        pytest.param(
            ShiftedUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="utc-identity-with-overridden-nonzero-offset",
        ),
        pytest.param(object(), id="not-a-datetime"),
    ],
)
def test_require_utc_clock_rejects_every_noncanonical_value(value: object) -> None:
    with pytest.raises(ClockContractError, match=r"tzinfo exactly datetime\.UTC"):
        require_utc_clock(value)


def test_system_clock_returns_a_value_in_the_fixed_utc_zone() -> None:
    value = SystemClock().now()

    assert value.tzinfo is UTC
    assert require_utc_clock(value) is value
