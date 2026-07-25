"""Shared deterministic clock-boundary fixtures."""

from datetime import datetime, timedelta, timezone, tzinfo
from typing import cast

import pytest


class _NamedFoldZeroTimezone(tzinfo):
    """Expose a zero offset without being Python's fixed UTC singleton."""

    def utcoffset(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def dst(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        del value
        return "named-fold-zero"


_CLOCK_BASE = datetime(2026, 7, 25, 12, 0)
_INVALID_CLOCK_VALUES = (
    pytest.param(_CLOCK_BASE, id="naive"),
    pytest.param(
        _CLOCK_BASE.replace(tzinfo=timezone(timedelta(hours=2))),
        id="positive-offset",
    ),
    pytest.param(
        _CLOCK_BASE.replace(tzinfo=timezone(timedelta(hours=-5))),
        id="negative-offset",
    ),
    pytest.param(
        _CLOCK_BASE.replace(tzinfo=_NamedFoldZeroTimezone(), fold=1),
        id="named-fold-zero-offset",
    ),
)


@pytest.fixture(params=_INVALID_CLOCK_VALUES)
def invalid_clock_value(request: pytest.FixtureRequest) -> datetime:
    """Return each clock representation forbidden by the fixed-UTC contract."""

    return cast(datetime, request.param)
