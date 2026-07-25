"""Unit and property tests for the unused canonical UTC codec primitives."""

import re
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from enum import IntEnum
from typing import Self, overload

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
    CanonicalUtcError,
    from_epoch_microseconds,
    normalize_aware_to_utc,
    parse_canonical_utc,
    require_canonical_utc,
    serialize_canonical_utc,
    to_epoch_microseconds,
)

CANONICAL_TEXT_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z"
)
SQLITE_MIN_INTEGER = -(2**63)
SQLITE_MAX_INTEGER = 2**63 - 1


class SeasonalZeroTimezone(tzinfo):
    """Model a named zone that is zero-offset only during part of the year."""

    def utcoffset(self, value: datetime | None) -> timedelta:
        if value is not None and 4 <= value.month <= 10:
            return timedelta(hours=1)
        return timedelta(0)

    def dst(self, value: datetime | None) -> timedelta:
        if value is not None and 4 <= value.month <= 10:
            return timedelta(hours=1)
        return timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        del value
        return "seasonal-zero"


class FoldTimezone(tzinfo):
    """Model two distinct instants represented by one repeated local wall time."""

    def utcoffset(self, value: datetime | None) -> timedelta:
        if value is not None and value.fold:
            return -timedelta(hours=5)
        return -timedelta(hours=4)

    def dst(self, value: datetime | None) -> timedelta:
        if value is not None and value.fold:
            return timedelta(0)
        return timedelta(hours=1)

    def tzname(self, value: datetime | None) -> str:
        if value is not None and value.fold:
            return "fold-standard"
        return "fold-daylight"


class NaiveLikeTimezone(tzinfo):
    """Carry a tzinfo object while still representing a naive datetime."""

    def utcoffset(self, value: datetime | None) -> None:
        del value
        return None

    def dst(self, value: datetime | None) -> None:
        del value
        return None

    def tzname(self, value: datetime | None) -> str:
        del value
        return "naive-like"


class FlakyTimezone(tzinfo):
    """Return one valid offset and then fail on the next inspection."""

    def __init__(self) -> None:
        self.calls = 0

    def utcoffset(self, value: datetime | None) -> timedelta:
        del value
        self.calls += 1
        if self.calls == 1:
            return timedelta(0)
        raise RuntimeError("unstable timezone")

    def dst(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        del value
        return "flaky"


class ThirdInspectionShiftTimezone(tzinfo):
    """Change offset only if normalization inspects the source a third time."""

    def __init__(self) -> None:
        self.calls = 0

    def utcoffset(self, value: datetime | None) -> timedelta:
        del value
        self.calls += 1
        if self.calls <= 2:
            return timedelta(0)
        return timedelta(hours=1)

    def dst(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        del value
        return "third-inspection-shift"


class ShiftedUtcDatetime(datetime):
    """Prove UTC zone identity alone cannot establish built-in datetime semantics."""

    def utcoffset(self) -> timedelta:
        return timedelta(hours=1)


class LyingTimedelta(timedelta):
    """Hide a nonzero stored duration from ordinary inequality checks."""

    def __ne__(self, value: object) -> bool:
        del value
        return False


class LyingOffsetUtcDatetime(datetime):
    """Return a nonzero offset whose timedelta subtype lies during comparison."""

    def utcoffset(self) -> timedelta:
        return LyingTimedelta(hours=1)


class ExplodingUtcDatetime(datetime):
    """Expose a hostile datetime implementation at the validator boundary."""

    def utcoffset(self) -> timedelta:
        raise RuntimeError("untrusted datetime implementation")


class MasqueradingUtcDatetime(datetime):
    """Report fixed UTC while retaining a different timezone in the C payload."""

    @property
    def tzinfo(self) -> tzinfo:
        return UTC

    def utcoffset(self) -> timedelta:
        return timedelta(0)


class MisleadingComponentsDatetime(datetime):
    """Override public components without changing the stored datetime value."""

    @property
    def year(self) -> int:
        return 99_999

    @property
    def microsecond(self) -> int:
        return 1_000_000

    def isoformat(
        self,
        sep: str = "T",
        timespec: str = "auto",
    ) -> str:
        del sep, timespec
        return "not-canonical"


class HostileProjectionDatetime(MisleadingComponentsDatetime):
    """Reject subclass conversion shortcuts while preserving canonical stored state."""

    @overload  # type: ignore[override]
    def __sub__(self, value: datetime, /) -> timedelta: ...

    @overload
    def __sub__(self, value: timedelta, /) -> Self: ...

    def __sub__(self, value: timedelta | datetime, /) -> Self | timedelta:
        del value
        raise AssertionError("projection must not call subclass subtraction")

    def astimezone(self, tz: tzinfo | None = None) -> Self:
        del tz
        raise AssertionError("projection must not call astimezone")

    def timestamp(self) -> float:
        raise AssertionError("projection must not call timestamp")


class HostileEpochInteger(int):
    """Lie through every overridable integer operation."""

    def __int__(self) -> int:
        raise AssertionError("decoder must not call the integer override")

    def __index__(self) -> int:
        raise AssertionError("decoder must not call the index override")

    def __lt__(self, value: object) -> bool:
        del value
        raise AssertionError("decoder must compare the stored base integer")

    def __le__(self, value: object) -> bool:
        del value
        raise AssertionError("decoder must compare the stored base integer")

    def __add__(self, value: object) -> int:
        del value
        raise AssertionError("decoder must use the stored base integer")


class IntegerLike:
    """Expose integer conversion without actually being an integer."""

    def __int__(self) -> int:
        raise AssertionError("decoder must reject before conversion")

    def __index__(self) -> int:
        raise AssertionError("decoder must reject before conversion")


class MasqueradingIntegerLike(IntegerLike):
    """Spoof ``isinstance(value, int)`` without being an integer."""

    @property  # type: ignore[misc]
    def __class__(self) -> type[int]:  # type: ignore[override]
        return int


class EpochMarker(IntEnum):
    """Exercise a conventional integer subclass."""

    EPOCH = 0


def test_require_canonical_utc_returns_the_original_object() -> None:
    value = datetime(2026, 7, 25, 14, 30, 15, 123456, tzinfo=UTC)

    assert require_canonical_utc(value) is value


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
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(timedelta(0), "named-zero")),
            id="named-zero-offset",
        ),
        pytest.param(
            datetime(2026, 1, 15, 14, 30, tzinfo=SeasonalZeroTimezone()),
            id="rule-based-zero-offset",
        ),
        pytest.param(
            ShiftedUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="utc-identity-with-overridden-offset",
        ),
        pytest.param(
            LyingOffsetUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="utc-identity-with-lying-offset",
        ),
        pytest.param(
            ExplodingUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="utc-identity-with-exploding-offset",
        ),
        pytest.param(
            MasqueradingUtcDatetime(
                2026,
                7,
                25,
                14,
                30,
                tzinfo=timezone(timedelta(hours=5)),
            ),
            id="reported-utc-with-different-stored-zone",
        ),
        pytest.param(date(2026, 7, 25), id="date-not-datetime"),
        pytest.param("2026-07-25T14:30:00.000000Z", id="text"),
        pytest.param(object(), id="arbitrary-object"),
    ],
)
def test_require_canonical_utc_rejects_noncanonical_values(value: object) -> None:
    with pytest.raises(CanonicalUtcError, match=r"tzinfo exactly datetime\.UTC"):
        require_canonical_utc(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            datetime(
                2026,
                7,
                25,
                14,
                30,
                15,
                123456,
                tzinfo=timezone(timedelta(hours=5, minutes=30)),
            ),
            datetime(2026, 7, 25, 9, 0, 15, 123456, tzinfo=UTC),
            id="positive-offset",
        ),
        pytest.param(
            datetime(
                2026,
                7,
                25,
                14,
                30,
                15,
                123456,
                tzinfo=timezone(-timedelta(hours=4)),
            ),
            datetime(2026, 7, 25, 18, 30, 15, 123456, tzinfo=UTC),
            id="negative-offset",
        ),
        pytest.param(
            datetime(
                2026,
                7,
                25,
                14,
                30,
                15,
                100000,
                tzinfo=timezone(timedelta(hours=5, minutes=30, seconds=45, microseconds=123456)),
            ),
            datetime(2026, 7, 25, 8, 59, 29, 976544, tzinfo=UTC),
            id="subminute-offset-with-microsecond-borrow",
        ),
        pytest.param(
            datetime(
                2026,
                1,
                15,
                14,
                30,
                15,
                123456,
                tzinfo=timezone(timedelta(0), "named-zero"),
            ),
            datetime(2026, 1, 15, 14, 30, 15, 123456, tzinfo=UTC),
            id="named-zero-offset",
        ),
        pytest.param(
            datetime(
                2026,
                1,
                15,
                14,
                30,
                15,
                123456,
                tzinfo=SeasonalZeroTimezone(),
            ),
            datetime(2026, 1, 15, 14, 30, 15, 123456, tzinfo=UTC),
            id="rule-based-zero-offset",
        ),
        pytest.param(
            datetime(2026, 11, 1, 1, 30, tzinfo=FoldTimezone(), fold=0),
            datetime(2026, 11, 1, 5, 30, tzinfo=UTC),
            id="fold-first-instant",
        ),
        pytest.param(
            datetime(2026, 11, 1, 1, 30, tzinfo=FoldTimezone(), fold=1),
            datetime(2026, 11, 1, 6, 30, tzinfo=UTC),
            id="fold-second-instant",
        ),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, 15, 123456, tzinfo=UTC),
            datetime(2026, 7, 25, 14, 30, 15, 123456, tzinfo=UTC),
            id="already-fixed-utc",
        ),
        pytest.param(
            datetime.min.replace(tzinfo=timezone(-timedelta(hours=1))),
            datetime(1, 1, 1, 1, tzinfo=UTC),
            id="representable-calendar-minimum",
        ),
        pytest.param(
            datetime.max.replace(tzinfo=timezone(timedelta(hours=1))),
            datetime(9999, 12, 31, 22, 59, 59, 999999, tzinfo=UTC),
            id="representable-calendar-maximum",
        ),
    ],
)
def test_normalize_aware_to_utc_preserves_the_instant(
    value: datetime,
    expected: datetime,
) -> None:
    normalized = normalize_aware_to_utc(value)

    assert normalized == expected
    assert normalized.tzinfo is UTC


@given(
    naive=st.datetimes(
        min_value=datetime(2, 1, 2),
        max_value=datetime(9998, 12, 30, 23, 59, 59, 999999),
    ),
    offset_microseconds=st.integers(
        min_value=-86_399_999_999,
        max_value=86_399_999_999,
    ),
)
def test_normalize_aware_to_utc_preserves_generated_fixed_offset_instants(
    naive: datetime,
    offset_microseconds: int,
) -> None:
    offset = timedelta(microseconds=offset_microseconds)
    aware = naive.replace(tzinfo=timezone(offset))

    normalized = normalize_aware_to_utc(aware)

    assert normalized.tzinfo is UTC
    assert normalized.utcoffset() == timedelta(0)
    assert normalized.replace(tzinfo=None) == naive - offset


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime(2026, 7, 25, 14, 30), id="naive"),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=NaiveLikeTimezone()),
            id="tzinfo-without-offset",
        ),
        pytest.param(
            MasqueradingUtcDatetime(2026, 7, 25, 14, 30),
            id="internally-naive-subclass",
        ),
        pytest.param(date(2026, 7, 25), id="date-not-datetime"),
        pytest.param(object(), id="arbitrary-object"),
    ],
)
def test_normalize_aware_to_utc_rejects_nonaware_values(value: object) -> None:
    with pytest.raises(CanonicalUtcError, match="aware datetime"):
        normalize_aware_to_utc(value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(
            datetime.min.replace(tzinfo=timezone(timedelta(hours=1))),
            id="underflow",
        ),
        pytest.param(
            datetime.max.replace(tzinfo=timezone(-timedelta(hours=1))),
            id="overflow",
        ),
        pytest.param(
            ShiftedUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="hostile-utc-subclass",
        ),
        pytest.param(
            LyingOffsetUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="lying-offset-subclass",
        ),
        pytest.param(
            MasqueradingUtcDatetime(
                2026,
                7,
                25,
                14,
                30,
                tzinfo=timezone(timedelta(hours=5)),
            ),
            id="inconsistent-subclass-state",
        ),
    ],
)
def test_normalize_aware_to_utc_rejects_unrepresentable_or_hostile_values(
    value: datetime,
) -> None:
    with pytest.raises(CanonicalUtcError, match=r"cannot be normalized to datetime\.UTC"):
        normalize_aware_to_utc(value)


def test_normalize_aware_to_utc_wraps_an_unstable_timezone_error() -> None:
    value = datetime(2026, 7, 25, 14, 30, tzinfo=FlakyTimezone())

    with pytest.raises(CanonicalUtcError):
        normalize_aware_to_utc(value)


def test_normalize_aware_to_utc_does_not_reinspect_validated_timezone() -> None:
    source_timezone = ThirdInspectionShiftTimezone()
    value = datetime(2026, 7, 25, 14, 30, 15, 123456, tzinfo=source_timezone)

    normalized = normalize_aware_to_utc(value)

    assert normalized == datetime(2026, 7, 25, 14, 30, 15, 123456, tzinfo=UTC)
    assert source_timezone.calls == 2


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            datetime(2026, 7, 25, 9, 0, 15, 0, tzinfo=UTC),
            "2026-07-25T09:00:15.000000Z",
            id="zero-microseconds",
        ),
        pytest.param(
            datetime(2026, 7, 25, 9, 0, 15, 1, tzinfo=UTC),
            "2026-07-25T09:00:15.000001Z",
            id="one-microsecond",
        ),
        pytest.param(
            datetime(2026, 7, 25, 9, 0, 15, 999999, tzinfo=UTC),
            "2026-07-25T09:00:15.999999Z",
            id="maximum-microsecond",
        ),
        pytest.param(
            datetime.min.replace(tzinfo=UTC),
            "0001-01-01T00:00:00.000000Z",
            id="minimum-calendar-value",
        ),
        pytest.param(
            datetime.max.replace(tzinfo=UTC),
            "9999-12-31T23:59:59.999999Z",
            id="maximum-calendar-value",
        ),
    ],
)
def test_serialize_canonical_utc_emits_exact_fixed_width_text(
    value: datetime,
    expected: str,
) -> None:
    serialized = serialize_canonical_utc(value)

    assert serialized == expected
    assert len(serialized) == 27
    assert CANONICAL_TEXT_PATTERN.fullmatch(serialized) is not None


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime(2026, 7, 25, 14, 30), id="naive"),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(timedelta(hours=1))),
            id="nonzero-offset",
        ),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(timedelta(0), "named-zero")),
            id="named-zero-offset",
        ),
        pytest.param("2026-07-25T14:30:00.000000Z", id="text"),
    ],
)
def test_serialize_canonical_utc_rejects_values_that_need_normalization(
    value: object,
) -> None:
    with pytest.raises(CanonicalUtcError, match=r"tzinfo exactly datetime\.UTC"):
        serialize_canonical_utc(value)


def test_serialize_canonical_utc_uses_stored_components_not_overrides() -> None:
    value = MisleadingComponentsDatetime(
        2026,
        7,
        25,
        9,
        0,
        15,
        123456,
        tzinfo=UTC,
    )

    assert serialize_canonical_utc(value) == "2026-07-25T09:00:15.123456Z"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            "2026-07-25T09:00:15.123456Z",
            datetime(2026, 7, 25, 9, 0, 15, 123456, tzinfo=UTC),
            id="ordinary",
        ),
        pytest.param(
            "2000-02-29T23:59:59.000001Z",
            datetime(2000, 2, 29, 23, 59, 59, 1, tzinfo=UTC),
            id="leap-day",
        ),
        pytest.param(
            "0001-01-01T00:00:00.000000Z",
            datetime.min.replace(tzinfo=UTC),
            id="minimum-calendar-value",
        ),
        pytest.param(
            "9999-12-31T23:59:59.999999Z",
            datetime.max.replace(tzinfo=UTC),
            id="maximum-calendar-value",
        ),
    ],
)
def test_parse_canonical_utc_accepts_only_canonical_text(
    text: str,
    expected: datetime,
) -> None:
    parsed = parse_canonical_utc(text)

    assert parsed == expected
    assert parsed.tzinfo is UTC


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("", id="empty"),
        pytest.param("2026-07-25T09:00:15Z", id="missing-fraction"),
        pytest.param("2026-07-25T09:00:15.0Z", id="one-fractional-digit"),
        pytest.param("2026-07-25T09:00:15.12345Z", id="five-fractional-digits"),
        pytest.param("2026-07-25T09:00:15.1234567Z", id="seven-fractional-digits"),
        pytest.param("2026-07-25T09:00:15.123456+00:00", id="numeric-utc-offset"),
        pytest.param("2026-07-25T10:00:15.123456+01:00", id="nonzero-offset"),
        pytest.param("2026-07-25T09:00:15.123456z", id="lowercase-z"),
        pytest.param("2026-07-25 09:00:15.123456Z", id="space-separator"),
        pytest.param("2026-07-25t09:00:15.123456Z", id="lowercase-t"),
        pytest.param("2026-07-25T09:00:15,123456Z", id="comma-fraction"),
        pytest.param("20260725T090015.123456Z", id="basic-form"),
        pytest.param(" 2026-07-25T09:00:15.123456Z", id="leading-space"),
        pytest.param("2026-07-25T09:00:15.123456Z ", id="trailing-space"),
        pytest.param("2026-07-25T09:00:15.123456Z\n", id="trailing-newline"),
        pytest.param(
            "\u06f2\u06f0\u06f2\u06f6-\u06f0\u06f7-\u06f2\u06f5"
            "T\u06f0\u06f9:\u06f0\u06f0:\u06f1\u06f5."
            "\u06f1\u06f2\u06f3\u06f4\u06f5\u06f6Z",
            id="non-ascii-digits",
        ),
        pytest.param("0000-01-01T00:00:00.000000Z", id="year-zero"),
        pytest.param("2026-13-01T00:00:00.000000Z", id="invalid-month"),
        pytest.param("2026-02-29T00:00:00.000000Z", id="invalid-day"),
        pytest.param("2026-07-25T24:00:00.000000Z", id="invalid-hour"),
        pytest.param("2026-07-25T09:60:00.000000Z", id="invalid-minute"),
        pytest.param("2026-07-25T09:00:60.000000Z", id="leap-second"),
    ],
)
def test_parse_canonical_utc_rejects_noncanonical_or_invalid_text(text: str) -> None:
    with pytest.raises(CanonicalUtcError, match="canonical UTC text"):
        parse_canonical_utc(text)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="none"),
        pytest.param(b"2026-07-25T09:00:15.123456Z", id="bytes"),
        pytest.param(20260725, id="integer"),
    ],
)
def test_parse_canonical_utc_rejects_nontext_values(value: object) -> None:
    with pytest.raises(CanonicalUtcError, match="canonical UTC text"):
        parse_canonical_utc(value)


@given(value=st.datetimes(timezones=st.just(UTC)))
def test_canonical_utc_text_round_trip_is_exact(value: datetime) -> None:
    serialized = serialize_canonical_utc(value)
    parsed = parse_canonical_utc(serialized)

    assert CANONICAL_TEXT_PATTERN.fullmatch(serialized) is not None
    assert serialize_canonical_utc(parsed) == serialized
    assert parsed == value
    assert parsed.tzinfo is UTC


def test_canonical_utc_fold_flag_does_not_change_the_canonical_text() -> None:
    ordinary = datetime(2026, 7, 25, 9, 0, 15, 123456, tzinfo=UTC, fold=0)
    folded = ordinary.replace(fold=1)

    assert require_canonical_utc(folded) is folded
    assert serialize_canonical_utc(folded) == serialize_canonical_utc(ordinary)
    parsed = parse_canonical_utc(serialize_canonical_utc(folded))
    assert parsed == folded
    assert parsed.fold == 0


def test_epoch_microsecond_bounds_are_exact_and_fit_sqlite_integer_storage() -> None:
    assert MIN_EPOCH_MICROSECONDS == -62_135_596_800_000_000
    assert MAX_EPOCH_MICROSECONDS == 253_402_300_799_999_999
    assert SQLITE_MIN_INTEGER <= MIN_EPOCH_MICROSECONDS
    assert MAX_EPOCH_MICROSECONDS <= SQLITE_MAX_INTEGER


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            datetime.min.replace(tzinfo=UTC),
            MIN_EPOCH_MICROSECONDS,
            id="calendar-minimum",
        ),
        pytest.param(
            datetime(1969, 12, 30, 23, 59, 59, 999999, tzinfo=UTC),
            -86_400_000_001,
            id="negative-day-minus-one-microsecond",
        ),
        pytest.param(
            datetime(1969, 12, 31, tzinfo=UTC),
            -86_400_000_000,
            id="negative-day-boundary",
        ),
        pytest.param(
            datetime(1969, 12, 31, 0, 0, 0, 1, tzinfo=UTC),
            -86_399_999_999,
            id="negative-day-plus-one-microsecond",
        ),
        pytest.param(
            datetime(1969, 12, 31, 23, 59, 58, 999999, tzinfo=UTC),
            -1_000_001,
            id="negative-second-minus-one-microsecond",
        ),
        pytest.param(
            datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC),
            -1_000_000,
            id="negative-second-boundary",
        ),
        pytest.param(
            datetime(1969, 12, 31, 23, 59, 59, 1, tzinfo=UTC),
            -999_999,
            id="negative-fraction",
        ),
        pytest.param(
            datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            -1,
            id="epoch-minus-one-microsecond",
        ),
        pytest.param(datetime(1970, 1, 1, tzinfo=UTC), 0, id="epoch"),
        pytest.param(
            datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
            1,
            id="epoch-plus-one-microsecond",
        ),
        pytest.param(
            datetime(1970, 1, 1, 0, 0, 0, 999999, tzinfo=UTC),
            999_999,
            id="positive-fraction",
        ),
        pytest.param(
            datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC),
            1_000_000,
            id="positive-second-boundary",
        ),
        pytest.param(
            datetime.max.replace(tzinfo=UTC),
            MAX_EPOCH_MICROSECONDS,
            id="calendar-maximum",
        ),
    ],
)
def test_to_epoch_microseconds_projects_exact_landmarks(
    value: datetime,
    expected: int,
) -> None:
    assert to_epoch_microseconds(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            MIN_EPOCH_MICROSECONDS,
            datetime.min.replace(tzinfo=UTC),
            id="calendar-minimum",
        ),
        pytest.param(
            -86_400_000_001,
            datetime(1969, 12, 30, 23, 59, 59, 999999, tzinfo=UTC),
            id="negative-day-minus-one-microsecond",
        ),
        pytest.param(
            -86_400_000_000,
            datetime(1969, 12, 31, tzinfo=UTC),
            id="negative-day-boundary",
        ),
        pytest.param(
            -86_399_999_999,
            datetime(1969, 12, 31, 0, 0, 0, 1, tzinfo=UTC),
            id="negative-day-plus-one-microsecond",
        ),
        pytest.param(
            -1_000_001,
            datetime(1969, 12, 31, 23, 59, 58, 999999, tzinfo=UTC),
            id="negative-second-minus-one-microsecond",
        ),
        pytest.param(
            -1_000_000,
            datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC),
            id="negative-second-boundary",
        ),
        pytest.param(
            -999_999,
            datetime(1969, 12, 31, 23, 59, 59, 1, tzinfo=UTC),
            id="negative-fraction",
        ),
        pytest.param(
            -1,
            datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            id="epoch-minus-one-microsecond",
        ),
        pytest.param(0, datetime(1970, 1, 1, tzinfo=UTC), id="epoch"),
        pytest.param(
            1,
            datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
            id="epoch-plus-one-microsecond",
        ),
        pytest.param(
            999_999,
            datetime(1970, 1, 1, 0, 0, 0, 999999, tzinfo=UTC),
            id="positive-fraction",
        ),
        pytest.param(
            1_000_000,
            datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC),
            id="positive-second-boundary",
        ),
        pytest.param(
            MAX_EPOCH_MICROSECONDS,
            datetime.max.replace(tzinfo=UTC),
            id="calendar-maximum",
        ),
    ],
)
def test_from_epoch_microseconds_decodes_exact_landmarks(
    value: int,
    expected: datetime,
) -> None:
    decoded = from_epoch_microseconds(value)

    assert type(decoded) is datetime
    assert decoded == expected
    assert decoded.tzinfo is UTC


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime(2026, 7, 25, 14, 30), id="naive"),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(timedelta(hours=1))),
            id="nonzero-offset",
        ),
        pytest.param(
            datetime(2026, 7, 25, 14, 30, tzinfo=timezone(timedelta(0), "named-zero")),
            id="named-zero-offset",
        ),
        pytest.param(
            ShiftedUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="overridden-offset",
        ),
        pytest.param(
            LyingOffsetUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="lying-offset",
        ),
        pytest.param(
            ExplodingUtcDatetime(2026, 7, 25, 14, 30, tzinfo=UTC),
            id="exploding-offset",
        ),
        pytest.param(
            MasqueradingUtcDatetime(
                2026,
                7,
                25,
                14,
                30,
                tzinfo=timezone(timedelta(hours=5)),
            ),
            id="masquerading-zone",
        ),
        pytest.param("1970-01-01T00:00:00.000000Z", id="text"),
    ],
)
def test_to_epoch_microseconds_reuses_the_strict_canonical_contract(value: object) -> None:
    with pytest.raises(CanonicalUtcError, match=r"tzinfo exactly datetime\.UTC"):
        to_epoch_microseconds(value)


def test_to_epoch_microseconds_uses_stored_components_and_base_arithmetic() -> None:
    value = HostileProjectionDatetime(
        2026,
        7,
        25,
        9,
        0,
        15,
        123456,
        tzinfo=UTC,
    )
    expected = to_epoch_microseconds(datetime(2026, 7, 25, 9, 0, 15, 123456, tzinfo=UTC))

    assert to_epoch_microseconds(value) == expected


def test_fixed_utc_fold_flag_does_not_change_epoch_projection() -> None:
    ordinary = datetime(2026, 7, 25, 9, 0, 15, 123456, tzinfo=UTC, fold=0)
    folded = ordinary.replace(fold=1)

    assert to_epoch_microseconds(folded) == to_epoch_microseconds(ordinary)
    assert from_epoch_microseconds(to_epoch_microseconds(folded)).fold == 0


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(True, id="true"),
        pytest.param(False, id="false"),
        pytest.param(0.0, id="float"),
        pytest.param("0", id="text"),
        pytest.param(b"0", id="bytes"),
        pytest.param(None, id="none"),
        pytest.param(datetime(1970, 1, 1, tzinfo=UTC), id="datetime"),
        pytest.param(timedelta(0), id="timedelta"),
        pytest.param(IntegerLike(), id="integer-like"),
        pytest.param(MasqueradingIntegerLike(), id="spoofed-integer-class"),
    ],
)
def test_from_epoch_microseconds_rejects_noninteger_values(value: object) -> None:
    with pytest.raises(CanonicalUtcError, match="integer"):
        from_epoch_microseconds(value)


def test_masquerading_integer_like_reaches_the_isinstance_trap() -> None:
    value = MasqueradingIntegerLike()

    assert isinstance(value, int)
    with pytest.raises(CanonicalUtcError, match="integer"):
        from_epoch_microseconds(value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(MIN_EPOCH_MICROSECONDS - 1, id="below-calendar-minimum"),
        pytest.param(MAX_EPOCH_MICROSECONDS + 1, id="above-calendar-maximum"),
        pytest.param(SQLITE_MIN_INTEGER, id="sqlite-minimum"),
        pytest.param(SQLITE_MAX_INTEGER, id="sqlite-maximum"),
        pytest.param(-(1 << 4096), id="enormous-negative"),
        pytest.param(1 << 4096, id="enormous-positive"),
    ],
)
def test_from_epoch_microseconds_rejects_out_of_range_integers(value: int) -> None:
    with pytest.raises(CanonicalUtcError, match="datetime range"):
        from_epoch_microseconds(value)


def test_from_epoch_microseconds_copies_hostile_integer_subclass_storage() -> None:
    value = HostileEpochInteger(1)

    assert from_epoch_microseconds(value) == datetime(
        1970,
        1,
        1,
        0,
        0,
        0,
        1,
        tzinfo=UTC,
    )


def test_from_epoch_microseconds_accepts_conventional_integer_subclasses() -> None:
    assert from_epoch_microseconds(EpochMarker.EPOCH) == datetime(1970, 1, 1, tzinfo=UTC)


@given(value=st.datetimes(timezones=st.just(UTC)))
def test_epoch_microsecond_datetime_round_trip_is_exact(value: datetime) -> None:
    projected = to_epoch_microseconds(value)
    decoded = from_epoch_microseconds(projected)

    assert decoded == value
    assert decoded.tzinfo is UTC
    assert to_epoch_microseconds(decoded) == projected


@given(
    value=st.integers(
        min_value=MIN_EPOCH_MICROSECONDS,
        max_value=MAX_EPOCH_MICROSECONDS,
    )
)
def test_epoch_microsecond_integer_round_trip_is_exact(value: int) -> None:
    decoded = from_epoch_microseconds(value)

    assert type(decoded) is datetime
    assert decoded.tzinfo is UTC
    assert to_epoch_microseconds(decoded) == value


@given(
    left=st.integers(
        min_value=MIN_EPOCH_MICROSECONDS,
        max_value=MAX_EPOCH_MICROSECONDS,
    ),
    right=st.integers(
        min_value=MIN_EPOCH_MICROSECONDS,
        max_value=MAX_EPOCH_MICROSECONDS,
    ),
)
def test_epoch_microsecond_projection_preserves_total_order(left: int, right: int) -> None:
    left_datetime = from_epoch_microseconds(left)
    right_datetime = from_epoch_microseconds(right)

    assert (left_datetime < right_datetime) is (left < right)
    assert (left_datetime == right_datetime) is (left == right)


@given(
    value=st.integers(
        min_value=MIN_EPOCH_MICROSECONDS,
        max_value=MAX_EPOCH_MICROSECONDS - 1,
    )
)
def test_adjacent_epoch_microseconds_remain_exactly_distinct(value: int) -> None:
    earlier = from_epoch_microseconds(value)
    later = from_epoch_microseconds(value + 1)

    assert later - earlier == timedelta(microseconds=1)
    assert to_epoch_microseconds(later) - to_epoch_microseconds(earlier) == 1


@pytest.mark.parametrize(
    "earlier",
    [
        pytest.param(
            datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            id="unix-epoch",
        ),
        pytest.param(
            datetime(1999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            id="millennium",
        ),
        pytest.param(
            datetime(2000, 2, 29, 23, 59, 59, 999999, tzinfo=UTC),
            id="leap-day",
        ),
        pytest.param(
            datetime(9998, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            id="late-calendar-boundary",
        ),
    ],
)
def test_one_microsecond_adjacency_crosses_calendar_boundaries_exactly(
    earlier: datetime,
) -> None:
    later = earlier + timedelta(microseconds=1)

    assert to_epoch_microseconds(later) == to_epoch_microseconds(earlier) + 1
