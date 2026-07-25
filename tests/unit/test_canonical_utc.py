"""Unit and property tests for the unused canonical UTC codec primitives."""

import re
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wealth.domain.canonical_utc import (
    CanonicalUtcError,
    normalize_aware_to_utc,
    parse_canonical_utc,
    require_canonical_utc,
    serialize_canonical_utc,
)

CANONICAL_TEXT_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z"
)


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
