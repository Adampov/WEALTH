"""Pure canonical UTC value and text-codec primitives.

These helpers are intentionally not wired into existing models, serializers, adapters, or
persistence paths. Normalization is explicit so internal callers cannot silently coerce an
invalid timestamp at a strict boundary.
"""

import re
from datetime import UTC, datetime, timedelta
from typing import Final

__all__ = [
    "CanonicalUtcError",
    "normalize_aware_to_utc",
    "parse_canonical_utc",
    "require_canonical_utc",
    "serialize_canonical_utc",
]

_CANONICAL_UTC_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?P<year>[0-9]{4})-"
    r"(?P<month>[0-9]{2})-"
    r"(?P<day>[0-9]{2})T"
    r"(?P<hour>[0-9]{2}):"
    r"(?P<minute>[0-9]{2}):"
    r"(?P<second>[0-9]{2})\."
    r"(?P<microsecond>[0-9]{6})Z"
)


class CanonicalUtcError(ValueError):
    """Reject a value or text representation outside the canonical UTC contract."""


def _copy_stored_offset(value: timedelta | None) -> timedelta | None:
    """Copy a possibly subclassed offset from its stored base fields."""

    if value is None:
        return None
    return timedelta(
        days=timedelta.days.__get__(value),
        seconds=timedelta.seconds.__get__(value),
        microseconds=timedelta.microseconds.__get__(value),
    )


def require_canonical_utc(value: object) -> datetime:
    """Return an existing fixed-UTC datetime unchanged or fail closed."""

    if not isinstance(value, datetime):
        raise CanonicalUtcError(
            "value must be a datetime with tzinfo exactly datetime.UTC and zero offset"
        )
    try:
        reported_timezone = value.tzinfo
        stored_timezone = datetime.tzinfo.__get__(value)
        reported_offset = _copy_stored_offset(value.utcoffset())
        stored_offset = _copy_stored_offset(datetime.utcoffset(value))
    except Exception as error:
        raise CanonicalUtcError(
            "value must be a datetime with tzinfo exactly datetime.UTC and zero offset"
        ) from error
    if (
        reported_timezone is not UTC
        or stored_timezone is not UTC
        or reported_offset != timedelta(0)
        or stored_offset != timedelta(0)
    ):
        raise CanonicalUtcError(
            "value must be a datetime with tzinfo exactly datetime.UTC and zero offset"
        )
    return value


def normalize_aware_to_utc(value: object) -> datetime:
    """Convert one explicitly supplied aware datetime edge value to fixed UTC."""

    if not isinstance(value, datetime):
        raise CanonicalUtcError("value must be an aware datetime")
    try:
        reported_timezone = value.tzinfo
        stored_timezone = datetime.tzinfo.__get__(value)
        reported_offset = _copy_stored_offset(value.utcoffset())
        stored_offset = _copy_stored_offset(datetime.utcoffset(value))
    except Exception as error:
        raise CanonicalUtcError("value must be an aware datetime") from error
    if (
        reported_timezone is None
        or stored_timezone is None
        or reported_offset is None
        or stored_offset is None
    ):
        raise CanonicalUtcError("value must be an aware datetime")
    if reported_timezone is not stored_timezone or reported_offset != stored_offset:
        raise CanonicalUtcError("value cannot be normalized to datetime.UTC")

    try:
        stored_wall_time = datetime(
            datetime.year.__get__(value),
            datetime.month.__get__(value),
            datetime.day.__get__(value),
            datetime.hour.__get__(value),
            datetime.minute.__get__(value),
            datetime.second.__get__(value),
            datetime.microsecond.__get__(value),
            fold=datetime.fold.__get__(value),
        )
        normalized = (stored_wall_time - stored_offset).replace(tzinfo=UTC)
        return require_canonical_utc(normalized)
    except Exception as error:
        raise CanonicalUtcError("value cannot be normalized to datetime.UTC") from error


def serialize_canonical_utc(value: object) -> str:
    """Serialize one fixed-UTC datetime as exact six-digit RFC 3339 ``Z`` text."""

    canonical = require_canonical_utc(value)
    return (
        f"{datetime.year.__get__(canonical):04d}-"
        f"{datetime.month.__get__(canonical):02d}-"
        f"{datetime.day.__get__(canonical):02d}T"
        f"{datetime.hour.__get__(canonical):02d}:"
        f"{datetime.minute.__get__(canonical):02d}:"
        f"{datetime.second.__get__(canonical):02d}."
        f"{datetime.microsecond.__get__(canonical):06d}Z"
    )


def parse_canonical_utc(value: object) -> datetime:
    """Parse only exact six-digit RFC 3339 ``Z`` text into fixed UTC."""

    if not isinstance(value, str):
        raise CanonicalUtcError("value must use canonical UTC text YYYY-MM-DDTHH:MM:SS.ffffffZ")
    match = _CANONICAL_UTC_PATTERN.fullmatch(value)
    if match is None:
        raise CanonicalUtcError("value must use canonical UTC text YYYY-MM-DDTHH:MM:SS.ffffffZ")

    components = {name: int(component) for name, component in match.groupdict().items()}
    try:
        return datetime(
            components["year"],
            components["month"],
            components["day"],
            components["hour"],
            components["minute"],
            components["second"],
            components["microsecond"],
            tzinfo=UTC,
        )
    except ValueError as error:
        raise CanonicalUtcError(
            "value must use canonical UTC text YYYY-MM-DDTHH:MM:SS.ffffffZ"
        ) from error
