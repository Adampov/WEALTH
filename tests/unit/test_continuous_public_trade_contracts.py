"""Tests for pure continuous public-trade planning and lifecycle contracts."""

from datetime import UTC, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import cast
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from wealth.domain.continuous_public_trade import (
    MAX_CONTRACT_INTEGER,
    ContinuousPublicTradeAttachment,
    ContinuousPublicTradePlan,
    ContinuousPublicTradePlanStatus,
    ContinuousPublicTradePolicy,
    ContinuousPublicTradeServiceStatus,
    ContinuousPublicTradeStreamCheckpoint,
    ContinuousPublicTradeStreamStatus,
    ContinuousPublicTradeTransitionKind,
    plan_continuous_public_trade_window,
    validate_continuous_public_trade_service_transition,
    validate_continuous_public_trade_stream_transition,
)
from wealth.domain.market import InstrumentType

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
POLICY_FINGERPRINT = f"sha256:{'a' * 64}"
OTHER_POLICY_FINGERPRINT = f"sha256:{'b' * 64}"
CREATION_FINGERPRINT = f"sha256:{'c' * 64}"
VALID_SERVICE_TRANSITIONS = frozenset(
    {
        (None, ContinuousPublicTradeServiceStatus.STARTING),
        (
            ContinuousPublicTradeServiceStatus.STARTING,
            ContinuousPublicTradeServiceStatus.RUNNING,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.STOPPED,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.PAUSED,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.FAILED,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.RUN_LIMIT,
        ),
    }
)


class IntSubclass(int):
    """An int subclass that strict whole-millisecond fields must reject."""


class StringSubclass(str):
    """A string subclass that exact identity and fingerprint fields must reject."""


class DatetimeSubclass(datetime):
    """A datetime subclass that the exact injected-clock boundary must reject."""


class ZeroOffsetTimezone(tzinfo):
    """A non-canonical zero-offset timezone implementation."""

    def utcoffset(self, dt: datetime | None) -> timedelta:
        return timedelta(0)

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str:
        return "ZERO"


def policy(**updates: object) -> ContinuousPublicTradePolicy:
    """Build one small finite operating policy."""

    values: dict[str, object] = {
        "window_size_ms": 1_000,
        "settlement_lag_ms": 250,
        "max_catchup_span_ms": 5_000,
        "max_jobs_per_invocation": 3,
        "max_requests_per_job": 100,
        "max_records_per_job": 10_000,
        "policy_fingerprint": POLICY_FINGERPRINT,
    }
    values.update(updates)
    return ContinuousPublicTradePolicy.model_validate(values)


def attachment(**updates: object) -> ContinuousPublicTradeAttachment:
    """Build one immutable bounded-child attachment."""

    values: dict[str, object] = {
        "job_id": UUID(int=101),
        "window_start_epoch_ms": 10_000,
        "window_end_epoch_ms": 15_000,
        "policy_fingerprint": POLICY_FINGERPRINT,
        "creation_fingerprint": CREATION_FINGERPRINT,
    }
    values.update(updates)
    return ContinuousPublicTradeAttachment.model_validate(values)


def checkpoint(**updates: object) -> ContinuousPublicTradeStreamCheckpoint:
    """Build one active stream with no child attached."""

    values: dict[str, object] = {
        "stream_id": UUID(int=1),
        "source": "binance.public-rest",
        "venue": "BINANCE",
        "instrument": "BTC-USDT",
        "provider_symbol": "BTCUSDT",
        "instrument_type": InstrumentType.SPOT,
        "request_variant": "aggregate-trades",
        "policy_fingerprint": POLICY_FINGERPRINT,
        "stream_start_epoch_ms": 0,
        "cursor_epoch_ms": 10_000,
        "status": ContinuousPublicTradeStreamStatus.ACTIVE,
        "version": 1,
    }
    values.update(updates)
    return ContinuousPublicTradeStreamCheckpoint.model_validate(values)


def copy_checkpoint(
    current: ContinuousPublicTradeStreamCheckpoint,
    **updates: object,
) -> ContinuousPublicTradeStreamCheckpoint:
    """Build one strict successor candidate."""

    values = current.model_dump()
    values.update(updates)
    return ContinuousPublicTradeStreamCheckpoint.model_validate(values)


def due_plan(
    *,
    current: ContinuousPublicTradeStreamCheckpoint | None = None,
    operating_policy: ContinuousPublicTradePolicy | None = None,
    now: datetime | None = None,
) -> ContinuousPublicTradePlan:
    """Plan one due range with deterministic candidate identity."""

    return plan_continuous_public_trade_window(
        checkpoint=current or checkpoint(),
        policy=operating_policy or policy(),
        now=now or EPOCH + timedelta(milliseconds=20_250),
        candidate_job_id=UUID(int=202),
        candidate_creation_fingerprint=CREATION_FINGERPRINT,
    )


@pytest.mark.parametrize(
    "field",
    [
        "window_size_ms",
        "settlement_lag_ms",
        "max_catchup_span_ms",
        "max_jobs_per_invocation",
        "max_requests_per_job",
        "max_records_per_job",
    ],
)
@pytest.mark.parametrize(
    "invalid",
    [
        True,
        False,
        1.0,
        "1",
        Decimal("1"),
        IntSubclass(1),
    ],
)
def test_policy_rejects_every_non_exact_builtin_integer(field: str, invalid: object) -> None:
    with pytest.raises(ValidationError, match=field):
        policy(**{field: invalid})


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("window_size_ms", 0),
        ("window_size_ms", -1),
        ("settlement_lag_ms", -1),
        ("max_catchup_span_ms", 0),
        ("max_jobs_per_invocation", 0),
        ("max_requests_per_job", 0),
        ("max_records_per_job", 0),
    ],
)
def test_policy_rejects_nonfinite_work_or_invalid_millisecond_bounds(
    field: str,
    invalid: int,
) -> None:
    with pytest.raises(ValidationError):
        policy(**{field: invalid})


@pytest.mark.parametrize(
    "field",
    [
        "window_size_ms",
        "settlement_lag_ms",
        "max_catchup_span_ms",
        "max_jobs_per_invocation",
        "max_requests_per_job",
        "max_records_per_job",
    ],
)
def test_policy_integer_fields_reject_values_above_the_finite_contract_bound(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        policy(**{field: MAX_CONTRACT_INTEGER + 1})


def test_exact_maximum_contract_integer_is_accepted_when_invariants_allow() -> None:
    maximum_policy = policy(
        window_size_ms=1,
        settlement_lag_ms=MAX_CONTRACT_INTEGER,
        max_catchup_span_ms=MAX_CONTRACT_INTEGER,
        max_jobs_per_invocation=MAX_CONTRACT_INTEGER,
        max_requests_per_job=MAX_CONTRACT_INTEGER,
        max_records_per_job=MAX_CONTRACT_INTEGER,
    )
    maximum_checkpoint = checkpoint(
        cursor_epoch_ms=MAX_CONTRACT_INTEGER,
        version=MAX_CONTRACT_INTEGER,
    )
    maximum_attachment = attachment(
        window_start_epoch_ms=MAX_CONTRACT_INTEGER - 1,
        window_end_epoch_ms=MAX_CONTRACT_INTEGER,
    )

    assert maximum_policy.max_catchup_span_ms == MAX_CONTRACT_INTEGER
    assert maximum_checkpoint.cursor_epoch_ms == MAX_CONTRACT_INTEGER
    assert maximum_attachment.window_end_epoch_ms == MAX_CONTRACT_INTEGER


def test_policy_requires_catchup_span_to_preserve_the_utc_grid() -> None:
    with pytest.raises(ValidationError, match=r"multiple|align|grid"):
        policy(max_catchup_span_ms=1_500)


@pytest.mark.parametrize(
    "value",
    [
        "sha256:" + "A" * 64,
        "a" * 64,
        "sha256:" + "a" * 63,
        "sha256:" + "g" * 64,
        " sha256:" + "a" * 64,
    ],
)
def test_policy_and_attachment_reject_malformed_fingerprints(value: str) -> None:
    with pytest.raises(ValidationError, match="fingerprint"):
        policy(policy_fingerprint=value)
    with pytest.raises(ValidationError, match="fingerprint"):
        attachment(creation_fingerprint=value)


def test_string_subclasses_are_rejected_for_identity_and_fingerprint_fields() -> None:
    with pytest.raises(ValidationError, match="source"):
        checkpoint(source=StringSubclass("binance.public-rest"))
    with pytest.raises(ValidationError, match="policy_fingerprint"):
        policy(policy_fingerprint=StringSubclass(POLICY_FINGERPRINT))
    with pytest.raises(ValidationError, match="creation_fingerprint"):
        attachment(creation_fingerprint=StringSubclass(CREATION_FINGERPRINT))


@pytest.mark.parametrize(
    "field",
    ["source", "venue", "instrument", "provider_symbol", "request_variant"],
)
@pytest.mark.parametrize("value", ["", " leading", "trailing ", "has space", "has\ttab"])
def test_stream_identity_is_nonempty_and_canonical(field: str, value: str) -> None:
    with pytest.raises(ValidationError):
        checkpoint(**{field: value})


def test_models_reject_unknown_enums_are_frozen_and_forbid_extra_fields() -> None:
    with pytest.raises(ValidationError):
        checkpoint(status="unknown")
    with pytest.raises(ValidationError):
        checkpoint(instrument_type="unknown")

    current = checkpoint()
    with pytest.raises(ValidationError, match="frozen"):
        current.version = 2
    with pytest.raises(ValidationError, match="Extra inputs"):
        ContinuousPublicTradeStreamCheckpoint.model_validate(
            {**current.model_dump(), "automatic_recovery": True}
        )
    with pytest.raises(ValidationError, match="Extra inputs"):
        ContinuousPublicTradePolicy.model_validate({**policy().model_dump(), "unbounded": True})
    with pytest.raises(ValidationError, match="Extra inputs"):
        ContinuousPublicTradeAttachment.model_validate(
            {**attachment().model_dump(), "replace_existing": True}
        )
    planned = due_plan()
    with pytest.raises(ValidationError, match="frozen"):
        planned.status = ContinuousPublicTradePlanStatus.WAITING
    with pytest.raises(ValidationError, match="Extra inputs"):
        ContinuousPublicTradePlan.model_validate(
            {**planned.model_dump(), "schedule_successor": True}
        )


def test_schema_versions_and_uuid_identities_are_exact() -> None:
    with pytest.raises(ValidationError, match="schema_version"):
        policy(schema_version="2.0")
    with pytest.raises(ValidationError, match="schema_version"):
        checkpoint(schema_version=StringSubclass("1.0"))
    with pytest.raises(ValidationError, match="stream_id"):
        checkpoint(stream_id=str(UUID(int=1)))
    with pytest.raises(ValidationError, match="job_id"):
        attachment(job_id=str(UUID(int=101)))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stream_start_epoch_ms", True),
        ("stream_start_epoch_ms", 0.0),
        ("stream_start_epoch_ms", IntSubclass(0)),
        ("cursor_epoch_ms", False),
        ("cursor_epoch_ms", "10000"),
        ("cursor_epoch_ms", IntSubclass(10_000)),
        ("version", True),
        ("version", 1.0),
        ("version", IntSubclass(1)),
    ],
)
def test_checkpoint_rejects_non_exact_integer_state(field: str, value: object) -> None:
    with pytest.raises(ValidationError, match=field):
        checkpoint(**{field: value})


@pytest.mark.parametrize(
    "field",
    ["stream_start_epoch_ms", "cursor_epoch_ms", "version"],
)
def test_checkpoint_integer_fields_reject_values_above_the_finite_contract_bound(
    field: str,
) -> None:
    with pytest.raises(ValidationError, match=field):
        checkpoint(**{field: MAX_CONTRACT_INTEGER + 1})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("window_start_epoch_ms", True),
        ("window_start_epoch_ms", 10_000.0),
        ("window_start_epoch_ms", IntSubclass(10_000)),
        ("window_end_epoch_ms", False),
        ("window_end_epoch_ms", "15000"),
        ("window_end_epoch_ms", IntSubclass(15_000)),
    ],
)
def test_attachment_rejects_non_exact_integer_ranges(field: str, value: object) -> None:
    with pytest.raises(ValidationError, match=field):
        attachment(**{field: value})


@pytest.mark.parametrize(
    "field",
    ["window_start_epoch_ms", "window_end_epoch_ms"],
)
def test_attachment_boundaries_reject_values_above_the_finite_contract_bound(
    field: str,
) -> None:
    with pytest.raises(ValidationError, match=field):
        attachment(**{field: MAX_CONTRACT_INTEGER + 1})


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (10_000, 10_000),
        (10_001, 10_000),
        (-1, 1_000),
    ],
)
def test_attachment_rejects_empty_regressive_or_negative_ranges(start: int, end: int) -> None:
    with pytest.raises(ValidationError, match=r"range|window|start"):
        attachment(window_start_epoch_ms=start, window_end_epoch_ms=end)


def test_checkpoint_rejects_regressive_cursor_and_inconsistent_pause_or_attachment() -> None:
    with pytest.raises(ValidationError, match="cursor"):
        checkpoint(stream_start_epoch_ms=11_000)
    with pytest.raises(ValidationError, match="pause"):
        checkpoint(status=ContinuousPublicTradeStreamStatus.PAUSED)
    with pytest.raises(ValidationError, match="pause"):
        checkpoint(pause_reason="schema_drift")
    with pytest.raises(ValidationError, match="policy"):
        checkpoint(attachment=attachment(policy_fingerprint=OTHER_POLICY_FINGERPRINT))
    with pytest.raises(ValidationError, match=r"cursor|start"):
        checkpoint(attachment=attachment(window_start_epoch_ms=9_000))


@pytest.mark.parametrize(
    "invalid_now",
    [
        EPOCH.replace(tzinfo=None),
        EPOCH.astimezone(timezone(timedelta(hours=3))),
        EPOCH.replace(tzinfo=timezone(timedelta(0), "ZERO")),
        EPOCH.replace(tzinfo=ZoneInfo("UTC")),
        EPOCH.replace(tzinfo=ZeroOffsetTimezone()),
        DatetimeSubclass(1970, 1, 1, tzinfo=UTC),
    ],
)
def test_planner_accepts_only_exact_builtin_datetime_with_datetime_utc(
    invalid_now: datetime,
) -> None:
    with pytest.raises((TypeError, ValueError), match=r"UTC|datetime"):
        due_plan(now=invalid_now)


def test_planner_rejects_an_unrepresentable_eligible_boundary_explicitly() -> None:
    maximum_policy = policy(
        window_size_ms=MAX_CONTRACT_INTEGER,
        settlement_lag_ms=MAX_CONTRACT_INTEGER,
        max_catchup_span_ms=MAX_CONTRACT_INTEGER,
    )

    with pytest.raises(ValueError, match="representable contract range"):
        plan_continuous_public_trade_window(
            checkpoint=checkpoint(cursor_epoch_ms=0),
            policy=maximum_policy,
            now=datetime.min.replace(tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("offset_microseconds", "expected_status", "expected_eligible"),
    [
        (-1, ContinuousPublicTradePlanStatus.WAITING, 9_000),
        (0, ContinuousPublicTradePlanStatus.ATTACHED_JOB, 10_000),
        (1, ContinuousPublicTradePlanStatus.ATTACHED_JOB, 10_000),
        (999, ContinuousPublicTradePlanStatus.ATTACHED_JOB, 10_000),
    ],
)
def test_exact_pre_at_post_eligibility_and_microsecond_floor(
    offset_microseconds: int,
    expected_status: ContinuousPublicTradePlanStatus,
    expected_eligible: int,
) -> None:
    current = checkpoint(cursor_epoch_ms=9_000)
    now = EPOCH + timedelta(milliseconds=10_250, microseconds=offset_microseconds)

    result = plan_continuous_public_trade_window(
        checkpoint=current,
        policy=policy(),
        now=now,
        candidate_job_id=UUID(int=202),
        candidate_creation_fingerprint=CREATION_FINGERPRINT,
    )

    assert result.status is expected_status
    assert result.latest_eligible_end_epoch_ms == expected_eligible
    if expected_status is ContinuousPublicTradePlanStatus.ATTACHED_JOB:
        assert result.attachment is not None
        assert result.attachment.window_start_epoch_ms == 9_000
        assert result.attachment.window_end_epoch_ms == 10_000
    else:
        assert result.attachment is None


def test_settlement_lag_and_grid_are_applied_before_finite_catchup() -> None:
    current = checkpoint(cursor_epoch_ms=2_000)
    operating_policy = policy(
        window_size_ms=2_000,
        settlement_lag_ms=750,
        max_catchup_span_ms=4_000,
    )

    result = due_plan(
        current=current,
        operating_policy=operating_policy,
        now=EPOCH + timedelta(milliseconds=10_749, microseconds=999),
    )

    assert result.latest_eligible_end_epoch_ms == 8_000
    assert result.attachment is not None
    assert result.attachment.window_start_epoch_ms == 2_000
    assert result.attachment.window_end_epoch_ms == 6_000


def test_planner_rejects_stream_or_cursor_that_is_not_on_the_policy_grid() -> None:
    with pytest.raises(ValueError, match=r"align|grid"):
        due_plan(current=checkpoint(cursor_epoch_ms=10_001))
    with pytest.raises(ValueError, match=r"align|grid"):
        due_plan(current=checkpoint(stream_start_epoch_ms=1))


def test_planner_rejects_misaligned_or_widened_existing_attachment() -> None:
    with pytest.raises(ValueError, match=r"align|grid"):
        due_plan(current=checkpoint(attachment=attachment(window_end_epoch_ms=14_500)))
    with pytest.raises(ValueError, match=r"finite|span|exceed"):
        due_plan(current=checkpoint(attachment=attachment(window_end_epoch_ms=16_000)))


def test_target_end_is_the_minimum_of_eligibility_and_finite_catchup() -> None:
    truncated = due_plan()
    assert truncated.latest_eligible_end_epoch_ms == 20_000
    assert truncated.attachment is not None
    assert truncated.attachment.window_start_epoch_ms == 10_000
    assert truncated.attachment.window_end_epoch_ms == 15_000

    eligible_first = due_plan(
        operating_policy=policy(max_catchup_span_ms=20_000),
        now=EPOCH + timedelta(milliseconds=13_250),
    )
    assert eligible_first.latest_eligible_end_epoch_ms == 13_000
    assert eligible_first.attachment is not None
    assert eligible_first.attachment.window_end_epoch_ms == 13_000


def test_manual_hold_preserves_cursor_and_exact_attachment_without_a_candidate() -> None:
    existing = attachment()
    held = checkpoint(
        status=ContinuousPublicTradeStreamStatus.PAUSED,
        pause_reason="schema_drift",
        attachment=existing,
    )

    result = plan_continuous_public_trade_window(
        checkpoint=held,
        policy=policy(),
        now=EPOCH + timedelta(milliseconds=20_250),
    )

    assert result.status is ContinuousPublicTradePlanStatus.HELD
    assert result.cursor_epoch_ms == held.cursor_epoch_ms
    assert result.attachment is existing


def test_existing_attachment_is_returned_unchanged_and_needs_no_candidate() -> None:
    existing = attachment()
    current = checkpoint(attachment=existing)

    result = plan_continuous_public_trade_window(
        checkpoint=current,
        policy=policy(),
        now=EPOCH + timedelta(milliseconds=20_250),
    )

    assert result.status is ContinuousPublicTradePlanStatus.ATTACHED_JOB
    assert result.attachment is existing
    assert result.attachment.model_dump() == existing.model_dump()


@pytest.mark.parametrize(
    "invalid_attachment",
    [
        attachment(window_start_epoch_ms=9_000),
        attachment(policy_fingerprint=OTHER_POLICY_FINGERPRINT),
    ],
)
def test_held_plan_model_rejects_attachment_cursor_or_policy_mismatch(
    invalid_attachment: ContinuousPublicTradeAttachment,
) -> None:
    with pytest.raises(ValidationError, match=r"attachment|cursor|policy"):
        ContinuousPublicTradePlan(
            status=ContinuousPublicTradePlanStatus.HELD,
            stream_id=UUID(int=1),
            policy_fingerprint=POLICY_FINGERPRINT,
            cursor_epoch_ms=10_000,
            latest_eligible_end_epoch_ms=20_000,
            attachment=invalid_attachment,
        )


def test_direct_plan_rejects_waiting_while_a_closed_range_is_eligible() -> None:
    with pytest.raises(ValidationError, match=r"waiting|eligible"):
        ContinuousPublicTradePlan(
            status=ContinuousPublicTradePlanStatus.WAITING,
            stream_id=UUID(int=1),
            policy_fingerprint=POLICY_FINGERPRINT,
            cursor_epoch_ms=10_000,
            latest_eligible_end_epoch_ms=11_000,
            attachment=None,
        )


def test_existing_attachment_precedes_an_earlier_current_eligibility_boundary() -> None:
    existing = attachment(window_end_epoch_ms=15_000)
    result = plan_continuous_public_trade_window(
        checkpoint=checkpoint(attachment=existing),
        policy=policy(),
        now=EPOCH + timedelta(milliseconds=9_250),
    )

    assert result.status is ContinuousPublicTradePlanStatus.ATTACHED_JOB
    assert result.latest_eligible_end_epoch_ms == 9_000
    assert result.attachment is existing
    assert result.attachment.model_dump() == existing.model_dump()


@pytest.mark.parametrize("cursor", [20_000, 21_000])
def test_cursor_at_or_after_eligible_boundary_waits_without_an_attachment(cursor: int) -> None:
    result = plan_continuous_public_trade_window(
        checkpoint=checkpoint(cursor_epoch_ms=cursor),
        policy=policy(),
        now=EPOCH + timedelta(milliseconds=20_250),
    )

    assert result.status is ContinuousPublicTradePlanStatus.WAITING
    assert result.cursor_epoch_ms == cursor
    assert result.attachment is None


@pytest.mark.parametrize(
    ("candidate_job_id", "candidate_creation_fingerprint"),
    [
        (None, CREATION_FINGERPRINT),
        (UUID(int=202), None),
        ("00000000-0000-0000-0000-0000000000ca", CREATION_FINGERPRINT),
        (UUID(int=202), "not-a-sha256"),
        (UUID(int=202), "sha256:" + "C" * 64),
    ],
)
def test_due_work_requires_exact_candidate_uuid_and_creation_fingerprint(
    candidate_job_id: object,
    candidate_creation_fingerprint: object,
) -> None:
    with pytest.raises((TypeError, ValueError), match=r"candidate|UUID|fingerprint"):
        plan_continuous_public_trade_window(
            checkpoint=checkpoint(),
            policy=policy(),
            now=EPOCH + timedelta(milliseconds=20_250),
            candidate_job_id=cast(UUID | None, candidate_job_id),
            candidate_creation_fingerprint=cast(str | None, candidate_creation_fingerprint),
        )


def test_planner_rejects_policy_fingerprint_mismatch() -> None:
    with pytest.raises(ValueError, match="policy"):
        due_plan(operating_policy=policy(policy_fingerprint=OTHER_POLICY_FINGERPRINT))


@given(
    window_ms=st.integers(min_value=1, max_value=10_000),
    settlement_ms=st.integers(min_value=0, max_value=10_000),
    cursor_windows=st.integers(min_value=0, max_value=100),
    eligible_delta=st.integers(min_value=0, max_value=100),
    catchup_windows=st.integers(min_value=1, max_value=20),
    fractional_microseconds=st.integers(min_value=0, max_value=999),
)
def test_property_planner_never_opens_skips_overlaps_or_widens_a_window(
    window_ms: int,
    settlement_ms: int,
    cursor_windows: int,
    eligible_delta: int,
    catchup_windows: int,
    fractional_microseconds: int,
) -> None:
    cursor = cursor_windows * window_ms
    eligible = (cursor_windows + eligible_delta) * window_ms
    operating_policy = policy(
        window_size_ms=window_ms,
        settlement_lag_ms=settlement_ms,
        max_catchup_span_ms=catchup_windows * window_ms,
    )
    current = checkpoint(
        stream_start_epoch_ms=0,
        cursor_epoch_ms=cursor,
    )
    now = EPOCH + timedelta(
        milliseconds=eligible + settlement_ms,
        microseconds=fractional_microseconds,
    )

    result = plan_continuous_public_trade_window(
        checkpoint=current,
        policy=operating_policy,
        now=now,
        candidate_job_id=UUID(int=202),
        candidate_creation_fingerprint=CREATION_FINGERPRINT,
    )

    assert result.latest_eligible_end_epoch_ms == eligible
    assert result.cursor_epoch_ms == cursor
    if eligible_delta == 0:
        assert result.status is ContinuousPublicTradePlanStatus.WAITING
        assert result.attachment is None
        return

    assert result.status is ContinuousPublicTradePlanStatus.ATTACHED_JOB
    assert result.attachment is not None
    assert result.attachment.window_start_epoch_ms == cursor
    assert result.attachment.window_end_epoch_ms == min(
        eligible,
        cursor + catchup_windows * window_ms,
    )
    assert result.attachment.window_end_epoch_ms <= eligible
    assert result.attachment.window_end_epoch_ms > cursor
    assert result.attachment.window_end_epoch_ms % window_ms == 0


def test_stream_attach_retain_hold_resume_and_exact_completion_are_valid() -> None:
    initial = checkpoint()
    attached_child = attachment()
    attached = copy_checkpoint(initial, attachment=attached_child, version=2)
    retained = copy_checkpoint(attached, version=3)
    held = copy_checkpoint(
        retained,
        status=ContinuousPublicTradeStreamStatus.PAUSED,
        pause_reason="schema_drift",
        version=4,
    )
    resumed = copy_checkpoint(
        held,
        status=ContinuousPublicTradeStreamStatus.ACTIVE,
        pause_reason=None,
        version=5,
    )
    completed = copy_checkpoint(
        resumed,
        cursor_epoch_ms=attached_child.window_end_epoch_ms,
        attachment=None,
        version=6,
    )

    validate_continuous_public_trade_stream_transition(
        initial,
        attached,
        ContinuousPublicTradeTransitionKind.ATTACH,
        policy=policy(),
    )
    validate_continuous_public_trade_stream_transition(
        attached,
        retained,
        ContinuousPublicTradeTransitionKind.RETAIN,
        policy=policy(),
    )
    validate_continuous_public_trade_stream_transition(
        retained,
        held,
        ContinuousPublicTradeTransitionKind.MANUAL_HOLD,
        policy=policy(),
    )
    validate_continuous_public_trade_stream_transition(
        held,
        resumed,
        ContinuousPublicTradeTransitionKind.MANUAL_RESUME,
        policy=policy(),
    )
    validate_continuous_public_trade_stream_transition(
        resumed,
        completed,
        ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
        policy=policy(),
        completed_job_id=attached_child.job_id,
    )


def test_stream_transition_requires_exactly_one_version_step() -> None:
    initial = checkpoint()
    for version in (1, 3):
        successor = copy_checkpoint(initial, version=version)
        with pytest.raises(ValueError, match="version"):
            validate_continuous_public_trade_stream_transition(
                initial,
                successor,
                ContinuousPublicTradeTransitionKind.RETAIN,
                policy=policy(),
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stream_id", UUID(int=999)),
        ("source", "coinbase.public-rest"),
        ("venue", "COINBASE"),
        ("instrument", "ETH-USD"),
        ("provider_symbol", "ETH-USD"),
        ("instrument_type", InstrumentType.PERPETUAL_FUTURE),
        ("request_variant", "trades"),
        ("policy_fingerprint", OTHER_POLICY_FINGERPRINT),
        ("stream_start_epoch_ms", 1_000),
    ],
)
def test_stream_transition_preserves_identity_and_policy_fingerprint(
    field: str,
    value: object,
) -> None:
    initial = checkpoint()
    successor = copy_checkpoint(initial, version=2, **{field: value})
    with pytest.raises(ValueError, match=r"identity|policy|immutable"):
        validate_continuous_public_trade_stream_transition(
            initial,
            successor,
            ContinuousPublicTradeTransitionKind.RETAIN,
            policy=policy(),
        )


@pytest.mark.parametrize(
    ("kind", "updates"),
    [
        (
            ContinuousPublicTradeTransitionKind.MANUAL_HOLD,
            {
                "status": ContinuousPublicTradeStreamStatus.PAUSED,
                "pause_reason": "schema_drift",
                "cursor_epoch_ms": 11_000,
            },
        ),
        (
            ContinuousPublicTradeTransitionKind.MANUAL_RESUME,
            {
                "status": ContinuousPublicTradeStreamStatus.ACTIVE,
                "pause_reason": None,
                "cursor_epoch_ms": 11_000,
            },
        ),
    ],
)
def test_hold_and_resume_cannot_change_progress(
    kind: ContinuousPublicTradeTransitionKind,
    updates: dict[str, object],
) -> None:
    initial = checkpoint()
    if kind is ContinuousPublicTradeTransitionKind.MANUAL_RESUME:
        initial = checkpoint(
            status=ContinuousPublicTradeStreamStatus.PAUSED,
            pause_reason="schema_drift",
        )
    successor = copy_checkpoint(initial, version=2, **updates)
    with pytest.raises(ValueError, match=r"cursor|progress"):
        validate_continuous_public_trade_stream_transition(
            initial,
            successor,
            kind,
            policy=policy(),
        )


def test_attach_requires_exact_cursor_start_and_cannot_attach_while_paused() -> None:
    initial = checkpoint()
    with pytest.raises(ValidationError, match=r"cursor|start|gap"):
        copy_checkpoint(
            initial,
            attachment=attachment(window_start_epoch_ms=11_000),
            version=2,
        )

    held = checkpoint(
        status=ContinuousPublicTradeStreamStatus.PAUSED,
        pause_reason="manual_hold",
    )
    proposed = copy_checkpoint(held, attachment=attachment(), version=2)
    with pytest.raises(ValueError, match=r"attach|active|paused|hold"):
        validate_continuous_public_trade_stream_transition(
            held,
            proposed,
            ContinuousPublicTradeTransitionKind.ATTACH,
            policy=policy(),
        )


@pytest.mark.parametrize(
    ("kind", "invalid_attachment", "message"),
    [
        (
            ContinuousPublicTradeTransitionKind.ATTACH,
            attachment(window_end_epoch_ms=14_500),
            "align",
        ),
        (
            ContinuousPublicTradeTransitionKind.ATTACH,
            attachment(window_end_epoch_ms=16_000),
            "finite catch-up span",
        ),
        (
            ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
            attachment(window_end_epoch_ms=14_500),
            "align",
        ),
        (
            ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
            attachment(window_end_epoch_ms=16_000),
            "finite catch-up span",
        ),
    ],
)
def test_attach_and_child_completion_reject_off_grid_or_widened_attachment(
    kind: ContinuousPublicTradeTransitionKind,
    invalid_attachment: ContinuousPublicTradeAttachment,
    message: str,
) -> None:
    if kind is ContinuousPublicTradeTransitionKind.ATTACH:
        previous = checkpoint()
        current = copy_checkpoint(previous, attachment=invalid_attachment, version=2)
        completed_job_id = None
    else:
        previous = checkpoint(attachment=invalid_attachment)
        current = copy_checkpoint(
            previous,
            cursor_epoch_ms=invalid_attachment.window_end_epoch_ms,
            attachment=None,
            version=2,
        )
        completed_job_id = invalid_attachment.job_id

    with pytest.raises(ValueError, match=message):
        validate_continuous_public_trade_stream_transition(
            previous,
            current,
            kind,
            policy=policy(),
            completed_job_id=completed_job_id,
        )


@pytest.mark.parametrize(
    "changed_attachment",
    [
        attachment(job_id=UUID(int=999)),
        attachment(window_end_epoch_ms=14_000),
        attachment(creation_fingerprint=f"sha256:{'d' * 64}"),
    ],
)
def test_attached_child_cannot_be_replaced(
    changed_attachment: ContinuousPublicTradeAttachment,
) -> None:
    initial = checkpoint(attachment=attachment())
    successor = copy_checkpoint(initial, attachment=changed_attachment, version=2)
    with pytest.raises(ValueError, match=r"retain|attachment|replace|immutable"):
        validate_continuous_public_trade_stream_transition(
            initial,
            successor,
            ContinuousPublicTradeTransitionKind.RETAIN,
            policy=policy(),
        )


def test_attachment_cannot_clear_or_cursor_advance_without_exact_child_completion() -> None:
    existing = attachment()
    initial = checkpoint(attachment=existing)

    cleared = copy_checkpoint(initial, attachment=None, version=2)
    with pytest.raises(ValueError, match=r"retain|attachment|clear|completion"):
        validate_continuous_public_trade_stream_transition(
            initial,
            cleared,
            ContinuousPublicTradeTransitionKind.RETAIN,
            policy=policy(),
        )

    advanced = initial.model_copy(update={"cursor_epoch_ms": 15_000, "version": 2})
    with pytest.raises(ValueError, match=r"retain|cursor|completion"):
        validate_continuous_public_trade_stream_transition(
            initial,
            advanced,
            ContinuousPublicTradeTransitionKind.RETAIN,
            policy=policy(),
        )


def test_noncompletion_transition_rejects_completed_job_identity() -> None:
    initial = checkpoint()
    successor = copy_checkpoint(initial, version=2)
    with pytest.raises(ValueError, match="completed_job_id"):
        validate_continuous_public_trade_stream_transition(
            initial,
            successor,
            ContinuousPublicTradeTransitionKind.RETAIN,
            policy=policy(),
            completed_job_id=UUID(int=101),
        )


@pytest.mark.parametrize(
    ("cursor", "completed_job_id"),
    [
        (14_000, UUID(int=101)),
        (16_000, UUID(int=101)),
        (15_000, UUID(int=999)),
        (15_000, None),
    ],
)
def test_child_completion_requires_exact_job_and_exact_attached_end(
    cursor: int,
    completed_job_id: UUID | None,
) -> None:
    existing = attachment()
    initial = checkpoint(attachment=existing)
    successor = copy_checkpoint(
        initial,
        cursor_epoch_ms=cursor,
        attachment=None,
        version=2,
    )
    with pytest.raises(ValueError, match=r"job|end|cursor|completion"):
        validate_continuous_public_trade_stream_transition(
            initial,
            successor,
            ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
            policy=policy(),
            completed_job_id=completed_job_id,
        )


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        (None, ContinuousPublicTradeServiceStatus.STARTING),
        (
            ContinuousPublicTradeServiceStatus.STARTING,
            ContinuousPublicTradeServiceStatus.RUNNING,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.STOPPED,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.PAUSED,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.FAILED,
        ),
        (
            ContinuousPublicTradeServiceStatus.RUNNING,
            ContinuousPublicTradeServiceStatus.RUN_LIMIT,
        ),
    ],
)
def test_service_transition_matrix_accepts_only_defined_edges(
    previous: ContinuousPublicTradeServiceStatus | None,
    current: ContinuousPublicTradeServiceStatus,
) -> None:
    validate_continuous_public_trade_service_transition(previous, current)


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        (previous, current)
        for previous in (None, *tuple(ContinuousPublicTradeServiceStatus))
        for current in ContinuousPublicTradeServiceStatus
        if (previous, current) not in VALID_SERVICE_TRANSITIONS
    ],
)
def test_service_transition_matrix_rejects_all_undefined_and_terminal_edges(
    previous: ContinuousPublicTradeServiceStatus | None,
    current: ContinuousPublicTradeServiceStatus,
) -> None:
    with pytest.raises(ValueError, match=r"service|transition|starting|terminal"):
        validate_continuous_public_trade_service_transition(previous, current)
