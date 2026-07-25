"""Strict contract coverage for the unused timestamp-byte evidence foundation."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from wealth.adapters.sqlite_preflight import SQLITE_TIMESTAMP_EXTRACTION_PLANS
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_KEY_COLUMNS,
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_TARGETS,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteTimestampCellEvidence,
    SQLiteTimestampExtractionPlan,
    SQLiteTimestampExtractionTarget,
    SQLiteTimestampRowEvidence,
    SQLiteTimestampTableEvidence,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PREFLIGHT_ADAPTER_PATH = REPOSITORY_ROOT / "src" / "wealth" / "adapters" / "sqlite_preflight.py"

EXPECTED_TARGETS: dict[
    SQLiteStoreFamily,
    tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...],
] = {
    SQLiteStoreFamily.MARKET: (
        (
            "candle_conflicts",
            ("existing_record_id", "incoming_record_id"),
            ("open_time", "detected_at"),
        ),
        ("canonical_candles", ("record_id",), ("open_time",)),
        ("raw_market_payloads", ("record_id",), ("observed_at", "processed_at")),
    ),
    SQLiteStoreFamily.ORDER_FLOW: (
        ("canonical_order_flow_records", ("record_id",), ("event_time",)),
        (
            "order_flow_conflicts",
            ("existing_record_id", "incoming_record_id"),
            ("event_time", "detected_at"),
        ),
        (
            "raw_order_flow_payloads",
            ("record_id",),
            ("observed_at", "processed_at"),
        ),
    ),
    SQLiteStoreFamily.HISTORICAL_COLLECTION: (
        ("collection_jobs", ("job_id",), ("next_window_start",)),
        ("collection_transitions", ("job_id", "version"), ("recorded_at",)),
        ("source_health_observations", ("observation_id",), ("observed_at",)),
    ),
    SQLiteStoreFamily.CONTINUOUS_COLLECTION: (
        (
            "continuous_collection_checkpoints",
            ("collection_id",),
            (
                "next_window_start",
                "active_window_end_exclusive",
                "next_retry_at",
            ),
        ),
        (
            "continuous_collection_transitions",
            ("collection_id", "version"),
            ("recorded_at",),
        ),
    ),
    SQLiteStoreFamily.COLLECTOR_SERVICE: (
        ("collector_service_heartbeats", ("run_id", "sequence"), ("observed_at",)),
        ("collector_service_runs", ("run_id",), ("observed_at",)),
    ),
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION: (
        (
            "public_trade_collection_jobs",
            ("job_id",),
            (
                "window_start",
                "window_end_exclusive",
                "next_window_start",
                "pending_window_end_exclusive",
                "created_at",
                "updated_at",
                "lease_expires_at",
            ),
        ),
        (
            "public_trade_collection_leases",
            ("job_id", "lease_token"),
            ("acquired_at",),
        ),
        (
            "public_trade_collection_transitions",
            ("job_id", "version"),
            ("recorded_at",),
        ),
        (
            "public_trade_source_health",
            ("job_id", "checkpoint_version"),
            (
                "range_start",
                "range_end_exclusive",
                "next_window_start",
                "pending_window_end_exclusive",
                "observed_at",
            ),
        ),
    ),
    SQLiteStoreFamily.RATE_BUDGET: (
        ("rate_budget_reservations", ("reservation_id",), ("requested_at",)),
        (
            "rate_budget_state",
            ("budget_key",),
            ("theoretical_arrival_us", "last_observed_us"),
        ),
    ),
    SQLiteStoreFamily.RECONCILIATION: (
        ("reconciliation_observations", ("observation_id",), ("recorded_at",)),
    ),
}


def _cell(
    column_name: str,
    value: str,
    *,
    storage_class: SQLiteStorageClass = SQLiteStorageClass.TEXT,
) -> SQLiteTimestampCellEvidence:
    encoded = value.encode()
    return SQLiteTimestampCellEvidence(
        column_name=column_name,
        storage_class=storage_class,
        blob_hex=encoded.hex().upper(),
        byte_length=len(encoded),
    )


def test_timestamp_plan_registry_is_exact_complete_and_pinned() -> None:
    assert tuple(plan.family for plan in SQLITE_TIMESTAMP_EXTRACTION_PLANS) == tuple(
        SQLiteStoreFamily
    )
    assert len(SQLITE_TIMESTAMP_EXTRACTION_PLANS) == 8
    assert sum(len(plan.targets) for plan in SQLITE_TIMESTAMP_EXTRACTION_PLANS) == 20
    assert (
        sum(
            len(target.timestamp_columns)
            for plan in SQLITE_TIMESTAMP_EXTRACTION_PLANS
            for target in plan.targets
        )
        == 37
    )

    for plan in SQLITE_TIMESTAMP_EXTRACTION_PLANS:
        assert plan.schema_version == "1.0"
        assert plan.layout_version == 1
        assert plan.max_rows_per_target == MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET
        assert (
            tuple(
                (
                    target.table_name,
                    target.stable_row_key_columns,
                    target.timestamp_columns,
                )
                for target in plan.targets
            )
            == EXPECTED_TARGETS[plan.family]
        )


def test_target_contract_rejects_aliases_duplicates_overlaps_and_unknown_fields() -> None:
    valid = {
        "table_name": "sample_rows",
        "stable_row_key_columns": ("record_id",),
        "timestamp_columns": ("observed_at",),
    }
    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampExtractionTarget.model_validate({**valid, "limit": 1})
    for alias in ("rowid", "_rowid_", "oid"):
        with pytest.raises(ValidationError, match="stable row-key"):
            SQLiteTimestampExtractionTarget.model_validate(
                {**valid, "stable_row_key_columns": (alias,)}
            )
    with pytest.raises(ValidationError, match="stable row-key"):
        SQLiteTimestampExtractionTarget.model_validate(
            {**valid, "stable_row_key_columns": ("record_id", "record_id")}
        )
    with pytest.raises(ValidationError, match="stable row-key"):
        SQLiteTimestampExtractionTarget.model_validate(
            {**valid, "stable_row_key_columns": ("record_id", "RECORD_ID")}
        )
    with pytest.raises(ValidationError, match="timestamp columns"):
        SQLiteTimestampExtractionTarget.model_validate(
            {**valid, "timestamp_columns": ("observed_at", "observed_at")}
        )
    with pytest.raises(ValidationError, match="timestamp columns"):
        SQLiteTimestampExtractionTarget.model_validate(
            {**valid, "timestamp_columns": ("observed_at", "OBSERVED_AT")}
        )
    with pytest.raises(ValidationError, match="may not be used"):
        SQLiteTimestampExtractionTarget.model_validate(
            {**valid, "timestamp_columns": ("record_id",)}
        )
    with pytest.raises(ValidationError, match="may not be used"):
        SQLiteTimestampExtractionTarget.model_validate(
            {**valid, "timestamp_columns": ("RECORD_ID",)}
        )
    with pytest.raises(ValidationError, match="stable row-key"):
        SQLiteTimestampExtractionTarget.model_validate(
            {
                **valid,
                "stable_row_key_columns": tuple(
                    f"key_{index}" for index in range(MAX_SQLITE_TIMESTAMP_KEY_COLUMNS + 1)
                ),
            }
        )
    with pytest.raises(ValidationError, match="timestamp columns"):
        SQLiteTimestampExtractionTarget.model_validate(
            {
                **valid,
                "timestamp_columns": tuple(
                    f"timestamp_{index}"
                    for index in range(MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET + 1)
                ),
            }
        )


def test_plan_contract_is_strict_frozen_canonical_and_fixed_bound() -> None:
    plan = SQLITE_TIMESTAMP_EXTRACTION_PLANS[0]
    payload = plan.model_dump()

    for invalid_limit in (True, "64", 0, 63, 65):
        with pytest.raises(ValidationError):
            SQLiteTimestampExtractionPlan.model_validate(
                {**payload, "max_rows_per_target": invalid_limit}
            )
    with pytest.raises(ValidationError, match="canonical table order"):
        SQLiteTimestampExtractionPlan.model_validate(
            {**payload, "targets": tuple(reversed(plan.targets))}
        )
    with pytest.raises(ValidationError, match="each table only once"):
        SQLiteTimestampExtractionPlan.model_validate(
            {**payload, "targets": (plan.targets[0], plan.targets[0])}
        )
    case_alias = plan.targets[0].model_copy(
        update={"table_name": plan.targets[0].table_name.upper()}
    )
    with pytest.raises(ValidationError, match="each table only once"):
        SQLiteTimestampExtractionPlan.model_validate(
            {**payload, "targets": (plan.targets[0], case_alias)}
        )
    bounded_targets = tuple(
        plan.targets[0].model_copy(update={"table_name": f"table_{index:02d}"})
        for index in range(MAX_SQLITE_TIMESTAMP_TARGETS + 1)
    )
    with pytest.raises(ValidationError, match="bounded target count"):
        SQLiteTimestampExtractionPlan.model_validate({**payload, "targets": bounded_targets})
    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampExtractionPlan.model_validate({**payload, "offset": 0})
    with pytest.raises(ValidationError):
        plan.family = SQLiteStoreFamily.ORDER_FLOW


def test_cell_evidence_preserves_exact_bytes_and_rejects_fabrication() -> None:
    evidence = SQLiteTimestampCellEvidence(
        column_name="observed_at",
        storage_class=SQLiteStorageClass.BLOB,
        blob_hex="00FF8041",
        byte_length=4,
    )
    assert evidence.blob_hex == "00FF8041"
    with pytest.raises(ValidationError, match="byte_length"):
        SQLiteTimestampCellEvidence(
            column_name="observed_at",
            storage_class=SQLiteStorageClass.TEXT,
            blob_hex="41",
            byte_length=2,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampCellEvidence(
            column_name="observed_at",
            storage_class=SQLiteStorageClass.TEXT,
            blob_hex="ff",
            byte_length=1,
        )
    with pytest.raises(ValidationError, match="SQLite NULL"):
        SQLiteTimestampCellEvidence(
            column_name="observed_at",
            storage_class=SQLiteStorageClass.NULL,
            blob_hex="00",
            byte_length=1,
        )
    with pytest.raises(ValidationError):
        evidence.byte_length = 3


def test_table_evidence_rejects_wrong_shape_duplicate_keys_and_unstable_order() -> None:
    target = SQLiteTimestampExtractionTarget(
        table_name="sample_rows",
        stable_row_key_columns=("record_id",),
        timestamp_columns=("observed_at",),
    )
    row_a = SQLiteTimestampRowEvidence(
        row_ordinal=0,
        stable_row_key=(_cell("record_id", "a"),),
        timestamp_cells=(_cell("observed_at", "2026-01-01T00:00:00Z"),),
    )
    row_b = SQLiteTimestampRowEvidence(
        row_ordinal=1,
        stable_row_key=(_cell("record_id", "b"),),
        timestamp_cells=(_cell("observed_at", "2026-01-02T00:00:00Z"),),
    )

    evidence = SQLiteTimestampTableEvidence(
        target_ordinal=0,
        target=target,
        rows=(row_a, row_b),
    )
    assert evidence.rows == (row_a, row_b)
    duplicate = row_b.model_copy(
        update={"stable_row_key": row_a.stable_row_key},
    )
    with pytest.raises(ValidationError, match="unique"):
        SQLiteTimestampTableEvidence(
            target_ordinal=0,
            target=target,
            rows=(row_a, duplicate),
        )
    reversed_a = row_a.model_copy(update={"row_ordinal": 1})
    reversed_b = row_b.model_copy(update={"row_ordinal": 0})
    with pytest.raises(ValidationError, match="deterministic"):
        SQLiteTimestampTableEvidence(
            target_ordinal=0,
            target=target,
            rows=(reversed_b, reversed_a),
        )
    wrong_column = row_a.model_copy(update={"timestamp_cells": (_cell("created_at", "x"),)})
    with pytest.raises(ValidationError, match="does not match"):
        SQLiteTimestampTableEvidence(
            target_ordinal=0,
            target=target,
            rows=(wrong_column,),
        )

    with pytest.raises(ValidationError, match="may not contain SQLite NULL"):
        SQLiteTimestampRowEvidence(
            row_ordinal=0,
            stable_row_key=(
                SQLiteTimestampCellEvidence(
                    column_name="record_id",
                    storage_class=SQLiteStorageClass.NULL,
                    blob_hex="",
                    byte_length=0,
                ),
            ),
            timestamp_cells=(_cell("observed_at", "x"),),
        )
    with pytest.raises(ValidationError, match="row-key evidence"):
        SQLiteTimestampRowEvidence(
            row_ordinal=0,
            stable_row_key=(
                _cell("record_id", "a"),
                _cell("RECORD_ID", "b"),
            ),
            timestamp_cells=(_cell("observed_at", "x"),),
        )
    with pytest.raises(ValidationError, match="may not overlap"):
        SQLiteTimestampRowEvidence(
            row_ordinal=0,
            stable_row_key=(_cell("record_id", "a"),),
            timestamp_cells=(_cell("RECORD_ID", "x"),),
        )


def test_adapter_has_no_timestamp_parser_or_runtime_consumer_dependency() -> None:
    source = PREFLIGHT_ADAPTER_PATH.read_text(encoding="utf-8")

    assert "datetime" not in source
    assert "canonical_utc" not in source
    assert "fromisoformat" not in source
    assert "strptime" not in source
