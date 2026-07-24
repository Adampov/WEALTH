"""Read-only JSON command line for collector service operational health."""

import argparse
import json
import sys
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Literal, Never, TextIO
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from wealth.adapters.foundation import SystemClock
from wealth.adapters.sqlite_collector_service import (
    SQLiteCollectorServiceHeartbeatStore,
    SQLiteCollectorServiceStorageError,
)
from wealth.application.collector_health import (
    CollectorServiceHealthClockRegressionError,
    CollectorServiceHealthMonitor,
    CollectorServiceHealthPolicy,
)
from wealth.domain.collector_service import (
    CollectorServiceAlertSeverity,
    CollectorServiceHealthAssessment,
    CollectorServiceHealthReport,
)
from wealth.ports.foundation import Clock

COLLECTOR_HEALTH_EXIT_OK = 0
COLLECTOR_HEALTH_EXIT_WARNING = 1
COLLECTOR_HEALTH_EXIT_CRITICAL = 2
COLLECTOR_HEALTH_EXIT_UNKNOWN = 3


class CollectorHealthCommandStatus(StrEnum):
    """Monitoring-compatible result classes for one command invocation."""

    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class CollectorHealthCommandOutput(BaseModel):
    """Stable successful JSON envelope for operator and monitoring consumers."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    command: Literal["collector_health"] = "collector_health"
    status: CollectorHealthCommandStatus
    report: CollectorServiceHealthReport
    alerts: tuple[CollectorServiceHealthAssessment, ...]

    @model_validator(mode="after")
    def output_matches_report(self) -> "CollectorHealthCommandOutput":
        """Tie command status and explicit alerts to the validated health report."""

        if self.alerts != self.report.alerts:
            raise ValueError("command alerts must match health report alerts")
        if any(
            alert.alert_severity is CollectorServiceAlertSeverity.CRITICAL for alert in self.alerts
        ):
            expected = CollectorHealthCommandStatus.CRITICAL
        elif any(
            alert.alert_severity is CollectorServiceAlertSeverity.WARNING for alert in self.alerts
        ):
            expected = CollectorHealthCommandStatus.WARNING
        elif not self.report.assessments:
            expected = CollectorHealthCommandStatus.UNKNOWN
        else:
            expected = CollectorHealthCommandStatus.OK
        if self.status is not expected:
            raise ValueError("command status must match the highest report severity")
        return self


class CollectorHealthCommandError(BaseModel):
    """Stable JSON error envelope written only to standard error."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    command: Literal["collector_health"] = "collector_health"
    status: Literal["unknown"] = "unknown"
    error_code: str = Field(min_length=1, max_length=128)
    detail: str = Field(min_length=1, max_length=1_000)


class CollectorHealthCliUsageError(ValueError):
    """Expose parser failures without allowing argparse to exit as critical."""


class CollectorHealthArgumentParser(argparse.ArgumentParser):
    """Route invalid operator input through the structured unknown result."""

    def error(self, message: str) -> Never:
        raise CollectorHealthCliUsageError(message)


def build_parser() -> argparse.ArgumentParser:
    """Build the deterministic collector health command interface."""

    parser = CollectorHealthArgumentParser(
        prog="wealth-collector-health",
        description="Read collector service health from an existing SQLite database.",
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Existing collector service SQLite database path.",
    )
    parser.add_argument(
        "--collection-id",
        required=True,
        type=_uuid_argument,
        help="Continuous collection UUID.",
    )
    parser.add_argument(
        "--stale-after-seconds",
        default=600.0,
        type=float,
        help="Nonterminal heartbeat age that becomes stale (default: 600).",
    )
    parser.add_argument(
        "--run-limit",
        default=1,
        type=int,
        help="Newest service runs to include, from 1 to 1000 (default: 1).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Indent JSON output for human inspection.",
    )
    return parser


def run_collector_health_cli(
    argv: Sequence[str],
    *,
    stdout: TextIO,
    stderr: TextIO,
    clock: Clock,
) -> int:
    """Execute the read-only command and return a monitoring-safe exit code."""

    try:
        arguments = build_parser().parse_args(argv)
        database = Path(str(arguments.database))
        collection_id = arguments.collection_id
        if not isinstance(collection_id, UUID):
            raise CollectorHealthCliUsageError("collection ID did not parse as a UUID")
        stale_after_seconds = float(arguments.stale_after_seconds)
        run_limit = int(arguments.run_limit)
        pretty = bool(arguments.pretty)
        policy = CollectorServiceHealthPolicy(
            stale_after_seconds=stale_after_seconds,
        )
        store = SQLiteCollectorServiceHeartbeatStore(database, read_only=True)
        report = CollectorServiceHealthMonitor(
            heartbeat_store=store,
            clock=clock,
            policy=policy,
        ).report(
            collection_id,
            run_limit=run_limit,
        )
        status = _command_status(report)
        output = CollectorHealthCommandOutput(
            status=status,
            report=report,
            alerts=report.alerts,
        )
    except CollectorHealthCliUsageError as error:
        _write_error(stderr, "invalid_arguments", str(error), pretty=False)
        return COLLECTOR_HEALTH_EXIT_UNKNOWN
    except SQLiteCollectorServiceStorageError as error:
        _write_error(stderr, f"storage_{error.code.value}", str(error), pretty=False)
        return COLLECTOR_HEALTH_EXIT_UNKNOWN
    except CollectorServiceHealthClockRegressionError as error:
        _write_error(stderr, "clock_regression", str(error), pretty=False)
        return COLLECTOR_HEALTH_EXIT_UNKNOWN
    except OSError as error:
        _write_error(stderr, "filesystem_error", str(error), pretty=False)
        return COLLECTOR_HEALTH_EXIT_UNKNOWN
    except ValueError as error:
        _write_error(stderr, "invalid_arguments", str(error), pretty=False)
        return COLLECTOR_HEALTH_EXIT_UNKNOWN

    _write_json(stdout, output, pretty=pretty)
    return {
        CollectorHealthCommandStatus.OK: COLLECTOR_HEALTH_EXIT_OK,
        CollectorHealthCommandStatus.WARNING: COLLECTOR_HEALTH_EXIT_WARNING,
        CollectorHealthCommandStatus.CRITICAL: COLLECTOR_HEALTH_EXIT_CRITICAL,
        CollectorHealthCommandStatus.UNKNOWN: COLLECTOR_HEALTH_EXIT_UNKNOWN,
    }[status]


def main() -> int:
    """Run the production command with wall-clock time and process streams."""

    return run_collector_health_cli(
        sys.argv[1:],
        stdout=sys.stdout,
        stderr=sys.stderr,
        clock=SystemClock(),
    )


def _command_status(
    report: CollectorServiceHealthReport,
) -> CollectorHealthCommandStatus:
    if any(
        alert.alert_severity is CollectorServiceAlertSeverity.CRITICAL for alert in report.alerts
    ):
        return CollectorHealthCommandStatus.CRITICAL
    if any(
        alert.alert_severity is CollectorServiceAlertSeverity.WARNING for alert in report.alerts
    ):
        return CollectorHealthCommandStatus.WARNING
    if not report.assessments:
        return CollectorHealthCommandStatus.UNKNOWN
    return CollectorHealthCommandStatus.OK


def _uuid_argument(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a valid UUID") from error


def _write_error(
    stream: TextIO,
    error_code: str,
    detail: str,
    *,
    pretty: bool,
) -> None:
    _write_json(
        stream,
        CollectorHealthCommandError(
            error_code=error_code,
            detail=detail[:1_000] or "unspecified_error",
        ),
        pretty=pretty,
    )


def _write_json(
    stream: TextIO,
    payload: BaseModel,
    *,
    pretty: bool,
) -> None:
    serialized = json.dumps(
        payload.model_dump(mode="json"),
        ensure_ascii=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
        sort_keys=True,
    )
    stream.write(serialized)
    stream.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
