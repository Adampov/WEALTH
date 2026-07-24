"""Interruptible local service lifecycle around continuous candle polling."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from wealth.application.continuous_collection import (
    ContinuousCollectionCycleResult,
    ContinuousCollectionCycleStatus,
)
from wealth.domain.collector_service import (
    CollectorCycleStatus,
    CollectorServiceHeartbeat,
    CollectorServiceStatus,
)
from wealth.domain.continuous_collection import ContinuousCollectionCheckpoint
from wealth.ports.collector_service import (
    CollectorServiceHeartbeatStore,
    CollectorServiceHeartbeatWriteStatus,
    ShutdownSignal,
)
from wealth.ports.foundation import Clock, IdGenerator

MAX_COLLECTOR_SERVICE_CYCLES = 10_000


class ContinuousCollectorProcess(Protocol):
    """Expose only the continuous operations required by the service runner."""

    def checkpoint(self, collection_id: UUID) -> ContinuousCollectionCheckpoint:
        """Return the current validated continuous cursor."""

    def run_cycle(self, collection_id: UUID) -> ContinuousCollectionCycleResult:
        """Attempt one bounded continuous collection cycle."""


@dataclass(frozen=True, slots=True)
class CollectorServiceRunResult:
    """Return the terminal durable observation for one local invocation."""

    run_id: UUID
    heartbeat: CollectorServiceHeartbeat
    cycles_attempted: int


@dataclass(frozen=True, slots=True)
class ContinuousCollectorServiceRunner:
    """Run supervised cycles until a bounded or operator-visible terminal state."""

    collector: ContinuousCollectorProcess
    heartbeat_store: CollectorServiceHeartbeatStore
    clock: Clock
    id_generator: IdGenerator
    shutdown: ShutdownSignal
    worker_id: str

    def __post_init__(self) -> None:
        if (
            not self.worker_id
            or self.worker_id != self.worker_id.strip()
            or any(character.isspace() for character in self.worker_id)
            or len(self.worker_id) > 128
        ):
            raise ValueError("collector service worker_id must be canonical and bounded")

    def run(
        self,
        collection_id: UUID,
        *,
        cycle_limit: int,
    ) -> CollectorServiceRunResult:
        """Run interruptible cycles and persist every lifecycle transition."""

        if not 1 <= cycle_limit <= MAX_COLLECTOR_SERVICE_CYCLES:
            raise ValueError(f"cycle_limit must be between 1 and {MAX_COLLECTOR_SERVICE_CYCLES}")
        run_id = self.id_generator.new()
        checkpoint = self.collector.checkpoint(collection_id)
        heartbeat = CollectorServiceHeartbeat(
            heartbeat_id=self.id_generator.new(),
            run_id=run_id,
            collection_id=collection_id,
            worker_id=self.worker_id,
            sequence=1,
            observed_at=self._now(),
            status=CollectorServiceStatus.STARTING,
            cycles_attempted=0,
            checkpoint_version=checkpoint.version,
            next_window_start=checkpoint.next_window_start,
        )
        self._append(heartbeat)
        if self.shutdown.requested():
            terminal = self._next_heartbeat(
                heartbeat,
                checkpoint=checkpoint,
                status=CollectorServiceStatus.STOPPED,
                cycles_attempted=0,
                last_cycle_status=None,
                reason_code="shutdown_requested",
            )
            self._append(terminal)
            return self._result(terminal)

        cycles_attempted = 0
        while True:
            if self.shutdown.requested():
                terminal = self._next_heartbeat(
                    heartbeat,
                    checkpoint=checkpoint,
                    status=CollectorServiceStatus.STOPPED,
                    cycles_attempted=cycles_attempted,
                    last_cycle_status=heartbeat.last_cycle_status,
                    reason_code="shutdown_requested",
                )
                self._append(terminal)
                return self._result(terminal)

            cycle = self.collector.run_cycle(collection_id)
            checkpoint = cycle.checkpoint
            cycles_attempted += 1
            cycle_status = CollectorCycleStatus(cycle.status.value)
            if cycle.status is ContinuousCollectionCycleStatus.PAUSED:
                terminal = self._next_heartbeat(
                    heartbeat,
                    checkpoint=checkpoint,
                    status=CollectorServiceStatus.PAUSED,
                    cycles_attempted=cycles_attempted,
                    last_cycle_status=cycle_status,
                    reason_code=checkpoint.pause_reason or "collection_paused",
                )
                self._append(terminal)
                return self._result(terminal)
            if cycle.status in {
                ContinuousCollectionCycleStatus.ALREADY_RUNNING,
                ContinuousCollectionCycleStatus.CHECKPOINT_CONFLICT,
                ContinuousCollectionCycleStatus.LOST_LEASE,
            }:
                terminal = self._next_heartbeat(
                    heartbeat,
                    checkpoint=checkpoint,
                    status=CollectorServiceStatus.FAILED,
                    cycles_attempted=cycles_attempted,
                    last_cycle_status=cycle_status,
                    reason_code=cycle.status.value,
                )
                self._append(terminal)
                return self._result(terminal)

            heartbeat = self._next_heartbeat(
                heartbeat,
                checkpoint=checkpoint,
                status=CollectorServiceStatus.RUNNING,
                cycles_attempted=cycles_attempted,
                last_cycle_status=cycle_status,
                reason_code=None,
            )
            self._append(heartbeat)
            if cycles_attempted >= cycle_limit:
                terminal = self._next_heartbeat(
                    heartbeat,
                    checkpoint=checkpoint,
                    status=CollectorServiceStatus.CYCLE_LIMIT,
                    cycles_attempted=cycles_attempted,
                    last_cycle_status=cycle_status,
                    reason_code="cycle_limit_reached",
                )
                self._append(terminal)
                return self._result(terminal)
            if cycle.wait_seconds and self.shutdown.wait(cycle.wait_seconds):
                terminal = self._next_heartbeat(
                    heartbeat,
                    checkpoint=checkpoint,
                    status=CollectorServiceStatus.STOPPED,
                    cycles_attempted=cycles_attempted,
                    last_cycle_status=cycle_status,
                    reason_code="shutdown_requested",
                )
                self._append(terminal)
                return self._result(terminal)

    def _next_heartbeat(
        self,
        previous: CollectorServiceHeartbeat,
        *,
        checkpoint: ContinuousCollectionCheckpoint,
        status: CollectorServiceStatus,
        cycles_attempted: int,
        last_cycle_status: CollectorCycleStatus | None,
        reason_code: str | None,
    ) -> CollectorServiceHeartbeat:
        return CollectorServiceHeartbeat(
            heartbeat_id=self.id_generator.new(),
            run_id=previous.run_id,
            collection_id=previous.collection_id,
            worker_id=previous.worker_id,
            sequence=previous.sequence + 1,
            observed_at=self._now(),
            status=status,
            cycles_attempted=cycles_attempted,
            checkpoint_version=checkpoint.version,
            next_window_start=checkpoint.next_window_start,
            last_cycle_status=last_cycle_status,
            reason_code=reason_code,
        )

    def _append(self, heartbeat: CollectorServiceHeartbeat) -> None:
        write = self.heartbeat_store.append(heartbeat)
        if write.status is not CollectorServiceHeartbeatWriteStatus.INSERTED:
            raise RuntimeError(
                f"collector service heartbeat was not durably appended: {write.status.value}"
            )

    def _now(self) -> datetime:
        now = self.clock.now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("collector service clock must return timezone-aware timestamps")
        return now

    @staticmethod
    def _result(heartbeat: CollectorServiceHeartbeat) -> CollectorServiceRunResult:
        return CollectorServiceRunResult(
            run_id=heartbeat.run_id,
            heartbeat=heartbeat,
            cycles_attempted=heartbeat.cycles_attempted,
        )
