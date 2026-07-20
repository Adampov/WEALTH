"""Foundation health-event use case."""

from dataclasses import dataclass

from wealth.domain.events import DomainEvent, Environment, EventType
from wealth.ports.foundation import Clock, EventLogger, EventStore, IdGenerator


@dataclass(frozen=True, slots=True)
class HealthCheckService:
    """Validate, store, and log one synthetic health event."""

    clock: Clock
    ids: IdGenerator
    store: EventStore
    logger: EventLogger

    def run(self, environment: Environment) -> DomainEvent:
        """Create a deterministic event using only injected external capabilities."""

        now = self.clock.now()
        event = DomainEvent(
            event_id=self.ids.new(),
            correlation_id=self.ids.new(),
            event_type=EventType.SYSTEM_HEALTH,
            source="wealth.health",
            environment=environment,
            event_time=now,
            observed_at=now,
            processed_at=now,
            payload={"status": "ok"},
        )
        self.store.append(event)
        self.logger.record(event)
        return event
