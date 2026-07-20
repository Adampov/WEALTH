"""Foundation command-line entry points."""

from wealth.adapters.foundation import InMemoryEventStore, SystemClock, Uuid4Generator
from wealth.application.health import HealthCheckService
from wealth.domain.events import Environment
from wealth.observability.logging import StandardLibraryEventLogger, configure_json_logger


def main() -> int:
    """Run the synthetic, local-only foundation health slice."""

    logger = configure_json_logger()
    service = HealthCheckService(
        clock=SystemClock(),
        ids=Uuid4Generator(),
        store=InMemoryEventStore(),
        logger=StandardLibraryEventLogger(logger),
    )
    service.run(Environment.DEVELOPMENT)
    return 0
