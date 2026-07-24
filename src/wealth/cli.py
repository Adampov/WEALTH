"""Foundation command-line entry points."""

import logging

from wealth.adapters.foundation import InMemoryEventStore, SystemClock, Uuid4Generator
from wealth.application.health import HealthCheckService
from wealth.observability.logging import StandardLibraryEventLogger, configure_json_logger
from wealth.settings import RuntimeSettings


def main() -> int:
    """Run the synthetic, local-only foundation health slice."""

    settings = RuntimeSettings()
    logger = configure_json_logger(level=getattr(logging, settings.log_level))
    service = HealthCheckService(
        clock=SystemClock(),
        ids=Uuid4Generator(),
        store=InMemoryEventStore(),
        logger=StandardLibraryEventLogger(logger),
    )
    service.run(settings.environment)
    return 0
