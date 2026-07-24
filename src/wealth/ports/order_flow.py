"""Persistence port for quality-approved canonical order-flow records."""

from typing import Protocol

from wealth.domain.order_flow_quality import (
    OrderFlowRecord,
    OrderFlowStream,
    OrderFlowWriteResult,
)


class OrderFlowStore(Protocol):
    """Persist canonical order flow without silent replacement."""

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        """Insert once or return an explicit idempotency outcome."""

    def records_for_stream(self, stream: OrderFlowStream) -> tuple[OrderFlowRecord, ...]:
        """Return an immutable, deterministically ordered stream snapshot."""
