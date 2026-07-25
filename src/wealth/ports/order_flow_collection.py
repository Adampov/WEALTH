"""Persistence boundary for public-trade collection control state."""

from typing import Protocol
from uuid import UUID

from wealth.domain.order_flow_collection import (
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionHealthSummary,
    PublicTradeCollectionTransition,
    PublicTradeSourceHealthObservation,
)
from wealth.ports.collection import CollectionCheckpointWriteResult

DEFAULT_PUBLIC_TRADE_HEALTH_PAGE_SIZE = 100
MAX_PUBLIC_TRADE_HEALTH_PAGE_SIZE = 1_000
DEFAULT_PUBLIC_TRADE_TRANSITION_PAGE_SIZE = 100
MAX_PUBLIC_TRADE_TRANSITION_PAGE_SIZE = 1_000


class PublicTradeCollectionTransitionReader(Protocol):
    """Read bounded, causally validated public-trade checkpoint history."""

    def transitions_for_job(
        self,
        job_id: UUID,
        *,
        after_checkpoint_version: int | None = None,
        limit: int = DEFAULT_PUBLIC_TRADE_TRANSITION_PAGE_SIZE,
    ) -> tuple[PublicTradeCollectionTransition, ...]:
        """Return an ascending page after one previously returned checkpoint version."""


class PublicTradeCollectionCheckpointStore(Protocol):
    """Persist checkpoint transitions and health evidence atomically."""

    def create(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> CollectionCheckpointWriteResult:
        """Insert one pending job or return an explicit duplicate or conflict."""

    def get(self, job_id: UUID) -> PublicTradeCollectionCheckpoint | None:
        """Reload and validate the current public-trade checkpoint."""

    def transition(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        expected_version: int,
        expected_lease_token: UUID | None = None,
        health: PublicTradeSourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        """Compare-and-swap one lease-authorized transition and optional health record."""

    def health_for_job(
        self,
        job_id: UUID,
        *,
        after_checkpoint_version: int | None = None,
        limit: int = DEFAULT_PUBLIC_TRADE_HEALTH_PAGE_SIZE,
    ) -> tuple[PublicTradeSourceHealthObservation, ...]:
        """Return one bounded page of health evidence in checkpoint order."""

    def health_summary(self, job_id: UUID) -> PublicTradeCollectionHealthSummary:
        """Return aggregate counters after validating the stored health evidence."""
