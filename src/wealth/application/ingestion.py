"""Fail-closed historical candle ingestion."""

from dataclasses import dataclass, field

from wealth.application.quality import CandleSequenceAuditor
from wealth.domain.quality import (
    CandleSequenceReport,
    CandleStream,
    CandleWriteResult,
    DataQualityStatus,
)
from wealth.ports.market import (
    CandleFetchBatch,
    CandleStore,
    HistoricalCandleRequest,
    HistoricalCandleSource,
)


@dataclass(frozen=True, slots=True)
class HistoricalCandleIngestionResult:
    """Observed batch, quality decision, and explicit persistence outcomes."""

    batch: CandleFetchBatch
    quality: CandleSequenceReport
    writes: tuple[CandleWriteResult, ...]

    @property
    def accepted(self) -> bool:
        """Return whether the complete requested batch passed the gate."""

        return self.quality.status is DataQualityStatus.PASS


@dataclass(frozen=True, slots=True)
class HistoricalCandleIngestor:
    """Fetch, audit, and only then persist one closed candle window."""

    source: HistoricalCandleSource
    store: CandleStore
    auditor: CandleSequenceAuditor = field(default_factory=CandleSequenceAuditor)

    def ingest(self, request: HistoricalCandleRequest) -> HistoricalCandleIngestionResult:
        """Reject incomplete or ambiguous batches before storage."""

        batch = self.source.fetch(request)
        stream = CandleStream(
            source=batch.source,
            venue=batch.venue,
            instrument=request.instrument,
            instrument_type=request.instrument_type,
            timeframe=request.timeframe,
        )
        quality = self.auditor.audit(
            stream=stream,
            window_start=request.window_start,
            window_end_exclusive=request.window_end_exclusive,
            records=batch.records,
        )
        writes = (
            tuple(self.store.append(record) for record in batch.records)
            if quality.status is DataQualityStatus.PASS
            else ()
        )
        return HistoricalCandleIngestionResult(
            batch=batch,
            quality=quality,
            writes=writes,
        )
