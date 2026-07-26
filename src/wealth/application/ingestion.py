"""Fail-closed historical candle ingestion."""

from dataclasses import dataclass, field

from wealth.application.quality import CandleSequenceAuditor
from wealth.domain.quality import (
    CandleSequenceReport,
    CandleStream,
    CandleWriteResult,
    CandleWriteStatus,
    DataQualityStatus,
    RawPayloadWriteResult,
    RawPayloadWriteStatus,
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
    raw_write: RawPayloadWriteResult | None
    writes: tuple[CandleWriteResult, ...]

    @property
    def accepted(self) -> bool:
        """Return whether quality and exact persistence evidence passed the gate."""

        raw_write = self.raw_write
        if (
            self.quality.status is not DataQualityStatus.PASS
            or raw_write is None
            or raw_write.incoming_record_id != self.batch.raw_payload.record_id
        ):
            return False
        if raw_write.status is RawPayloadWriteStatus.INSERTED:
            if raw_write.existing_record_id is not None:
                return False
        elif raw_write.status is RawPayloadWriteStatus.DUPLICATE:
            if raw_write.existing_record_id != self.batch.raw_payload.record_id:
                return False
        else:
            return False
        if len(self.writes) != len(self.batch.records):
            return False

        for write, record in zip(self.writes, self.batch.records, strict=True):
            if write.incoming_record_id != record.record_id:
                return False
            if write.status is CandleWriteStatus.INSERTED:
                if write.existing_record_id is not None:
                    return False
            elif write.status is CandleWriteStatus.DUPLICATE:
                if write.existing_record_id is None:
                    return False
            else:
                return False
        return True


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
        persistence = (
            self.store.append_batch(batch) if quality.status is DataQualityStatus.PASS else None
        )
        return HistoricalCandleIngestionResult(
            batch=batch,
            quality=quality,
            raw_write=persistence.raw_payload if persistence is not None else None,
            writes=persistence.candles if persistence is not None else (),
        )
