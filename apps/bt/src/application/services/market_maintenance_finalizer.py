"""One outermost lifecycle for every high-churn Market writer."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from src.application.contracts.market_maintenance import (
    MaintenanceEvidenceStatus,
    MaintenanceOutcome,
    MarketMaintenanceRecord,
    MarketOperationOutcome,
)
from src.infrastructure.db.market.market_compaction import (
    MarketCompactor,
    MarketMaintenanceEvidence,
)
from src.infrastructure.db.market.market_maintenance_evidence import (
    write_market_maintenance_evidence,
)
from src.infrastructure.db.market.market_writer_resources import (
    ClosedMarketHandlesToken,
    MarketMaintenanceAuthority,
    MarketWriterSession,
    ReadOnlyMarketResources,
)


class MarketCompactorLike(Protocol):
    def maintain(
        self, authority: MarketMaintenanceAuthority
    ) -> MarketMaintenanceEvidence: ...


@dataclass(frozen=True)
class MarketFinalizationDecision:
    terminal_outcome: MarketOperationOutcome
    maintenance: MarketMaintenanceRecord
    error: str | None = None


async def finalize_market_operation_joined(
    finalizer: "MarketMaintenanceFinalizer",
    *,
    operation_outcome: MarketOperationOutcome,
    publish_terminal: Callable[[MarketFinalizationDecision], None],
    operation_error: str | None = None,
) -> MarketFinalizationDecision:
    """Defer caller cancellation until the finalizer thread is fully joined."""
    task = asyncio.create_task(
        asyncio.to_thread(
            finalizer.finalize,
            operation_outcome=operation_outcome,
            operation_error=operation_error,
            publish_terminal=publish_terminal,
        )
    )
    cancellation_received = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancellation_received = True
    decision = task.result()
    if cancellation_received:
        raise asyncio.CancelledError
    return decision


def _passed_record(
    *,
    operation: str,
    recorded_at: str,
    evidence: MarketMaintenanceEvidence,
) -> MarketMaintenanceRecord:
    return MarketMaintenanceRecord(
        evidenceStatus=MaintenanceEvidenceStatus.VALID,
        outcome=MaintenanceOutcome.PASSED,
        operation=operation,
        recordedAt=recorded_at,
        compacted=evidence.compacted,
        trigger=evidence.trigger.value,
        beforeBytes=evidence.before_bytes,
        afterBytes=evidence.after_bytes,
        durationMs=evidence.duration_ms,
        validation=evidence.validation,
        schemaFingerprint=evidence.schema_fingerprint,
        tableCounts=evidence.table_counts,
        semanticDigests=evidence.semantic_digests,
    )


class MarketMaintenanceFinalizer:
    """Hold exclusive ownership through evidence and terminal publication."""

    def __init__(
        self,
        *,
        session: MarketWriterSession,
        operation: str,
        attach: Callable[[ReadOnlyMarketResources, MarketMaintenanceRecord], None],
        compactor: MarketCompactorLike | None = None,
        evidence_writer: Callable[[Path, MarketMaintenanceRecord], None] = (
            write_market_maintenance_evidence
        ),
        now: Callable[[], str] = lambda: datetime.now(UTC).isoformat(),
    ) -> None:
        self._session = session
        self._operation = operation
        self._attach = attach
        self._compactor = compactor or MarketCompactor()
        self._evidence_writer = evidence_writer
        self._now = now

    def finalize(
        self,
        *,
        operation_outcome: MarketOperationOutcome,
        publish_terminal: Callable[[MarketFinalizationDecision], None],
        operation_error: str | None = None,
    ) -> MarketFinalizationDecision:
        recorded_at = self._now()
        token: ClosedMarketHandlesToken | None = None
        resources: ReadOnlyMarketResources | None = None
        evidence: MarketMaintenanceEvidence | None = None
        lifecycle_errors: list[BaseException] = []

        try:
            token = self._session.close_writable_handles()
            authority = self._session.authorize_maintenance(token)
            evidence = self._compactor.maintain(authority)
        except BaseException as exc:
            lifecycle_errors.append(exc)

        if token is not None and not self._session.fenced:
            try:
                resources = self._session.reopen_read_only(token)
            except BaseException as exc:
                lifecycle_errors.append(exc)

        record = (
            _passed_record(
                operation=self._operation,
                recorded_at=recorded_at,
                evidence=evidence,
            )
            if evidence is not None and not lifecycle_errors
            else MarketMaintenanceRecord.failed(
                operation=self._operation,
                recorded_at=recorded_at,
                error=str(lifecycle_errors[0])
                if lifecycle_errors
                else "Maintenance evidence was not produced",
            )
        )

        try:
            self._evidence_writer(self._session.factory.market_root, record)
        except BaseException as exc:
            lifecycle_errors.append(exc)
            record = MarketMaintenanceRecord.failed(
                operation=self._operation,
                recorded_at=recorded_at,
                error=f"Maintenance evidence write failed: {exc}",
            )

        if resources is not None:
            try:
                self._attach(resources, record)
            except BaseException as exc:
                lifecycle_errors.append(exc)
                record = MarketMaintenanceRecord.failed(
                    operation=self._operation,
                    recorded_at=recorded_at,
                    error=f"Read-only resource/evidence attach failed: {exc}",
                )
                try:
                    self._evidence_writer(self._session.factory.market_root, record)
                except BaseException as evidence_exc:
                    lifecycle_errors.append(evidence_exc)

        if lifecycle_errors:
            error = f"Market maintenance incomplete: {lifecycle_errors[0]}"
            decision = MarketFinalizationDecision(
                terminal_outcome=MarketOperationOutcome.FAILED,
                maintenance=record,
                error=error,
            )
        else:
            decision = MarketFinalizationDecision(
                terminal_outcome=operation_outcome,
                maintenance=record,
                error=operation_error,
            )

        try:
            publish_terminal(decision)
        except BaseException:
            self._session.fenced = True
            raise
        if token is not None and resources is not None and not self._session.fenced:
            self._session.release_after_read_only_reopen(token)
        return decision
