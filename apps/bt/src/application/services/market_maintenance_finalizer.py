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
    stage_terminal: Callable[[MarketFinalizationDecision], None] = lambda _decision: (
        None
    ),
    publish_terminal: Callable[[MarketFinalizationDecision], None],
    operation_error: str | None = None,
) -> MarketFinalizationDecision:
    """Defer caller cancellation until the finalizer thread is fully joined."""
    task = asyncio.create_task(
        asyncio.to_thread(
            finalizer.finalize,
            operation_outcome=operation_outcome,
            operation_error=operation_error,
            stage_terminal=stage_terminal,
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
        release_complete: Callable[[], None] = lambda: None,
        compactor: MarketCompactorLike | None = None,
        evidence_writer: Callable[[Path, MarketMaintenanceRecord], None] = (
            write_market_maintenance_evidence
        ),
        now: Callable[[], str] = lambda: datetime.now(UTC).isoformat(),
    ) -> None:
        self._session = session
        self._operation = operation
        self._attach = attach
        self._release_complete = release_complete
        self._compactor = compactor or MarketCompactor()
        self._evidence_writer = evidence_writer
        self._now = now

    def finalize(
        self,
        *,
        operation_outcome: MarketOperationOutcome,
        stage_terminal: Callable[
            [MarketFinalizationDecision], None
        ] = lambda _decision: None,
        publish_terminal: Callable[[MarketFinalizationDecision], None],
        operation_error: str | None = None,
    ) -> MarketFinalizationDecision:
        recorded_at = self._now()
        token: ClosedMarketHandlesToken | None = None
        resources: ReadOnlyMarketResources | None = None
        evidence: MarketMaintenanceEvidence | None = None
        lifecycle_errors: list[BaseException] = []
        ownership_released = False

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

        stage_terminal(decision)
        if token is not None and resources is not None and not self._session.fenced:
            try:
                self._session.release_after_read_only_reopen(token)
                ownership_released = True
            except BaseException as exc:
                self._session.fenced = True
                release_record = MarketMaintenanceRecord.failed(
                    operation=self._operation,
                    recorded_at=recorded_at,
                    error=f"Writer ownership release incomplete: {exc}",
                )
                decision = MarketFinalizationDecision(
                    terminal_outcome=MarketOperationOutcome.FAILED,
                    maintenance=release_record,
                    error=(
                        "Market maintenance incomplete: writer ownership release "
                        f"failed: {exc}. Retry with {release_record.recoveryCommand}."
                    ),
                )
                try:
                    self._evidence_writer(
                        self._session.factory.market_root,
                        release_record,
                    )
                except BaseException as evidence_exc:
                    exc.add_note(
                        "Failed to persist writer-release failure evidence: "
                        f"{evidence_exc}"
                    )
                publish_terminal(decision)
                return decision

        try:
            self._evidence_writer(self._session.factory.market_root, record)
        except BaseException as exc:
            record = MarketMaintenanceRecord.failed(
                operation=self._operation,
                recorded_at=recorded_at,
                error=f"Maintenance evidence write failed: {exc}",
            )
            decision = MarketFinalizationDecision(
                terminal_outcome=MarketOperationOutcome.FAILED,
                maintenance=record,
                error=f"Market maintenance incomplete: {exc}",
            )
            try:
                self._evidence_writer(self._session.factory.market_root, record)
            except BaseException as evidence_exc:
                exc.add_note(f"Failed to persist failed evidence: {evidence_exc}")

        try:
            publish_terminal(decision)
        except BaseException as exc:
            self._session.fenced = True
            publication_record = MarketMaintenanceRecord.failed(
                operation=self._operation,
                recorded_at=recorded_at,
                error=f"Terminal publication incomplete: {exc}",
            )
            try:
                self._evidence_writer(
                    self._session.factory.market_root,
                    publication_record,
                )
            except BaseException as evidence_exc:
                exc.add_note(
                    "Failed to persist terminal-publication failure evidence: "
                    f"{evidence_exc}"
                )
            raise
        if ownership_released:
            self._release_complete()
        return decision
