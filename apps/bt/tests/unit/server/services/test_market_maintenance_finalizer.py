from __future__ import annotations

import asyncio
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Callable

import pytest

from src.application.contracts.market_maintenance import (
    MaintenanceOutcome,
    MarketOperationOutcome,
)
from src.application.services.market_maintenance_finalizer import (
    MarketFinalizationDecision,
    MarketMaintenanceFinalizer,
    finalize_market_operation_joined,
)
from src.infrastructure.db.market.market_compaction import (
    CompactionTrigger,
    DuckDbSizeSnapshot,
    MarketMaintenanceEvidence,
)


def _passed_evidence() -> MarketMaintenanceEvidence:
    size = DuckDbSizeSnapshot(
        block_size=262_144,
        total_blocks=4,
        used_blocks=4,
        free_blocks=0,
        wal_bytes=0,
    )
    return MarketMaintenanceEvidence(
        compacted=False,
        trigger=CompactionTrigger.NONE,
        before=size,
        after=size,
        before_bytes=1_048_576,
        after_bytes=1_048_576,
        duration_ms=3.5,
        validation="passed",
        schema_fingerprint="schema",
        table_counts={},
        semantic_digests={},
    )


class _Session:
    def __init__(self, market_root: Path, events: list[str]) -> None:
        self.factory = SimpleNamespace(market_root=market_root)
        self.events = events
        self.fenced = False
        self.token = object()
        self.resources = object()

    def close_writable_handles(self) -> object:
        self.events.append("close")
        return self.token

    def authorize_maintenance(self, token: object) -> object:
        assert token is self.token
        self.events.append("authorize")
        return object()

    def reopen_read_only(self, token: object) -> object:
        assert token is self.token
        self.events.append("reopen")
        return self.resources

    def release_after_read_only_reopen(self, token: object) -> None:
        assert token is self.token
        self.events.append("release")


class _Compactor:
    def __init__(
        self,
        events: list[str],
        *,
        error: Exception | None = None,
    ) -> None:
        self.events = events
        self.error = error

    def maintain(self, _authority: object) -> MarketMaintenanceEvidence:
        self.events.append("maintain")
        if self.error is not None:
            raise self.error
        return _passed_evidence()


def _finalize(
    tmp_path: Path,
    *,
    operation_outcome: MarketOperationOutcome = MarketOperationOutcome.SUCCEEDED,
    operation_error: str | None = None,
    compactor_error: Exception | None = None,
    evidence_writer: Callable[[Path, object], None] | None = None,
) -> tuple[list[str], MarketFinalizationDecision]:
    events: list[str] = []
    session = _Session(tmp_path, events)
    decisions: list[MarketFinalizationDecision] = []

    def write(_root: Path, _record: object) -> None:
        events.append("evidence")

    def attach(resources: object, _record: object) -> None:
        assert resources is session.resources
        events.append("attach")

    def publish(decision: MarketFinalizationDecision) -> None:
        decisions.append(decision)
        events.append("terminal")

    finalizer = MarketMaintenanceFinalizer(
        session=session,  # type: ignore[arg-type]
        operation="incremental_sync",
        compactor=_Compactor(events, error=compactor_error),  # type: ignore[arg-type]
        evidence_writer=evidence_writer or write,  # type: ignore[arg-type]
        attach=attach,  # type: ignore[arg-type]
        now=lambda: "2026-07-16T00:00:00+00:00",
    )
    finalizer.finalize(
        operation_outcome=operation_outcome,
        operation_error=operation_error,
        publish_terminal=publish,
    )
    return events, decisions[0]


def test_finalizer_publishes_terminal_only_after_maintenance_evidence_and_reopen(
    tmp_path: Path,
) -> None:
    events, decision = _finalize(tmp_path)

    assert events == [
        "close",
        "authorize",
        "maintain",
        "reopen",
        "evidence",
        "attach",
        "terminal",
        "release",
    ]
    assert decision.terminal_outcome is MarketOperationOutcome.SUCCEEDED
    assert decision.maintenance.outcome is MaintenanceOutcome.PASSED
    assert decision.maintenance.schemaFingerprint == "schema"
    assert decision.maintenance.tableCounts == {}
    assert decision.maintenance.semanticDigests == {}


def test_maintenance_failure_overrides_success_but_reopens_and_releases(
    tmp_path: Path,
) -> None:
    events, decision = _finalize(
        tmp_path,
        compactor_error=RuntimeError("hard cap remains exceeded"),
    )

    assert events == [
        "close",
        "authorize",
        "maintain",
        "reopen",
        "evidence",
        "attach",
        "terminal",
        "release",
    ]
    assert decision.terminal_outcome is MarketOperationOutcome.FAILED
    assert decision.maintenance.outcome is MaintenanceOutcome.FAILED
    assert decision.error is not None
    assert "hard cap remains exceeded" in decision.error


def test_operation_failure_is_published_after_successful_maintenance(
    tmp_path: Path,
) -> None:
    events, decision = _finalize(
        tmp_path,
        operation_outcome=MarketOperationOutcome.FAILED,
        operation_error="fetch failed",
    )

    assert events[-2:] == ["terminal", "release"]
    assert decision.terminal_outcome is MarketOperationOutcome.FAILED
    assert decision.error == "fetch failed"
    assert decision.maintenance.outcome is MaintenanceOutcome.PASSED


def test_evidence_write_failure_has_precedence_over_cancellation(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    def fail_evidence(_root: Path, _record: object) -> None:
        events.append("evidence-write-failed")
        raise OSError("fsync failed")

    lifecycle_events, decision = _finalize(
        tmp_path,
        operation_outcome=MarketOperationOutcome.CANCELLED,
        evidence_writer=fail_evidence,
    )

    assert events == ["evidence-write-failed"]
    assert lifecycle_events[-2:] == ["terminal", "release"]
    assert decision.terminal_outcome is MarketOperationOutcome.FAILED
    assert decision.error is not None
    assert "fsync failed" in decision.error


def test_fenced_close_failure_publishes_failed_terminal_without_reopen_or_release(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    session = _Session(tmp_path, events)

    def fail_close() -> object:
        events.append("close")
        session.fenced = True
        raise RuntimeError("handle close failed")

    session.close_writable_handles = fail_close  # type: ignore[method-assign]
    decisions: list[MarketFinalizationDecision] = []
    finalizer = MarketMaintenanceFinalizer(
        session=session,  # type: ignore[arg-type]
        operation="intraday_sync",
        compactor=_Compactor(events),  # type: ignore[arg-type]
        evidence_writer=lambda _root, _record: events.append("evidence"),  # type: ignore[arg-type]
        attach=lambda _resources, _record: events.append("attach"),  # type: ignore[arg-type]
        now=lambda: "2026-07-16T00:00:00+00:00",
    )

    finalizer.finalize(
        operation_outcome=MarketOperationOutcome.SUCCEEDED,
        publish_terminal=lambda decision: (
            decisions.append(decision),
            events.append("terminal"),
        ),
    )

    assert events == ["close", "evidence", "terminal"]
    assert decisions[0].terminal_outcome is MarketOperationOutcome.FAILED
    assert "handle close failed" in (decisions[0].error or "")


def test_terminal_publication_failure_fences_session_without_releasing(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    session = _Session(tmp_path, events)
    finalizer = MarketMaintenanceFinalizer(
        session=session,  # type: ignore[arg-type]
        operation="incremental_sync",
        compactor=_Compactor(events),  # type: ignore[arg-type]
        evidence_writer=lambda _root, _record: events.append("evidence"),  # type: ignore[arg-type]
        attach=lambda _resources, _record: events.append("attach"),  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="terminal store unavailable"):
        finalizer.finalize(
            operation_outcome=MarketOperationOutcome.SUCCEEDED,
            publish_terminal=lambda _decision: (_ for _ in ()).throw(
                RuntimeError("terminal store unavailable")
            ),
        )

    assert session.fenced is True
    assert "release" not in events


@pytest.mark.asyncio
async def test_joined_finalizer_defers_caller_cancellation_until_thread_finishes() -> (
    None
):
    started = threading.Event()
    finish = threading.Event()
    published = threading.Event()

    class BlockingFinalizer:
        def finalize(self, **kwargs: object) -> MarketFinalizationDecision:
            started.set()
            finish.wait()
            decision = MarketFinalizationDecision(
                terminal_outcome=MarketOperationOutcome.SUCCEEDED,
                maintenance=SimpleNamespace(),  # type: ignore[arg-type]
            )
            kwargs["publish_terminal"](decision)  # type: ignore[operator]
            published.set()
            return decision

    task = asyncio.create_task(
        finalize_market_operation_joined(
            BlockingFinalizer(),  # type: ignore[arg-type]
            operation_outcome=MarketOperationOutcome.SUCCEEDED,
            publish_terminal=lambda _decision: None,
        )
    )
    assert await asyncio.to_thread(started.wait, 1)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert not published.is_set()

    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert published.is_set()


def test_all_market_writer_entrypoints_use_common_finalizer_without_legacy_hooks() -> (
    None
):
    bt_root = Path(__file__).parents[4]
    sources = {
        relative: (bt_root / relative).read_text(encoding="utf-8")
        for relative in (
            "src/application/services/sync_service.py",
            "src/entrypoints/http/routes/db.py",
            "src/entrypoints/cli/intraday.py",
            "src/entrypoints/cli/market.py",
            "src/infrastructure/db/market/market_writer_resources.py",
        )
    }
    combined = "\n".join(sources.values())

    for obsolete_name in (
        "reopen_read_only_and_release",
        "close_time_series_store",
        "close_market_db",
        "on_finish",
        "_restore_market_resources_after_sync",
        "_restore_read_only_market_resources",
    ):
        assert obsolete_name not in combined

    assert (
        "MarketMaintenanceFinalizer"
        in sources["src/application/services/sync_service.py"]
    )
    assert (
        "_finalize_direct_market_write" in sources["src/entrypoints/http/routes/db.py"]
    )
    assert "MarketMaintenanceFinalizer" in sources["src/entrypoints/cli/intraday.py"]
    assert "MarketMaintenanceFinalizer" in sources["src/entrypoints/cli/market.py"]
