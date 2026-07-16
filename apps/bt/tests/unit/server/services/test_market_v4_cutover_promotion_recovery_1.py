"""Market v4 cutover promotion recovery tests."""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import time
from typing import cast

import pytest

import src.application.services.market_v4_cutover.promotion_rollback as cutover_module
from src.application.services.market_v4_cutover.contracts import (
    PromotionAppendResult,
    PromotionAppendStatus,
    PromotionIdentityEvidence,
    PromotionJournalRecord,
    PromotionState,
    RetainedPromotionPreparation,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.application.services.market_v4_cutover.promotion_contracts import (
    RetainedPromotionContext,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root, market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _retained_promotion_source,
    _TestAtomicExchange,
    _run_retained_promotion,
    _market_identity_at_root,
)


@pytest.mark.parametrize(
    ("evidence_group", "path", "replacement"),
    [
        ("no_sync", ("noSync",), False),
        ("no_jquants", ("noJQuants",), False),
        ("retained_report_sha", ("retainedReport", "reportSha256"), "0" * 64),
        ("retained_report_code", ("retainedReport", "codeVersion"), "tampered"),
        ("source_report_sha", ("sourceReport", "reportSha256"), "1" * 64),
        ("source_report_code", ("sourceReport", "codeVersion"), "tampered"),
        ("target_fingerprint", ("fingerprints", "targetRoot"), "2" * 64),
        ("retained_fingerprint", ("fingerprints", "retainedRoot"), "3" * 64),
        ("configuration_fingerprint", ("fingerprints", "configuration"), "4" * 64),
        (
            "backup_payload_identity",
            ("payloadIdentities", "backup", "marketDuckdb", "inode"),
            999_999,
        ),
        ("atomic_exchange", ("filesystemEvidence", "atomicExchange"), False),
        ("api_checks", ("apiChecks",), ["/api/db/sync"]),
        ("server_join", ("serverProcessJoined",), False),
        ("worker_join", ("workerProcessJoined",), False),
        ("semantic_smoke", ("semanticSmoke", "schemaVersion"), 3),
        (
            "backup_evidence",
            ("backupEvidence", "physicalIdentityDistinct"),
            False,
        ),
        (
            "runtime_cleanup",
            ("runtimeCleanup", "activeRuntimeRemoved"),
            False,
        ),
        ("activation_mode", ("activationMode",), "copy"),
        ("rollback_contract", ("rollbackInstructions",), ""),
    ],
)
def test_promotion_committed_recovery_rejects_coordinated_report_marker_tamper(
    tmp_path: Path,
    evidence_group: str,
    path: tuple[str, ...],
    replacement: object,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])

    def crash_after_commit(stage: str) -> None:
        if stage == "committed_journaled":
            raise RuntimeError("simulated process crash after commit")

    service._promotion_boundary_hook = crash_after_commit
    with pytest.raises(CutoverSafetyError, match="cleanup incomplete"):
        _run_retained_promotion(service, config)

    report_path = data_root / (
        "operations/market-v4-cutover/reports/market-v4-active-20260716/report.json"
    )
    report = json.loads(report_path.read_text())
    target: dict[str, object] = report
    for component in path[:-1]:
        target = target[component]  # type: ignore[assignment]
    target[path[-1]] = replacement
    report_path.write_text(json.dumps(report))
    marker_path = data_root / cast(str, report["sourceConsumed"]["markerPath"])
    marker = json.loads(marker_path.read_text())
    marker["promotionReportSha256"] = service._sha256(report_path)
    marker_path.write_text(json.dumps(marker))
    staging = data_root / (
        "operations/market-v4-cutover/cleanup-staging/market-v4-active-20260716"
    )
    assert staging.is_dir(), evidence_group

    service._promotion_boundary_hook = lambda _stage: None
    with pytest.raises(CutoverSafetyError, match="Committed promotion report"):
        service._recover_retained_promotion(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )

    assert staging.is_dir(), evidence_group


def test_promotion_exchange_exception_reinspects_completed_exchange(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)

    class SwapThenRaise(_TestAtomicExchange):
        def exchange(self, *args: object) -> None:
            super().exchange(*args)  # type: ignore[arg-type]
            raise OSError("indeterminate exchange result")

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(), "market-v4-active-20260716", now=service.now
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service.atomic_exchange = SwapThenRaise()
        monkeypatch.setattr(
            service,
            "_restore_under_lease",
            lambda _backup: (_ for _ in ()).throw(
                AssertionError("backup fallback must not run after completed exchange")
            ),
        )

        service._rollback_retained_promotion(
            RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_backup_fallback_journal_is_truthful(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("exchange unavailable")

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(), "market-v4-active-20260716", now=service.now
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        service._rollback_retained_promotion(
            RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )
        final = journal.read_validated()[-1].identities

    assert final.rollback_mode == "backup_restore"
    assert final.quarantine_current is not None
    assert final.quarantine_current["directory"] == final.active_before_directory
    assert service._payload_manifest_entries(final.active_current["payload"]) == (
        service._payload_manifest_entries(final.active_before_payload)
    )


def test_promotion_recovery_validated_terminates_without_fake_exchange(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    monkeypatch.setattr(
        service,
        "_detach_retained_artifacts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            CutoverSafetyError("stop after validated")
        ),
    )
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        with pytest.raises(CutoverSafetyError, match="stop after validated"):
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    assert (
        service._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )
    with managed_root.ManagedRootFd.open(data_root) as managed:
        recovered = PromotionJournal(managed, report_id, now=service.now)
        recovered.recover(recovered.recovery_attempt_id())
        states = tuple(record.state for record in recovered.read_validated())
    assert states == (PromotionState.VALIDATED, PromotionState.ROLLED_BACK)


def test_promotion_recovery_runtimes_detached_restores_exact_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    runtime_names = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name.startswith(".cutover-runtime-")
    }
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        original_append = journal.append

        def stop_before_prepared(
            state: PromotionState,
            **kwargs: object,
        ) -> PromotionAppendResult:
            if state is PromotionState.PREPARED:
                return PromotionAppendResult(
                    PromotionAppendStatus.NOT_COMMITTED,
                    None,
                    "attempt-prepared-not-committed",
                )
            return original_append(state, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(journal, "append", stop_before_prepared)
        with pytest.raises(CutoverSafetyError, match="not committed"):
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    assert (
        service._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )
    assert runtime_names <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }


def test_promotion_committed_recovery_rejects_report_supplied_quarantine_path(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    result, _states = _run_retained_promotion(service, config)
    report_path = data_root / result.report_path
    report = json.loads(report_path.read_text())
    report["quarantinePath"] = (
        "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716/payload"
    )
    report_path.write_text(json.dumps(report))
    marker = data_root / report["sourceConsumed"]["markerPath"]
    marker_payload = json.loads(marker.read_text())
    marker_payload["promotionReportSha256"] = service._sha256(report_path)
    marker.write_text(json.dumps(marker_payload))

    with pytest.raises(CutoverSafetyError, match="quarantine is invalid"):
        service._recover_retained_promotion(
            result.report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )


def test_promotion_recovery_missing_success_report_after_exchange_rolls_back(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    report_id = "market-v4-active-20260716"
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        base = journal.read_validated()[-1].identities
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        exchanged = service._promotion_identities(
            base,
            active_current=service._market_location_identity(
                service._active_lease_fd_root()
            ),
            retained_current=service._market_location_identity(
                service._retained_lease_fd_root()
            ),
            quarantine_current=None,
            holding_current=base.holding_current,
        )
        service._append_preparation_state(journal, PromotionState.EXCHANGED, exchanged)

    assert not (
        data_root / f"operations/market-v4-cutover/reports/{report_id}/report.json"
    ).exists()
    assert (
        service._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )
    assert _market_identity_at_root(service, data_root) == active_before


@pytest.mark.parametrize("rollback_mode", ["atomic_exchange", "backup_restore"])
def test_promotion_recovery_resumes_after_exchanged_back_without_duplicate_append(
    tmp_path: Path,
    rollback_mode: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("exchange unavailable")

    def crash_after_exchanged_back(stage: str) -> None:
        if stage == "exchanged_back_journaled":
            raise RuntimeError("crash after exchanged-back")

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        if rollback_mode == "backup_restore":
            service.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        service._promotion_boundary_hook = crash_after_exchanged_back
        with pytest.raises(RuntimeError, match="crash after exchanged-back"):
            service._rollback_retained_promotion(
                RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )

    service._promotion_boundary_hook = lambda _stage: None
    assert (
        service._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )
    with managed_root.ManagedRootFd.open(data_root) as managed:
        recovered = PromotionJournal(managed, report_id, now=service.now)
        recovered.recover(recovered.recovery_attempt_id())
        records = recovered.read_validated()
    assert sum(record.state is PromotionState.EXCHANGED_BACK for record in records) == 1
    assert records[-1].state is PromotionState.ROLLED_BACK
    assert records[-1].identities.rollback_mode == rollback_mode


def test_promotion_recovery_reconciles_empty_owned_temp_duplicate_after_exchange_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    report_id = "market-v4-active-20260716"
    retained_market = retained_root / "market-timeseries"
    original_temp = retained_market / "duckdb-tmp"
    original_temp.mkdir()
    original_temp_inode = original_temp.stat().st_ino

    original_append = service._append_preparation_state

    def create_temp_collision_after_cleanup_journal(
        journal: PromotionJournal,
        state: PromotionState,
        identities: PromotionIdentityEvidence,
    ) -> PromotionJournalRecord:
        record = original_append(journal, state, identities)
        if state is PromotionState.CLEANUP_STAGED:
            (data_root / "market-timeseries/duckdb-tmp").mkdir()
        return record

    monkeypatch.setattr(
        service,
        "_append_preparation_state",
        create_temp_collision_after_cleanup_journal,
    )

    def stop_after_exchange_back(stage: str) -> None:
        if stage == "exchanged_back_journaled":
            raise CutoverSafetyError("injected crash after exchange-back")

    service._promotion_boundary_hook = stop_after_exchange_back
    with pytest.raises(
        CutoverSafetyError,
        match="rollback recovery failed",
    ):
        _run_retained_promotion(service, config)

    with managed_root.ManagedRootFd.open(data_root) as managed:
        failed = PromotionJournal(managed, report_id, now=service.now)
        failed.recover(failed.recovery_attempt_id())
        assert failed.read_validated()[-1].state is PromotionState.EXCHANGED_BACK
    staged_temp = (
        data_root
        / "operations/market-v4-cutover/cleanup-staging"
        / report_id
        / "duckdb-tmp"
    )
    assert staged_temp.stat().st_ino == original_temp_inode
    assert original_temp.is_dir()
    assert original_temp.stat().st_ino != original_temp_inode
    assert not any(original_temp.iterdir())

    class ForbiddenExchange:
        def exchange(self, *_args: object) -> None:
            raise AssertionError("EXCHANGED_BACK recovery must not exchange again")

    interrupted = _service(data_root)
    interrupted.atomic_exchange = ForbiddenExchange()  # type: ignore[assignment]

    def crash_after_empty_collision_removal(stage: str) -> None:
        if stage == "rollback_owned_temp_collision_removed":
            raise CutoverSafetyError("injected crash after empty collision removal")

    interrupted._promotion_boundary_hook = crash_after_empty_collision_removal
    with pytest.raises(
        CutoverSafetyError,
        match="injected crash after empty collision removal",
    ):
        interrupted._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
    assert staged_temp.stat().st_ino == original_temp_inode
    assert not original_temp.exists()

    fresh = _service(data_root)
    fresh.atomic_exchange = ForbiddenExchange()  # type: ignore[assignment]
    assert (
        fresh._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )

    assert original_temp.stat().st_ino == original_temp_inode
    assert not staged_temp.parent.exists()
    with managed_root.ManagedRootFd.open(data_root) as managed:
        recovered = PromotionJournal(managed, report_id, now=fresh.now)
        recovered.recover(recovered.recovery_attempt_id())
        records = recovered.read_validated()
    assert records[-1].state is PromotionState.ROLLED_BACK
    assert sum(record.state is PromotionState.EXCHANGED_BACK for record in records) == 1


def test_promotion_rollback_reproves_parent_durability_after_swap_then_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )

        class SwapThenRaise:
            def exchange(
                self,
                managed_root: managed_root.ManagedRootFd,
                left: Path,
                right: Path,
            ) -> None:
                left_parent, left_name = managed_root.open_parent(left)
                right_parent, right_name = managed_root.open_parent(right)
                temporary = f".test-exchange-{time.time_ns()}"
                try:
                    os.rename(
                        left_name,
                        temporary,
                        src_dir_fd=left_parent,
                        dst_dir_fd=left_parent,
                    )
                    os.rename(
                        right_name,
                        left_name,
                        src_dir_fd=right_parent,
                        dst_dir_fd=left_parent,
                    )
                    os.rename(
                        temporary,
                        right_name,
                        src_dir_fd=left_parent,
                        dst_dir_fd=right_parent,
                    )
                finally:
                    os.close(left_parent)
                    os.close(right_parent)
                raise OSError(errno.EIO, "injected post-swap parent fsync failure")

        service.atomic_exchange = SwapThenRaise()
        expected_parent_inodes = {
            data_root.stat().st_ino,
            retained_root.stat().st_ino,
        }
        fsynced_parent_inodes: list[int] = []
        real_fsync = cutover_module.os.fsync

        def record_fsync(fd: int) -> None:
            inode = os.fstat(fd).st_ino
            if inode in expected_parent_inodes:
                fsynced_parent_inodes.append(inode)
            real_fsync(fd)

        monkeypatch.setattr(cutover_module.os, "fsync", record_fsync)
        service._rollback_retained_promotion(
            RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )

        assert expected_parent_inodes <= set(fsynced_parent_inodes)
        assert journal.read_validated()[-1].state is PromotionState.ROLLED_BACK


def test_promotion_rollback_failed_parent_durability_reproof_fences_leases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"
    leaked_fds: tuple[int, int]

    with pytest.raises(CutoverSafetyError, match="durability"):
        with service._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            journal = PromotionJournal(service._managed(), report_id, now=service.now)
            preparation = service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
            service.atomic_exchange.exchange(
                service._managed(),
                Path("market-timeseries"),
                service._managed_relative(retained_root / "market-timeseries"),
            )

            class SwapThenRaise(_TestAtomicExchange):
                def exchange(self, *args: object) -> None:
                    super().exchange(*args)  # type: ignore[arg-type]
                    raise OSError(errno.EIO, "injected post-swap parent fsync failure")

            service.atomic_exchange = SwapThenRaise()
            assert service._active_lease is not None
            assert service._retained_lease is not None
            leaked_fds = (service._active_lease.fd, service._retained_lease.fd)
            parent_inodes = {data_root.stat().st_ino, retained_root.stat().st_ino}
            real_fsync = cutover_module.os.fsync

            def fail_reproof(fd: int) -> None:
                if os.fstat(fd).st_ino in parent_inodes:
                    raise OSError(errno.EIO, "injected durability re-fsync failure")
                real_fsync(fd)

            monkeypatch.setattr(cutover_module.os, "fsync", fail_reproof)
            service._rollback_retained_promotion(
                RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )

    try:
        with managed_root.ManagedRootFd.open(data_root) as managed:
            recovered = PromotionJournal(managed, report_id, now=service.now)
            recovered.recover(recovered.recovery_attempt_id())
            assert recovered.read_validated()[-1].state is PromotionState.PREPARED
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_operation_lease.MarketOperationLease.acquire_existing(
                    root, exclusive=True
                )
    finally:
        for fd in leaked_fds:
            os.close(fd)


@pytest.mark.parametrize(
    "crash_boundary",
    ["first_artifact", "all_artifacts"],
)
def test_promotion_recovery_reconciles_split_partial_artifact_restoration(
    tmp_path: Path,
    crash_boundary: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"
    preparation: RetainedPromotionPreparation
    move_count = 0

    def crash_during_restore(stage: str) -> None:
        nonlocal move_count
        if stage.startswith("rollback_artifact_moved:"):
            move_count += 1
            if crash_boundary == "first_artifact" and move_count == 1:
                raise RuntimeError("crash after first artifact")
        if (
            stage == "rollback_artifacts_reconciled"
            and crash_boundary == "all_artifacts"
        ):
            raise RuntimeError("crash after all artifacts")

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service._promotion_boundary_hook = crash_during_restore
        with pytest.raises(RuntimeError, match="crash after"):
            service._rollback_retained_promotion(
                RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )

    if crash_boundary == "first_artifact":
        # Prove a second restart may stop at another exact move and remains resumable.
        def crash_once_more(stage: str) -> None:
            if stage == "rollback_artifacts_reconciled":
                raise RuntimeError("second recovery crash")

        service._promotion_boundary_hook = crash_once_more
        with pytest.raises(RuntimeError, match="second recovery crash"):
            service._recover_retained_promotion(
                report_id,
                retained_report_id="market-v4-retained-20260715-r13",
                backup_id="market-v3-pre-v4-20260716",
            )

    service._promotion_boundary_hook = lambda _stage: None
    assert (
        service._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )
        is None
    )
    retained_market_fd = os.open(
        retained_root / "market-timeseries", os.O_RDONLY | os.O_DIRECTORY
    )
    try:
        for artifact in preparation.detached_artifacts:
            assert (
                service._held_artifact_evidence(retained_market_fd, artifact.name)
                == artifact
            )
    finally:
        os.close(retained_market_fd)
