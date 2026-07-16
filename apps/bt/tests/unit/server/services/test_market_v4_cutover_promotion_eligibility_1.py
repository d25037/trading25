"""Market v4 cutover promotion eligibility tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil

import pytest

import src.application.services.market_v4_cutover.promotion_eligibility as cutover_module
import src.application.services.market_v4_cutover.filesystem as filesystem_module
from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    PromotionAppendResult,
    PromotionAppendStatus,
    PromotionState,
    SmokeConfig,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root, market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _changing_code_version,
    _retained_promotion_source,
    _prepare_retained_promotion,
)


@pytest.mark.parametrize(
    "mutation",
    [
        "retained_report_sha_drift",
        "source_report_sha_drift",
        "provenance_drift",
        "retained_configuration_drift",
        "active_root_drift",
        "code_drift",
        "schema_v3",
        "wrong_adjustment_mode",
        "inexact_lineage",
        "database_identity_drift",
        "parquet_identity_drift",
        "source_root_replacement",
        "source_ancestor_replacement",
        "source_market_leaf_replacement",
        "live_retained_lease",
        "nonempty_wal",
        "mismatched_smoke_config",
        "unexpected_retained_artifact",
        "cross_device",
        "unavailable_exchange",
        "existing_report",
        "existing_journal",
        "existing_journal_control",
        "existing_journal_lock",
        "existing_holding",
        "existing_quarantine",
        "existing_backup_id",
        "existing_consumed_marker",
    ],
)
def test_promote_retained_rejects_ineligible_source_before_any_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    retained_report_id = "market-v4-retained-20260715-r13"
    backup_id = "market-v3-pre-v4-20260716"
    source_report_id = "market-v4-rehearsal-20260715-r10"
    retained_report_path = data_root / (
        f"operations/market-v4-cutover/reports/{retained_report_id}/report.json"
    )
    source_report_path = data_root / (
        f"operations/market-v4-cutover/reports/{source_report_id}/report.json"
    )
    competing_lease: market_operation_lease.MarketOperationLease | None = None
    if mutation == "retained_report_sha_drift":
        original_snapshot = service._promotion._eligibility._promotion_report_snapshot
        calls = 0

        def drift_retained_report(report: str):
            nonlocal calls
            result = original_snapshot(report)
            if report == retained_report_id and calls == 0:
                retained_report_path.write_bytes(
                    retained_report_path.read_bytes() + b" "
                )
                calls += 1
            return result

        monkeypatch.setattr(
            service._promotion._eligibility,
            "_promotion_report_snapshot",
            drift_retained_report,
        )
    elif mutation == "source_report_sha_drift":
        original_snapshot = service._promotion._eligibility._promotion_report_snapshot
        calls = 0

        def drift_source_report(report: str):
            nonlocal calls
            result = original_snapshot(report)
            if report == source_report_id and calls == 0:
                source_report_path.write_bytes(source_report_path.read_bytes() + b" ")
                calls += 1
            return result

        monkeypatch.setattr(
            service._promotion._eligibility,
            "_promotion_report_snapshot",
            drift_source_report,
        )
    elif mutation == "provenance_drift":
        report = json.loads(retained_report_path.read_text())
        report["sourceRehearsalCodeVersion"] = "0" * 8
        retained_report_path.write_text(json.dumps(report))
    elif mutation == "retained_configuration_drift":
        (retained_root / "config/default.yaml").write_text("drift: true\n")
    elif mutation == "active_root_drift":
        (data_root / "config/default.yaml").write_text("drift: true\n")
    elif mutation == "code_drift":
        service._workspace.code_version, _calls = _changing_code_version("deadbeef", "cafebabe")
    elif mutation == "schema_v3":
        service._workspace.duckdb = FakeDuckDb(
            MarketSourceMetadata(3, "local_projection_v2_event_time")
        )
    elif mutation == "wrong_adjustment_mode":
        service._workspace.duckdb = FakeDuckDb(MarketSourceMetadata(4, "local_projection_v1"))
    elif mutation == "inexact_lineage":
        service._workspace.duckdb = FakeDuckDb(
            MarketSourceMetadata(
                4,
                "local_projection_v2_event_time",
                adjusted_metrics_ready=False,
            )
        )
    elif mutation == "database_identity_drift":
        database = retained_root / "market-timeseries/market.duckdb"
        database.write_bytes(database.read_bytes() + b"drift")
    elif mutation == "parquet_identity_drift":
        parquet = retained_root / "market-timeseries/parquet/stock_data/part.parquet"
        parquet.write_bytes(parquet.read_bytes() + b"drift")
    elif mutation == "source_root_replacement":
        detached = retained_root.with_name("root.detached")
        retained_root.rename(detached)
        retained_root.symlink_to(detached, target_is_directory=True)
    elif mutation == "source_ancestor_replacement":
        rehearsals = retained_root.parents[1]
        rehearsals.rename(rehearsals.with_name("rehearsals.detached"))
        rehearsals.mkdir()
    elif mutation == "source_market_leaf_replacement":
        market = retained_root / "market-timeseries"
        detached = retained_root / "market-timeseries.detached"
        market.rename(detached)
        shutil.copytree(detached, market)
    elif mutation == "live_retained_lease":
        competing_lease = market_operation_lease.MarketOperationLease.acquire(
            retained_root, exclusive=True
        )
    elif mutation == "nonempty_wal":
        (retained_root / "market-timeseries/market.duckdb.wal").write_bytes(b"pending")
    elif mutation == "mismatched_smoke_config":
        config = SmokeConfig("9984", config.strategy, config.dataset_preset)
    elif mutation == "unexpected_retained_artifact":
        (retained_root / "market-timeseries/unexpected").write_bytes(b"foreign")
    elif mutation == "cross_device":

        def cross_device(
            _retained_lease: market_operation_lease.MarketOperationLease,
        ) -> None:
            raise CutoverSafetyError("same device")

        monkeypatch.setattr(
            service._promotion._eligibility,
            "_assert_promotion_exchange_capability",
            cross_device,
        )
    elif mutation == "unavailable_exchange":
        monkeypatch.setattr(filesystem_module.sys, "platform", "linux")
    else:
        destination = {
            "existing_report": Path("reports") / report_id,
            "existing_journal": Path("journals") / report_id,
            "existing_journal_control": Path("journal-controls") / report_id,
            "existing_journal_lock": Path("journal-locks") / f"{report_id}.lock",
            "existing_holding": Path("holding") / report_id,
            "existing_quarantine": Path("quarantine") / report_id,
            "existing_backup_id": Path("backups") / backup_id,
            "existing_consumed_marker": Path("consumed") / f"{retained_report_id}.json",
        }[mutation]
        existing = data_root / "operations/market-v4-cutover" / destination
        if mutation == "existing_consumed_marker":
            existing.parent.mkdir(parents=True)
            existing.write_text("consumed")
        else:
            existing.mkdir(parents=True)

    mutation_events: list[str] = []

    def mutation_forbidden(*_args: object, **_kwargs: object) -> None:
        mutation_events.append("mutation")
        raise AssertionError("promotion mutation hook ran during eligibility")

    monkeypatch.setattr(service._backups, "_backup_under_lease", mutation_forbidden)
    monkeypatch.setattr(service._workspace, "_managed_mutation_hook", mutation_forbidden)
    monkeypatch.setattr(service._workspace, "_rename_at_hook", mutation_forbidden)
    monkeypatch.setattr(service._workspace.atomic_exchange, "exchange", mutation_forbidden)
    service._workspace.runtime = FakeRuntime()

    try:
        with pytest.raises(CutoverSafetyError):
            with service._promotion._transaction._retained_promotion_eligibility_scope(
                report_id=report_id,
                retained_report_id=retained_report_id,
                backup_id=backup_id,
                config=config,
            ):
                raise AssertionError("ineligible promotion entered operation scope")
    finally:
        if competing_lease is not None:
            competing_lease.release()

    assert mutation_events == []
    assert service._workspace.runtime.start_calls == 0


def test_promote_retained_accepts_recorded_report_code_under_newer_clean_code(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    retained_report = data_root / (
        "operations/market-v4-cutover/reports/"
        "market-v4-retained-20260715-r13/report.json"
    )
    payload = json.loads(retained_report.read_text())
    payload["codeVersion"] = "59f41f2e"
    retained_report.write_text(json.dumps(payload))
    service._workspace.code_version = lambda: "feedface"

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        assert eligibility.retained_report_id == "market-v4-retained-20260715-r13"


@pytest.mark.parametrize("missing_lock", ["active", "retained"])
def test_promote_retained_requires_existing_lock_without_recreating_it(
    tmp_path: Path,
    missing_lock: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    root = data_root if missing_lock == "active" else retained_root
    lock = root / ".market-timeseries.operation.lock"
    lock.unlink()

    with pytest.raises(CutoverSafetyError, match="lock"):
        with service._promotion._transaction._retained_promotion_eligibility_scope(
            report_id="market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ):
            raise AssertionError("missing lock entered eligibility scope")

    assert not lock.exists()


def test_promote_retained_existing_lease_acquisition_is_metadata_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    real_open = cutover_module.os.open
    lock_open_flags: list[int] = []

    def record_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if os.fspath(path) == ".market-timeseries.operation.lock":
            lock_open_flags.append(flags)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(cutover_module.os, "open", record_open)
    monkeypatch.setattr(
        cutover_module.os,
        "fchmod",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("eligibility lease changed lock metadata")
        ),
    )

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ):
        pass

    assert len(lock_open_flags) == 2
    assert all(flags & os.O_CREAT == 0 for flags in lock_open_flags)


def test_existing_operation_lease_rejects_lock_replacement_at_flock_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "xdg"
    data_root.mkdir()
    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass
    lock = data_root / ".market-timeseries.operation.lock"
    real_flock = market_operation_lease.fcntl.flock
    replaced = False

    def replace_before_flock(fd: int, operation: int) -> None:
        nonlocal replaced
        if operation & market_operation_lease.fcntl.LOCK_EX and not replaced:
            replaced = True
            lock.rename(lock.with_suffix(".detached"))
            lock.write_bytes(b"replacement")
        real_flock(fd, operation)

    monkeypatch.setattr(market_operation_lease.fcntl, "flock", replace_before_flock)

    with pytest.raises(CutoverSafetyError, match="identity"):
        market_operation_lease.MarketOperationLease.acquire_existing(
            data_root, exclusive=True
        )


@pytest.mark.parametrize("late_drift", ["source_report", "active_payload"])
def test_promote_retained_rejects_late_eligibility_drift_before_yield(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    late_drift: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    retained_report_id = "market-v4-retained-20260715-r13"
    source_report = data_root / (
        "operations/market-v4-cutover/reports/"
        "market-v4-rehearsal-20260715-r10/report.json"
    )
    active_database = data_root / "market-timeseries/market.duckdb"
    original_snapshot = service._promotion._eligibility._promotion_report_snapshot
    retained_reads = 0

    def drift_at_final_boundary(report_id: str):
        nonlocal retained_reads
        result = original_snapshot(report_id)
        if report_id == retained_report_id:
            retained_reads += 1
            if retained_reads == 4:
                if late_drift == "source_report":
                    source_report.write_bytes(source_report.read_bytes() + b" ")
                else:
                    active_database.write_bytes(active_database.read_bytes() + b"drift")
        return result

    monkeypatch.setattr(
        service._promotion._eligibility,
        "_promotion_report_snapshot",
        drift_at_final_boundary,
    )

    with pytest.raises(CutoverSafetyError, match="changed|identity"):
        with service._promotion._transaction._retained_promotion_eligibility_scope(
            report_id="market-v4-active-20260716",
            retained_report_id=retained_report_id,
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ):
            raise AssertionError("late drift entered eligibility scope")


def test_promote_retained_uses_active_then_retained_lock_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    acquired: list[Path] = []
    original_acquire = market_operation_lease.MarketOperationLease.acquire_existing

    def recording_acquire(
        cls: type[market_operation_lease.MarketOperationLease],
        root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
    ) -> market_operation_lease.MarketOperationLease:
        del cls
        acquired.append(root)
        return original_acquire(root, exclusive=exclusive, blocking=blocking)

    monkeypatch.setattr(
        market_operation_lease.MarketOperationLease,
        "acquire_existing",
        classmethod(recording_acquire),
    )

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        assert eligibility.retained_root == retained_root

    assert acquired == [data_root, retained_root]


def test_promote_retained_holds_both_leases_through_eligibility_scope(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)

    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ):
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_operation_lease.MarketOperationLease.acquire(
                    root,
                    exclusive=True,
                )

    for root in (data_root, retained_root):
        with market_operation_lease.MarketOperationLease.acquire(root, exclusive=True):
            pass


def test_promotion_creates_and_verifies_backup_inside_active_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    events: list[str] = []
    original_verify = service._backups._verify_backup_managed

    def verify(backup_id: str, *, require_current_root: bool = True):
        assert service._workspace._active_lease is not None
        events.append("backup_verified")
        return original_verify(
            backup_id,
            require_current_root=require_current_root,
        )

    monkeypatch.setattr(service._backups, "_verify_backup_managed", verify)
    monkeypatch.setattr(
        service._backups,
        "_preflight_under_lease",
        lambda: (_ for _ in ()).throw(
            AssertionError("promotion used full-rebuild preflight")
        ),
    )

    preparation, records = _prepare_retained_promotion(service, config)

    backup = data_root / (
        "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716"
    )
    assert events == ["backup_verified"]
    assert (backup / "payload/market.duckdb").read_bytes() == b"duckdb-v3"
    assert (backup / "payload/parquet/stock_data/part.parquet").read_bytes() == b"rows"
    assert preparation.backup_manifest_sha256 == service._workspace._sha256(
        backup / "manifest.json"
    )
    assert service._promotion._promotion_evidence._payload_manifest_entries(preparation.backup_payload_identity) == (
        service._promotion._promotion_evidence._payload_manifest_entries(
            preparation.eligibility.active_market_identity
        )
    )
    backup_database = preparation.backup_payload_identity["marketDuckdb"]
    active_database = preparation.eligibility.active_market_identity["marketDuckdb"]
    assert isinstance(backup_database, dict)
    assert isinstance(active_database, dict)
    assert (backup_database["device"], backup_database["inode"]) != (
        active_database["device"],
        active_database["inode"],
    )
    assert [record.state for record in records] == [
        PromotionState.VALIDATED,
        PromotionState.RUNTIMES_DETACHED,
        PromotionState.PREPARED,
    ]


def test_promotion_backup_requires_payload_bytes_plus_reserve_not_rebuild_space(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    source_bytes = sum(
        path.stat().st_size
        for path in (data_root / "market-timeseries").rglob("*")
        if path.is_file()
    )
    required_bytes = source_bytes + max(source_bytes // 20, 1)
    assert required_bytes < source_bytes * 4
    service, _retained_root, config = _retained_promotion_source(data_root)
    service._workspace.disk_free_bytes = lambda _path: required_bytes

    preparation, _records = _prepare_retained_promotion(service, config)

    assert preparation.backup_manifest_sha256

    low_root = _market_root(tmp_path / "low")
    low_service, _retained_root, low_config = _retained_promotion_source(low_root)
    low_service._workspace.disk_free_bytes = lambda _path: required_bytes - 1
    with low_service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=low_config,
    ) as eligibility:
        journal = PromotionJournal(
            low_service._workspace.managed(),
            "market-v4-active-20260716",
            now=lambda: "2026-07-16T00:00:00Z",
        )
        with pytest.raises(CutoverSafetyError, match="free space"):
            low_service._promotion._artifacts._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
    assert not (
        low_root / "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716"
    ).exists()


def test_promotion_rejects_backup_identity_mismatch_before_detach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    runtime = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13"
    )
    original_verify = service._backups._verify_backup_managed

    def drift_after_verify(backup_id: str, *, require_current_root: bool = True):
        result = original_verify(
            backup_id,
            require_current_root=require_current_root,
        )
        database = data_root / "market-timeseries/market.duckdb"
        database.write_bytes(database.read_bytes() + b"drift")
        return result

    monkeypatch.setattr(service._backups, "_verify_backup_managed", drift_after_verify)

    with pytest.raises(CutoverSafetyError, match="identity"):
        _prepare_retained_promotion(service, config)

    assert runtime.is_dir()
    assert not (
        data_root / "operations/market-v4-cutover/holding/market-v4-active-20260716"
    ).exists()


def test_promotion_rejects_duplicate_backup_manifest_path_before_detach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    runtime = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13"
    )
    original_copy = service._backups._copy_backup_under_snapshot

    def duplicate_manifest_path(*args: object, **kwargs: object) -> None:
        original_copy(*args, **kwargs)
        manifest = data_root / (
            "operations/market-v4-cutover/backups/"
            "market-v3-pre-v4-20260716/manifest.json"
        )
        payload = json.loads(manifest.read_text())
        payload["files"].append(dict(payload["files"][0]))
        manifest.chmod(0o600)
        manifest.write_text(json.dumps(payload))

    monkeypatch.setattr(
        service._backups,
        "_copy_backup_under_snapshot",
        duplicate_manifest_path,
    )

    with pytest.raises(CutoverSafetyError, match="duplicate"):
        _prepare_retained_promotion(service, config)

    assert runtime.is_dir()
    assert not (
        data_root / "operations/market-v4-cutover/holding/market-v4-active-20260716"
    ).exists()


def test_promotion_detaches_only_report_proven_runtimes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    fsynced_directories: list[Path] = []
    original_fsync_dir = managed_root.ManagedRootFd.fsync_dir

    def record_fsync_dir(
        managed: managed_root.ManagedRootFd,
        relative: Path,
    ) -> None:
        fsynced_directories.append(relative)
        original_fsync_dir(managed, relative)

    monkeypatch.setattr(
        managed_root.ManagedRootFd,
        "fsync_dir",
        record_fsync_dir,
    )
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "market-v4-retained-20260715-r12",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    market = retained_root / "market-timeseries"
    source_runtime = market / ".cutover-runtime-market-v4-rehearsal-20260715-r10"
    source_runtime.mkdir()
    (source_runtime / "evidence").write_text("source")
    (market / "duckdb-tmp").mkdir()
    (market / "market.duckdb.wal").touch()

    preparation, _records = _prepare_retained_promotion(service, config)

    assert preparation.detached_runtime_names == (
        ".cutover-runtime-market-v4-rehearsal-20260715-r10",
        ".cutover-runtime-market-v4-retained-20260715-r12",
        ".cutover-runtime-market-v4-retained-20260715-r13",
    )
    assert preparation.holding_directory_identity == {
        "device": preparation.holding_root.stat().st_dev,
        "inode": preparation.holding_root.stat().st_ino,
    }
    assert {artifact.name for artifact in preparation.detached_artifacts} == {
        ".cutover-runtime-market-v4-rehearsal-20260715-r10",
        ".cutover-runtime-market-v4-retained-20260715-r12",
        ".cutover-runtime-market-v4-retained-20260715-r13",
        "duckdb-tmp",
        "market.duckdb.wal",
    }
    source_evidence = next(
        artifact
        for artifact in preparation.detached_artifacts
        if artifact.name == ".cutover-runtime-market-v4-rehearsal-20260715-r10"
    )
    assert source_evidence.kind == "directory"
    assert source_evidence.files["evidence"]["sha256"] == service._workspace._sha256(
        preparation.holding_root
        / ".cutover-runtime-market-v4-rehearsal-20260715-r10/evidence"
    )
    assert set(path.name for path in preparation.holding_root.iterdir()) == {
        ".cutover-runtime-market-v4-rehearsal-20260715-r10",
        ".cutover-runtime-market-v4-retained-20260715-r12",
        ".cutover-runtime-market-v4-retained-20260715-r13",
        "duckdb-tmp",
        "market.duckdb.wal",
    }
    assert set(path.name for path in market.iterdir()) == {"market.duckdb", "parquet"}
    assert Path("operations/market-v4-cutover") in fsynced_directories
    assert Path("operations/market-v4-cutover/holding") in fsynced_directories


def test_promotion_rejects_prefix_matched_unproven_runtime(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    unproven = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13-copy"
    )
    unproven.mkdir()

    with pytest.raises(CutoverSafetyError, match="unexpected artifact"):
        _prepare_retained_promotion(service, config)

    assert unproven.is_dir()
    assert not (
        data_root / "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716"
    ).exists()


def test_promotion_requires_canonical_payload_after_detach(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        (retained_root / "market-timeseries/foreign").write_text("unexpected")
        journal = PromotionJournal(
            service._workspace.managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        with pytest.raises(CutoverSafetyError, match="canonical|unexpected"):
            service._promotion._artifacts._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
        assert journal.read_validated() == ()


def test_promotion_aborts_on_not_committed_journal_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    runtime = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13"
    )
    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._workspace.managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        monkeypatch.setattr(
            journal,
            "append",
            lambda *_args, **_kwargs: PromotionAppendResult(
                PromotionAppendStatus.NOT_COMMITTED,
                None,
                "attempt-not-committed",
            ),
        )
        with pytest.raises(CutoverSafetyError, match="not committed"):
            service._promotion._artifacts._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    assert runtime.is_dir()
    assert not (
        data_root / f"operations/market-v4-cutover/holding/{report_id}"
    ).exists()


def test_promotion_indeterminate_journal_append_fences_both_leases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    leaked_fds: tuple[int, int]
    with pytest.raises(CutoverSafetyError, match="indeterminate"):
        with service._promotion._transaction._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            assert service._workspace._active_lease is not None
            assert service._workspace._retained_lease is not None
            leaked_fds = (service._workspace._active_lease.fd, service._workspace._retained_lease.fd)
            journal = PromotionJournal(
                service._workspace.managed(),
                report_id,
                now=lambda: "2026-07-16T00:00:00Z",
            )
            monkeypatch.setattr(
                journal,
                "append",
                lambda *_args, **_kwargs: PromotionAppendResult(
                    PromotionAppendStatus.INDETERMINATE,
                    None,
                    "attempt-indeterminate",
                ),
            )
            service._promotion._artifacts._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    try:
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_operation_lease.MarketOperationLease.acquire_existing(
                    root,
                    exclusive=True,
                )
    finally:
        for fd in leaked_fds:
            os.close(fd)
