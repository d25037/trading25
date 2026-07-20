"""Market v5 cutover cutover failure recovery tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil

import pytest

from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
)
from src.application.services.market_v4_cutover.errors import (
    RuntimeStopError,
    WorkerShutdownError,
)
from src.application.services.market_v4_cutover.service import MarketV4CutoverService
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
)


def test_cutover_defers_restore_when_active_server_stop_is_unproven(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)

    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            if self.stop_calls >= 3:
                raise RuntimeStopError(
                    "injected unjoined process",
                    process_joined=False,
                )

    runtime = UnjoinedRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("stop-deferred-backup")
    service.rehearse(
        "stop-deferred-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    restore_called = False

    def forbidden_restore(_backup_id: str) -> None:
        nonlocal restore_called
        restore_called = True
        raise AssertionError("restore must not run while the server may be alive")

    monkeypatch.setattr(service._backups, "restore", forbidden_restore)
    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "stop-deferred-active",
            rehearsal_report_id="stop-deferred-rehearsal",
            backup_id="stop-deferred-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert restore_called is False
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/stop-deferred-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert (
        data_root / "operations/market-v5-cutover/backups/stop-deferred-backup"
    ).is_dir()


def test_cutover_unjoined_stop_keeps_primary_and_secondary_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)

    class OriginalActiveSmokeError(RuntimeError):
        pass

    class UnjoinedCleanupRuntime(FakeRuntime):
        def stop(self, api: FakeApi) -> None:
            self.stop_calls += 1
            if self.stop_calls == 3:
                raise RuntimeStopError(
                    "injected unjoined cleanup",
                    process_joined=False,
                )

    runtime = UnjoinedCleanupRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("secondary-stop-backup")
    service.rehearse(
        "secondary-stop-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original_smoke = service._runtime_smoke.smoke
    smoke_calls = 0

    def fail_active_smoke(*args: object, **kwargs: object) -> object:
        nonlocal smoke_calls
        smoke_calls += 1
        if smoke_calls == 2:
            raise OriginalActiveSmokeError("injected active smoke failure")
        return original_smoke(*args, **kwargs)

    monkeypatch.setattr(service._runtime_smoke, "smoke", fail_active_smoke)

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "secondary-stop-active",
            rehearsal_report_id="secondary-stop-rehearsal",
            backup_id="secondary-stop-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/secondary-stop-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert report["errorType"] == "OriginalActiveSmokeError"
    assert report["stopErrorType"] == "RuntimeStopError"
    assert report["serverProcessJoined"] is False


def test_cutover_unjoined_active_server_transfers_active_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class LeaseHoldingRuntime(FakeRuntime):
        inherited_by_api: dict[int, int] = {}
        retained_unjoined_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            api = super().start(**kwargs)  # type: ignore[arg-type]
            self.inherited_by_api[id(api)] = os.dup(int(kwargs["lease_fd"]))
            return api

        def stop(self, api: FakeApi) -> None:
            self.stop_calls += 1
            inherited_fd = self.inherited_by_api.pop(id(api))
            if self.stop_calls == 3:
                self.retained_unjoined_fd = inherited_fd
                raise RuntimeStopError(
                    "injected unjoined active server",
                    process_joined=False,
                )
            os.close(inherited_fd)

    runtime = LeaseHoldingRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("active-lease-transfer-backup")
    service.rehearse(
        "active-lease-transfer-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "active-lease-transfer-cutover",
            rehearsal_report_id="active-lease-transfer-rehearsal",
            backup_id="active-lease-transfer-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_unjoined_fd)

    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_cutover_unjoined_staging_server_transfers_staging_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class LeaseHoldingRuntime(FakeRuntime):
        inherited_by_api: dict[int, int] = {}
        retained_unjoined_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            api = super().start(**kwargs)  # type: ignore[arg-type]
            self.inherited_by_api[id(api)] = os.dup(int(kwargs["lease_fd"]))
            return api

        def stop(self, api: FakeApi) -> None:
            self.stop_calls += 1
            inherited_fd = self.inherited_by_api[id(api)]
            if self.stop_calls >= 2:
                self.retained_unjoined_fd = inherited_fd
                raise RuntimeStopError(
                    "injected unjoined staging server",
                    process_joined=False,
                )
            os.close(inherited_fd)
            del self.inherited_by_api[id(api)]

    runtime = LeaseHoldingRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("staging-lease-transfer-backup")
    service.rehearse(
        "staging-lease-transfer-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "staging-lease-transfer-cutover",
            rehearsal_report_id="staging-lease-transfer-rehearsal",
            backup_id="staging-lease-transfer-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    staging_root = (
        data_root
        / "operations/market-v5-cutover/staging/staging-lease-transfer-cutover/root"
    )
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                staging_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_unjoined_fd)

    with market_operation_lease.MarketOperationLease.acquire(
        staging_root,
        exclusive=True,
    ):
        pass
    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_cutover_keyboard_interrupt_after_activation_restores_and_reraises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()
    service.backup("interrupt-backup")
    service.rehearse("interrupt-rehearsal", config, inherited_environment={})
    original_smoke = service._runtime_smoke.smoke
    smoke_calls = 0

    def interrupt_active_smoke(*args: object, **kwargs: object) -> object:
        nonlocal smoke_calls
        smoke_calls += 1
        if smoke_calls == 2:
            raise KeyboardInterrupt("operator interrupt")
        return original_smoke(*args, **kwargs)

    monkeypatch.setattr(service._runtime_smoke, "smoke", interrupt_active_smoke)

    with pytest.raises(KeyboardInterrupt, match="operator interrupt"):
        service.cutover(
            "interrupt-active",
            rehearsal_report_id="interrupt-rehearsal",
            backup_id="interrupt-backup",
            config=config,
            inherited_environment={},
        )

    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 3
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/interrupt-active/report.json"
        ).read_text()
    )
    assert report["status"] == "failed_restored"
    assert report["errorType"] == "KeyboardInterrupt"


def test_cutover_unjoined_staging_worker_transfers_staging_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        fail_inspect = False
        retained_guard_fd = -1

        def inspect(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            if self.fail_inspect:
                self.retained_guard_fd = os.dup(guard_lease_fd)
                raise WorkerShutdownError(
                    "injected unjoined staging worker",
                    process_joined=False,
                )
            return super().inspect(
                directory_fd,
                filename,
                guard_lease_fd=guard_lease_fd,
            )

    duckdb = GuardHoldingDuckDb(
        MarketSourceMetadata(5, "provider_adjusted_v1")
    )
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(data_root, duckdb=duckdb, runtime=runtime)
    service.backup("staging-worker-transfer-backup")
    service.rehearse(
        "staging-worker-transfer-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    duckdb.fail_inspect = True

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "staging-worker-transfer-cutover",
            rehearsal_report_id="staging-worker-transfer-rehearsal",
            backup_id="staging-worker-transfer-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    staging_root = (
        data_root
        / "operations/market-v5-cutover/staging/staging-worker-transfer-cutover/root"
    )
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_operation_lease.MarketOperationLease.acquire(
                staging_root,
                exclusive=False,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_operation_lease.MarketOperationLease.acquire(
        staging_root,
        exclusive=True,
    ):
        pass


def test_cutover_restore_failure_keeps_primary_and_restore_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("secondary-restore-backup")
    service.rehearse(
        "secondary-restore-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    class InjectedRestoreError(OSError):
        pass

    def fail_restore(_backup_id: str) -> None:
        raise InjectedRestoreError("injected restore failure")

    monkeypatch.setattr(service._backups, "restore", fail_restore)

    with pytest.raises(CutoverSafetyError, match="explicit restore also failed"):
        service.cutover(
            "secondary-restore-active",
            rehearsal_report_id="secondary-restore-rehearsal",
            backup_id="secondary-restore-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/secondary-restore-active/report.json"
        ).read_text()
    )
    assert report["status"] == "restore_failed"
    assert report["errorType"] == "CutoverSafetyError"
    assert report["restoreErrorType"] == "InjectedRestoreError"


def test_cutover_defers_restore_when_active_start_fails_before_api_unjoined(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)

    class ActiveStartFailureRuntime(FakeRuntime):
        starts = 0

        def start(self, **kwargs: object) -> FakeApi:
            self.starts += 1
            if self.starts == 3:
                raise RuntimeStopError(
                    "active startup child remains alive",
                    process_joined=False,
                )
            return super().start(**kwargs)  # type: ignore[arg-type]

    runtime = ActiveStartFailureRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("start-unjoined-backup")
    service.rehearse(
        "start-unjoined-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    restore_called = False

    def forbidden_restore(_backup_id: str) -> None:
        nonlocal restore_called
        restore_called = True
        raise AssertionError("restore must not run while startup child may be alive")

    monkeypatch.setattr(service._backups, "restore", forbidden_restore)
    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "start-unjoined-active",
            rehearsal_report_id="start-unjoined-rehearsal",
            backup_id="start-unjoined-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert restore_called is False
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/start-unjoined-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert report["errorType"] == "RuntimeStopError"


def test_cutover_restores_when_active_start_failure_proves_child_joined(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class JoinedActiveStartFailureRuntime(FakeRuntime):
        starts = 0

        def start(self, **kwargs: object) -> FakeApi:
            self.starts += 1
            if self.starts == 3:
                raise RuntimeStopError(
                    "active startup child joined",
                    process_joined=True,
                )
            return super().start(**kwargs)  # type: ignore[arg-type]

    runtime = JoinedActiveStartFailureRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()
    service.backup("start-joined-backup")
    service.rehearse(
        "start-joined-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "start-joined-active",
            rehearsal_report_id="start-joined-rehearsal",
            backup_id="start-joined-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original


def test_cutover_defers_restore_when_duckdb_worker_join_is_unproven(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("worker-stop-deferred-backup")
    service.rehearse(
        "worker-stop-deferred-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original_smoke = service._runtime_smoke.smoke
    smoke_calls = 0

    def fail_active_smoke(*args: object, **kwargs: object) -> object:
        nonlocal smoke_calls
        smoke_calls += 1
        if smoke_calls == 2:
            try:
                raise RuntimeError("primary active smoke failure")
            except RuntimeError as primary:
                raise WorkerShutdownError(
                    "injected unjoined DuckDB worker",
                    process_joined=False,
                ) from primary
        return original_smoke(*args, **kwargs)

    monkeypatch.setattr(service._runtime_smoke, "smoke", fail_active_smoke)
    restore_called = False

    def forbidden_restore(_backup_id: str) -> None:
        nonlocal restore_called
        restore_called = True
        raise AssertionError("restore must not run while a DuckDB worker may be alive")

    monkeypatch.setattr(service._backups, "restore", forbidden_restore)
    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "worker-stop-deferred-active",
            rehearsal_report_id="worker-stop-deferred-rehearsal",
            backup_id="worker-stop-deferred-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert restore_called is False
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/worker-stop-deferred-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert report["errorType"] == "WorkerShutdownError"
    assert report["workerProcessJoined"] is False


def test_restore_keeps_exact_backup_active_if_displaced_tree_quarantine_fails(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    service.backup("before")
    active = data_root / "market-timeseries"
    (active / "market.duckdb").write_bytes(b"failed-v4")
    calls = 0

    def fail_stage_once(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if source.name.startswith("market-timeseries.restore-"):
            raise OSError("injected activation failure")

    service._workspace._rename_at_hook = fail_stage_once
    result = service.restore("before")

    assert (active / "market.duckdb").read_bytes() == b"duckdb-v3"
    assert result.quarantine_path == "market-timeseries.restore-before"
    assert (data_root / result.quarantine_path / "market.duckdb").read_bytes() == (
        b"failed-v4"
    )
    assert calls == 1


def test_restore_can_repeat_same_backup_without_quarantine_collision(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    service.backup("before")

    first = service.restore("before")
    second = service.restore("before")

    assert first.quarantine_path != second.quarantine_path
    assert first.quarantine_path and (data_root / first.quarantine_path).exists()
    assert second.quarantine_path and (data_root / second.quarantine_path).exists()


def test_unknown_or_dirty_code_identity_fails_before_backup_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    for identity in ("unknown", "deadbeef-dirty"):
        service = MarketV4CutoverService(
            data_root,
            duckdb=FakeDuckDb(),
            runtime=FakeRuntime(),
            disk_free_bytes=lambda _path: 10_000_000,
            now=lambda: "2026-07-15T12:00:00Z",
            code_version=lambda identity=identity: identity,
        )
        with pytest.raises(CutoverSafetyError, match="code identity"):
            service.backup(f"backup-{identity}")


def test_root_fingerprint_binds_filesystem_and_config_strategy_content(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("mode: one\n")
    (data_root / "strategies/production").mkdir(parents=True)
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.write_text("value: one\n")
    service = _service(data_root)
    first = service.root_fingerprint(data_root)
    strategy.write_text("value: two\n")
    assert service.root_fingerprint(data_root) != first

    copied = tmp_path / "copied-root"
    shutil.copytree(data_root, copied)
    assert service.root_fingerprint(copied) != service.root_fingerprint(data_root)
