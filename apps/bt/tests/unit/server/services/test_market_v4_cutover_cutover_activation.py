"""Market v5 cutover cutover activation tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _changing_code_version,
    _TestAtomicExchange,
)


class _RecordingAtomicExchange(_TestAtomicExchange):
    def __init__(self) -> None:
        self.require_calls = 0
        self.exchanges: list[tuple[Path, Path]] = []

    def require_capability(self) -> None:
        self.require_calls += 1

    def exchange(self, managed_root, left: Path, right: Path) -> None:
        self.exchanges.append((left, right))
        super().exchange(managed_root, left, right)


def test_cutover_rejects_active_tree_drift_after_backup_before_exchange(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    atomic = _RecordingAtomicExchange()

    class ActiveTreeEditingRuntime(FakeRuntime):
        def start(self, **kwargs):
            api = super().start(**kwargs)
            if self.start_calls == 2:
                (data_root / "market-timeseries/market.duckdb").write_bytes(
                    b"changed-after-backup"
                )
            return api

    runtime = ActiveTreeEditingRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
        atomic_exchange=atomic,
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.backup("exact-backup")
    service.rehearse("exact-rehearsal", config, inherited_environment={})

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "reject-drift",
            rehearsal_report_id="exact-rehearsal",
            backup_id="exact-backup",
            config=config,
            inherited_environment={},
        )

    assert atomic.exchanges == []
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/reject-drift/report.json"
        ).read_text()
    )
    assert "no longer exactly matches" in report["errorMessage"]


def test_cutover_rejects_post_exchange_api_lineage_misdirection_and_restores(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    drifted = FakeApi().provider_vintage
    drifted = {**drifted, "sourceFingerprint": "b" * 64}
    runtime = FakeRuntime(
        apis=[FakeApi(), FakeApi(), FakeApi(provider_vintage=drifted)]
    )
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.backup("lineage-backup")
    service.rehearse("lineage-rehearsal", config, inherited_environment={})

    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "lineage-misdirection",
            rehearsal_report_id="lineage-rehearsal",
            backup_id="lineage-backup",
            config=config,
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_full_rebuild_activation_uses_atomic_exchange_before_quarantine(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    atomic = _RecordingAtomicExchange()
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()]),
        atomic_exchange=atomic,
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.backup("atomic-backup")
    service.rehearse("atomic-rehearsal", config, inherited_environment={})

    service.cutover(
        "atomic-active",
        rehearsal_report_id="atomic-rehearsal",
        backup_id="atomic-backup",
        config=config,
        inherited_environment={},
    )

    assert atomic.require_calls >= 1
    assert atomic.exchanges == [
        (
            Path("market-timeseries"),
            Path(
                "operations/market-v5-cutover/staging/atomic-active/root/market-timeseries"
            ),
        )
    ]
    quarantined = list(
        (data_root / "operations/market-v5-cutover/quarantine").iterdir()
    )
    assert len(quarantined) == 1
    assert (quarantined[0] / "market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_failure_stops_owned_server_and_restores_backup(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("backup-001")
    service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()

    with pytest.raises(CutoverSafetyError, match="restored backup backup-001"):
        service.cutover(
            "active-failed",
            rehearsal_report_id="rehearsal-001",
            backup_id="backup-001",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 3
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original
    assert (data_root / "operations/market-v5-cutover/backups/backup-001").exists()


def test_cutover_stage_failure_leaves_active_market_identity_untouched(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("stage-failure-backup")
    service.rehearse(
        "stage-failure-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    active = data_root / "market-timeseries"
    active_before = active.stat()
    database_before = (active / "market.duckdb").read_bytes()

    with pytest.raises(CutoverSafetyError):
        service.cutover(
            "stage-failure-active",
            rehearsal_report_id="stage-failure-rehearsal",
            backup_id="stage-failure-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    active_after = active.stat()
    assert (active_after.st_dev, active_after.st_ino) == (
        active_before.st_dev,
        active_before.st_ino,
    )
    assert (active / "market.duckdb").read_bytes() == database_before
    quarantine = data_root / "operations/market-v5-cutover/quarantine"
    assert not quarantine.exists() or not list(quarantine.iterdir())
    assert (
        data_root
        / "operations/market-v5-cutover/staging/stage-failure-active/root/market-timeseries"
    ).is_dir()


def test_cutover_preactivation_failure_report_survives_code_drift(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("preactivation-drift-backup")
    service.rehearse(
        "preactivation-drift-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    runtime.apis.append(FakeApi(invalid_lineage=True))
    code_version, _calls = _changing_code_version(
        "deadbeef", "deadbeef", "deadbeef-dirty"
    )
    service._workspace.code_version = code_version
    active_before = (data_root / "market-timeseries/market.duckdb").read_bytes()

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "preactivation-code-drift",
            rehearsal_report_id="preactivation-drift-rehearsal",
            backup_id="preactivation-drift-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == active_before
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/preactivation-code-drift/report.json"
        ).read_text()
    )
    assert report["status"] == "failed_active_untouched"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "CutoverSafetyError"


def test_cutover_postactivation_identity_drift_restores_backup(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()
    service.backup("postactivation-drift-backup")
    service.rehearse(
        "postactivation-drift-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    code_version, _calls = _changing_code_version("deadbeef", "deadbeef-dirty")
    service._workspace.code_version = code_version

    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "postactivation-code-drift",
            rehearsal_report_id="postactivation-drift-rehearsal",
            backup_id="postactivation-drift-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/postactivation-code-drift/report.json"
        ).read_text()
    )
    assert report["status"] == "failed_restored"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "CutoverSafetyError"


def test_cutover_steady_code_identity_passes(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("steady-identity-backup")
    service.rehearse(
        "steady-identity-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    code_version, calls = _changing_code_version("deadbeef", "deadbeef")
    service._workspace.code_version = code_version

    result = service.cutover(
        "steady-identity-cutover",
        rehearsal_report_id="steady-identity-rehearsal",
        backup_id="steady-identity-backup",
        config=SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    report = json.loads((data_root / result.report_path).read_text())
    assert report["status"] == "passed"
    assert report["codeVersion"] == "deadbeef"
    assert calls == ["deadbeef", "deadbeef"]


def test_cutover_parent_swap_after_stage_start_never_touches_external_market(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-market"
    external.mkdir()
    external_db = external / "market.duckdb"
    external_db.write_bytes(b"external-must-not-change")

    class SwappingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                active = data_root / "market-timeseries"
                active.rename(data_root / "market-timeseries.detached")
                active.symlink_to(external, target_is_directory=True)
            return api

    runtime = SwappingRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("swap-after-start-backup")
    service.rehearse(
        "swap-after-start-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "swap-after-start-active",
            rehearsal_report_id="swap-after-start-rehearsal",
            backup_id="swap-after-start-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert external_db.read_bytes() == b"external-must-not-change"
    assert not (external / "market.duckdb.wal").exists()
    assert (
        data_root / "market-timeseries.detached/market.duckdb"
    ).read_bytes() == b"duckdb-v3"
    assert (
        data_root
        / "operations/market-v5-cutover/staging/swap-after-start-active/root/market-timeseries/market.duckdb"
    ).is_file()


def test_cutover_stage_root_swap_finishes_pinned_smoke_then_rejects_activation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-stage-root"
    (external / "market-timeseries").mkdir(parents=True)
    external_db = external / "market-timeseries/market.duckdb"
    external_db.write_bytes(b"external-stage-must-not-change")
    stage_api = FakeApi()

    class StageRootSwappingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                root = (
                    data_root
                    / "operations/market-v5-cutover/staging/stage-root-swap-active/root"
                )
                root.rename(root.with_name("root.detached"))
                root.symlink_to(external, target_is_directory=True)
            return api

    runtime = StageRootSwappingRuntime(apis=[FakeApi(), stage_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("stage-root-swap-backup")
    service.rehearse(
        "stage-root-swap-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "stage-root-swap-active",
            rehearsal_report_id="stage-root-swap-rehearsal",
            backup_id="stage-root-swap-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert any(
        path == "/api/db/validate" for _method, path, _payload in stage_api.calls
    )
    assert external_db.read_bytes() == b"external-stage-must-not-change"
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_market_leaf_swap_keeps_sync_on_inherited_directory_fd(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external_market = tmp_path / "external-market-leaf"
    external_market.mkdir()
    external_db = external_market / "market.duckdb"
    external_db.write_bytes(b"external-leaf-must-not-change")

    class DirectoryBoundSyncApi(FakeApi):
        market_fd: int | None = None

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            if method == "POST" and path == "/api/db/sync":
                assert self.market_fd is not None
                marker_fd = os.open(
                    "sync-marker",
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                    dir_fd=self.market_fd,
                )
                os.write(marker_fd, b"pinned")
                os.close(marker_fd)
            return super().request(method, path, payload)

    stage_api = DirectoryBoundSyncApi()

    class MarketLeafSwappingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                assert api is stage_api
                stage_api.market_fd = market_fd
                root = (
                    data_root
                    / "operations/market-v5-cutover/staging/market-leaf-swap-active/root"
                )
                market = root / "market-timeseries"
                market.rename(root / "market-timeseries.detached")
                market.symlink_to(external_market, target_is_directory=True)
            return api

    runtime = MarketLeafSwappingRuntime(apis=[FakeApi(), stage_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("market-leaf-swap-backup")
    service.rehearse(
        "market-leaf-swap-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "market-leaf-swap-active",
            rehearsal_report_id="market-leaf-swap-rehearsal",
            backup_id="market-leaf-swap-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    detached_market = (
        data_root
        / "operations/market-v5-cutover/staging/market-leaf-swap-active/root/market-timeseries.detached"
    )
    assert (detached_market / "sync-marker").read_bytes() == b"pinned"
    assert external_db.read_bytes() == b"external-leaf-must-not-change"
    assert not (external_market / "sync-marker").exists()
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_cross_parent_market_move_confines_non_market_writes(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-parent"
    (external / "datasets").mkdir(parents=True)
    external_dataset_marker = external / "datasets/marker"
    external_dataset_marker.write_bytes(b"external-dataset")
    external_portfolio = external / "portfolio.db"
    external_portfolio.write_bytes(b"external-portfolio")

    class RuntimePathWritingApi(FakeApi):
        market_fd: int | None = None
        environment: dict[str, str] | None = None

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            if method == "POST" and path == "/api/db/sync":
                assert self.market_fd is not None
                assert self.environment is not None
                dataset_path = Path(self.environment["DATASET_BASE_PATH"]) / "marker"
                dataset_fd = os.open(
                    dataset_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                    dir_fd=self.market_fd,
                )
                os.write(dataset_fd, b"pinned-runtime")
                os.close(dataset_fd)
                portfolio_fd = os.open(
                    self.environment["PORTFOLIO_DB_PATH"],
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                    dir_fd=self.market_fd,
                )
                os.write(portfolio_fd, b"pinned-runtime")
                os.close(portfolio_fd)
            return super().request(method, path, payload)

    stage_api = RuntimePathWritingApi()

    class CrossParentMovingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                assert api is stage_api
                stage_api.market_fd = market_fd
                stage_api.environment = environment
                stage_root = (
                    data_root
                    / "operations/market-v5-cutover/staging/cross-parent-active/root"
                )
                market = stage_root / "market-timeseries"
                market.rename(external / "moved-stage-market")
                market.mkdir()
                (market / "market.duckdb").write_bytes(b"replacement")
            return api

    runtime = CrossParentMovingRuntime(apis=[FakeApi(), stage_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("cross-parent-backup")
    service.rehearse(
        "cross-parent-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "cross-parent-active",
            rehearsal_report_id="cross-parent-rehearsal",
            backup_id="cross-parent-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    moved_runtime = external / "moved-stage-market/.cutover-runtime-cross-parent-active"
    assert (moved_runtime / "datasets/marker").read_bytes() == b"pinned-runtime"
    assert (moved_runtime / "portfolio.db").read_bytes() == b"pinned-runtime"
    assert external_dataset_marker.read_bytes() == b"external-dataset"
    assert external_portfolio.read_bytes() == b"external-portfolio"
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_report_write_failure_is_inside_restore_boundary(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("backup-001")
    service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original_write = service._reports._write_report

    def fail_active_report(report_id: str, report: dict[str, object]) -> Path:
        if report_id == "active-write-fail":
            raise OSError("injected fsync failure")
        return original_write(report_id, report)

    service._reports._write_report = fail_active_report  # type: ignore[method-assign]
    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "active-write-fail",
            rehearsal_report_id="rehearsal-001",
            backup_id="backup-001",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"
