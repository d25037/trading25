"""Market v4 cutover retained rehearsal tests."""

from __future__ import annotations

from collections.abc import Callable
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
    SmokeResult,
)
from src.application.services.market_v4_cutover.errors import (
    RuntimeStopError,
    WorkerShutdownError,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root, market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _changing_code_version,
    _retained_source,
    _read_operation_report,
)


def test_rehearsal_uses_isolated_paths_and_credential_safe_report(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    rehearsal_api = FakeApi()
    runtime = FakeRuntime(apis=[rehearsal_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    result = service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={
            "JQUANTS_API_KEY": "super-secret",
            "TRADING25_DATA_DIR": "/active/leak",
            "MARKET_TIMESERIES_DIR": "/active/market",
            "DATASET_BASE_PATH": "/active/datasets",
            "PORTFOLIO_DB_PATH": "/active/portfolio.db",
            "TRADING25_STRATEGIES_DIR": "/active/strategies",
            "TRADING25_BACKTEST_DIR": "/active/backtest",
            "TRADING25_DEFAULT_CONFIG_PATH": "/active/default.yaml",
        },
    )

    report_path = data_root / result.report_path
    report_text = report_path.read_text()
    report = json.loads(report_text)
    assert report["status"] == "passed"
    assert report["reportId"] == "rehearsal-001"
    assert report["rehearsalMode"] == "full_rebuild"
    assert report["serverProcessJoined"] is True
    assert report["workerProcessJoined"] is True
    assert report["targetRootFingerprint"] == service.root_fingerprint(data_root)
    assert "super-secret" not in report_text
    assert str(data_root) not in report_text
    assert runtime.stop_calls == 1
    environment = runtime.environments[0]
    runtime_name = ".cutover-runtime-rehearsal-001"
    assert environment["TRADING25_DATA_DIR"] == runtime_name
    assert environment["MARKET_TIMESERIES_DIR"] == "."
    assert environment["MARKET_DB_PATH"] == "market.duckdb"
    assert environment["DATASET_BASE_PATH"] == f"{runtime_name}/datasets"
    assert environment["PORTFOLIO_DB_PATH"] == f"{runtime_name}/portfolio.db"
    assert environment["TRADING25_STRATEGIES_DIR"] == f"{runtime_name}/strategies"
    assert environment["TRADING25_BACKTEST_DIR"] == f"{runtime_name}/backtest"
    assert (
        environment["TRADING25_DEFAULT_CONFIG_PATH"]
        == f"{runtime_name}/config/default.yaml"
    )
    assert environment["JQUANTS_API_KEY"] == "super-secret"
    assert "TRADING25_RUNTIME_CAPABILITY" not in environment
    api_calls = runtime.environments and report["apiChecks"]
    assert "/api/db/adjusted-metrics/materialize" not in api_calls
    sync_payload = next(
        payload
        for method, path, payload in rehearsal_api.calls
        if method == "POST" and path == "/api/db/sync"
    )
    assert sync_payload is not None
    assert sync_payload["resetBeforeSync"] is False


def test_operation_report_emits_supplied_rehearsal_provenance(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    market_identity = {"device": 1, "inode": 2}

    report = service._reports._operation_report(
        report_id="retained-rehearsal",
        phase="rehearsal",
        status="passed",
        duration_seconds=1.0,
        api_checks=(),
        server_log="rehearsals/retained-rehearsal/server.log",
        evidence=None,
        phases=(),
        config=SmokeConfig("7203", "production/smoke", "primeMarket"),
        code_version="deadbeef",
        rehearsal_mode="retained_market_smoke",
        source_rehearsal_report_id="full-rehearsal",
        source_rehearsal_code_version="deadbeef",
        source_retained_root_fingerprint="root-fingerprint",
        source_market_identity_before=market_identity,
        source_market_identity_after=market_identity,
    )

    assert report["sourceRehearsalReportId"] == "full-rehearsal"
    assert report["sourceRehearsalCodeVersion"] == "deadbeef"
    assert report["sourceRetainedRootFingerprint"] == "root-fingerprint"
    assert report["sourceMarketIdentityBefore"] == market_identity
    assert report["sourceMarketIdentityAfter"] == market_identity


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_source_report",
        "same_report_id",
        "wrong_smoke_config",
        "active_fingerprint_drift",
        "source_status_cleanup_deferred",
        "source_server_unjoined",
        "source_worker_unjoined",
        "source_root_symlink",
        "configuration_drift",
        "schema_v3",
        "wrong_adjustment_mode",
    ],
)
def test_rehearse_retained_rejects_ineligible_source(
    tmp_path: Path,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(
        data_root,
        source_report_id=source_id,
    )
    report_id = "retained-r12"
    source_report_path = (
        data_root / "operations/market-v4-cutover/reports" / source_id / "report.json"
    )
    source_report = json.loads(source_report_path.read_text())
    if mutation == "missing_source_report":
        source_report_path.unlink()
    elif mutation == "same_report_id":
        report_id = source_id
    elif mutation == "wrong_smoke_config":
        source_report["smokeConfig"] = {
            **source_report["smokeConfig"],
            "symbol": "9984",
        }
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "active_fingerprint_drift":
        source_report["targetRootFingerprint"] = "0" * 64
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_status_cleanup_deferred":
        source_report["status"] = "stop_failed_cleanup_deferred"
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_server_unjoined":
        source_report["serverProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_worker_unjoined":
        source_report["workerProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_root_symlink":
        external = tmp_path / "external-retained"
        shutil.move(retained_root, external)
        retained_root.symlink_to(external, target_is_directory=True)
    elif mutation == "configuration_drift":
        (retained_root / "config/default.yaml").write_text("drift: true\n")
    elif mutation == "schema_v3":
        service._workspace.duckdb = FakeDuckDb(
            MarketSourceMetadata(3, "local_projection_v2_event_time")
        )
    elif mutation == "wrong_adjustment_mode":
        service._workspace.duckdb = FakeDuckDb(
            MarketSourceMetadata(4, "local_projection_v1")
        )

    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            report_id,
            source_rehearsal_report_id=source_id,
            config=config,
            inherited_environment={},
        )

    assert runtime.start_calls == 0
    if mutation != "same_report_id":
        assert not (
            data_root / "operations/market-v4-cutover/reports" / report_id
        ).exists()


@pytest.mark.parametrize("source_status", ["passed", "failed"])
def test_rehearse_retained_smokes_current_code_without_market_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_status: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(
        data_root,
        source_report_id=source_id,
        status=source_status,
    )
    api = FakeApi()
    runtime = FakeRuntime(apis=[api])
    service._workspace.runtime = runtime
    smoke_result = SmokeResult(
        schema_version=4,
        adjustment_mode="local_projection_v2_event_time",
        checks=("market_metadata", "semantic_smoke"),
        api_paths=("/api/db/stats", "/api/analytics/fundamentals/7203"),
        lineage={"readyBasisCount": 2},
    )
    monkeypatch.setattr(
        service._runtime_smoke,
        "smoke",
        lambda *_args, **_kwargs: smoke_result,
    )

    result = service.rehearse_retained(
        "retained-r12",
        source_rehearsal_report_id=source_id,
        config=config,
        inherited_environment={},
    )

    report = _read_operation_report(data_root, "retained-r12")
    assert result.report_id == "retained-r12"
    assert runtime.start_calls == 1
    assert runtime.stop_calls == 1
    assert runtime.environments[0]["TRADING25_RUNTIME_CAPABILITY"] == (
        "retained_market_smoke"
    )
    assert all("/api/db/sync" not in path for _method, path, _payload in api.calls)
    assert all("materialize" not in path for _method, path, _payload in api.calls)
    assert report["status"] == "passed"
    assert report["codeVersion"] == "deadbeef"
    assert report["rehearsalMode"] == "retained_market_smoke"
    assert report["sourceRehearsalReportId"] == source_id
    assert report["sourceRehearsalCodeVersion"] == "cafebabe"
    assert report["sourceRetainedRootFingerprint"] == service.root_fingerprint(
        retained_root
    )
    assert report["sourceMarketIdentityBefore"] == report["sourceMarketIdentityAfter"]
    assert report["apiChecks"] == list(smoke_result.api_paths)
    assert report["schemaCoverage"] == {
        "schemaVersion": 4,
        "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        "adjustedMetrics": smoke_result.lineage,
    }
    assert report["phases"][0]["name"] == "retained_market_smoke"
    assert report["serverProcessJoined"] is True
    assert report["workerProcessJoined"] is True
    runtime_root = retained_root / "market-timeseries/.cutover-runtime-retained-r12"
    assert (runtime_root / "config/default.yaml").is_file()
    assert (runtime_root / "strategies/production/smoke.yaml").is_file()


def test_rehearse_retained_real_smoke_traverses_semantic_paths_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    api = FakeApi()
    service._workspace.runtime = FakeRuntime(apis=[api])

    result = service.rehearse_retained(
        "retained-real-smoke",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )

    report = _read_operation_report(data_root, result.report_id)
    paths = [path for _method, path, _payload in api.calls]
    assert report["apiChecks"] == paths
    assert "/api/db/stats" in paths
    assert "/api/db/validate" in paths
    assert "/api/fundamentals/compute" in paths
    assert "/api/analytics/screening/jobs" in paths
    assert "/api/analytics/fundamental-ranking" in paths
    assert "/api/dataset" in paths
    assert any(path.endswith("/info") for path in paths)
    assert any("/sample?count=1" in path for path in paths)
    assert all("/api/db/sync" not in path for path in paths)
    assert all("materialize" not in path for path in paths)
    assert all("stocks/refresh" not in path for path in paths)
    parquet_identity = report["sourceMarketIdentityBefore"]["parquetSha256"]
    parquet_file_identity = parquet_identity["stock_data/part.parquet"]
    assert isinstance(parquet_file_identity["device"], int)
    assert isinstance(parquet_file_identity["inode"], int)
    assert parquet_file_identity["size"] == len(b"retained-rows")
    assert len(parquet_file_identity["sha256"]) == 64


@pytest.mark.parametrize("existing_destination", ["report", "runtime"])
def test_rehearse_retained_rejects_destinations_before_creating_peer_artifact(
    tmp_path: Path,
    existing_destination: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    report_id = f"retained-existing-{existing_destination}"
    report_dir = data_root / "operations/market-v4-cutover/reports" / report_id
    runtime_dir = retained_root / "market-timeseries" / f".cutover-runtime-{report_id}"
    if existing_destination == "report":
        report_dir.mkdir(parents=True)
    else:
        runtime_dir.mkdir(parents=True)
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            report_id,
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert runtime.start_calls == 0
    if existing_destination == "report":
        assert not runtime_dir.exists()
    else:
        assert not report_dir.exists()


def test_rehearse_retained_requires_ready_lineage_before_resource_creation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service._workspace.duckdb.inspect = lambda *_args, **_kwargs: SimpleNamespace(
        schema_version=4,
        adjustment_mode="local_projection_v2_event_time",
        adjusted_metrics_ready=False,
    )
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-lineage-not-ready",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert runtime.start_calls == 0
    assert not (
        retained_root / "market-timeseries/.cutover-runtime-retained-lineage-not-ready"
    ).exists()
    assert not (
        data_root / "operations/market-v4-cutover/reports/retained-lineage-not-ready"
    ).exists()


def test_rehearse_retained_preserves_foreign_runtime_created_during_reservation_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime_name = ".cutover-runtime-retained-runtime-race"
    runtime_path = retained_root / "market-timeseries" / runtime_name
    original_open_dir = managed_root.ManagedRootFd.open_dir
    raced = False

    def create_foreign_runtime_before_exclusive_open(
        managed: managed_root.ManagedRootFd,
        relative: Path,
        *,
        create: bool = False,
        exclusive_leaf: bool = False,
    ) -> int:
        nonlocal raced
        if relative == Path("market-timeseries") / runtime_name and not raced:
            raced = True
            runtime_path.mkdir()
            (runtime_path / "foreign-owner").write_text("keep")
        return original_open_dir(
            managed,
            relative,
            create=create,
            exclusive_leaf=exclusive_leaf,
        )

    monkeypatch.setattr(
        managed_root.ManagedRootFd,
        "open_dir",
        create_foreign_runtime_before_exclusive_open,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse_retained(
            "retained-runtime-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert raced is True
    assert (runtime_path / "foreign-owner").read_text() == "keep"


@pytest.mark.parametrize("mutated_input", ["config", "strategy"])
def test_rehearse_retained_rejects_descriptor_configuration_mutation_during_smoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutated_input: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )

    def mutate(*_args: object, **_kwargs: object) -> SmokeResult:
        target = (
            retained_root / "config/default.yaml"
            if mutated_input == "config"
            else retained_root / "strategies/production/smoke.yaml"
        )
        target.write_text("mutated: true\n")
        return smoke_result

    monkeypatch.setattr(service._runtime_smoke, "smoke", mutate)
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            f"retained-mutated-{mutated_input}",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )
    report = _read_operation_report(data_root, f"retained-mutated-{mutated_input}")
    assert report["status"] == "failed"


def test_rehearse_retained_rejects_incoherent_runtime_strategy_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    original_copy = managed_root.ManagedRootFd.copy_tree_create

    def raced_copy(managed, source: Path, target: Path) -> None:
        strategy = retained_root / "strategies/production/smoke.yaml"
        original = strategy.read_bytes()
        strategy.write_text("raced: true\n")
        try:
            original_copy(managed, source, target)
        finally:
            strategy.write_bytes(original)

    monkeypatch.setattr(managed_root.ManagedRootFd, "copy_tree_create", raced_copy)
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-incoherent-runtime",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )


def test_rehearse_retained_publication_boundary_invalidates_drifted_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    mutated = False

    def drift_after_publish(stage: str) -> None:
        nonlocal mutated
        if stage == "after_publish" and not mutated:
            mutated = True
            (retained_root / "config/default.yaml").write_text("drift: true\n")

    service._workspace._report_publish_hook = drift_after_publish
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-publication-drift",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )
    report_path = (
        data_root
        / "operations/market-v4-cutover/reports/retained-publication-drift/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"


@pytest.mark.parametrize(
    "market_target", ["market.duckdb", "parquet/stock_data/part.parquet"]
)
def test_rehearse_retained_rejects_market_tree_mutation_after_smoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market_target: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )

    def mutate_market_after_smoke(*_args: object, **_kwargs: object) -> SmokeResult:
        target = retained_root / "market-timeseries" / market_target
        target.write_bytes(target.read_bytes() + b"changed")
        return smoke_result

    monkeypatch.setattr(
        service._runtime_smoke,
        "smoke",
        mutate_market_after_smoke,
    )

    with pytest.raises(CutoverSafetyError, match="retained Market tree changed"):
        service.rehearse_retained(
            "retained-mutated",
            source_rehearsal_report_id=source_id,
            config=config,
            inherited_environment={},
        )

    report = _read_operation_report(data_root, "retained-mutated")
    assert report["status"] == "failed"
    assert report["sourceMarketIdentityBefore"] != report["sourceMarketIdentityAfter"]
    assert runtime.stop_calls == 1


@pytest.mark.parametrize(
    ("failure", "expected_status", "server_joined", "worker_joined"),
    [
        ("code_drift", "failed", True, True),
        ("active_fingerprint_drift", "failed", True, True),
        ("smoke", "failed", True, True),
        ("runtime_stop_joined", "failed", True, True),
        ("runtime_stop_unjoined", "stop_failed_cleanup_deferred", False, True),
        ("worker_unjoined", "stop_failed_cleanup_deferred", True, False),
    ],
)
def test_rehearse_retained_failure_cleanup_and_join_verdicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    expected_status: str,
    server_joined: bool,
    worker_joined: bool,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, _retained_root, config = _retained_source(data_root)
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )

    class StopFailingRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise RuntimeStopError(
                "injected stop failure",
                process_joined=failure == "runtime_stop_joined",
            )

    runtime: FakeRuntime
    if failure.startswith("runtime_stop"):
        runtime = StopFailingRuntime(apis=[FakeApi()])
    else:
        runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime

    def smoke(*_args: object, **_kwargs: object) -> SmokeResult:
        if failure == "active_fingerprint_drift":
            (data_root / "config/default.yaml").write_text("drift: true\n")
        if failure == "smoke":
            raise RuntimeError("injected smoke failure")
        if failure == "worker_unjoined":
            raise WorkerShutdownError(
                "injected worker failure",
                process_joined=False,
            )
        return smoke_result

    monkeypatch.setattr(service._runtime_smoke, "smoke", smoke)
    if failure == "code_drift":
        service._workspace.code_version, _calls = _changing_code_version(
            "deadbeef",
            "deadbeef",
            "cafebabe",
        )

    with pytest.raises(CutoverSafetyError, match="Retained Market rehearsal failed"):
        service.rehearse_retained(
            f"retained-{failure}",
            source_rehearsal_report_id=source_id,
            config=config,
            inherited_environment={},
        )

    report = _read_operation_report(data_root, f"retained-{failure}")
    assert report["status"] == expected_status
    assert report["serverProcessJoined"] is server_joined
    assert report["workerProcessJoined"] is worker_joined
    assert runtime.start_calls == 1
    assert runtime.stop_calls >= 1


def test_rehearse_retained_rejects_path_replacement_without_writing_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime
    detached_root = retained_root.with_name("detached-root")
    original_prepare = service._market_identity._prepare_retained_runtime

    def replace_then_prepare(
        root: Path,
        *,
        runtime_name: str,
        root_fd: int | None = None,
        on_reserved: Callable[[], None] | None = None,
    ) -> None:
        root.rename(detached_root)
        shutil.copytree(detached_root, root)
        original_prepare(
            root,
            runtime_name=runtime_name,
            root_fd=root_fd,
            on_reserved=on_reserved,
        )

    monkeypatch.setattr(
        service._market_identity,
        "_prepare_retained_runtime",
        replace_then_prepare,
    )

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-path-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert not (
        retained_root / "market-timeseries/.cutover-runtime-retained-path-race"
    ).exists()
    assert runtime.start_calls == 0


def test_rehearse_retained_rejects_prelease_same_content_root_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime
    detached_root = retained_root.with_name("prelease-original-root")
    original_acquire = market_operation_lease.MarketOperationLease.acquire
    replaced = False

    def replace_before_acquire(
        cls: type[market_operation_lease.MarketOperationLease],
        lease_root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
    ) -> market_operation_lease.MarketOperationLease:
        del cls
        nonlocal replaced
        if lease_root == retained_root and not replaced:
            replaced = True
            lease_root.rename(detached_root)
            shutil.copytree(detached_root, lease_root)
        return original_acquire(
            lease_root,
            exclusive=exclusive,
            blocking=blocking,
        )

    monkeypatch.setattr(
        market_operation_lease.MarketOperationLease,
        "acquire",
        classmethod(replace_before_acquire),
    )

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-prelease-root-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert replaced is True
    assert runtime.start_calls == 0
    assert not (
        retained_root / "market-timeseries/.cutover-runtime-retained-prelease-root-race"
    ).exists()


def test_rehearse_retained_rejects_ancestor_symlink_to_leased_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime
    source_directory = retained_root.parent
    detached_source_directory = source_directory.with_name("detached-source-directory")
    original_prepare = service._market_identity._prepare_retained_runtime
    substituted = False

    def substitute_ancestor_then_prepare(
        root: Path,
        *,
        runtime_name: str,
        root_fd: int | None = None,
        on_reserved: Callable[[], None] | None = None,
    ) -> None:
        nonlocal substituted
        source_directory.rename(detached_source_directory)
        source_directory.symlink_to(
            detached_source_directory,
            target_is_directory=True,
        )
        substituted = True
        original_prepare(
            root,
            runtime_name=runtime_name,
            root_fd=root_fd,
            on_reserved=on_reserved,
        )

    monkeypatch.setattr(
        service._market_identity,
        "_prepare_retained_runtime",
        substitute_ancestor_then_prepare,
    )

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-ancestor-symlink-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert substituted is True
    assert runtime.start_calls == 0
    assert not (
        detached_source_directory
        / "root/market-timeseries/.cutover-runtime-retained-ancestor-symlink-race"
    ).exists()


def test_rehearse_retained_revalidates_code_immediately_before_runtime_start(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.runtime = runtime
    service._workspace.code_version, calls = _changing_code_version(
        "deadbeef", "cafebabe"
    )

    with pytest.raises(CutoverSafetyError, match="Retained Market rehearsal failed"):
        service.rehearse_retained(
            "retained-prestart-code-drift",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert calls == ["deadbeef", "cafebabe"]
    assert runtime.start_calls == 0
