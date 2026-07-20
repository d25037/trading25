"""Market v5 cutover cutover contracts tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.models import validate_strategy_config_dict_strict
from src.domains.strategy.runtime.screening_profile import (
    load_strategy_screening_config,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _write_report,
    _service,
)


def test_cutover_rechecks_fingerprint_after_runtime_start_and_restores(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("mode: market\n")
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.parent.mkdir(parents=True)
    strategy.write_text("value: before\n")

    class StartEditingRuntime(FakeRuntime):
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
                strategy.write_text("value: changed-during-start\n")
            return api

    runtime = StartEditingRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    service.backup("before-start-race")
    service.rehearse(
        "passing-before-start-race",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "active-start-race",
            rehearsal_report_id="passing-before-start-race",
            backup_id="before-start-race",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"
    report_path = (
        data_root / "operations/market-v5-cutover/reports/active-start-race/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"


@pytest.mark.parametrize("mutation", ["mkdir", "copy", "write"])
def test_operation_parent_swap_never_writes_external_tree(
    tmp_path: Path,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-operations"
    external.mkdir()
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=runtime,
    )
    swapped = False

    def swap_parent(stage: str) -> None:
        nonlocal swapped
        if stage != mutation or swapped:
            return
        swapped = True
        cutover_root = data_root / "operations/market-v5-cutover"
        detached = data_root / "operations/market-v5-cutover.detached"
        cutover_root.rename(detached)
        cutover_root.symlink_to(external, target_is_directory=True)

    service._workspace._managed_mutation_hook = swap_parent
    with pytest.raises(CutoverSafetyError):
        if mutation in {"mkdir", "copy"}:
            service.backup(f"swap-{mutation}")
        else:
            service.rehearse(
                "swap-write",
                SmokeConfig("7203", "production/smoke", "primeMarket"),
                inherited_environment={},
            )

    assert swapped is True
    assert list(external.iterdir()) == []


def test_cutover_requires_exact_passing_rehearsal_and_verified_backup(
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

    with pytest.raises(CutoverSafetyError, match="passing rehearsal report"):
        service.cutover(
            "active-bad",
            rehearsal_report_id="missing",
            backup_id="backup-001",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    result = service.cutover(
        "active-001",
        rehearsal_report_id="rehearsal-001",
        backup_id="backup-001",
        config=SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    report = json.loads((data_root / result.report_path).read_text())
    assert report["status"] == "passed"
    assert report["backupManifest"] == "backups/backup-001/manifest.json"
    assert report["rehearsalReportId"] == "rehearsal-001"
    assert report["phases"][-1]["name"] == "activated_market_smoke"
    assert report["activeProviderVintage"] == report["stagedProviderVintage"]
    assert report["activeProviderVintage"] == report["schemaCoverage"][
        "providerVintage"
    ]
    assert len(report["activeBackupTreeSha256"]) == 64
    assert runtime.stop_calls == 3


def test_cutover_rejects_retained_rehearsal_mode_before_backup(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse("full-rebuild", config, inherited_environment={})
    report_path = (
        data_root
        / "operations/market-v5-cutover/reports/full-rebuild/report.json"
    )
    report = json.loads(report_path.read_text())
    report["rehearsalMode"] = "retained_market_smoke"
    report["sourceRehearsalReportId"] = "full-rebuild"
    report["sourceRehearsalCodeVersion"] = "deadbeef"
    report["sourceRetainedRootFingerprint"] = "retained-root"
    report["sourceMarketIdentityBefore"] = {"device": 1, "inode": 2}
    report["sourceMarketIdentityAfter"] = {"device": 1, "inode": 2}
    report["reportId"] = "retained-is-ineligible"
    _write_report(data_root, "retained-is-ineligible", report)

    with pytest.raises(CutoverSafetyError, match="Market v5 full-rebuild"):
        service.cutover(
            "must-not-activate-retained",
            rehearsal_report_id="retained-is-ineligible",
            backup_id="not-checked",
            config=config,
            inherited_environment={},
        )

    assert not (
        data_root
        / "operations/market-v5-cutover/staging/must-not-activate-retained"
    ).exists()


@pytest.mark.parametrize(
    "malformation",
    [
        "missing_mode",
        "missing_server_join",
        "false_server_join",
        "missing_worker_join",
        "false_worker_join",
    ],
)
def test_cutover_rejects_rehearsal_without_explicit_passing_evidence(
    tmp_path: Path,
    malformation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    smoke_config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse(
        "passing-rehearsal",
        smoke_config,
        inherited_environment={},
    )
    report = json.loads(
        (
            data_root
            / "operations/market-v5-cutover/reports/passing-rehearsal/report.json"
        ).read_text()
    )
    report.update(
        {
            "rehearsalMode": "full_rebuild",
            "serverProcessJoined": True,
            "workerProcessJoined": True,
        }
    )
    field = {
        "missing_mode": "rehearsalMode",
        "missing_server_join": "serverProcessJoined",
        "false_server_join": "serverProcessJoined",
        "missing_worker_join": "workerProcessJoined",
        "false_worker_join": "workerProcessJoined",
    }[malformation]
    if malformation.startswith("missing_"):
        report.pop(field)
    else:
        report[field] = False
    report_id = f"malformed-{malformation}"
    report["reportId"] = report_id
    _write_report(data_root, report_id, report)

    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-{malformation}",
            rehearsal_report_id=report_id,
            backup_id="unverified-backup",
            config=smoke_config,
            inherited_environment={},
        )

    assert not (
        data_root / "operations/market-v5-cutover/staging" / f"active-{malformation}"
    ).exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_sync_status",
        "missing_sync_phase",
        "missing_semantic_phase",
        "malformed_schema_coverage",
        "forbidden_noncanonical_api",
        "extra_failed_phase",
        "boolean_provider_counter",
        "missing_provider_plan",
        "provider_asof_drift",
        "provider_range_drift",
        "coverage_drift",
        "source_fingerprint_drift",
        "basis_date_drift",
    ],
)
def test_cutover_rejects_inexact_full_rebuild_evidence_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(5, "provider_adjusted_v1")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse("full-exact-evidence", config, inherited_environment={})
    report_path = data_root / (
        "operations/market-v5-cutover/reports/full-exact-evidence/report.json"
    )
    report = json.loads(report_path.read_text())
    if mutation == "missing_sync_status":
        report["apiChecks"] = [
            path for path in report["apiChecks"] if "/api/db/sync/jobs/" not in path
        ]
    elif mutation == "missing_sync_phase":
        report["phases"] = [
            phase
            for phase in report["phases"]
            if phase["name"] != "initial_sync_and_provider_vintage"
        ]
    elif mutation == "missing_semantic_phase":
        report["phases"] = [
            phase for phase in report["phases"] if phase["name"] != "semantic_smoke"
        ]
    elif mutation == "forbidden_noncanonical_api":
        report["apiChecks"].append("/api/db/stocks/refresh")
    elif mutation == "extra_failed_phase":
        report["phases"].append(
            {"name": "unexpected", "status": "failed", "durationSeconds": 0}
        )
    elif mutation == "boolean_provider_counter":
        report["schemaCoverage"]["providerVintage"][
            "sourceStatementKeyCount"
        ] = True
    elif mutation == "missing_provider_plan":
        report["schemaCoverage"]["providerVintage"].pop("providerPlan")
    elif mutation == "provider_asof_drift":
        report["schemaCoverage"]["providerVintage"]["providerAsOf"] = "2026-07-19"
    elif mutation == "provider_range_drift":
        report["schemaCoverage"]["providerVintage"]["providerAsOfRange"] = {
            "min": "2026-07-19",
            "max": "2026-07-20",
        }
    elif mutation == "coverage_drift":
        report["schemaCoverage"]["providerVintage"]["effectiveCoverage"] = {
            "min": "2026-07-20",
            "max": "2016-07-20",
        }
    elif mutation == "source_fingerprint_drift":
        report["schemaCoverage"]["providerVintage"]["sourceFingerprint"] = "bad"
    elif mutation == "basis_date_drift":
        report["schemaCoverage"]["providerVintage"][
            "fundamentalsAdjustmentBasisDate"
        ] = "not-a-date"
    else:
        report["schemaCoverage"] = {"schemaVersion": 5}
    report_path.write_text(json.dumps(report))
    backup_verified = False

    def unexpected_backup_verification(_backup_id: str):
        nonlocal backup_verified
        backup_verified = True
        raise AssertionError("backup verification must not run")

    monkeypatch.setattr(
        service._backups,
        "verify_backup",
        unexpected_backup_verification,
    )
    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-full-inexact-{mutation}",
            rehearsal_report_id="full-exact-evidence",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )

    assert backup_verified is False


def test_full_rebuild_runbook_and_repository_smoke_strategy_are_operational() -> None:
    repository_root = Path(__file__).resolve().parents[6]
    runbook = (repository_root / "docs/runbooks/market-v5-cutover.md").read_text()
    strategy_path = (
        repository_root
        / "apps/bt/config/strategies/production/cutover_smoke.yaml"
    )

    for required_text in (
        "Stop FastAPI",
        "JQUANTS_PLAN",
        "market-cutover preflight",
        "market-cutover rehearse",
        "market-cutover backup",
        "market-cutover cutover",
        "market-cutover restore",
        "provider_adjusted_v1",
        "Market schema 5",
        "Dataset schema 4",
        "providerVintage",
        "immutable v4 backup",
        "exact rollback",
        "benchmark_market_v5_sync.py",
        "representativeEvidence",
    ):
        assert required_text in runbook
    assert "promote-retained" not in runbook
    assert "rehearse-retained" not in runbook
    assert "local_projection_v2_event_time" not in runbook

    loader = ConfigLoader(str(repository_root / "apps/bt/config"))
    strategy_config = loader.load_strategy_config("production/cutover_smoke")
    validated = validate_strategy_config_dict_strict(strategy_config)
    screening = load_strategy_screening_config(
        loader,
        "production/cutover_smoke",
    )

    assert strategy_path.is_file()
    assert validated.shared_config is not None
    assert validated.shared_config.data_source == "market"
    assert validated.shared_config.universe_preset
    assert screening.screening_support == "supported"
    assert screening.entry_decidability in {
        "pre_open_decidable",
        "requires_same_session_observation",
    }


def test_owned_runtime_resolves_repository_cutover_smoke_strategy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    isolated_root = data_root / "operations/market-v5-cutover/runtime-resolution"
    runtime_name = ".cutover-runtime-resolution"
    selected_strategy = data_root / "strategies/production/cutover_smoke.yaml"
    selected_strategy.parent.mkdir(parents=True)
    selected_strategy.write_text("name: selected-root-conflict\n")
    selected_payload = selected_strategy.read_bytes()
    selected_fingerprint = service.configuration_fingerprint(data_root)

    with service._workspace.managed_root_scope():
        service._runtime_smoke.prepare_isolated_root(
            isolated_root,
            runtime_name=runtime_name,
        )

    environment = service._runtime_smoke.isolated_environment(
        {},
        lease_fd=10,
        root_fd=11,
        runtime_name=runtime_name,
    )
    monkeypatch.chdir(isolated_root / "market-timeseries")
    for key, value in environment.items():
        monkeypatch.setenv(key, value)

    loader = ConfigLoader()
    strategy_config = loader.load_strategy_config("production/cutover_smoke")
    validated = validate_strategy_config_dict_strict(strategy_config)

    assert selected_strategy.read_bytes() == selected_payload
    assert service.configuration_fingerprint(data_root) == selected_fingerprint
    assert (
        isolated_root
        / "market-timeseries"
        / runtime_name
        / "strategies/production/cutover_smoke.yaml"
    ).read_bytes() == (
        Path(__file__).resolve().parents[6]
        / "apps/bt/config/strategies/production/cutover_smoke.yaml"
    ).read_bytes()
    assert os.environ["TRADING25_STRATEGIES_DIR"] == (
        f"{runtime_name}/strategies"
    )
    assert validated.shared_config is not None
    assert validated.shared_config.data_source == "market"
