"""Market v4 cutover cutover contracts tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.errors import (
    RetainedMarketMutationError,
)
from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
    SmokeResult,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _write_report,
    _service,
    _retained_source,
    _read_operation_report,
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
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
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
        data_root / "operations/market-v4-cutover/reports/active-start-race/report.json"
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
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    swapped = False

    def swap_parent(stage: str) -> None:
        nonlocal swapped
        if stage != mutation or swapped:
            return
        swapped = True
        cutover_root = data_root / "operations/market-v4-cutover"
        detached = data_root / "operations/market-v4-cutover.detached"
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
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
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
    assert runtime.stop_calls == 3


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
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
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
            / "operations/market-v4-cutover/reports/passing-rehearsal/report.json"
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
        data_root / "operations/market-v4-cutover/staging" / f"active-{malformation}"
    ).exists()


@pytest.mark.parametrize(
    "malformation",
    [
        "missing_source_report_id",
        "empty_source_report_id",
        "missing_source_code_version",
        "empty_source_code_version",
        "missing_source_root_fingerprint",
        "empty_source_root_fingerprint",
        "missing_market_identity_before",
        "missing_market_identity_after",
        "changed_market_identity_after",
    ],
)
def test_cutover_rejects_retained_rehearsal_without_exact_source_evidence(
    tmp_path: Path,
    malformation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
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
            / "operations/market-v4-cutover/reports/passing-rehearsal/report.json"
        ).read_text()
    )
    report.update(
        {
            "rehearsalMode": "retained_market_smoke",
            "serverProcessJoined": True,
            "workerProcessJoined": True,
            "sourceRehearsalReportId": "passing-rehearsal",
            "sourceRehearsalCodeVersion": "deadbeef",
            "sourceRetainedRootFingerprint": "retained-root-fingerprint",
            "sourceMarketIdentityBefore": {"device": 1, "inode": 2},
            "sourceMarketIdentityAfter": {"device": 1, "inode": 2},
        }
    )
    field = {
        "missing_source_report_id": "sourceRehearsalReportId",
        "empty_source_report_id": "sourceRehearsalReportId",
        "missing_source_code_version": "sourceRehearsalCodeVersion",
        "empty_source_code_version": "sourceRehearsalCodeVersion",
        "missing_source_root_fingerprint": "sourceRetainedRootFingerprint",
        "empty_source_root_fingerprint": "sourceRetainedRootFingerprint",
        "missing_market_identity_before": "sourceMarketIdentityBefore",
        "missing_market_identity_after": "sourceMarketIdentityAfter",
        "changed_market_identity_after": "sourceMarketIdentityAfter",
    }[malformation]
    if malformation.startswith("missing_"):
        report.pop(field)
    elif malformation.startswith("empty_"):
        report[field] = ""
    else:
        report[field] = {"device": 1, "inode": 3}
    report_id = f"retained-{malformation}"
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
        data_root / "operations/market-v4-cutover/staging" / f"active-{malformation}"
    ).exists()


@pytest.mark.parametrize(
    "source_mutation",
    [
        "missing_report",
        "wrong_report_id",
        "wrong_phase",
        "wrong_status",
        "wrong_target_fingerprint",
        "unjoined_server",
        "unjoined_worker",
        "root_symlink",
        "root_fingerprint_drift",
    ],
)
def test_cutover_reresolves_retained_rehearsal_provenance_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )
    monkeypatch.setattr(
        service._runtime_smoke,
        "smoke",
        lambda *_args, **_kwargs: smoke_result,
    )
    service.rehearse_retained(
        "retained-cutover-evidence",
        source_rehearsal_report_id=source_id,
        config=config,
        inherited_environment={},
    )
    source_report_path = (
        data_root / "operations/market-v4-cutover/reports" / source_id / "report.json"
    )
    source_report = json.loads(source_report_path.read_text())
    if source_mutation == "missing_report":
        source_report_path.unlink()
    elif source_mutation == "wrong_report_id":
        source_report["reportId"] = "different-source"
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "wrong_phase":
        source_report["phase"] = "cutover"
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "wrong_status":
        source_report["status"] = "stop_failed_cleanup_deferred"
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "wrong_target_fingerprint":
        source_report["targetRootFingerprint"] = "0" * 64
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "unjoined_server":
        source_report["serverProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "unjoined_worker":
        source_report["workerProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "root_symlink":
        detached = tmp_path / "cutover-detached-retained"
        retained_root.rename(detached)
        retained_root.symlink_to(detached, target_is_directory=True)
    elif source_mutation == "root_fingerprint_drift":
        (retained_root / "config/default.yaml").write_text("drift: true\n")

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
            f"active-provenance-{source_mutation}",
            rehearsal_report_id="retained-cutover-evidence",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )

    assert backup_verified is False
    assert not (
        data_root
        / "operations/market-v4-cutover/staging"
        / f"active-provenance-{source_mutation}"
    ).exists()


@pytest.mark.parametrize(
    "evidence_mutation",
    [
        "missing_api_checks",
        "malformed_schema_coverage",
        "missing_retained_phase",
        "forged_equal_identity",
        "post_report_market_replacement",
    ],
)
def test_cutover_rejects_inexact_retained_evidence_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence_mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "retained-exact-evidence",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    report_path = (
        data_root
        / "operations/market-v4-cutover/reports/retained-exact-evidence/report.json"
    )
    report = json.loads(report_path.read_text())
    if evidence_mutation == "missing_api_checks":
        report.pop("apiChecks")
    elif evidence_mutation == "malformed_schema_coverage":
        report["schemaCoverage"] = {
            "schemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
            "adjustedMetrics": {"readyBasisCount": 0},
        }
    elif evidence_mutation == "missing_retained_phase":
        report["phases"] = []
    elif evidence_mutation == "forged_equal_identity":
        report["sourceMarketIdentityBefore"] = {"forged": True}
        report["sourceMarketIdentityAfter"] = {"forged": True}
    elif evidence_mutation == "post_report_market_replacement":
        market_db = retained_root / "market-timeseries/market.duckdb"
        market_db.write_bytes(market_db.read_bytes() + b"replaced")
    if evidence_mutation != "post_report_market_replacement":
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
            f"active-inexact-{evidence_mutation}",
            rehearsal_report_id="retained-exact-evidence",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )
    assert backup_verified is False


def test_cutover_rejects_retained_evidence_without_screening_status_poll_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "retained-without-screening-poll",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    report_path = data_root / (
        "operations/market-v4-cutover/reports/"
        "retained-without-screening-poll/report.json"
    )
    report = json.loads(report_path.read_text())
    report["apiChecks"] = [
        path
        for path in report["apiChecks"]
        if path != "/api/analytics/screening/jobs/screen-1"
    ]
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
            "active-without-screening-poll",
            rehearsal_report_id="retained-without-screening-poll",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )

    assert backup_verified is False


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_sync_status",
        "missing_sync_phase",
        "missing_semantic_phase",
        "malformed_schema_coverage",
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
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse("full-exact-evidence", config, inherited_environment={})
    report_path = data_root / (
        "operations/market-v4-cutover/reports/full-exact-evidence/report.json"
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
            if phase["name"] != "initial_sync_and_adjusted_metrics_pit"
        ]
    elif mutation == "missing_semantic_phase":
        report["phases"] = [
            phase for phase in report["phases"] if phase["name"] != "semantic_smoke"
        ]
    else:
        report["schemaCoverage"] = {"schemaVersion": 4}
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


def test_cutover_accepts_exact_retained_evidence(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service.backup("retained-exact-backup")
    service.rehearse_retained(
        "retained-exact-cutover",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )

    result = service.cutover(
        "active-from-retained-exact",
        rehearsal_report_id="retained-exact-cutover",
        backup_id="retained-exact-backup",
        config=config,
        inherited_environment={},
    )

    assert _read_operation_report(data_root, result.report_id)["status"] == "passed"


def test_rehearse_retained_mutation_failure_preserves_completed_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    original_smoke = service._runtime_smoke.smoke

    def mutate_after_real_smoke(*args: object, **kwargs: object) -> SmokeResult:
        result = original_smoke(*args, **kwargs)
        market_db = retained_root / "market-timeseries/market.duckdb"
        market_db.write_bytes(market_db.read_bytes() + b"changed")
        return result

    monkeypatch.setattr(
        service._runtime_smoke,
        "smoke",
        mutate_after_real_smoke,
    )
    with pytest.raises(CutoverSafetyError) as captured:
        service.rehearse_retained(
            "retained-preserved-failure-evidence",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )
    assert isinstance(captured.value.__cause__, RetainedMarketMutationError)
    report = _read_operation_report(data_root, "retained-preserved-failure-evidence")
    assert report["apiChecks"]
    assert report["phases"][0]["name"] == "retained_market_smoke"


def test_retained_runbook_enumerates_all_forbidden_mutations() -> None:
    runbook = (
        Path(__file__).resolve().parents[6] / "docs/runbooks/market-v4-cutover.md"
    ).read_text()
    for operation in (
        "sync",
        "reset",
        "repair",
        "stock refresh",
        "intraday sync",
        "adjusted-metric materialization",
    ):
        assert operation in runbook
