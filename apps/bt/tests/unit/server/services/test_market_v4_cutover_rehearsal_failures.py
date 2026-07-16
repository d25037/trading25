"""Market v4 cutover rehearsal failures tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    SmokeConfig,
)
from src.application.services.market_v4_cutover.errors import (
    RuntimeStopError,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import market_operation_lease
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
    FakeApi,
    _market_root,
    _service,
    _changing_code_version,
)


def test_rehearsal_failure_report_keeps_start_identity_and_original_error(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class OriginalRebuildError(RuntimeError):
        pass

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise OriginalRebuildError("injected rebuild failure")

    runtime = FakeRuntime(apis=[FailingApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    code_version, _calls = _changing_code_version("deadbeef", "deadbeef-dirty")
    service.code_version = code_version

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-original-error",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-original-error/report.json"
        ).read_text()
    )
    assert runtime.stop_calls == 1
    assert report["status"] == "failed"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "OriginalRebuildError"


@pytest.mark.parametrize(
    ("process_joined", "expected_status"),
    [
        (False, "stop_failed_cleanup_deferred"),
        (True, "failed"),
    ],
)
def test_rehearsal_cleanup_join_verdict_preserves_primary_error(
    tmp_path: Path,
    process_joined: bool,
    expected_status: str,
) -> None:
    data_root = _market_root(tmp_path)

    class OriginalRebuildError(RuntimeError):
        pass

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise OriginalRebuildError("injected rebuild failure")

    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise RuntimeStopError(
                "owned rehearsal process stop result",
                process_joined=process_joined,
            )

    runtime = UnjoinedRuntime(apis=[FailingApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-unjoined-cleanup",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-unjoined-cleanup/report.json"
        ).read_text()
    )
    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 1
    assert report["status"] == expected_status
    assert report["errorType"] == "OriginalRebuildError"
    assert report["stopErrorType"] == "RuntimeStopError"
    assert report["serverProcessJoined"] is process_joined
    assert report["codeVersion"] == "deadbeef"


def test_rehearsal_cancel_failure_is_diagnostic_when_stop_proves_join(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class CancelFailingRuntime(FakeRuntime):
        def cancel_owned_work(self, _api: FakeApi) -> None:
            self.cancel_calls += 1
            raise OSError("injected cancel failure")

    runtime = CancelFailingRuntime(apis=[FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-cancel-diagnostic",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-cancel-diagnostic/report.json"
        ).read_text()
    )
    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 1
    assert report["status"] == "failed"
    assert report["errorType"] == "CutoverSafetyError"
    assert report["cleanupErrorType"] == "OSError"
    assert "stopErrorType" not in report
    assert report["serverProcessJoined"] is True


def test_rehearsal_report_preserves_bounded_redacted_terminal_job_diagnostic(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    secret = "jquants-secret-value"

    class FailedSyncApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            if path == "/api/db/sync":
                return {"jobId": "sync-1", "status": "pending"}
            if path == "/api/db/sync/jobs/sync-1":
                return {
                    "jobId": "sync-1",
                    "status": "failed",
                    "progress": {
                        "stage": "stock_data",
                        "message": f"bulk plan unavailable under {data_root}",
                    },
                    "result": {
                        "errors": ["bulk coverage missing", "no REST fallback"],
                    },
                    "error": f"BulkFetchRequiredError token={secret} " + ("x" * 4_000),
                }
            return super().request(method, path, payload)

    service = _service(data_root, runtime=FakeRuntime(apis=[FailedSyncApi()]))

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-job-diagnostic",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={"JQUANTS_API_KEY": secret},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-job-diagnostic/report.json"
        ).read_text()
    )
    diagnostic = report["errorMessage"]
    assert "sync job ended with status failed" in diagnostic
    assert "stage=stock_data" in diagnostic
    assert "bulk plan unavailable" in diagnostic
    assert "BulkFetchRequiredError" in diagnostic
    assert "bulk coverage missing" in diagnostic
    assert secret not in diagnostic
    assert str(data_root) not in diagnostic
    assert "<redacted-secret>" in diagnostic
    assert "<data-root>" in diagnostic
    assert len(diagnostic) <= 1_024


@pytest.mark.parametrize(
    ("process_joined", "expected_status"),
    [
        (False, "stop_failed_cleanup_deferred"),
        (True, "failed"),
    ],
)
def test_rehearsal_startup_error_uses_embedded_join_verdict(
    tmp_path: Path,
    process_joined: bool,
    expected_status: str,
) -> None:
    data_root = _market_root(tmp_path)

    class StartupFailingRuntime(FakeRuntime):
        retained_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            inherited_fd = os.dup(int(kwargs["lease_fd"]))
            if process_joined:
                os.close(inherited_fd)
            else:
                self.retained_lease_fd = inherited_fd
            raise RuntimeStopError(
                "injected startup join verdict",
                process_joined=process_joined,
            )

    runtime = StartupFailingRuntime()
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            f"rehearsal-startup-joined-{process_joined}",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports"
            / f"rehearsal-startup-joined-{process_joined}/report.json"
        ).read_text()
    )
    assert report["status"] == expected_status
    assert report["errorType"] == "RuntimeStopError"
    assert report["stopErrorType"] == "RuntimeStopError"
    assert report["serverProcessJoined"] is process_joined

    rehearsal_root = (
        data_root
        / "operations/market-v4-cutover/rehearsals"
        / f"rehearsal-startup-joined-{process_joined}/root"
    )
    if not process_joined:
        try:
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_operation_lease.MarketOperationLease.acquire(
                    rehearsal_root,
                    exclusive=False,
                )
        finally:
            os.close(runtime.retained_lease_fd)

    with market_operation_lease.MarketOperationLease.acquire(
        rehearsal_root,
        exclusive=True,
    ):
        pass


def test_rehearsal_identity_drift_cannot_publish_passed_report(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    code_version, _calls = _changing_code_version("deadbeef", "deadbeef-dirty")
    service.code_version = code_version

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-code-drift",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-code-drift/report.json"
        ).read_text()
    )
    assert report["status"] == "failed"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "CutoverSafetyError"


def test_rehearsal_rejects_concurrent_strategy_edit_and_stale_report(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("mode: market\n")
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.parent.mkdir(parents=True)
    strategy.write_text("value: before\n")

    class EditingRuntime(FakeRuntime):
        def stop(self, api: FakeApi) -> None:
            super().stop(api)
            strategy.write_text("value: after\n")

    runtime = EditingRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    original_fingerprint = service.root_fingerprint(data_root)

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "raced-rehearsal",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/raced-rehearsal/report.json"
        ).read_text()
    )
    assert report["status"] == "failed"
    assert report["targetRootFingerprint"] == original_fingerprint
    with pytest.raises(CutoverSafetyError, match="passing rehearsal report"):
        service.cutover(
            "active-from-stale",
            rehearsal_report_id="raced-rehearsal",
            backup_id="unused",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )


@pytest.mark.parametrize("failure_stage", ["after_temp_fsync", "after_publish"])
def test_rehearsal_report_publish_failure_never_leaves_passed_evidence(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    failed_once = False

    def inject(stage: str) -> None:
        nonlocal failed_once
        if stage == failure_stage and not failed_once:
            failed_once = True
            raise OSError(f"injected {stage}")

    service._report_publish_hook = inject
    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            f"report-{failure_stage}",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report_path = (
        data_root
        / f"operations/market-v4-cutover/reports/report-{failure_stage}/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"
    assert not list(report_path.parent.glob(".report.json.*.tmp"))


@pytest.mark.parametrize("failure_stage", ["after_temp_fsync", "after_publish"])
def test_active_report_publish_failure_restores_without_passed_evidence(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("before-report-failure")
    service.rehearse(
        "passing-before-report-failure",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    failed_once = False

    def inject(stage: str) -> None:
        nonlocal failed_once
        if stage == failure_stage and not failed_once:
            failed_once = True
            raise OSError(f"injected {stage}")

    service._report_publish_hook = inject
    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            f"active-{failure_stage}",
            rehearsal_report_id="passing-before-report-failure",
            backup_id="before-report-failure",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"
    report_path = (
        data_root
        / f"operations/market-v4-cutover/reports/active-{failure_stage}/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"
    assert not list(report_path.parent.glob(".report.json.*.tmp"))
