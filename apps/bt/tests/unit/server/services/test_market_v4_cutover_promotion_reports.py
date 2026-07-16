"""Market v4 cutover promotion reports tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.application.services.market_v4_cutover.contracts import (
    PromotionState,
    RetainedPromotionPreparation,
    RetainedPromotionReportExpectation,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeRuntime,
    FakeApi,
    _market_root,
    _retained_promotion_source,
    _TestAtomicExchange,
    _run_retained_promotion,
)


def test_promote_retained_report_contract_is_exact_and_strict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    captured_expectations: list[RetainedPromotionReportExpectation] = []
    original_builder = service._build_retained_promotion_report

    def capture_expectation(
        expectation: RetainedPromotionReportExpectation,
    ) -> dict[str, object]:
        captured_expectations.append(expectation)
        return original_builder(expectation)

    monkeypatch.setattr(
        service,
        "_build_retained_promotion_report",
        capture_expectation,
    )

    result, _states = _run_retained_promotion(service, config)
    report = json.loads((data_root / result.report_path).read_text())
    assert len(captured_expectations) >= 1
    expectation = captured_expectations[-1]

    assert report["activationMode"] == "retained_atomic_exchange"
    assert report["reportId"] == "market-v4-active-20260716"
    assert report["codeVersion"] == "deadbeef"
    assert report["retainedReport"]["reportId"] == "market-v4-retained-20260715-r13"
    assert report["sourceReport"]["reportId"] == "market-v4-rehearsal-20260715-r10"
    for evidence in (report["retainedReport"], report["sourceReport"]):
        assert set(evidence) == {"reportId", "codeVersion", "reportSha256"}
        assert len(evidence["reportSha256"]) == 64
    assert set(report["payloadIdentities"]) == {
        "activeBefore",
        "backup",
        "retainedSource",
        "activated",
        "activeAfter",
    }
    backup_payload = report["payloadIdentities"]["backup"]
    active_before_payload = report["payloadIdentities"]["activeBefore"]
    assert service._payload_manifest_entries(backup_payload) == (
        service._payload_manifest_entries(active_before_payload)
    )
    assert backup_payload != active_before_payload
    assert report["backupEvidence"]["contentEquivalentToActiveBefore"] is True
    assert report["backupEvidence"]["physicalIdentityDistinct"] is True
    assert (
        report["payloadIdentities"]["retainedSource"]
        == report["payloadIdentities"]["activated"]
        == report["payloadIdentities"]["activeAfter"]
    )
    assert report["filesystemEvidence"]["sameDevice"] is True
    assert report["filesystemEvidence"]["atomicExchange"] is True
    assert report["journal"] == {
        "operationId": "market-v4-active-20260716",
        "finalState": "committed",
    }
    assert report["backupId"] == "market-v3-pre-v4-20260716"
    assert report["quarantinePath"].endswith("/quarantine/market-v4-active-20260716")
    assert report["runtimeCleanup"]["activeRuntimeRemoved"] is True
    assert report["runtimeCleanup"]["removedArtifacts"] == []
    assert report["runtimeCleanup"]["cleanupDisposition"] == "pending_post_commit"
    assert report["runtimeCleanup"]["cleanupStagingPath"].endswith(
        "/cleanup-staging/market-v4-active-20260716"
    )
    assert report["runtimeCleanup"]["cleanupResultPath"].endswith(
        "/cleanup-results/market-v4-active-20260716.json"
    )
    assert (data_root / report["runtimeCleanup"]["cleanupResultPath"]).is_file()
    assert (
        report["runtimeCleanup"]["holdingDirectory"]
        == (expectation.runtime_cleanup["holdingDirectory"])
    )
    assert report["noSync"] is True
    assert report["noJQuants"] is True
    assert report["serverProcessJoined"] is True
    assert report["workerProcessJoined"] is True
    assert report["sourceConsumed"]["retainedReportId"] == (
        "market-v4-retained-20260715-r13"
    )
    marker = data_root / report["sourceConsumed"]["markerPath"]
    assert marker.is_file()
    with managed_root.ManagedRootFd.open(data_root) as managed:
        recovered_journal = PromotionJournal(
            managed,
            "market-v4-active-20260716",
            now=service.now,
        )
        recovered_journal.recover(recovered_journal.recovery_attempt_id())
        committed = recovered_journal.read_validated()[-1]
    assert committed.state is PromotionState.COMMITTED
    assert committed.identities.promotion_report_sha256 == service._sha256(
        data_root / result.report_path
    )
    assert report["rollbackInstructions"]
    assert not service._retained_promotion_report_contract_valid(report)
    assert service._retained_promotion_report_contract_valid(
        report,
        expectation=expectation,
    )

    missing = json.loads(json.dumps(report))
    missing.pop("noSync")
    extra = json.loads(json.dumps(report))
    extra["compatibility"] = True
    mismatch = json.loads(json.dumps(report))
    mismatch["activationMode"] = "copy"
    extra_api = json.loads(json.dumps(report))
    extra_api["apiChecks"].append("/api/db/sync")
    nested_extra = json.loads(json.dumps(report))
    nested_extra["retainedReport"]["compatibility"] = True
    directory_mismatch = json.loads(json.dumps(report))
    directory_mismatch["filesystemEvidence"]["activeAfterDirectory"]["inode"] += 1
    missing_semantic_check = json.loads(json.dumps(report))
    missing_semantic_check["semanticSmoke"]["checks"].pop()
    empty_lineage = json.loads(json.dumps(report))
    empty_lineage["semanticSmoke"]["adjustedMetrics"] = {}
    retained_sha_mismatch = json.loads(json.dumps(report))
    retained_sha_mismatch["retainedReport"]["reportSha256"] = "0" * 64
    source_code_mismatch = json.loads(json.dumps(report))
    source_code_mismatch["sourceReport"]["codeVersion"] = "invented-code-version"
    backup_inode_mismatch = json.loads(json.dumps(report))
    backup_inode_mismatch["payloadIdentities"]["backup"]["marketDuckdb"]["inode"] += 1
    backup_evidence_mismatch = json.loads(json.dumps(report))
    backup_evidence_mismatch["backupEvidence"]["physicalIdentityDistinct"] = False
    artifact_inode_mismatch = json.loads(json.dumps(report))
    artifact_inode_mismatch["runtimeCleanup"]["detachedArtifacts"][0]["identity"][
        "inode"
    ] += 1
    artifact_name_mismatch = json.loads(json.dumps(report))
    artifact_name_mismatch["runtimeCleanup"]["detachedArtifacts"][0]["name"] = (
        "invented-runtime"
    )
    quarantine_mismatch = json.loads(json.dumps(report))
    quarantine_mismatch["quarantinePath"] += "-other"
    journal_mismatch = json.loads(json.dumps(report))
    journal_mismatch["journal"]["finalState"] = "report_persisted"
    for mutation_index, candidate in enumerate(
        (
            missing,
            extra,
            mismatch,
            extra_api,
            nested_extra,
            directory_mismatch,
            missing_semantic_check,
            empty_lineage,
            retained_sha_mismatch,
            source_code_mismatch,
            backup_inode_mismatch,
            backup_evidence_mismatch,
            artifact_inode_mismatch,
            artifact_name_mismatch,
            quarantine_mismatch,
            journal_mismatch,
        )
    ):
        assert not service._retained_promotion_report_contract_valid(
            candidate,
            expectation=expectation,
        ), mutation_index


def test_promote_retained_rejects_same_byte_backup_inode_swap_during_report_publish(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    backup_database = data_root / (
        "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716/"
        "payload/market.duckdb"
    )
    original_identity: tuple[int, int] | None = None

    def replace_with_same_bytes(stage: str) -> None:
        nonlocal original_identity
        if stage != "after_temp_fsync" or original_identity is not None:
            return
        original_stat = backup_database.stat()
        original_identity = (original_stat.st_dev, original_stat.st_ino)
        replacement = backup_database.with_name("market.duckdb.replacement")
        payload_directory = backup_database.parent
        directory_mode = payload_directory.stat().st_mode
        payload_directory.chmod(0o700)
        try:
            replacement.write_bytes(backup_database.read_bytes())
            replacement.chmod(original_stat.st_mode)
            replacement.replace(backup_database)
        finally:
            payload_directory.chmod(directory_mode)

    service._report_publish_hook = replace_with_same_bytes

    with pytest.raises(CutoverSafetyError, match="backup physical identity changed"):
        _run_retained_promotion(service, config)

    assert original_identity is not None
    assert (backup_database.stat().st_dev, backup_database.stat().st_ino) != (
        original_identity
    )
    assert not (
        data_root
        / "operations/market-v4-cutover/reports/market-v4-active-20260716/report.json"
    ).exists()


@pytest.mark.parametrize("mutation", ["replacement", "missing", "extra"])
def test_promote_retained_rejects_held_artifact_identity_drift_before_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    original_cleanup = service._complete_committed_promotion_cleanup
    observed: dict[str, Path] = {}

    def mutate_then_cleanup(
        preparation: RetainedPromotionPreparation,
        *,
        operation_id: str,
        report_sha256: str,
    ) -> None:
        root = service._cleanup_staging_root(operation_id)
        names = tuple(artifact.name for artifact in preparation.detached_artifacts)
        target_name = preparation.detached_runtime_names[0]
        target = root / target_name
        if mutation == "replacement":
            original = root / f"{target_name}.original"
            target.rename(original)
            target.mkdir()
            (target / "replacement").write_text("foreign")
            observed["original"] = original
            observed["replacement"] = target
        elif mutation == "missing":
            moved = root.parent / f"{target_name}.moved"
            target.rename(moved)
            observed["moved"] = moved
        else:
            foreign = root / "foreign"
            foreign.write_text("foreign")
            observed["foreign"] = foreign
        for name in names:
            if name != target_name or mutation == "extra":
                assert (root / name).exists()
        original_cleanup(
            preparation,
            operation_id=operation_id,
            report_sha256=report_sha256,
        )

    monkeypatch.setattr(
        service, "_complete_committed_promotion_cleanup", mutate_then_cleanup
    )

    with pytest.raises(CutoverSafetyError, match="cleanup incomplete"):
        _run_retained_promotion(service, config)

    assert observed
    assert all(path.exists() for path in observed.values())
