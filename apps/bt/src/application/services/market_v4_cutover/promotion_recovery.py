"""Focused Market v5 cutover responsibility module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    OperationResult,
    PromotionIdentityEvidence,
    RetainedPromotionPreparation,
    SmokeResult,
)
from .promotion_cleanup import PromotionCleanupService
from .promotion_evidence import PromotionEvidenceService
from .promotion_reports import PromotionReportService
from .workspace import CutoverWorkspace


class PromotionRecoveryService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        promotion_evidence: PromotionEvidenceService,
        promotion_reports: PromotionReportService,
        cleanup: PromotionCleanupService,
    ) -> None:
        self._workspace = workspace
        self._promotion_evidence = promotion_evidence
        self._promotion_reports = promotion_reports
        self._cleanup = cleanup

    def _validate_committed_promotion_recovery(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        base: PromotionIdentityEvidence,
        preparation: RetainedPromotionPreparation,
    ) -> OperationResult:
        report_path = self._workspace.operations_root / "reports" / report_id / "report.json"
        try:
            report = json.loads(
                self._workspace.managed().read_bytes(self._workspace._managed_relative(report_path))
            )
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion report is missing or invalid"
            ) from exc
        if not isinstance(report, dict) or set(report) != self._promotion_reports._PROMOTION_REPORT_KEYS:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion report contract is invalid"
            )
        quarantine_path = self._workspace.operations_root / "quarantine" / report_id
        expected_quarantine_value = self._workspace._managed_relative(quarantine_path).as_posix()
        if report.get("quarantinePath") != expected_quarantine_value:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion quarantine is invalid"
            )
        report_sha256 = self._workspace._sha256(report_path)
        if base.promotion_report_sha256 != report_sha256:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion report SHA mismatch"
            )
        active = self._promotion_evidence._market_location_identity(self._promotion_evidence._active_lease_fd_root())
        if not self._cleanup._location_matches(
            active,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        ):
            raise _managed_root.CutoverSafetyError(
                "Committed promotion active identity mismatch"
            )
        quarantine = self._promotion_reports._payload_location_identity(quarantine_path)
        if not self._cleanup._location_matches(
            quarantine,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        ):
            raise _managed_root.CutoverSafetyError(
                "Committed promotion quarantine identity mismatch"
            )
        try:
            semantic = cast(dict[str, object], report["semanticSmoke"])
            smoke_result = SmokeResult(
                schema_version=cast(int, semantic["schemaVersion"]),
                adjustment_mode=cast(str, semantic["stockPriceAdjustmentMode"]),
                checks=tuple(cast(list[str], semantic["checks"])),
                api_paths=tuple(cast(list[str], report["apiChecks"])),
                lineage=cast(dict[str, object], semantic["providerVintage"]),
            )
            expectation = self._promotion_reports._retained_promotion_report_expectation(
                operation_id=report_id,
                created_at=cast(str, report["createdAt"]),
                code_version=cast(str, report["codeVersion"]),
                preparation=preparation,
                base=base,
                active_location=active,
                active_after=active,
                quarantine=quarantine_path,
                quarantine_location=quarantine,
                smoke_result=smoke_result,
                verify_cleanup_staging=False,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion report contract is invalid"
            ) from exc
        if not self._promotion_reports._retained_promotion_report_contract_valid(
            report,
            expectation=expectation,
        ):
            raise _managed_root.CutoverSafetyError(
                "Committed promotion report contract is invalid"
            )
        consumed = report.get("sourceConsumed")
        if not isinstance(consumed, dict):
            raise _managed_root.CutoverSafetyError(
                "Committed promotion consumed evidence is invalid"
            )
        marker_path = self._workspace.operations_root / "consumed" / f"{retained_report_id}.json"
        expected_marker_value = self._workspace._managed_relative(marker_path).as_posix()
        if consumed.get("markerPath") != expected_marker_value:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion consumed marker is invalid"
            )
        try:
            marker_bytes = self._workspace.managed().read_bytes(
                self._workspace._managed_relative(marker_path)
            )
            marker = json.loads(marker_bytes)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion consumed marker is missing or invalid"
            ) from exc
        if marker != {
            "schemaVersion": 1,
            "retainedReportId": retained_report_id,
            "operationId": report_id,
            "promotionReportSha256": report_sha256,
        }:
            raise _managed_root.CutoverSafetyError(
                "Committed promotion consumed marker mismatch"
            )
        cleanup = report.get("runtimeCleanup")
        if not isinstance(cleanup, dict) or not (
            cleanup.get("cleanupStagingPath")
            == self._workspace._managed_relative(self._cleanup._cleanup_staging_root(report_id)).as_posix()
            and cleanup.get("cleanupControlPath")
            == self._workspace._managed_relative(self._cleanup._cleanup_control_path(report_id)).as_posix()
            and cleanup.get("cleanupResultPath")
            == self._workspace._managed_relative(self._cleanup._cleanup_result_path(report_id)).as_posix()
            and cleanup.get("cleanupDisposition") == "pending_post_commit"
            and cleanup.get("removedArtifacts") == []
        ):
            raise _managed_root.CutoverSafetyError(
                "Committed promotion cleanup evidence mismatch"
            )
        self._cleanup._complete_committed_promotion_cleanup(
            preparation,
            operation_id=report_id,
            report_sha256=report_sha256,
        )
        return OperationResult(
            report_id,
            report_path.relative_to(self._workspace.data_root).as_posix(),
        )

    def _validate_rolled_back_promotion_recovery(
        self,
        *,
        report_id: str,
        retained_root: Path,
        base: PromotionIdentityEvidence,
    ) -> None:
        active = self._promotion_evidence._market_location_identity(self._promotion_evidence._active_lease_fd_root())
        retained = self._promotion_reports._payload_location_identity(retained_root / "market-timeseries")
        if active != base.active_current or retained != base.retained_current:
            raise _managed_root.CutoverSafetyError(
                "Rolled-back promotion identity mismatch"
            )
        quarantine_path = self._workspace.operations_root / "quarantine" / report_id
        quarantine = self._cleanup._promotion_location_if_present(quarantine_path)
        if base.rollback_mode == "backup_restore":
            if (
                quarantine != base.quarantine_current
                or quarantine is None
                or quarantine["directory"] != base.active_before_directory
                or self._promotion_evidence._payload_manifest_entries(
                    cast(dict[str, object], active["payload"])
                )
                != self._promotion_evidence._payload_manifest_entries(base.active_before_payload)
            ):
                raise _managed_root.CutoverSafetyError(
                    "Rolled-back backup restore evidence mismatch"
                )
        elif base.rollback_mode == "atomic_exchange":
            if quarantine is not None or not self._cleanup._location_matches(
                active,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            ):
                raise _managed_root.CutoverSafetyError(
                    "Rolled-back atomic exchange evidence mismatch"
                )
        elif base.rollback_mode is not None:
            raise _managed_root.CutoverSafetyError(
                "Rolled-back promotion mode is invalid"
            )
        for artifact_root in (
            self._workspace.operations_root / "holding" / report_id,
            self._cleanup._cleanup_staging_root(report_id),
        ):
            try:
                self._workspace.managed().stat(self._workspace._managed_relative(artifact_root))
            except FileNotFoundError:
                continue
            raise _managed_root.CutoverSafetyError(
                "Rolled-back artifact staging still exists"
            )
