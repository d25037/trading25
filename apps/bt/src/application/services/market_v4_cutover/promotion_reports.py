"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import os
from pathlib import Path
import re
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    PromotionIdentityEvidence,
    PromotionState,
    RetainedPromotionPreparation,
    RetainedPromotionReportExpectation,
    SmokeResult,
)
from .journal_validation import JournalValidator
from .duckdb_service import MarketIdentityService
from .promotion_evidence import PromotionEvidenceService
from .promotion_eligibility import PromotionEligibilityService
from .smoke import RuntimeSmokeService
from .workspace import CutoverWorkspace


class PromotionReportService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        market_identity: MarketIdentityService,
        promotion_evidence: PromotionEvidenceService,
        eligibility: PromotionEligibilityService,
        runtime_smoke: RuntimeSmokeService,
    ) -> None:
        self._workspace = workspace
        self._market_identity = market_identity
        self._promotion_evidence = promotion_evidence
        self._eligibility = eligibility
        self._runtime_smoke = runtime_smoke

    _PROMOTION_REPORT_KEYS = frozenset(
        {
            "schemaVersion",
            "reportId",
            "phase",
            "status",
            "activationMode",
            "createdAt",
            "codeVersion",
            "retainedReport",
            "sourceReport",
            "fingerprints",
            "payloadIdentities",
            "filesystemEvidence",
            "backupId",
            "backupManifestSha256",
            "backupFileSetSha256",
            "backupEvidence",
            "journal",
            "quarantinePath",
            "runtimeCleanup",
            "noSync",
            "noJQuants",
            "apiChecks",
            "serverProcessJoined",
            "workerProcessJoined",
            "semanticSmoke",
            "sourceConsumed",
            "rollbackInstructions",
        }
    )
    _PROMOTION_ENVIRONMENT_ALLOWLIST = frozenset(
        {"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "PYTHONPATH"}
    )
    _PROMOTION_CREDENTIAL_KEY_TOKENS = (
        "JQUANTS",
        "KEY",
        "TOKEN",
        "SECRET",
        "CREDENTIAL",
        "PLAN",
    )
    _PROMOTION_SMOKE_CHECKS = (
        "market_metadata",
        "adjusted_metrics_lineage",
        "fundamentals_parity",
        "screening",
        "fundamental_ranking",
        "dataset_create_info_open",
    )
    _PROMOTION_ZERO_LINEAGE_KEYS = (
        "invalidProviderWindowCount",
        "invalidAdjustmentEventCount",
        "providerAdjustedMismatchCount",
        "invalidCurrentBasisStateCount",
        "pendingCurrentBasisCodeCount",
        "missingAdjustedStatementRows",
        "extraAdjustedStatementRows",
        "staleAdjustedStatementRows",
        "wrongBasisAdjustedStatementRows",
        "orphanAdjustedStatementRows",
    )
    _PROMOTION_POSITIVE_LINEAGE_KEYS = (
        "sourceStatementKeyCount",
        "expectedAdjustedStatementRows",
        "currentBasisStatementCount",
        "currentBasisStateCount",
        "providerWindowCount",
        "readyProviderWindowCount",
    )

    def _retained_promotion_report_contract_valid(
        self,
        report: object,
        *,
        expectation: RetainedPromotionReportExpectation | None = None,
    ) -> bool:
        if not isinstance(report, dict) or set(report) != self._PROMOTION_REPORT_KEYS:
            return False
        if expectation is None or report != expectation.to_report():
            return False

        mappings = self._promotion_report_mappings(report)
        if mappings is None:
            return False
        retained = mappings["retained"]
        source = mappings["source"]
        fingerprints = mappings["fingerprints"]
        payloads = mappings["payloads"]
        filesystem = mappings["filesystem"]
        semantic = mappings["semantic"]
        lineage = semantic["providerVintage"]
        exact_lineage = bool(
            isinstance(lineage, dict)
            and set(lineage)
            == set(self._PROMOTION_ZERO_LINEAGE_KEYS)
            | set(self._PROMOTION_POSITIVE_LINEAGE_KEYS)
            and all(lineage.get(key) == 0 for key in self._PROMOTION_ZERO_LINEAGE_KEYS)
            and all(
                type(lineage.get(key)) is int and cast(int, lineage[key]) > 0
                for key in self._PROMOTION_POSITIVE_LINEAGE_KEYS
            )
        )
        sha_values = (
            retained["reportSha256"],
            source["reportSha256"],
            report["backupManifestSha256"],
            report["backupFileSetSha256"],
            fingerprints["targetRoot"],
            fingerprints["retainedRoot"],
            fingerprints["configuration"],
        )
        identity_values = (
            payloads["activeBefore"],
            payloads["backup"],
            payloads["retainedSource"],
            payloads["activated"],
            payloads["activeAfter"],
        )
        directory_values = (
            filesystem["activeBeforeDirectory"],
            filesystem["retainedSourceDirectory"],
            filesystem["activatedDirectory"],
            filesystem["activeAfterDirectory"],
            filesystem["quarantineDirectory"],
        )
        return self._promotion_report_values_valid(
            report=report,
            mappings=mappings,
            exact_lineage=exact_lineage,
            sha_values=sha_values,
            identity_values=identity_values,
            directory_values=directory_values,
            exact_api_checks=self._promotion_api_checks_valid(report),
        )

    @staticmethod
    def _promotion_report_mappings(
        report: dict[str, object],
    ) -> dict[str, dict[str, object]] | None:
        def exact(value: object, keys: set[str]) -> dict[str, object] | None:
            return value if isinstance(value, dict) and set(value) == keys else None

        specifications = {
            "retained": ("retainedReport", {"reportId", "codeVersion", "reportSha256"}),
            "source": ("sourceReport", {"reportId", "codeVersion", "reportSha256"}),
            "fingerprints": (
                "fingerprints",
                {"targetRoot", "retainedRoot", "configuration"},
            ),
            "payloads": (
                "payloadIdentities",
                {
                    "activeBefore",
                    "backup",
                    "retainedSource",
                    "activated",
                    "activeAfter",
                },
            ),
            "filesystem": (
                "filesystemEvidence",
                {
                    "sameDevice",
                    "atomicExchange",
                    "activeBeforeDirectory",
                    "retainedSourceDirectory",
                    "activatedDirectory",
                    "activeAfterDirectory",
                    "quarantineDirectory",
                },
            ),
            "journal": ("journal", {"operationId", "finalState"}),
            "cleanup": (
                "runtimeCleanup",
                {
                    "holdingDirectory",
                    "detachedRuntimeNames",
                    "detachedArtifacts",
                    "removedArtifacts",
                    "cleanupStagingPath",
                    "cleanupControlPath",
                    "cleanupResultPath",
                    "cleanupDisposition",
                    "activeRuntime",
                    "activeRuntimeRemoved",
                },
            ),
            "backup_evidence": (
                "backupEvidence",
                {
                    "manifestSha256",
                    "fileSetSha256",
                    "contentEquivalentToActiveBefore",
                    "physicalIdentityDistinct",
                },
            ),
            "semantic": (
                "semanticSmoke",
                {
                    "schemaVersion",
                    "stockPriceAdjustmentMode",
                    "checks",
                    "providerVintage",
                },
            ),
            "consumed": (
                "sourceConsumed",
                {"retainedReportId", "markerPath"},
            ),
        }
        result = {
            name: exact(report[field], keys)
            for name, (field, keys) in specifications.items()
        }
        if any(value is None for value in result.values()):
            return None
        return cast(dict[str, dict[str, object]], result)

    @staticmethod
    def _promotion_api_checks_valid(report: dict[str, object]) -> bool:
        api_checks = report["apiChecks"]
        report_id = report["reportId"]
        if (
            not isinstance(report_id, str)
            or not isinstance(api_checks, list)
            or len(api_checks) != 12
            or not all(isinstance(path, str) for path in api_checks)
        ):
            return False
        screening_job = cast(str, api_checks[5]).removeprefix(
            "/api/analytics/screening/jobs/"
        )
        dataset_job = cast(str, api_checks[9]).removeprefix("/api/dataset/jobs/")
        dataset_name = f"cutover-smoke-{report_id.replace('.', '-')}-active"
        return bool(
            api_checks[0] == "/api/db/stats"
            and api_checks[1] == "/api/db/validate"
            and cast(str, api_checks[2]).startswith("/api/analytics/fundamentals/")
            and cast(str, api_checks[2]) != "/api/analytics/fundamentals/"
            and api_checks[3] == "/api/fundamentals/compute"
            and api_checks[4] == "/api/analytics/screening/jobs"
            and bool(screening_job)
            and api_checks[5] == f"/api/analytics/screening/jobs/{screening_job}"
            and api_checks[6] == f"/api/analytics/screening/result/{screening_job}"
            and api_checks[7] == "/api/analytics/fundamental-ranking"
            and api_checks[8] == "/api/dataset"
            and bool(dataset_job)
            and api_checks[9] == f"/api/dataset/jobs/{dataset_job}"
            and api_checks[10] == f"/api/dataset/{dataset_name}/info"
            and api_checks[11] == f"/api/dataset/{dataset_name}/sample?count=1"
        )

    def _promotion_report_values_valid(
        self,
        *,
        report: dict[str, object],
        mappings: dict[str, dict[str, object]],
        exact_lineage: bool,
        sha_values: tuple[object, ...],
        identity_values: tuple[object, ...],
        directory_values: tuple[object, ...],
        exact_api_checks: bool,
    ) -> bool:
        retained = mappings["retained"]
        source = mappings["source"]
        payloads = mappings["payloads"]
        filesystem = mappings["filesystem"]
        journal = mappings["journal"]
        cleanup = mappings["cleanup"]
        backup_evidence = mappings["backup_evidence"]
        semantic = mappings["semantic"]
        consumed = mappings["consumed"]
        return bool(
            report["schemaVersion"] == 1
            and report["phase"] == "promotion"
            and report["status"] == "passed"
            and report["activationMode"] == "retained_atomic_exchange"
            and isinstance(report["reportId"], str)
            and isinstance(report["createdAt"], str)
            and isinstance(report["codeVersion"], str)
            and all(
                isinstance(value, str) and bool(value)
                for value in (
                    retained["reportId"],
                    retained["codeVersion"],
                    source["reportId"],
                    source["codeVersion"],
                    report["backupId"],
                    report["quarantinePath"],
                    report["rollbackInstructions"],
                    consumed["retainedReportId"],
                    consumed["markerPath"],
                )
            )
            and all(
                isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in sha_values
            )
            and all(JournalValidator._payload_valid(value) for value in identity_values)
            and all(
                JournalValidator._directory_valid(value) for value in directory_values
            )
            and self._promotion_evidence._payload_manifest_entries(
                cast(dict[str, object], payloads["activeBefore"])
            )
            == self._promotion_evidence._payload_manifest_entries(
                cast(dict[str, object], payloads["backup"])
            )
            and self._promotion_evidence._payload_physical_identity_distinct(
                cast(dict[str, object], payloads["activeBefore"]),
                cast(dict[str, object], payloads["backup"]),
            )
            and payloads["retainedSource"]
            == payloads["activated"]
            == payloads["activeAfter"]
            and filesystem["sameDevice"] is True
            and filesystem["atomicExchange"] is True
            and filesystem["retainedSourceDirectory"]
            == filesystem["activatedDirectory"]
            == filesystem["activeAfterDirectory"]
            and filesystem["activeBeforeDirectory"] == filesystem["quarantineDirectory"]
            and len(
                {
                    cast(dict[str, int], directory)["device"]
                    for directory in directory_values
                }
            )
            == 1
            and journal["operationId"] == report["reportId"]
            and journal["finalState"] == PromotionState.COMMITTED.value
            and JournalValidator._directory_valid(cleanup["holdingDirectory"])
            and isinstance(cleanup["detachedRuntimeNames"], list)
            and isinstance(cleanup["detachedArtifacts"], list)
            and isinstance(cleanup["removedArtifacts"], list)
            and cleanup["removedArtifacts"] == []
            and isinstance(cleanup["cleanupStagingPath"], str)
            and isinstance(cleanup["cleanupControlPath"], str)
            and isinstance(cleanup["cleanupResultPath"], str)
            and cleanup["cleanupDisposition"] == "pending_post_commit"
            and all(
                isinstance(name, str) and bool(name)
                for name in cleanup["detachedRuntimeNames"]
            )
            and isinstance(cleanup["activeRuntime"], str)
            and cleanup["activeRuntimeRemoved"] is True
            and report["noSync"] is True
            and report["noJQuants"] is True
            and backup_evidence["manifestSha256"] == report["backupManifestSha256"]
            and backup_evidence["fileSetSha256"] == report["backupFileSetSha256"]
            and backup_evidence["contentEquivalentToActiveBefore"] is True
            and backup_evidence["physicalIdentityDistinct"] is True
            and exact_api_checks
            and report["serverProcessJoined"] is True
            and report["workerProcessJoined"] is True
            and semantic["schemaVersion"] == 5
            and semantic["stockPriceAdjustmentMode"] == "provider_adjusted_v1"
            and semantic["checks"] == list(self._PROMOTION_SMOKE_CHECKS)
            and exact_lineage
            and consumed["retainedReportId"] == retained["reportId"]
            and consumed["markerPath"]
            == (f"operations/market-v4-cutover/consumed/{retained['reportId']}.json")
            and report["quarantinePath"]
            == (f"operations/market-v4-cutover/quarantine/{report['reportId']}")
        )

    def _promotion_runtime_environment(
        self,
        inherited: dict[str, str],
        *,
        lease_fd: int,
        root_fd: int,
        runtime_name: str,
    ) -> dict[str, str]:
        allowed = {
            key: value
            for key, value in inherited.items()
            if key in self._PROMOTION_ENVIRONMENT_ALLOWLIST
            and not any(
                token in key.upper()
                for token in self._PROMOTION_CREDENTIAL_KEY_TOKENS
            )
        }
        environment = self._runtime_smoke.isolated_environment(
            allowed,
            lease_fd=lease_fd,
            root_fd=root_fd,
            runtime_name=runtime_name,
        )
        environment["TRADING25_RUNTIME_CAPABILITY"] = "retained_market_smoke"
        if any(
            token in key.upper()
            for key in environment
            for token in self._PROMOTION_CREDENTIAL_KEY_TOKENS
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion runtime environment contains a credential capability"
            )
        return environment

    def _build_retained_promotion_report(
        self,
        expectation: RetainedPromotionReportExpectation,
    ) -> dict[str, object]:
        report = expectation.to_report()
        if not self._retained_promotion_report_contract_valid(
            report,
            expectation=expectation,
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion report contract is invalid"
            )
        return report

    def _retained_promotion_report_expectation(
        self,
        *,
        operation_id: str,
        created_at: str,
        code_version: str,
        preparation: RetainedPromotionPreparation,
        base: PromotionIdentityEvidence,
        active_location: dict[str, object],
        active_after: dict[str, object],
        quarantine: Path,
        quarantine_location: dict[str, object],
        smoke_result: SmokeResult,
        verify_cleanup_staging: bool = True,
    ) -> RetainedPromotionReportExpectation:
        eligibility = preparation.eligibility
        retained_report, retained_sha256, _retained_stat = (
            self._eligibility._promotion_report_snapshot(eligibility.retained_report_id)
        )
        source_report, source_sha256, _source_stat = self._eligibility._promotion_report_snapshot(
            eligibility.source_report_id
        )
        retained_code = retained_report.get("codeVersion")
        source_code = source_report.get("codeVersion")
        if (
            retained_sha256 != eligibility.retained_report_sha256
            or source_sha256 != eligibility.source_report_sha256
            or retained_report.get("sourceRehearsalReportId")
            != eligibility.source_report_id
            or not isinstance(retained_code, str)
            or not isinstance(source_code, str)
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion report provenance changed"
            )
        current_backup_identity = self._promotion_evidence._backup_payload_identity(preparation.backup_id)
        if current_backup_identity != preparation.backup_payload_identity:
            raise _managed_root.CutoverSafetyError(
                "Promotion backup physical identity changed"
            )
        if self._promotion_evidence._payload_manifest_entries(current_backup_identity) != (
            self._promotion_evidence._payload_manifest_entries(base.active_before_payload)
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion backup content identity changed"
            )
        if not self._promotion_evidence._payload_physical_identity_distinct(
            current_backup_identity,
            base.active_before_payload,
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion backup is not physically independent"
            )
        if verify_cleanup_staging:
            holding_fd = self._workspace.managed().open_dir(
                self._workspace._managed_relative(self._workspace.operations_root / "cleanup-staging" / operation_id)
            )
            try:
                if (
                    self._promotion_evidence._directory_identity_evidence(holding_fd)
                    != preparation.holding_directory_identity
                    or self._promotion_evidence._held_artifacts_evidence(holding_fd)
                    != preparation.detached_artifacts
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion cleanup staging evidence is invalid"
                    )
            finally:
                os.close(holding_fd)
        marker_relative = (
            Path("operations/market-v4-cutover/consumed")
            / f"{eligibility.retained_report_id}.json"
        )
        artifact_mappings = tuple(
            artifact.to_mapping() for artifact in preparation.detached_artifacts
        )
        return RetainedPromotionReportExpectation(
            report_id=operation_id,
            created_at=created_at,
            code_version=code_version,
            retained_report={
                "reportId": eligibility.retained_report_id,
                "codeVersion": retained_code,
                "reportSha256": retained_sha256,
            },
            source_report={
                "reportId": eligibility.source_report_id,
                "codeVersion": source_code,
                "reportSha256": source_sha256,
            },
            fingerprints={
                "targetRoot": eligibility.target_root_fingerprint,
                "retainedRoot": self._market_identity.root_fingerprint_at(
                    self._promotion_evidence._retained_lease_fd_root()
                ),
                "configuration": eligibility.configuration_fingerprint,
            },
            payload_identities={
                "activeBefore": base.active_before_payload,
                "backup": current_backup_identity,
                "retainedSource": base.retained_v4_payload,
                "activated": active_location["payload"],
                "activeAfter": active_after["payload"],
            },
            filesystem_evidence={
                "sameDevice": (
                    base.active_before_directory["device"]
                    == base.retained_v4_directory["device"]
                ),
                "atomicExchange": True,
                "activeBeforeDirectory": base.active_before_directory,
                "retainedSourceDirectory": base.retained_v4_directory,
                "activatedDirectory": active_location["directory"],
                "activeAfterDirectory": active_after["directory"],
                "quarantineDirectory": quarantine_location["directory"],
            },
            backup_id=preparation.backup_id,
            backup_manifest_sha256=preparation.backup_manifest_sha256,
            backup_file_set_sha256=preparation.backup_file_set_sha256,
            backup_evidence={
                "manifestSha256": preparation.backup_manifest_sha256,
                "fileSetSha256": preparation.backup_file_set_sha256,
                "contentEquivalentToActiveBefore": True,
                "physicalIdentityDistinct": True,
            },
            journal={
                "operationId": operation_id,
                "finalState": PromotionState.COMMITTED.value,
            },
            quarantine_path=self._workspace._managed_relative(quarantine).as_posix(),
            runtime_cleanup={
                "holdingDirectory": preparation.holding_directory_identity,
                "detachedRuntimeNames": list(preparation.detached_runtime_names),
                "detachedArtifacts": list(artifact_mappings),
                "removedArtifacts": [],
                "cleanupStagingPath": self._workspace._managed_relative(
                    self._workspace.operations_root / "cleanup-staging" / operation_id
                ).as_posix(),
                "cleanupControlPath": self._workspace._managed_relative(
                    self._workspace.operations_root / "cleanup-controls" / f"{operation_id}.json"
                ).as_posix(),
                "cleanupResultPath": self._workspace._managed_relative(
                    self._workspace.operations_root / "cleanup-results" / f"{operation_id}.json"
                ).as_posix(),
                "cleanupDisposition": "pending_post_commit",
                "activeRuntime": f".cutover-runtime-{operation_id}",
                "activeRuntimeRemoved": True,
            },
            no_sync=True,
            no_jquants=True,
            api_checks=smoke_result.api_paths,
            server_process_joined=True,
            worker_process_joined=True,
            semantic_smoke={
                "schemaVersion": smoke_result.schema_version,
                "stockPriceAdjustmentMode": smoke_result.adjustment_mode,
                "checks": list(smoke_result.checks),
                "providerVintage": smoke_result.lineage,
            },
            source_consumed={
                "retainedReportId": eligibility.retained_report_id,
                "markerPath": marker_relative.as_posix(),
            },
            rollback_instructions=(
                "Keep the immutable backup and quarantined v3 tree; use the "
                "retained-promotion recovery operation before any further mutation."
            ),
        )

    def _payload_location_identity(self, market_path: Path) -> dict[str, object]:
        market_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(market_path))
        try:
            return {
                "directory": self._promotion_evidence._directory_identity_evidence(market_fd),
                "payload": self._market_identity._market_payload_identity(market_fd),
            }
        finally:
            os.close(market_fd)

    @staticmethod
    def _promotion_identities(
        base: PromotionIdentityEvidence,
        *,
        active_current: dict[str, object] | None,
        retained_current: dict[str, object] | None,
        quarantine_current: dict[str, object] | None,
        holding_current: dict[str, object] | None,
        rollback_mode: str | None = None,
        promotion_report_sha256: str | None = None,
    ) -> PromotionIdentityEvidence:
        return PromotionIdentityEvidence(
            active_before_directory=base.active_before_directory,
            active_before_payload=base.active_before_payload,
            retained_v4_directory=base.retained_v4_directory,
            retained_v4_payload=base.retained_v4_payload,
            backup_manifest_sha256=base.backup_manifest_sha256,
            backup_file_set_sha256=base.backup_file_set_sha256,
            active_current=active_current,
            retained_current=retained_current,
            quarantine_current=quarantine_current,
            holding_current=holding_current,
            detached_runtime_names=base.detached_runtime_names,
            detached_artifacts=base.detached_artifacts,
            rollback_mode=(
                base.rollback_mode if rollback_mode is None else rollback_mode
            ),
            promotion_report_sha256=(
                base.promotion_report_sha256
                if promotion_report_sha256 is None
                else promotion_report_sha256
            ),
        )
