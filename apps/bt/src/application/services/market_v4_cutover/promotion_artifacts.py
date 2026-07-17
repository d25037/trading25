"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    DetachedArtifactEvidence,
    PromotionIdentityEvidence,
    PromotionState,
    RetainedPromotionEligibility,
    RetainedPromotionPreparation,
)
from .filesystem import _DIR_OPEN_FLAGS
from .journal import PromotionJournal
from .promotion_contracts import RetainedPromotionContext
from .backup import MarketBackupService
from .duckdb_service import MarketIdentityService
from .promotion_cleanup import PromotionCleanupService
from .promotion_evidence import PromotionEvidenceService
from .promotion_eligibility import PromotionEligibilityService
from .promotion_reports import PromotionReportService
from .promotion_rollback import PromotionRollbackService
from .workspace import CutoverWorkspace
from . import filesystem


class PromotionArtifactService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        market_identity: MarketIdentityService,
        backups: MarketBackupService,
        eligibility: PromotionEligibilityService,
        promotion_evidence: PromotionEvidenceService,
        promotion_reports: PromotionReportService,
        cleanup: PromotionCleanupService,
        rollback: PromotionRollbackService,
    ) -> None:
        self._workspace = workspace
        self._market_identity = market_identity
        self._backups = backups
        self._eligibility = eligibility
        self._promotion_evidence = promotion_evidence
        self._promotion_reports = promotion_reports
        self._cleanup = cleanup
        self._rollback = rollback

    @staticmethod
    def _validate_canonical_market_payload(market_fd: int) -> None:
        if set(os.listdir(market_fd)) != {"market.duckdb", "parquet"}:
            raise _managed_root.CutoverSafetyError(
                "Retained Market payload is not canonical"
            )
        database = os.stat("market.duckdb", dir_fd=market_fd, follow_symlinks=False)
        parquet = os.stat("parquet", dir_fd=market_fd, follow_symlinks=False)
        if not stat.S_ISREG(database.st_mode) or not stat.S_ISDIR(parquet.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Retained Market payload is not canonical"
            )

    def _retained_artifact_detachment_plan(
        self,
        eligibility: RetainedPromotionEligibility,
    ) -> tuple[tuple[str, ...], tuple[DetachedArtifactEvidence, ...]]:
        """Snapshot every allowed artifact before the first detach mutation."""

        retained_root_fd = self._promotion_evidence._retained_lease_fd_root()
        self._market_identity.assert_retained_root_identity(eligibility.retained_root, retained_root_fd)
        market_fd = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_root_fd
        )
        try:
            source_report, source_sha256, _source_stat = (
                self._eligibility._promotion_report_snapshot(eligibility.source_report_id)
            )
            source_code_version = source_report.get("codeVersion")
            if source_sha256 != eligibility.source_report_sha256 or not isinstance(
                source_code_version, str
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion source report changed before detach"
                )
            proven_runtimes = self._eligibility._proven_retained_runtime_names(
                retained_root_fd,
                source_report_id=eligibility.source_report_id,
                retained_report_id=eligibility.retained_report_id,
                source_report_code_version=source_code_version,
                source_market_identity=eligibility.source_market_identity,
                retained_root_fingerprint=self._market_identity.root_fingerprint_at(retained_root_fd),
                target_root_fingerprint=eligibility.target_root_fingerprint,
            )
            self._eligibility._validate_retained_market_allowlist(
                retained_root_fd,
                proven_runtime_names=proven_runtimes,
            )
            artifacts: list[DetachedArtifactEvidence] = []
            for name in (
                *proven_runtimes,
                "duckdb-tmp",
                "market.duckdb.wal",
                "maintenance.v1.json",
            ):
                try:
                    entry = os.stat(name, dir_fd=market_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if name in proven_runtimes:
                    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Proven retained runtime must be a real directory"
                        )
                elif name == "duckdb-tmp":
                    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Retained Market temporary artifact is invalid"
                        )
                    self._eligibility._assert_empty_directory(market_fd, name)
                elif name == "maintenance.v1.json":
                    if not stat.S_ISREG(entry.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Retained Market maintenance evidence is invalid"
                        )
                elif not stat.S_ISREG(entry.st_mode) or entry.st_size != 0:
                    raise _managed_root.CutoverSafetyError(
                        "Retained Market WAL artifact is invalid"
                    )
                artifacts.append(self._promotion_evidence._held_artifact_evidence(market_fd, name))
            present_runtime_names = tuple(
                artifact.name
                for artifact in artifacts
                if artifact.name in proven_runtimes
            )
            return present_runtime_names, tuple(
                sorted(artifacts, key=lambda artifact: artifact.name)
            )
        finally:
            os.close(market_fd)

    def _detach_retained_artifacts(
        self,
        eligibility: RetainedPromotionEligibility,
        *,
        holding_root: Path,
        planned_artifacts: tuple[DetachedArtifactEvidence, ...],
    ) -> None:
        retained_root_fd = self._promotion_evidence._retained_lease_fd_root()
        self._market_identity.assert_retained_root_identity(eligibility.retained_root, retained_root_fd)
        market_fd = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_root_fd
        )
        holding_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(holding_root))
        retained_market_identity = self._promotion_evidence._directory_identity_evidence(market_fd)
        holding_identity = self._promotion_evidence._directory_identity_evidence(holding_fd)
        try:
            for artifact in planned_artifacts:
                name = artifact.name
                if self._promotion_evidence._held_artifact_evidence(market_fd, name) != artifact:
                    raise _managed_root.CutoverSafetyError(
                        "Planned promotion artifact identity changed"
                    )
                self._workspace._rename_at_hook(
                    eligibility.retained_root / "market-timeseries" / name,
                    holding_root / name,
                )
                filesystem._rename_exclusive_at(market_fd, name, holding_fd, name)
                if self._promotion_evidence._held_artifact_evidence(holding_fd, name) != artifact:
                    raise _managed_root.CutoverSafetyError(
                        "Detached promotion artifact identity changed"
                    )
                self._workspace._promotion_boundary_hook(f"detach_artifact_{name}:moved")
                os.fsync(market_fd)
                self._workspace._promotion_boundary_hook(f"detach_artifact_{name}:source_fsynced")
                os.fsync(holding_fd)
                self._workspace._promotion_boundary_hook(f"detach_artifact_{name}:holding_fsynced")
            if retained_market_identity != self._promotion_evidence._directory_identity_evidence(market_fd):
                raise _managed_root.CutoverSafetyError(
                    "Retained Market directory identity changed"
                )
            if holding_identity != self._promotion_evidence._directory_identity_evidence(holding_fd):
                raise _managed_root.CutoverSafetyError(
                    "Promotion holding directory identity changed"
                )
            self._validate_canonical_market_payload(market_fd)
        finally:
            os.close(holding_fd)
            os.close(market_fd)
        if (
            self._market_identity.market_tree_identity(retained_root_fd)
            != eligibility.source_market_identity
        ):
            raise _managed_root.CutoverSafetyError(
                "Retained Market payload identity changed after detach"
            )

    def _prepare_retained_promotion_under_leases(
        self,
        eligibility: RetainedPromotionEligibility,
        *,
        backup_id: str,
        journal: PromotionJournal,
    ) -> RetainedPromotionPreparation:
        """Back up active v3 and durably detach proven retained runtimes."""

        backup_id = self._workspace._validate_id(backup_id, label="backup")
        self._assert_promotion_payloads_unchanged(eligibility)
        (
            backup_manifest_sha256,
            backup_file_set_sha256,
            backup_payload_identity,
        ) = self._create_verified_promotion_backup(
            eligibility,
            backup_id,
        )
        if self._market_identity.market_tree_identity(self._promotion_evidence._active_lease_fd_root()) != (
            eligibility.active_market_identity
        ):
            raise _managed_root.CutoverSafetyError(
                "Active Market payload identity changed after backup"
            )
        if self._promotion_evidence._backup_payload_identity(backup_id) != backup_payload_identity:
            raise _managed_root.CutoverSafetyError(
                "Backup physical payload identity changed"
            )

        active_location = self._promotion_evidence._market_location_identity(self._promotion_evidence._active_lease_fd_root())
        retained_location = self._promotion_evidence._market_location_identity(
            self._promotion_evidence._retained_lease_fd_root()
        )
        detached_runtime_names, planned_artifacts = (
            self._retained_artifact_detachment_plan(eligibility)
        )
        immutable = {
            "active_before_directory": cast(
                dict[str, int], active_location["directory"]
            ),
            "active_before_payload": eligibility.active_market_identity,
            "retained_v4_directory": cast(
                dict[str, int], retained_location["directory"]
            ),
            "retained_v4_payload": eligibility.source_market_identity,
            "backup_manifest_sha256": backup_manifest_sha256,
            "backup_file_set_sha256": backup_file_set_sha256,
        }
        validated = PromotionIdentityEvidence(
            **immutable,
            active_current=active_location,
            retained_current=retained_location,
            quarantine_current=None,
            holding_current=None,
            detached_runtime_names=detached_runtime_names,
            detached_artifacts=tuple(
                artifact.to_mapping() for artifact in planned_artifacts
            ),
        )
        self._promotion_evidence._append_preparation_state(journal, PromotionState.VALIDATED, validated)

        holding_parent = self._workspace.operations_root / "holding"
        self._workspace._prepare_managed_directory(holding_parent, exist_ok=True)
        self._workspace.managed().fsync_dir(self._workspace._managed_relative(self._workspace.operations_root))
        holding_root = holding_parent / journal.operation_id
        self._workspace._prepare_managed_directory(holding_root, exist_ok=False)
        self._workspace.managed().fsync_dir(self._workspace._managed_relative(holding_parent))
        holding_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(holding_root))
        try:
            os.fsync(holding_fd)
            holding_directory = self._promotion_evidence._directory_identity_evidence(holding_fd)
        finally:
            os.close(holding_fd)
        preparation = RetainedPromotionPreparation(
            eligibility=eligibility,
            backup_id=backup_id,
            backup_manifest_sha256=backup_manifest_sha256,
            backup_file_set_sha256=backup_file_set_sha256,
            backup_payload_identity=backup_payload_identity,
            holding_root=holding_root,
            holding_directory_identity=holding_directory,
            detached_runtime_names=detached_runtime_names,
            detached_artifacts=planned_artifacts,
        )
        try:
            self._detach_retained_artifacts(
                eligibility,
                holding_root=holding_root,
                planned_artifacts=planned_artifacts,
            )
            os.fsync(self._promotion_evidence._retained_lease_fd_root())
            retained_location = self._promotion_evidence._market_location_identity(
                self._promotion_evidence._retained_lease_fd_root()
            )
            holding_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(holding_root))
            try:
                detached_artifacts = self._promotion_evidence._held_artifacts_evidence(holding_fd)
            finally:
                os.close(holding_fd)
            if detached_artifacts != planned_artifacts:
                raise _managed_root.CutoverSafetyError(
                    "Detached runtime evidence is incomplete"
                )
            detached = PromotionIdentityEvidence(
                **immutable,
                active_current=active_location,
                retained_current=retained_location,
                quarantine_current=None,
                holding_current={
                    "directory": holding_directory,
                    "payload": eligibility.source_market_identity,
                },
                detached_runtime_names=detached_runtime_names,
                detached_artifacts=tuple(
                    artifact.to_mapping() for artifact in planned_artifacts
                ),
            )
            self._promotion_evidence._append_preparation_state(
                journal,
                PromotionState.RUNTIMES_DETACHED,
                detached,
            )
            self._promotion_evidence._append_preparation_state(journal, PromotionState.PREPARED, detached)
            return preparation
        except Exception as exc:
            if "journal append is indeterminate" in str(exc):
                raise
            try:
                self._rollback._rollback_retained_promotion(
                    RetainedPromotionContext(preparation, journal),
                    processes_joined=True,
                )
            except Exception as rollback_error:
                deferred_journal_error: Exception | None = None
                try:
                    last = journal.read_validated()[-1]
                    holding_current: dict[str, object] | None = None
                    try:
                        unresolved_holding_fd = self._workspace.managed().open_dir(
                            self._workspace._managed_relative(holding_root)
                        )
                    except FileNotFoundError:
                        pass
                    else:
                        try:
                            holding_current = {
                                "directory": self._promotion_evidence._directory_identity_evidence(
                                    unresolved_holding_fd
                                ),
                                "payload": eligibility.source_market_identity,
                            }
                        finally:
                            os.close(unresolved_holding_fd)
                    deferred = self._promotion_reports._promotion_identities(
                        last.identities,
                        active_current=self._promotion_evidence._market_location_identity(
                            self._promotion_evidence._active_lease_fd_root()
                        ),
                        retained_current=self._promotion_evidence._market_location_identity(
                            self._promotion_evidence._retained_lease_fd_root()
                        ),
                        quarantine_current=None,
                        holding_current=holding_current,
                    )
                    self._promotion_evidence._append_preparation_state(
                        journal,
                        PromotionState.ROLLBACK_DEFERRED,
                        deferred,
                    )
                except Exception as journal_error:
                    deferred_journal_error = journal_error
                for lease in (self._workspace._active_lease, self._workspace._retained_lease):
                    if lease is not None:
                        lease.unlock_on_release = False
                        lease.owns_fd = False
                raise _managed_root.CutoverSafetyError(
                    "Retained promotion preparation rollback deferred with both leases held"
                ) from (deferred_journal_error or rollback_error)
            raise exc

    def _assert_promotion_payloads_unchanged(
        self,
        eligibility: RetainedPromotionEligibility,
    ) -> None:
        if self._workspace._active_lease is None or self._workspace._retained_lease is None:
            raise _managed_root.CutoverSafetyError(
                "Active and retained Market operation leases are required"
            )
        if self._market_identity.market_tree_identity(self._promotion_evidence._active_lease_fd_root()) != (
            eligibility.active_market_identity
        ):
            raise _managed_root.CutoverSafetyError(
                "Active Market payload identity changed"
            )
        if self._market_identity.market_tree_identity(self._promotion_evidence._retained_lease_fd_root()) != (
            eligibility.source_market_identity
        ):
            raise _managed_root.CutoverSafetyError(
                "Retained Market payload identity changed"
            )

    def _create_verified_promotion_backup(
        self,
        eligibility: RetainedPromotionEligibility,
        backup_id: str,
    ) -> tuple[str, str, dict[str, object]]:
        source_bytes = sum(
            self._workspace.managed().stat(self._workspace._managed_relative(path)).st_size
            for path in self._workspace._source_files(self._workspace.market_root)
        )
        required_bytes = source_bytes + max(source_bytes // 20, 1)
        if self._workspace.disk_free_bytes(self._workspace.data_root) < required_bytes:
            raise _managed_root.CutoverSafetyError(
                f"Insufficient free space: require at least {required_bytes} bytes"
            )
        active_market_fd = os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=self._promotion_evidence._active_lease_fd_root(),
        )
        try:
            metadata = self._workspace.duckdb.inspect(
                active_market_fd,
                "market.duckdb",
                guard_lease_fd=self._workspace.active_lease_fd(),
            )
        finally:
            os.close(active_market_fd)
        code_version = self._workspace._active_code_version
        if code_version is None:
            raise _managed_root.CutoverSafetyError(
                "Operation code identity is unavailable"
            )
        self._backups._copy_backup_under_snapshot(
            backup_id,
            metadata,
            code_version=code_version,
        )
        return self._promotion_evidence._verified_backup_evidence(
            backup_id,
            expected_payload=eligibility.active_market_identity,
        )
