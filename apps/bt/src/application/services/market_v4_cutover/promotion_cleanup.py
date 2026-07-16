"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    DetachedArtifactEvidence,
    PromotionJournalRecord,
    PromotionState,
    RetainedPromotionPreparation,
)
from .filesystem import _DIR_OPEN_FLAGS
from .journal_validation import JournalValidator
from .duckdb_service import MarketIdentityService
from .promotion_evidence import PromotionEvidenceService
from .promotion_reports import PromotionReportService
from .workspace import CutoverWorkspace
from . import filesystem

PromotionRecoveryRecords = (
    tuple[PromotionJournalRecord, ...] | list[PromotionJournalRecord]
)


class PromotionCleanupService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        market_identity: MarketIdentityService,
        promotion_evidence: PromotionEvidenceService,
        promotion_reports: PromotionReportService,
    ) -> None:
        self._workspace = workspace
        self._market_identity = market_identity
        self._promotion_evidence = promotion_evidence
        self._promotion_reports = promotion_reports

    def _delete_held_promotion_artifacts(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        artifact_root: Path | None = None,
    ) -> None:
        holding_relative = self._workspace._managed_relative(
            artifact_root or preparation.holding_root
        )
        holding_fd = self._workspace.managed().open_dir(holding_relative)
        try:
            if (
                self._promotion_evidence._directory_identity_evidence(holding_fd)
                != preparation.holding_directory_identity
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion holding directory identity changed"
                )
            current_artifacts = self._promotion_evidence._held_artifacts_evidence(holding_fd)
            if current_artifacts != preparation.detached_artifacts:
                raise _managed_root.CutoverSafetyError(
                    "Promotion held artifact identity changed"
                )
            with _managed_root.ManagedRootFd(Path("."), os.dup(holding_fd)) as holding:
                for artifact in preparation.detached_artifacts:
                    if (
                        self._promotion_evidence._held_artifact_evidence(holding_fd, artifact.name)
                        != artifact
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Promotion held artifact identity changed before deletion"
                        )
                    if artifact.kind == "directory":
                        holding.remove_tree(Path(artifact.name))
                    elif artifact.kind == "regular":
                        os.unlink(artifact.name, dir_fd=holding_fd)
                    else:
                        raise _managed_root.CutoverSafetyError(
                            "Promotion held artifact kind is invalid"
                        )
            os.fsync(holding_fd)
            if os.listdir(holding_fd):
                raise _managed_root.CutoverSafetyError(
                    "Promotion holding cleanup is incomplete"
                )
        finally:
            os.close(holding_fd)

    def _cleanup_staging_root(self, operation_id: str) -> Path:
        return self._workspace.operations_root / "cleanup-staging" / operation_id

    def _cleanup_result_path(self, operation_id: str) -> Path:
        return self._workspace.operations_root / "cleanup-results" / f"{operation_id}.json"

    def _cleanup_control_path(self, operation_id: str) -> Path:
        return self._workspace.operations_root / "cleanup-controls" / f"{operation_id}.json"

    def _stage_held_promotion_artifacts(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        operation_id: str,
    ) -> Path:
        staging = self._cleanup_staging_root(operation_id)
        self._workspace._prepare_managed_directory(staging.parent, exist_ok=True)
        self._workspace._assert_managed_target_absent(staging)
        self._workspace._secure_rename(preparation.holding_root, staging)
        staging_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(staging))
        try:
            if (
                self._promotion_evidence._directory_identity_evidence(staging_fd)
                != preparation.holding_directory_identity
                or self._promotion_evidence._held_artifacts_evidence(staging_fd)
                != preparation.detached_artifacts
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion cleanup staging identity mismatch"
                )
        finally:
            os.close(staging_fd)
        return staging

    def _cleanup_result_payload(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        operation_id: str,
        report_sha256: str,
    ) -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "operationId": operation_id,
            "reportSha256": report_sha256,
            "stagingDirectory": preparation.holding_directory_identity,
            "deletedArtifacts": [
                artifact.to_mapping() for artifact in preparation.detached_artifacts
            ],
        }

    def _complete_committed_promotion_cleanup(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        operation_id: str,
        report_sha256: str,
    ) -> None:
        result = self._cleanup_result_path(operation_id)
        staging = self._cleanup_staging_root(operation_id)
        expected = self._cleanup_result_payload(
            preparation,
            operation_id=operation_id,
            report_sha256=report_sha256,
        )
        try:
            raw = self._workspace.managed().read_bytes(self._workspace._managed_relative(result))
        except FileNotFoundError:
            raw = None
        if raw is not None:
            try:
                actual = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise _managed_root.CutoverSafetyError(
                    "Promotion cleanup result is invalid"
                ) from exc
            if actual != expected:
                raise _managed_root.CutoverSafetyError(
                    "Promotion cleanup result identity mismatch"
                )
            try:
                self._workspace.managed().stat(self._workspace._managed_relative(staging))
            except FileNotFoundError:
                return
            raise _managed_root.CutoverSafetyError(
                "Promotion cleanup result exists while staging remains"
            )
        control = self._cleanup_control_path(operation_id)
        control_payload = {
            **expected,
            "kind": "cleanup_intent",
        }
        try:
            control_raw = self._workspace.managed().read_bytes(self._workspace._managed_relative(control))
        except FileNotFoundError:
            staging_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(staging))
            try:
                if (
                    self._promotion_evidence._directory_identity_evidence(staging_fd)
                    != preparation.holding_directory_identity
                    or self._promotion_evidence._held_artifacts_evidence(staging_fd)
                    != preparation.detached_artifacts
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion cleanup staging identity mismatch"
                    )
            finally:
                os.close(staging_fd)
            control_root = control.parent
            self._workspace._prepare_managed_directory(control_root, exist_ok=True)
            control_fd = self._workspace.managed().open_regular(
                self._workspace._managed_relative(control),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            try:
                self._workspace._write_all(
                    control_fd,
                    JournalValidator._canonical_json(control_payload),
                )
                os.fsync(control_fd)
            finally:
                os.close(control_fd)
            self._workspace.managed().fsync_dir(self._workspace._managed_relative(control_root))
            self._workspace._promotion_boundary_hook("cleanup_intent_fsynced")
        else:
            try:
                actual_control = json.loads(control_raw)
            except json.JSONDecodeError as exc:
                raise _managed_root.CutoverSafetyError(
                    "Promotion cleanup control is invalid"
                ) from exc
            if actual_control != control_payload:
                raise _managed_root.CutoverSafetyError(
                    "Promotion cleanup control identity mismatch"
                )
        try:
            staging_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(staging))
        except FileNotFoundError:
            staging_fd = None
        if staging_fd is not None:
            try:
                if (
                    self._promotion_evidence._directory_identity_evidence(staging_fd)
                    != preparation.holding_directory_identity
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion cleanup staging directory changed"
                    )
                expected_by_name = {
                    artifact.name: artifact
                    for artifact in preparation.detached_artifacts
                }
                current = self._promotion_evidence._held_artifacts_evidence(staging_fd)
                if any(
                    artifact.name not in expected_by_name
                    or artifact != expected_by_name[artifact.name]
                    for artifact in current
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion cleanup staging contains ambiguous artifacts"
                    )
                with _managed_root.ManagedRootFd(
                    Path("."), os.dup(staging_fd)
                ) as managed:
                    for artifact in current:
                        if artifact.kind == "directory":
                            managed.remove_tree(Path(artifact.name))
                        else:
                            os.unlink(artifact.name, dir_fd=staging_fd)
                os.fsync(staging_fd)
            finally:
                os.close(staging_fd)
            parent_fd, name = self._workspace.managed().open_parent(
                self._workspace._managed_relative(staging)
            )
            try:
                os.rmdir(name, dir_fd=parent_fd)
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        self._workspace._promotion_boundary_hook("cleanup_artifacts_deleted")
        result_root = result.parent
        self._workspace._prepare_managed_directory(result_root, exist_ok=True)
        fd = self._workspace.managed().open_regular(
            self._workspace._managed_relative(result),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        try:
            self._workspace._write_all(fd, JournalValidator._canonical_json(expected))
            os.fsync(fd)
        finally:
            os.close(fd)
        self._workspace.managed().fsync_dir(self._workspace._managed_relative(result_root))

    def _write_source_consumed_marker(
        self,
        *,
        retained_report_id: str,
        operation_id: str,
        promotion_report_sha256: str,
    ) -> Path:
        consumed_root = self._workspace.operations_root / "consumed"
        self._workspace._prepare_managed_directory(consumed_root, exist_ok=True)
        marker = consumed_root / f"{retained_report_id}.json"
        marker_fd = self._workspace.managed().open_regular(
            self._workspace._managed_relative(marker),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        try:
            payload = (
                JournalValidator._canonical_json(
                    {
                        "schemaVersion": 1,
                        "retainedReportId": retained_report_id,
                        "operationId": operation_id,
                        "promotionReportSha256": promotion_report_sha256,
                    }
                )
                + b"\n"
            )
            self._workspace._write_all(marker_fd, payload)
            os.fsync(marker_fd)
        finally:
            os.close(marker_fd)
        self._workspace.managed().fsync_dir(self._workspace._managed_relative(consumed_root))
        return marker

    def _promotion_location_if_present(
        self,
        market_path: Path,
    ) -> dict[str, object] | None:
        try:
            self._workspace.managed().stat(self._workspace._managed_relative(market_path))
        except FileNotFoundError:
            return None
        return self._promotion_reports._payload_location_identity(market_path)

    @staticmethod
    def _location_matches(
        location: dict[str, object] | None,
        *,
        directory: dict[str, int],
        payload: dict[str, object],
    ) -> bool:
        return location == {"directory": directory, "payload": payload}

    def _restore_held_promotion_artifacts(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        owned_temp_collision_recovery_records: PromotionRecoveryRecords | None = None,
    ) -> None:
        allow_owned_empty_temp_collision = (
            owned_temp_collision_recovery_records is not None
            and self._owned_temp_collision_recovery_proven(
                owned_temp_collision_recovery_records,
                preparation,
            )
        )
        artifact_root, holding_fd = self._open_promotion_artifact_root(preparation)
        retained_market = preparation.eligibility.retained_root / "market-timeseries"
        retained_fd = self._workspace.managed().open_dir(self._workspace._managed_relative(retained_market))
        try:
            if holding_fd is not None and (
                self._promotion_evidence._directory_identity_evidence(holding_fd)
                != preparation.holding_directory_identity
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion holding directory identity changed"
                )
            staged = (
                self._promotion_evidence._held_artifacts_evidence(holding_fd)
                if holding_fd is not None
                else ()
            )
            expected_by_name = {
                artifact.name: artifact for artifact in preparation.detached_artifacts
            }
            staged_by_name = {artifact.name: artifact for artifact in staged}
            if any(
                artifact.name not in expected_by_name
                or artifact != expected_by_name[artifact.name]
                for artifact in staged
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion held artifact identity changed"
                )
            retained_names = set(os.listdir(retained_fd))
            canonical_names = {"market.duckdb", "parquet"}
            retained_artifact_names = retained_names - canonical_names
            expected_names = set(expected_by_name)
            staged_names = set(staged_by_name)
            rollback_runtime_name = f".cutover-runtime-{preparation.holding_root.name}"
            rollback_runtime_present = rollback_runtime_name in retained_artifact_names
            if rollback_runtime_present:
                runtime_stat = os.stat(
                    rollback_runtime_name,
                    dir_fd=retained_fd,
                    follow_symlinks=False,
                )
                if not stat.S_ISDIR(runtime_stat.st_mode):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion rollback runtime identity changed"
                    )
            retained_expected_names = retained_artifact_names - {rollback_runtime_name}
            duplicate_names = staged_names & retained_expected_names
            unexpected_names = retained_expected_names - expected_names
            if unexpected_names or duplicate_names - {"duckdb-tmp"}:
                raise _managed_root.CutoverSafetyError(
                    "Promotion artifact set is incomplete or ambiguous during restoration"
                )
            if duplicate_names == {"duckdb-tmp"}:
                expected_temp = expected_by_name.get("duckdb-tmp")
                staged_temp = staged_by_name.get("duckdb-tmp")
                if (
                    not allow_owned_empty_temp_collision
                    or expected_temp is None
                    or expected_temp.kind != "directory"
                    or staged_temp != expected_temp
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion artifact set is incomplete or ambiguous during restoration"
                    )
                try:
                    collision_fd = os.open(
                        "duckdb-tmp",
                        _DIR_OPEN_FLAGS,
                        dir_fd=retained_fd,
                    )
                except OSError as exc:
                    raise _managed_root.CutoverSafetyError(
                        "Owned promotion DuckDB temp collision is not an empty real directory"
                    ) from exc
                try:
                    collision_stat = os.fstat(collision_fd)
                    collision_path_stat = os.stat(
                        "duckdb-tmp",
                        dir_fd=retained_fd,
                        follow_symlinks=False,
                    )
                    if (
                        not stat.S_ISDIR(collision_stat.st_mode)
                        or (collision_stat.st_dev, collision_stat.st_ino)
                        != (collision_path_stat.st_dev, collision_path_stat.st_ino)
                        or os.listdir(collision_fd)
                    ):
                        raise _managed_root.CutoverSafetyError(
                            "Owned promotion DuckDB temp collision is not an empty real directory"
                        )
                finally:
                    os.close(collision_fd)
                os.rmdir("duckdb-tmp", dir_fd=retained_fd)
                try:
                    os.fsync(retained_fd)
                except OSError as exc:
                    self._fence_promotion_leases()
                    raise _managed_root.CutoverSafetyError(
                        "Owned promotion DuckDB temp collision removal is not durable"
                    ) from exc
                self._workspace._promotion_boundary_hook("rollback_owned_temp_collision_removed")
                retained_artifact_names.remove("duckdb-tmp")
                retained_expected_names.remove("duckdb-tmp")
                duplicate_names = set()
            if (
                canonical_names - retained_names
                or retained_expected_names - expected_names
                or staged_names | retained_expected_names != expected_names
                or duplicate_names
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion artifact set is incomplete or ambiguous during restoration"
                )
            restore_from_staging: list[DetachedArtifactEvidence] = []
            for artifact in preparation.detached_artifacts:
                try:
                    retained_identity = self._promotion_evidence._held_artifact_evidence(
                        retained_fd, artifact.name
                    )
                except FileNotFoundError:
                    retained_identity = None
                staged_identity = staged_by_name.get(artifact.name)
                if (retained_identity is None) == (staged_identity is None):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion artifact is duplicated or missing during restoration"
                    )
                if retained_identity is not None:
                    if retained_identity != artifact:
                        raise _managed_root.CutoverSafetyError(
                            "Promotion restored artifact identity changed"
                        )
                    continue
                assert staged_identity == artifact
                restore_from_staging.append(artifact)
            if rollback_runtime_present:
                self._workspace._remove_market_runtime(retained_fd, rollback_runtime_name)
                os.fsync(retained_fd)
            for artifact in restore_from_staging:
                assert holding_fd is not None
                filesystem._rename_exclusive_at(
                    holding_fd,
                    artifact.name,
                    retained_fd,
                    artifact.name,
                )
                if self._promotion_evidence._held_artifact_evidence(retained_fd, artifact.name) != artifact:
                    raise _managed_root.CutoverSafetyError(
                        "Promotion runtime restoration identity changed"
                    )
                os.fsync(holding_fd)
                os.fsync(retained_fd)
                self._workspace._promotion_boundary_hook(
                    f"rollback_artifact_moved:{artifact.name}"
                )
            self._workspace._promotion_boundary_hook("rollback_artifacts_reconciled")
            if holding_fd is not None and os.listdir(holding_fd):
                raise _managed_root.CutoverSafetyError(
                    "Promotion holding restoration is incomplete"
                )
        finally:
            os.close(retained_fd)
            if holding_fd is not None:
                os.close(holding_fd)
        if artifact_root is None:
            return
        self._remove_empty_promotion_artifact_root(artifact_root)

    def _open_promotion_artifact_root(
        self,
        preparation: RetainedPromotionPreparation,
    ) -> tuple[Path | None, int | None]:
        candidates = (
            preparation.holding_root,
            self._cleanup_staging_root(preparation.holding_root.name),
        )
        opened: list[tuple[Path, int]] = []
        for candidate in candidates:
            try:
                opened.append(
                    (
                        candidate,
                        self._workspace.managed().open_dir(self._workspace._managed_relative(candidate)),
                    )
                )
            except FileNotFoundError:
                continue
        if len(opened) > 1:
            for _path, fd in opened:
                os.close(fd)
            raise _managed_root.CutoverSafetyError(
                "Promotion artifact staging identity is ambiguous"
            )
        return opened[0] if opened else (None, None)

    def _remove_empty_promotion_artifact_root(self, artifact_root: Path) -> None:
        parent_fd, name = self._workspace.managed().open_parent(
            self._workspace._managed_relative(artifact_root)
        )
        try:
            os.rmdir(name, dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    @staticmethod
    def _owned_temp_collision_recovery_proven(
        records: tuple[PromotionJournalRecord, ...] | list[PromotionJournalRecord],
        preparation: RetainedPromotionPreparation,
    ) -> bool:
        states = tuple(record.state for record in records)
        required_tail = (
            PromotionState.ACTIVE_SMOKE_PASSED,
            PromotionState.CLEANUP_STAGED,
            PromotionState.EXCHANGED_BACK,
        )
        if (
            len(states) < len(required_tail)
            or states[-3:] != required_tail
            or tuple(record.sequence for record in records[-3:])
            != tuple(range(records[-3].sequence, records[-3].sequence + 3))
            or any(
                record.operation_id != preparation.holding_root.name
                for record in records[-3:]
            )
            or records[-1].identities.rollback_mode != "atomic_exchange"
        ):
            return False
        expected = tuple(
            artifact.to_mapping() for artifact in preparation.detached_artifacts
        )
        if any(
            record.identities.detached_artifacts != expected for record in records[-3:]
        ):
            return False
        matching = tuple(
            artifact for artifact in expected if artifact.get("name") == "duckdb-tmp"
        )
        return len(matching) == 1 and matching[0].get("kind") == "directory"

    def _remove_incomplete_consumed_marker(
        self,
        *,
        retained_report_id: str,
        operation_id: str,
    ) -> None:
        marker = self._workspace.operations_root / "consumed" / f"{retained_report_id}.json"
        try:
            raw = self._workspace.managed().read_bytes(self._workspace._managed_relative(marker))
        except FileNotFoundError:
            return
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise _managed_root.CutoverSafetyError(
                "Incomplete promotion consumed marker is invalid"
            ) from exc
        if not (
            isinstance(value, dict)
            and value.get("schemaVersion") == 1
            and value.get("retainedReportId") == retained_report_id
            and value.get("operationId") == operation_id
            and set(value)
            == {
                "schemaVersion",
                "retainedReportId",
                "operationId",
                "promotionReportSha256",
            }
        ):
            raise _managed_root.CutoverSafetyError(
                "Incomplete promotion consumed marker identity mismatch"
            )
        parent_fd, name = self._workspace.managed().open_parent(self._workspace._managed_relative(marker))
        try:
            os.unlink(name, dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    def _fence_promotion_leases(self) -> None:
        for lease in (self._workspace._active_lease, self._workspace._retained_lease):
            if lease is not None:
                lease.unlock_on_release = False
                lease.owns_fd = False
