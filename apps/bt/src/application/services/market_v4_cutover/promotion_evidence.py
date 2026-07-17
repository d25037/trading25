"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)

from .contracts import (
    DetachedArtifactEvidence,
    PromotionAppendStatus,
    PromotionIdentityEvidence,
    PromotionJournalRecord,
    PromotionState,
    RetainedPromotionEligibility,
    SmokeConfig,
)
from .filesystem import _DIR_OPEN_FLAGS
from .journal import PromotionJournal
from .journal_validation import JournalValidator
from .duckdb_service import MarketIdentityService
from .evidence import MarketEvidence
from .promotion_eligibility import PromotionEligibilityService
from .workspace import CutoverWorkspace


class PromotionEvidenceService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: MarketEvidence,
        market_identity: MarketIdentityService,
        eligibility: PromotionEligibilityService,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence
        self._market_identity = market_identity
        self._eligibility = eligibility

    def _validate_retained_promotion_eligibility_under_leases(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        code_version: str,
        retained_lease: _market_operation_lease.MarketOperationLease,
    ) -> RetainedPromotionEligibility:
        report_id = self._workspace._validate_id(report_id, label="report")
        retained_report_id = self._workspace._validate_id(
            retained_report_id, label="retained report"
        )
        backup_id = self._workspace._validate_id(backup_id, label="backup")
        expected_smoke_config = {
            "symbol": config.symbol,
            "strategy": config.strategy,
            "datasetPreset": config.dataset_preset,
        }
        retained, retained_sha256, retained_stat = self._eligibility._promotion_report_snapshot(
            retained_report_id
        )
        source_report_value = retained.get("sourceRehearsalReportId")
        if not isinstance(source_report_value, str):
            raise _managed_root.CutoverSafetyError(
                "Retained report provenance is invalid"
            )
        source_report_id = self._workspace._validate_id(
            source_report_value, label="source rehearsal report"
        )
        source, source_sha256, source_stat = self._eligibility._promotion_report_snapshot(
            source_report_id
        )
        retained_again, retained_sha256_again, retained_stat_again = (
            self._eligibility._promotion_report_snapshot(retained_report_id)
        )
        source_again, source_sha256_again, source_stat_again = (
            self._eligibility._promotion_report_snapshot(source_report_id)
        )
        if (
            retained != retained_again
            or retained_sha256 != retained_sha256_again
            or retained_stat != retained_stat_again
            or source != source_again
            or source_sha256 != source_sha256_again
            or source_stat != source_stat_again
        ):
            raise _managed_root.CutoverSafetyError(
                "Promotion report provenance changed"
            )

        target_root_fingerprint = self._evidence.root_fingerprint(self._workspace.data_root)
        retained_valid = (
            retained.get("reportId") == retained_report_id
            and retained.get("phase") == "rehearsal"
            and retained.get("status") == "passed"
            and retained.get("rehearsalMode") == "retained_market_smoke"
            and retained.get("serverProcessJoined") is True
            and retained.get("workerProcessJoined") is True
            and retained.get("smokeConfig") == expected_smoke_config
            and retained.get("targetRootFingerprint") == target_root_fingerprint
            and self._eligibility._retained_report_contract_valid(
                retained,
                report_id=retained_report_id,
                config=config,
            )
        )
        source_valid = (
            source.get("reportId") == source_report_id
            and source.get("phase") == "rehearsal"
            and source.get("status") in {"passed", "failed"}
            and source.get("serverProcessJoined") is True
            and source.get("workerProcessJoined") is True
            and source.get("smokeConfig") == expected_smoke_config
            and source.get("targetRootFingerprint") == target_root_fingerprint
            and source.get("codeVersion") == retained.get("sourceRehearsalCodeVersion")
        )
        if not retained_valid or not source_valid:
            raise _managed_root.CutoverSafetyError(
                "An exact passing retained rehearsal report is required"
            )
        return self._validate_retained_promotion_source(
            report_id=report_id,
            retained_report_id=retained_report_id,
            backup_id=backup_id,
            code_version=code_version,
            retained_lease=retained_lease,
            retained=retained,
            retained_sha256=retained_sha256,
            source=source,
            source_sha256=source_sha256,
            source_report_id=source_report_id,
            target_root_fingerprint=target_root_fingerprint,
        )

    def _validate_retained_promotion_source(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        code_version: str,
        retained_lease: _market_operation_lease.MarketOperationLease,
        retained: dict[str, object],
        retained_sha256: str,
        source: dict[str, object],
        source_sha256: str,
        source_report_id: str,
        target_root_fingerprint: str,
    ) -> RetainedPromotionEligibility:
        retained_root = self._market_identity.retained_rehearsal_root(source_report_id)
        self._market_identity.assert_retained_root_identity(retained_root, retained_lease.root_fd)
        retained_root_fingerprint = self._market_identity.root_fingerprint_at(retained_lease.root_fd)
        if retained_root_fingerprint != retained.get("sourceRetainedRootFingerprint"):
            raise _managed_root.CutoverSafetyError("Retained root fingerprint mismatch")
        configuration_fingerprint = self._evidence.configuration_fingerprint(self._workspace.data_root)
        if (
            self._market_identity._configuration_fingerprint_at(retained_lease.root_fd)
            != configuration_fingerprint
        ):
            raise _managed_root.CutoverSafetyError(
                "Retained configuration differs from active"
            )

        source_market_identity = self._market_identity.market_tree_identity(retained_lease.root_fd)
        if source_market_identity != retained.get(
            "sourceMarketIdentityBefore"
        ) or source_market_identity != retained.get("sourceMarketIdentityAfter"):
            raise _managed_root.CutoverSafetyError(
                "Retained Market payload identity mismatch"
            )
        source_code_version = source.get("codeVersion")
        if not isinstance(source_code_version, str):
            raise _managed_root.CutoverSafetyError(
                "Promotion source report code is invalid"
            )
        proven_runtime_names = self._eligibility._proven_retained_runtime_names(
            retained_lease.root_fd,
            source_report_id=source_report_id,
            retained_report_id=retained_report_id,
            source_report_code_version=source_code_version,
            source_market_identity=source_market_identity,
            retained_root_fingerprint=retained_root_fingerprint,
            target_root_fingerprint=target_root_fingerprint,
        )
        self._eligibility._validate_retained_market_allowlist(
            retained_lease.root_fd,
            proven_runtime_names=proven_runtime_names,
        )
        retained_market_fd = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_lease.root_fd
        )
        try:
            metadata = self._workspace.duckdb.inspect(
                retained_market_fd,
                "market.duckdb",
                guard_lease_fd=retained_lease.fd,
            )
        finally:
            os.close(retained_market_fd)
        if metadata.schema_version != 4:
            raise _managed_root.CutoverSafetyError(
                "Retained Market schema v4 is required"
            )
        if metadata.adjustment_mode != "local_projection_v2_event_time":
            raise _managed_root.CutoverSafetyError(
                "Retained Market adjustment mode is incompatible"
            )
        if not metadata.adjusted_metrics_ready:
            raise _managed_root.CutoverSafetyError(
                "Retained adjusted-metric event-time lineage is not ready"
            )

        self._workspace.runtime.assert_quiescent(self._workspace.data_root)
        try:
            active_wal = self._workspace.managed().stat(
                Path("market-timeseries/market.duckdb.wal")
            )
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(active_wal.st_mode) or active_wal.st_size != 0:
                raise _managed_root.CutoverSafetyError(
                    "Nonempty or invalid active DuckDB WAL"
                )
        active_market_identity = self._market_identity.market_tree_identity(
            self._active_lease_fd_root()
        )

        for destination in (
            Path("operations/market-v4-cutover/reports") / report_id,
            Path("operations/market-v4-cutover/journals") / report_id,
            Path("operations/market-v4-cutover/journal-controls") / report_id,
            Path("operations/market-v4-cutover/journal-locks") / f"{report_id}.lock",
            Path("operations/market-v4-cutover/holding") / report_id,
            Path("operations/market-v4-cutover/cleanup-staging") / report_id,
            Path("operations/market-v4-cutover/cleanup-controls") / f"{report_id}.json",
            Path("operations/market-v4-cutover/cleanup-results") / f"{report_id}.json",
            Path("operations/market-v4-cutover/quarantine") / report_id,
            Path("operations/market-v4-cutover/backups") / backup_id,
            Path("operations/market-v4-cutover/consumed")
            / f"{retained_report_id}.json",
        ):
            self._eligibility._assert_promotion_destination_absent(destination)
        self._eligibility._assert_promotion_exchange_capability(retained_lease)
        self._workspace._require_unchanged_code_identity(code_version)
        self._market_identity.assert_retained_root_identity(retained_root, retained_lease.root_fd)
        if self._evidence.root_fingerprint(self._workspace.data_root) != target_root_fingerprint:
            raise _managed_root.CutoverSafetyError(
                "Active root changed during promotion validation"
            )
        if self._market_identity.market_tree_identity(retained_lease.root_fd) != source_market_identity:
            raise _managed_root.CutoverSafetyError(
                "Retained Market changed during promotion validation"
            )
        if (
            self._eligibility._proven_retained_runtime_names(
                retained_lease.root_fd,
                source_report_id=source_report_id,
                retained_report_id=retained_report_id,
                source_report_code_version=source_code_version,
                source_market_identity=source_market_identity,
                retained_root_fingerprint=retained_root_fingerprint,
                target_root_fingerprint=target_root_fingerprint,
            )
            != proven_runtime_names
        ):
            raise _managed_root.CutoverSafetyError(
                "Retained runtime provenance changed"
            )
        return RetainedPromotionEligibility(
            retained_report_id=retained_report_id,
            retained_report_sha256=retained_sha256,
            source_report_id=source_report_id,
            source_report_sha256=source_sha256,
            retained_root=retained_root,
            source_market_identity=source_market_identity,
            active_market_identity=active_market_identity,
            target_root_fingerprint=target_root_fingerprint,
            configuration_fingerprint=configuration_fingerprint,
        )

    def _active_lease_fd_root(self) -> int:
        if self._workspace._active_lease is None:
            raise _managed_root.CutoverSafetyError(
                "An active Market operation lease is required"
            )
        return self._workspace._active_lease.root_fd

    def _retained_lease_fd_root(self) -> int:
        if self._workspace._retained_lease is None:
            raise _managed_root.CutoverSafetyError(
                "A retained Market operation lease is required"
            )
        return self._workspace._retained_lease.root_fd

    @staticmethod
    def _directory_identity_evidence(directory_fd: int) -> dict[str, int]:
        directory = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Promotion location must be a real directory"
            )
        return {"device": directory.st_dev, "inode": directory.st_ino}

    def _market_location_identity(self, root_fd: int) -> dict[str, object]:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            directory = self._directory_identity_evidence(market_fd)
        finally:
            os.close(market_fd)
        return {
            "directory": directory,
            "payload": self._market_identity.market_tree_identity(root_fd),
        }

    @staticmethod
    def _manifest_file_set_sha256(manifest: dict[str, object]) -> str:
        entries = manifest.get("files")
        if not isinstance(entries, list):
            raise _managed_root.CutoverSafetyError(
                "Backup manifest file set is invalid"
            )
        return hashlib.sha256(JournalValidator._canonical_json(entries)).hexdigest()

    @staticmethod
    def _payload_manifest_entries(
        identity: dict[str, object],
    ) -> dict[str, tuple[int, str]]:
        database = identity.get("marketDuckdb")
        parquet = identity.get("parquetSha256")
        if not isinstance(database, dict) or not isinstance(parquet, dict):
            raise _managed_root.CutoverSafetyError(
                "Promotion payload identity is invalid"
            )
        entries: dict[str, tuple[int, str]] = {}

        def add(path: str, value: object) -> None:
            if (
                not isinstance(value, dict)
                or type(value.get("size")) is not int
                or not isinstance(value.get("sha256"), str)
            ):
                raise _managed_root.CutoverSafetyError(
                    "Promotion payload identity is invalid"
                )
            entries[path] = (cast(int, value["size"]), cast(str, value["sha256"]))

        add("market.duckdb", database)
        for path, value in parquet.items():
            if not isinstance(path, str):
                raise _managed_root.CutoverSafetyError(
                    "Promotion payload identity is invalid"
                )
            add(f"parquet/{path}", value)
        return entries

    @staticmethod
    def _payload_physical_identity_distinct(
        left: dict[str, object],
        right: dict[str, object],
    ) -> bool:
        def physical(value: object) -> tuple[int, int] | None:
            if not isinstance(value, dict):
                return None
            device = value.get("device")
            inode = value.get("inode")
            if type(device) is not int or type(inode) is not int:
                return None
            return cast(int, device), cast(int, inode)

        left_database = left.get("marketDuckdb")
        right_database = right.get("marketDuckdb")
        left_parquet = left.get("parquetSha256")
        right_parquet = right.get("parquetSha256")
        if not isinstance(left_parquet, dict) or not isinstance(right_parquet, dict):
            return False
        if set(left_parquet) != set(right_parquet):
            return False
        pairs = [(left_database, right_database)] + [
            (left_parquet[path], right_parquet[path]) for path in left_parquet
        ]
        return all(
            physical(left_value) is not None
            and physical(right_value) is not None
            and physical(left_value) != physical(right_value)
            for left_value, right_value in pairs
        )

    def _backup_payload_identity(self, backup_id: str) -> dict[str, object]:
        payload_fd = self._workspace.managed().open_dir(
            self._workspace._managed_relative(self._workspace.backups_root / backup_id / "payload")
        )
        try:
            return self._market_identity._market_payload_identity(payload_fd)
        finally:
            os.close(payload_fd)

    def _held_artifact_evidence(
        self,
        holding_fd: int,
        name: str,
    ) -> DetachedArtifactEvidence:
        if not name or "/" in name or name in {".", ".."}:
            raise _managed_root.CutoverSafetyError(
                "Promotion held artifact name is invalid"
            )
        entry = os.stat(name, dir_fd=holding_fd, follow_symlinks=False)
        if stat.S_ISREG(entry.st_mode):
            with _managed_root.ManagedRootFd(Path("."), os.dup(holding_fd)) as holding:
                file_stat, digest = self._market_identity.regular_file_identity(holding, Path(name))
            return DetachedArtifactEvidence(
                name=name,
                kind="regular",
                identity={
                    "device": file_stat.st_dev,
                    "inode": file_stat.st_ino,
                    "size": file_stat.st_size,
                    "sha256": digest,
                },
                directories={},
                files={},
            )
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Promotion held artifact must be regular or directory"
            )
        artifact_fd = os.open(name, _DIR_OPEN_FLAGS, dir_fd=holding_fd)
        directories: dict[str, dict[str, int]] = {}
        try:
            with _managed_root.ManagedRootFd(
                Path("."), os.dup(artifact_fd)
            ) as artifact:
                files: dict[str, dict[str, object]] = {}

                def walk(directory_fd: int, relative: Path) -> None:
                    directory = self._directory_identity_evidence(directory_fd)
                    directories[relative.as_posix() if relative.parts else "."] = (
                        directory
                    )
                    for child_name in sorted(os.listdir(directory_fd)):
                        child = os.stat(
                            child_name,
                            dir_fd=directory_fd,
                            follow_symlinks=False,
                        )
                        child_relative = relative / child_name
                        if stat.S_ISDIR(child.st_mode) and not stat.S_ISLNK(
                            child.st_mode
                        ):
                            child_fd = os.open(
                                child_name,
                                _DIR_OPEN_FLAGS,
                                dir_fd=directory_fd,
                            )
                            try:
                                walk(child_fd, child_relative)
                            finally:
                                os.close(child_fd)
                        elif stat.S_ISREG(child.st_mode):
                            file_stat, digest = self._market_identity.regular_file_identity(
                                artifact,
                                child_relative,
                            )
                            files[child_relative.as_posix()] = {
                                "device": file_stat.st_dev,
                                "inode": file_stat.st_ino,
                                "size": file_stat.st_size,
                                "sha256": digest,
                            }
                        else:
                            raise _managed_root.CutoverSafetyError(
                                "Promotion held artifact contains a symlink or special file"
                            )

                walk(artifact_fd, Path())
                return DetachedArtifactEvidence(
                    name=name,
                    kind="directory",
                    identity=cast(
                        dict[str, object],
                        self._directory_identity_evidence(artifact_fd),
                    ),
                    directories=directories,
                    files=files,
                )
        finally:
            os.close(artifact_fd)

    def _held_artifacts_evidence(
        self,
        holding_fd: int,
    ) -> tuple[DetachedArtifactEvidence, ...]:
        return tuple(
            self._held_artifact_evidence(holding_fd, name)
            for name in sorted(os.listdir(holding_fd))
        )

    def _verified_backup_evidence(
        self,
        backup_id: str,
        *,
        expected_payload: dict[str, object],
    ) -> tuple[str, str, dict[str, object]]:
        manifest_path = self._workspace.backups_root / backup_id / "manifest.json"
        manifest_bytes = self._workspace.managed().read_bytes(
            self._workspace._managed_relative(manifest_path)
        )
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Backup manifest is unreadable"
            ) from exc
        if not isinstance(manifest, dict):
            raise _managed_root.CutoverSafetyError("Backup manifest is invalid")
        entries = manifest.get("files")
        if not isinstance(entries, list):
            raise _managed_root.CutoverSafetyError(
                "Backup manifest file set is invalid"
            )
        actual: dict[str, tuple[int, str]] = {}
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("path"), str)
                or type(entry.get("bytes")) is not int
                or not isinstance(entry.get("sha256"), str)
            ):
                raise _managed_root.CutoverSafetyError(
                    "Backup manifest file entry is invalid"
                )
            path = cast(str, entry["path"])
            if path in actual:
                raise _managed_root.CutoverSafetyError(
                    "Backup manifest contains a duplicate path"
                )
            actual[path] = (
                cast(int, entry["bytes"]),
                cast(str, entry["sha256"]),
            )
        expected_entries = self._payload_manifest_entries(expected_payload)
        if any(actual.get(path) != identity for path, identity in expected_entries.items()):
            raise _managed_root.CutoverSafetyError("Backup payload identity mismatch")
        backup_payload_identity = self._backup_payload_identity(backup_id)
        if self._payload_manifest_entries(backup_payload_identity) != expected_entries:
            raise _managed_root.CutoverSafetyError(
                "Backup physical payload content mismatch"
            )
        if not self._payload_physical_identity_distinct(
            backup_payload_identity,
            expected_payload,
        ):
            raise _managed_root.CutoverSafetyError(
                "Backup physical payload identity is not distinct"
            )
        return (
            hashlib.sha256(manifest_bytes).hexdigest(),
            self._manifest_file_set_sha256(manifest),
            backup_payload_identity,
        )

    def _append_preparation_state(
        self,
        journal: PromotionJournal,
        state: PromotionState,
        identities: PromotionIdentityEvidence,
    ) -> PromotionJournalRecord:
        result = journal.append(state, identities=identities)
        if (
            result.status is PromotionAppendStatus.COMMITTED
            and result.record is not None
        ):
            return result.record
        if result.status is PromotionAppendStatus.INDETERMINATE:
            for lease in (self._workspace._active_lease, self._workspace._retained_lease):
                if lease is not None:
                    lease.unlock_on_release = False
                    lease.owns_fd = False
            raise _managed_root.CutoverSafetyError(
                f"Promotion journal append is indeterminate: {result.attempt_id}"
            )
        raise _managed_root.CutoverSafetyError(
            f"Promotion journal append was not committed: {result.attempt_id}"
        )
