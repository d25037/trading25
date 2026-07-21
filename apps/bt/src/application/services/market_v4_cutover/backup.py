"""Focused Market v5 cutover responsibility module."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import time
from typing import cast, Protocol

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    ActivationAttempt,
    ActivationState,
    BackupResult,
    ImmutableJsonValue,
    MarketTreeIdentity,
    MarketSourceMetadata,
    RestoreResult,
    SmokeConfig,
)
from .activation_journal import ActivationJournalRepository
from .workspace import CutoverWorkspace


class BackupEvidence(Protocol):
    def root_fingerprint(self, root: Path) -> str: ...


class MarketBackupService:
    def __init__(
        self,
        workspace: CutoverWorkspace,
        evidence: BackupEvidence,
    ) -> None:
        self._workspace = workspace
        self._evidence = evidence

    def preflight(self) -> MarketSourceMetadata:
        with self._workspace.exclusive_operation():
            return self._preflight_under_lease()

    def _preflight_under_lease(self) -> MarketSourceMetadata:
        self._workspace._validate_active_roots()
        self._workspace.atomic_exchange.require_capability()
        self._workspace.runtime.assert_quiescent(self._workspace.data_root)
        metadata = self._workspace._checkpoint(
            guard_lease_fd=self._workspace.active_lease_fd()
        )
        source_bytes = sum(
            self._workspace.managed()
            .stat(self._workspace._managed_relative(path))
            .st_size
            for path in self._workspace._source_files(self._workspace.market_root)
        )
        required_bytes = max(source_bytes * 4, 1)
        if self._workspace.disk_free_bytes(self._workspace.data_root) < required_bytes:
            raise _managed_root.CutoverSafetyError(
                f"Insufficient free space: require at least {required_bytes} bytes"
            )
        return metadata

    def backup(self, backup_id: str) -> BackupResult:
        with self._workspace.exclusive_operation() as code_version:
            return self._backup_under_lease(backup_id, code_version=code_version)

    def _backup_under_lease(
        self,
        backup_id: str,
        *,
        code_version: str,
    ) -> BackupResult:
        backup_id = cast(str, self._workspace._validate_id(backup_id, label="backup"))
        self._preflight_under_lease()
        with self._workspace._market_identity_guard() as market_fd:
            with self._workspace.duckdb.checkpoint_snapshot(
                market_fd,
                "market.duckdb",
                guard_lease_fd=self._workspace.active_lease_fd(),
            ) as metadata:
                try:
                    wal_stat = self._workspace.managed().stat(
                        Path("market-timeseries/market.duckdb.wal")
                    )
                except FileNotFoundError:
                    pass
                else:
                    if not stat.S_ISREG(wal_stat.st_mode) or wal_stat.st_size > 0:
                        raise _managed_root.CutoverSafetyError(
                            "Nonempty or invalid DuckDB WAL remains before backup copy"
                        )
                return self._copy_backup_under_snapshot(
                    backup_id,
                    metadata,
                    code_version=code_version,
                )

    def _copy_backup_under_snapshot(
        self,
        backup_id: str,
        metadata: MarketSourceMetadata,
        *,
        code_version: str,
    ) -> BackupResult:
        self._workspace._prepare_managed_directory(
            self._workspace.backups_root, exist_ok=True
        )
        destination = self._workspace.backups_root / backup_id
        if destination.exists() or destination.is_symlink():
            raise _managed_root.CutoverSafetyError(
                f"Backup destination already exists: {backup_id}"
            )
        self._workspace._managed_mutation_hook("mkdir")
        self._workspace._prepare_managed_directory(destination, exist_ok=False)
        payload = destination / "payload"
        self._workspace._prepare_managed_directory(payload, exist_ok=False)
        entries: list[dict[str, object]] = []
        try:
            for source in self._workspace._source_files(self._workspace.market_root):
                self._workspace._assert_managed_directory(payload)
                relative = source.relative_to(self._workspace.market_root)
                target = payload / relative
                self._workspace._prepare_managed_directory(target.parent, exist_ok=True)
                self._workspace._assert_managed_target_absent(target)
                copied_bytes, copied_sha256 = self._workspace._copy_regular_to_managed(
                    source, target
                )
                entries.append(
                    {
                        "path": relative.as_posix(),
                        "bytes": copied_bytes,
                        "sha256": copied_sha256,
                    }
                )
            manifest = {
                "backupId": backup_id,
                "createdAt": self._workspace.now(),
                "codeVersion": code_version,
                "sourceRootFingerprint": self._evidence.root_fingerprint(
                    self._workspace.data_root
                ),
                "source": {
                    "schemaVersion": metadata.schema_version,
                    "stockPriceAdjustmentMode": metadata.adjustment_mode,
                },
                "activeMarketTreeSha256": self._tree_entries_sha256(entries),
                "files": entries,
            }
            manifest_path = destination / "manifest.json"
            self._workspace._assert_managed_target_absent(manifest_path)
            self._workspace._write_managed_file_create(
                manifest_path,
                (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode(),
            )
            self._workspace.managed().fsync_tree(
                self._workspace._managed_relative(payload)
            )
            self._workspace.managed().fsync_dir(
                self._workspace._managed_relative(destination)
            )
            self.verify_backup(backup_id)
            self._workspace.managed().chmod_tree(
                self._workspace._managed_relative(destination),
                directory_mode=0o500,
                file_mode=0o400,
            )
            self._workspace.managed().fsync_dir(
                self._workspace._managed_relative(self._workspace.backups_root)
            )
        except Exception:
            try:
                self._workspace.managed().chmod_tree(
                    self._workspace._managed_relative(destination),
                    directory_mode=0o700,
                    file_mode=0o600,
                )
                self._workspace.managed().remove_tree(
                    self._workspace._managed_relative(destination),
                    missing_ok=True,
                )
            except FileNotFoundError:
                pass
            raise
        return BackupResult(backup_id)

    def verify_backup(self, backup_id: str) -> BackupResult:
        with self._workspace.managed_root_scope():
            return self._verify_backup_managed(backup_id)

    def _verify_backup_managed(
        self,
        backup_id: str,
        *,
        require_current_root: bool = True,
    ) -> BackupResult:
        backup_id = cast(str, self._workspace._validate_id(backup_id, label="backup"))
        destination = self._workspace.backups_root / backup_id
        self._workspace._assert_managed_directory(destination)
        self._workspace._assert_managed_directory(destination / "payload")
        manifest_path = destination / "manifest.json"
        try:
            manifest_mode = (
                self._workspace.managed()
                .stat(self._workspace._managed_relative(manifest_path))
                .st_mode
            )
        except FileNotFoundError:
            raise _managed_root.CutoverSafetyError(
                f"Backup manifest is missing: {backup_id}"
            )
        if stat.S_ISLNK(manifest_mode) or not stat.S_ISREG(manifest_mode):
            raise _managed_root.CutoverSafetyError(
                f"Backup manifest is invalid: {backup_id}"
            )
        try:
            manifest = json.loads(
                self._workspace.managed()
                .read_bytes(self._workspace._managed_relative(manifest_path))
                .decode("utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Backup manifest is unreadable"
            ) from exc
        if manifest.get("backupId") != backup_id:
            raise _managed_root.CutoverSafetyError("Backup manifest ID mismatch")
        if require_current_root and manifest.get(
            "sourceRootFingerprint"
        ) != self._evidence.root_fingerprint(self._workspace.data_root):
            raise _managed_root.CutoverSafetyError(
                "Backup source root fingerprint mismatch"
            )
        entries = manifest.get("files")
        if not isinstance(entries, list) or not entries:
            raise _managed_root.CutoverSafetyError("Backup manifest has no files")
        if not all(isinstance(entry, dict) for entry in entries):
            raise _managed_root.CutoverSafetyError(
                "Backup manifest file entry is invalid"
            )
        typed_entries = cast(list[dict[str, object]], entries)
        if manifest.get("activeMarketTreeSha256") != self._tree_entries_sha256(
            typed_entries
        ):
            raise _managed_root.CutoverSafetyError(
                "Backup manifest Market tree identity mismatch"
            )
        expected_paths: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                raise _managed_root.CutoverSafetyError(
                    "Backup manifest file entry is invalid"
                )
            relative = Path(entry["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise _managed_root.CutoverSafetyError(
                    "Backup manifest contains unsafe path"
                )
            expected_paths.add(relative.as_posix())
            target = destination / "payload" / relative
            target_relative = self._workspace._managed_relative(target)
            try:
                target_stat = self._workspace.managed().stat(target_relative)
            except FileNotFoundError:
                raise _managed_root.CutoverSafetyError(
                    f"Backup file is missing: {relative.as_posix()}"
                )
            if not stat.S_ISREG(target_stat.st_mode):
                raise _managed_root.CutoverSafetyError(
                    f"Backup file is invalid: {relative.as_posix()}"
                )
            if target_stat.st_size != entry.get("bytes"):
                raise _managed_root.CutoverSafetyError(
                    f"Backup size mismatch: {relative.as_posix()}"
                )
            if self._workspace.managed().sha256(target_relative) != entry.get("sha256"):
                raise _managed_root.CutoverSafetyError(
                    f"Backup checksum mismatch: {relative.as_posix()}"
                )
        actual_paths = {
            path.relative_to(destination / "payload").as_posix()
            for path in self._workspace._source_files(destination / "payload")
        }
        if actual_paths != expected_paths:
            raise _managed_root.CutoverSafetyError("Backup file set mismatch")
        return BackupResult(backup_id)

    @staticmethod
    def _tree_entries_sha256(entries: list[dict[str, object]]) -> str:
        canonical = [
            {
                "path": entry.get("path"),
                "bytes": entry.get("bytes"),
                "sha256": entry.get("sha256"),
            }
            for entry in sorted(entries, key=lambda item: str(item.get("path", "")))
        ]
        return hashlib.sha256(
            json.dumps(
                canonical,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
        ).hexdigest()

    def _market_tree_sha256(self, root: Path) -> str:
        entries = []
        for source in self._workspace._source_files(root):
            relative = source.relative_to(root)
            managed_relative = self._workspace._managed_relative(source)
            source_stat = self._workspace.managed().stat(managed_relative)
            entries.append(
                {
                    "path": relative.as_posix(),
                    "bytes": source_stat.st_size,
                    "sha256": self._workspace.managed().sha256(managed_relative),
                }
            )
        if not entries:
            raise _managed_root.CutoverSafetyError("Market tree is empty")
        return self._tree_entries_sha256(entries)

    def _market_tree_identity(
        self,
        root: Path,
        *,
        identity_path: str | None = None,
        payload: dict[str, ImmutableJsonValue] | None = None,
    ) -> MarketTreeIdentity:
        """Capture one exact descriptor and payload identity under the active lease."""

        root = self._workspace._assert_managed_directory(root)
        root_fd = self._workspace.managed().open_dir(
            self._workspace._managed_relative(root)
        )
        try:
            root_stat = os.fstat(root_fd)
            identity_payload = dict(payload or {})
            identity_payload["marketTreeSha256"] = self._market_tree_sha256(root)
            current_fd = self._workspace.managed().open_dir(
                self._workspace._managed_relative(root)
            )
            try:
                current_stat = os.fstat(current_fd)
            finally:
                os.close(current_fd)
            if (root_stat.st_dev, root_stat.st_ino) != (
                current_stat.st_dev,
                current_stat.st_ino,
            ):
                raise _managed_root.CutoverSafetyError(
                    "Market tree identity changed during capture"
                )
        finally:
            os.close(root_fd)
        return MarketTreeIdentity(
            path=(
                identity_path
                if identity_path is not None
                else root.relative_to(self._workspace.data_root).as_posix()
            ),
            directory={"device": root_stat.st_dev, "inode": root_stat.st_ino},
            payload=identity_payload,
        )

    def _assert_market_tree_identity(
        self,
        root: Path,
        expected: MarketTreeIdentity,
    ) -> None:
        actual = self._market_tree_identity(
            root,
            identity_path=expected.path,
            payload=dict(expected.payload),
        )
        if actual != expected:
            raise _managed_root.CutoverSafetyError(
                f"Market tree identity mismatch: {expected.path}"
            )

    def _prepare_activation_attempt(
        self,
        *,
        report_id: str,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        code_version: str,
        staged_market: Path,
        evidence: dict[str, object],
        backup_market_tree_sha256: str,
    ) -> ActivationAttempt:
        source = self._market_tree_identity(
            staged_market,
            payload=cast(
                dict[str, ImmutableJsonValue],
                {"schemaCoverage": evidence},
            ),
        )
        staged = MarketTreeIdentity(source.path, source.directory, source.payload)
        active_before = self._market_tree_identity(self._workspace.market_root)
        backup = self._market_tree_identity(
            self._workspace.backups_root / backup_id / "payload"
        )
        if any(
            identity.payload.get("marketTreeSha256")
            != backup_market_tree_sha256
            for identity in (active_before, backup)
        ):
            raise _managed_root.CutoverSafetyError(
                "Activation active or backup identity changed before journal prepare"
            )
        expected_active = MarketTreeIdentity(
            self._workspace.market_root.relative_to(
                self._workspace.data_root
            ).as_posix(),
            staged.directory,
            staged.payload,
        )
        return ActivationAttempt(
            report_id,
            rehearsal_report_id,
            backup_id,
            code_version,
            config,
            source,
            staged,
            active_before,
            backup,
            expected_active,
        )

    def _prepare_journaled_activation(
        self,
        *,
        journal: ActivationJournalRepository,
        report_id: str,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        code_version: str,
        staging_root: Path,
        evidence: dict[str, object],
        backup_market_tree_sha256: str,
    ) -> tuple[ActivationAttempt, Path, MarketTreeIdentity]:
        if (
            self._assert_active_matches_backup_under_lease(backup_id)
            != backup_market_tree_sha256
        ):
            raise _managed_root.CutoverSafetyError(
                "Selected backup identity changed before activation"
            )
        staged_market = staging_root / "market-timeseries"
        quarantine = (
            self._workspace.operations_root
            / "quarantine"
            / f"pre-cutover-{report_id}"
        )
        self._workspace._prepare_staged_activation(
            staged_market,
            quarantine=quarantine,
        )
        attempt = self._prepare_activation_attempt(
            report_id=report_id,
            rehearsal_report_id=rehearsal_report_id,
            backup_id=backup_id,
            config=config,
            code_version=code_version,
            staged_market=staged_market,
            evidence=evidence,
            backup_market_tree_sha256=backup_market_tree_sha256,
        )
        quarantine_identity = self._quarantine_identity(attempt, quarantine)
        journal.append(attempt, ActivationState.PREPARED)
        journal.append(attempt, ActivationState.EXCHANGE_STARTED)
        return attempt, quarantine, quarantine_identity

    def _quarantine_identity(
        self,
        attempt: ActivationAttempt,
        quarantine: Path,
    ) -> MarketTreeIdentity:
        return MarketTreeIdentity(
            quarantine.relative_to(self._workspace.data_root).as_posix(),
            attempt.active_before.directory,
            attempt.active_before.payload,
        )

    def _assert_activation_identities(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
    ) -> None:
        self._assert_market_tree_identity(
            self._workspace.market_root,
            attempt.expected_active,
        )
        self._assert_market_tree_identity(
            self._workspace.data_root / quarantine.path,
            quarantine,
        )

    def _assert_active_matches_backup_under_lease(self, backup_id: str) -> str:
        """Bind activation to the exact active tree captured by its backup."""

        self._verify_backup_managed(backup_id)
        manifest_path = self._workspace.backups_root / backup_id / "manifest.json"
        manifest = json.loads(
            self._workspace.managed()
            .read_bytes(self._workspace._managed_relative(manifest_path))
            .decode("utf-8")
        )
        expected = manifest.get("activeMarketTreeSha256")
        actual = self._market_tree_sha256(self._workspace.market_root)
        if not isinstance(expected, str) or actual != expected:
            raise _managed_root.CutoverSafetyError(
                "Active Market tree no longer exactly matches the selected backup"
            )
        return expected

    def restore(self, backup_id: str | None) -> RestoreResult:
        with self._workspace.exclusive_operation():
            return self._restore_under_lease(backup_id)

    def _restore_under_lease(self, backup_id: str | None) -> RestoreResult:
        backup_id = cast(str, self._workspace._validate_id(backup_id, label="backup"))
        self._workspace.runtime.assert_quiescent(self._workspace.data_root)
        try:
            active_database_stat = self._workspace.managed().stat(
                Path("market-timeseries/market.duckdb")
            )
        except FileNotFoundError:
            active_database_stat = None
        if active_database_stat is not None:
            if not stat.S_ISREG(active_database_stat.st_mode):
                raise _managed_root.CutoverSafetyError(
                    "Active market.duckdb is invalid"
                )
            self._workspace._checkpoint(
                guard_lease_fd=self._workspace.active_lease_fd()
            )
        try:
            wal_stat = self._workspace.managed().stat(
                Path("market-timeseries/market.duckdb.wal")
            )
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(wal_stat.st_mode) or wal_stat.st_size > 0:
                raise _managed_root.CutoverSafetyError(
                    "Cannot restore over a nonempty or invalid DuckDB WAL"
                )
        self._verify_backup_managed(backup_id, require_current_root=False)
        backup_payload = self._workspace.backups_root / backup_id / "payload"
        self._workspace._assert_managed_directory(backup_payload)
        stage = self._workspace.data_root / f"market-timeseries.restore-{backup_id}"
        try:
            self._workspace.managed().stat(self._workspace._managed_relative(stage))
        except FileNotFoundError:
            pass
        else:
            raise _managed_root.CutoverSafetyError(
                "Restore staging destination already exists"
            )
        self._workspace._assert_managed_target_absent(stage)
        self._workspace.managed().copy_tree_create(
            self._workspace._managed_relative(backup_payload),
            self._workspace._managed_relative(stage),
        )
        self._workspace._assert_managed_directory(stage)
        self._workspace.managed().chmod_tree(
            self._workspace._managed_relative(stage),
            directory_mode=0o700,
            file_mode=0o600,
        )
        self._verify_tree_copy(backup_payload, stage)
        self._workspace.managed().fsync_tree(self._workspace._managed_relative(stage))
        quarantine_relative: str | None = None
        try:
            self._workspace.managed().stat(
                self._workspace._managed_relative(self._workspace.market_root)
            )
            market_exists = True
        except FileNotFoundError:
            market_exists = False
        if market_exists:
            quarantine_root = self._workspace.operations_root / "quarantine"
            self._workspace._prepare_managed_directory(quarantine_root, exist_ok=True)
            quarantine = quarantine_root / (
                f"failed-{backup_id}-{time.time_ns()}-{secrets.token_hex(4)}"
            )
            self._workspace._assert_managed_target_absent(quarantine)
            self._workspace._validate_active_roots()
            self._workspace._assert_managed_directory(quarantine_root)
            self._workspace._assert_managed_directory(stage)
            try:
                self._workspace.atomic_exchange.exchange(
                    self._workspace.managed(),
                    self._workspace._managed_relative(self._workspace.market_root),
                    self._workspace._managed_relative(stage),
                )
            except Exception as exc:
                raise _managed_root.CutoverSafetyError(
                    "Atomic restore activation failed; original active tree is unchanged"
                ) from exc
            self._verify_tree_copy(backup_payload, self._workspace.market_root)
            try:
                self._workspace._secure_rename(stage, quarantine)
            except Exception:
                # The exact backup is already active. Preserve the displaced tree at
                # its descriptor-confined staging path for operator diagnosis.
                quarantine_relative = stage.relative_to(
                    self._workspace.data_root
                ).as_posix()
            else:
                quarantine_relative = quarantine.relative_to(
                    self._workspace.data_root
                ).as_posix()
        else:
            self._workspace._secure_rename(stage, self._workspace.market_root)
            self._verify_tree_copy(backup_payload, self._workspace.market_root)
        self._verify_backup_managed(backup_id, require_current_root=False)
        return RestoreResult(backup_id, quarantine_relative)

    def _verify_tree_copy(self, source: Path, target: Path) -> None:
        source_files = self._workspace._source_files(source)
        target_files = self._workspace._source_files(target)
        source_relatives = {path.relative_to(source) for path in source_files}
        target_relatives = {path.relative_to(target) for path in target_files}
        if source_relatives != target_relatives:
            raise _managed_root.CutoverSafetyError("Restore staging file set mismatch")
        for relative in source_relatives:
            source_file = source / relative
            target_file = target / relative
            source_relative = self._workspace._managed_relative(source_file)
            target_relative = self._workspace._managed_relative(target_file)
            source_stat = self._workspace.managed().stat(source_relative)
            target_stat = self._workspace.managed().stat(target_relative)
            if (
                source_stat.st_size != target_stat.st_size
                or self._workspace.managed().sha256(source_relative)
                != self._workspace.managed().sha256(target_relative)
            ):
                raise _managed_root.CutoverSafetyError(
                    f"Restore staging checksum mismatch: {relative.as_posix()}"
                )
