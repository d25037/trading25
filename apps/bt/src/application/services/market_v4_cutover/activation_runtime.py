"""Exact ownership gates for one journal-bound activation runtime."""

from __future__ import annotations

from enum import StrEnum
import os
from pathlib import Path
import stat

from src.infrastructure.db.market import managed_root as _managed_root

from .backup import MarketBackupService
from .contracts import ActivationAttempt, MarketTreeIdentity
from .project_paths import repository_cutover_smoke_strategy_path
from .smoke import overlay_repository_cutover_smoke_strategy
from .workspace import CutoverWorkspace


_REQUIRED_RUNTIME_DIRECTORIES = frozenset(
    {"backtest", "config", "datasets", "strategies"}
)
_ALLOWED_RUNTIME_DIRECTORIES = _REQUIRED_RUNTIME_DIRECTORIES | frozenset(
    {"duckdb-tmp", "xdg-data-home"}
)
_ALLOWED_RUNTIME_FILES = frozenset(
    {"portfolio.db", "portfolio.db-journal", "portfolio.db-shm", "portfolio.db-wal"}
)


class ActivationRuntimePlacement(StrEnum):
    ABSENT = "absent"
    ACTIVE = "active"
    RETAINED = "retained"


class ActivationRuntimeOwnership:
    """Validate and atomically retire one exact attempt runtime."""

    def __init__(
        self,
        workspace: CutoverWorkspace,
        backups: MarketBackupService,
    ) -> None:
        self._workspace = workspace
        self._backups = backups

    def staging_root(self, attempt: ActivationAttempt) -> Path:
        return (
            self._workspace.operations_root
            / "staging"
            / attempt.report_id
            / "root"
        )

    @staticmethod
    def runtime_name(attempt: ActivationAttempt) -> str:
        return f".cutover-runtime-{attempt.report_id}"

    def runtime_root(self, attempt: ActivationAttempt) -> Path:
        return self._workspace.market_root / self.runtime_name(attempt)

    def runtime_template(self, attempt: ActivationAttempt) -> Path:
        return self.staging_root(attempt) / f"runtime-template-{attempt.report_id}"

    def retained_root(self, attempt: ActivationAttempt) -> Path:
        return (
            self._workspace.operations_root
            / "recovery-runtime-quarantine"
            / attempt.report_id
        )

    def _exists(self, path: Path) -> bool:
        relative = self._workspace._managed_relative(path)
        try:
            self._workspace.managed().stat(relative)
        except FileNotFoundError:
            return False
        return True

    def exists(self, attempt: ActivationAttempt) -> bool:
        return self._exists(self.runtime_root(attempt))

    def placement(self, attempt: ActivationAttempt) -> ActivationRuntimePlacement:
        active_exists = self.exists(attempt)
        retained_exists = self._exists(self.retained_root(attempt))
        if active_exists:
            self.assert_exact(attempt)
        if retained_exists:
            self.assert_retained_exact(attempt)
        if active_exists and retained_exists:
            raise _managed_root.CutoverSafetyError(
                "Activation recovery runtime ownership is ambiguous"
            )
        if active_exists:
            return ActivationRuntimePlacement.ACTIVE
        if retained_exists:
            return ActivationRuntimePlacement.RETAINED
        return ActivationRuntimePlacement.ABSENT

    def assert_exact(self, attempt: ActivationAttempt) -> None:
        """Prove path, shape, static content, and single-link runtime ownership."""

        self._assert_exact_at(attempt, self.runtime_root(attempt))

    def assert_retained_exact(self, attempt: ActivationAttempt) -> None:
        self._assert_exact_at(attempt, self.retained_root(attempt))

    def _assert_exact_at(self, attempt: ActivationAttempt, root: Path) -> None:
        runtime_relative = self._workspace._managed_relative(root)
        directories, files = self._tree_manifest(runtime_relative)
        top_directories = {path.parts[0] for path in directories if len(path.parts) == 1}
        top_files = {path.parts[0] for path in files if len(path.parts) == 1}
        if (
            not _REQUIRED_RUNTIME_DIRECTORIES <= top_directories
            or not top_directories <= _ALLOWED_RUNTIME_DIRECTORIES
            or not top_files <= _ALLOWED_RUNTIME_FILES
        ):
            self._runtime_mismatch()

        staging_relative = self._workspace._managed_relative(
            self.staging_root(attempt)
        )
        expected_config = self._workspace.managed().sha256(
            staging_relative / "config" / "default.yaml"
        )
        actual_config_files = {
            path: digest
            for path, digest in files.items()
            if path.parts[0] == "config"
        }
        actual_config_directories = {
            path for path in directories if path.parts[0] == "config"
        }
        if (
            actual_config_directories != {Path("config")}
            or actual_config_files
            != {Path("config/default.yaml"): expected_config}
        ):
            self._runtime_mismatch()

        expected_strategy_directories, expected_strategy_files = self._tree_manifest(
            staging_relative / "strategies"
        )
        expected_strategy_directories = {
            Path("strategies") / path for path in expected_strategy_directories
        }
        expected_strategy_directories.add(Path("strategies"))
        expected_strategy_directories.add(Path("strategies/production"))
        expected_strategy_files = {
            Path("strategies") / path: digest
            for path, digest in expected_strategy_files.items()
        }
        smoke_source = repository_cutover_smoke_strategy_path()
        smoke_stat = smoke_source.lstat()
        if (
            not stat.S_ISREG(smoke_stat.st_mode)
            or stat.S_ISLNK(smoke_stat.st_mode)
        ):
            raise _managed_root.CutoverSafetyError(
                "Canonical cutover smoke strategy is not a regular file"
            )
        expected_strategy_files[Path("strategies/production/cutover_smoke.yaml")] = (
            self._workspace._sha256(smoke_source)
        )
        actual_strategy_directories = {
            path for path in directories if path.parts[0] == "strategies"
        }
        actual_strategy_files = {
            path: digest
            for path, digest in files.items()
            if path.parts[0] == "strategies"
        }
        if (
            actual_strategy_directories != expected_strategy_directories
            or actual_strategy_files != expected_strategy_files
        ):
            self._runtime_mismatch()

    def active_matches_expected(self, attempt: ActivationAttempt) -> bool:
        """Compare active identity after the exact owned runtime alone is excluded."""

        active_relative = self._workspace._managed_relative(
            self._workspace.market_root
        )
        active_fd = self._workspace.managed().open_dir(active_relative)
        try:
            active_stat = os.fstat(active_fd)
        finally:
            os.close(active_fd)
        if dict(attempt.expected_active.directory) != {
            "device": active_stat.st_dev,
            "inode": active_stat.st_ino,
        }:
            return False

        runtime_name = self.runtime_name(attempt)
        entries: list[dict[str, object]] = []
        for source in self._workspace._source_files(self._workspace.market_root):
            relative = source.relative_to(self._workspace.market_root)
            if relative.parts[0] == runtime_name:
                continue
            managed_relative = self._workspace._managed_relative(source)
            source_stat = self._workspace.managed().stat(managed_relative)
            entries.append(
                {
                    "path": relative.as_posix(),
                    "bytes": source_stat.st_size,
                    "sha256": self._workspace.managed().sha256(managed_relative),
                }
            )
        expected_sha = attempt.expected_active.payload.get("marketTreeSha256")
        return bool(entries) and (
            self._backups._tree_entries_sha256(entries) == expected_sha
        )

    def assert_activation_identities(
        self,
        attempt: ActivationAttempt,
        quarantine: MarketTreeIdentity,
    ) -> None:
        if not self.active_matches_expected(attempt):
            raise _managed_root.CutoverSafetyError(
                "Activation recovery active identity differs outside exact runtime"
            )
        self._backups._assert_market_tree_identity(
            self._workspace.data_root / quarantine.path,
            quarantine,
        )

    def prepare(self, attempt: ActivationAttempt) -> None:
        runtime_root = self.runtime_root(attempt)
        staging_root = self.staging_root(attempt)
        self._workspace._assert_managed_target_absent(runtime_root)
        self._workspace._prepare_managed_directory(runtime_root, exist_ok=False)
        for relative in ("datasets", "backtest", "config"):
            self._workspace._prepare_managed_directory(
                runtime_root / relative, exist_ok=False
            )
        self._workspace._copy_regular_to_managed(
            staging_root / "config" / "default.yaml",
            runtime_root / "config" / "default.yaml",
        )
        self._workspace.managed().copy_tree_create(
            self._workspace._managed_relative(staging_root / "strategies"),
            self._workspace._managed_relative(runtime_root / "strategies"),
        )
        overlay_repository_cutover_smoke_strategy(self._workspace, runtime_root)

    def activate_for_smoke(
        self,
        attempt: ActivationAttempt,
        placement: ActivationRuntimePlacement,
    ) -> None:
        if placement is ActivationRuntimePlacement.ACTIVE:
            self.assert_exact(attempt)
            return
        if placement is ActivationRuntimePlacement.RETAINED:
            self._workspace._assert_managed_target_absent(self.runtime_root(attempt))
            self._workspace._secure_rename(
                self.retained_root(attempt),
                self.runtime_root(attempt),
            )
            if self.placement(attempt) is not ActivationRuntimePlacement.ACTIVE:
                raise _managed_root.CutoverSafetyError(
                    "Retained activation runtime was not atomically reactivated"
                )
            return
        self.prepare(attempt)
        if self.placement(attempt) is not ActivationRuntimePlacement.ACTIVE:
            raise _managed_root.CutoverSafetyError(
                "Fresh activation runtime placement is invalid"
            )

    def retire(self, attempt: ActivationAttempt) -> None:
        if self.placement(attempt) is not ActivationRuntimePlacement.ACTIVE:
            raise _managed_root.CutoverSafetyError(
                "Only one exact active runtime can be atomically retired"
            )
        retained_root = self.retained_root(attempt)
        self._workspace._prepare_managed_directory(
            retained_root.parent,
            exist_ok=True,
        )
        self._workspace._assert_managed_target_absent(retained_root)
        self._workspace._secure_rename(self.runtime_root(attempt), retained_root)
        if self.placement(attempt) is not ActivationRuntimePlacement.RETAINED:
            raise _managed_root.CutoverSafetyError(
                "Activation runtime retirement did not reach exact retained state"
            )

    def _tree_manifest(
        self,
        relative: Path,
    ) -> tuple[set[Path], dict[Path, str]]:
        root_fd = self._workspace.managed().open_dir(relative)
        directories: set[Path] = set()
        files: dict[Path, str] = {}

        def walk(directory_fd: int, prefix: Path) -> None:
            for name in sorted(os.listdir(directory_fd)):
                entry = os.stat(
                    name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                child = prefix / name
                if stat.S_ISDIR(entry.st_mode):
                    directories.add(child)
                    child_fd = os.open(
                        name,
                        os.O_RDONLY
                        | getattr(os, "O_DIRECTORY", 0)
                        | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=directory_fd,
                    )
                    try:
                        walk(child_fd, child)
                    finally:
                        os.close(child_fd)
                elif stat.S_ISREG(entry.st_mode) and entry.st_nlink == 1:
                    files[child] = self._workspace.managed().sha256(relative / child)
                else:
                    self._runtime_mismatch()

        try:
            walk(root_fd, Path())
        finally:
            os.close(root_fd)
        return directories, files

    @staticmethod
    def _runtime_mismatch() -> None:
        raise _managed_root.CutoverSafetyError(
            "Activation recovery runtime does not exactly match journaled evidence"
        )
