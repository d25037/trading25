"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import re
import secrets
import stat
import time
from typing import Callable, Iterator

from src.infrastructure.db.market import managed_root as _managed_root
from src.infrastructure.db.market import (
    market_operation_lease as _market_operation_lease,
)
from .backup import BackupMixin
from .smoke import SmokeMixin
from .rehearsal import RehearsalMixin
from .full_rehearsal import FullRehearsalMixin
from .promotion_eligibility import PromotionEligibilityMixin
from .promotion_evidence import PromotionEvidenceMixin
from .promotion_artifacts import PromotionArtifactsMixin
from .promotion_reports import PromotionReportsMixin
from .promotion_cleanup import PromotionCleanupMixin
from .promotion_rollback import PromotionRollbackMixin
from .promotion_recovery import PromotionRecoveryMixin
from .promotion import PromotionMixin
from .promotion_transaction import PromotionTransactionMixin
from .activation import ActivationMixin
from .duckdb_service import DuckDbServiceMixin
from .rebuild import RebuildMixin
from .reports import ReportsMixin
from .leases import LeaseMixin
from . import filesystem
from .contracts import (
    AtomicExchange,
    DuckDbAdapter,
    MarketSourceMetadata,
    RuntimeAdapter,
)
from .filesystem import _FILE_NOFOLLOW, DarwinAtomicExchange
from .errors import WorkerShutdownError


class MarketV4CutoverService(
    LeaseMixin,
    BackupMixin,
    SmokeMixin,
    RehearsalMixin,
    FullRehearsalMixin,
    PromotionEligibilityMixin,
    PromotionEvidenceMixin,
    PromotionArtifactsMixin,
    PromotionReportsMixin,
    PromotionCleanupMixin,
    PromotionRollbackMixin,
    PromotionRecoveryMixin,
    PromotionMixin,
    PromotionTransactionMixin,
    ActivationMixin,
    DuckDbServiceMixin,
    RebuildMixin,
    ReportsMixin,
):
    def __init__(
        self,
        data_root: Path,
        *,
        duckdb: DuckDbAdapter,
        runtime: RuntimeAdapter,
        disk_free_bytes: Callable[[Path], int],
        now: Callable[[], str],
        code_version: Callable[[], str],
        atomic_exchange: AtomicExchange | None = None,
    ) -> None:
        self.data_root = _managed_root.lexical_absolute(data_root)
        self.duckdb = duckdb
        self.runtime = runtime
        self.disk_free_bytes = disk_free_bytes
        self.now = now
        self.code_version = code_version
        self.atomic_exchange = (
            DarwinAtomicExchange() if atomic_exchange is None else atomic_exchange
        )
        self._active_lease: _market_operation_lease.MarketOperationLease | None = None
        self._retained_lease: _market_operation_lease.MarketOperationLease | None = None
        self._active_code_version: str | None = None
        self._managed_root_fd: _managed_root.ManagedRootFd | None = None
        self._report_publish_hook: Callable[[str], None] = lambda _stage: None
        self._managed_mutation_hook: Callable[[str], None] = lambda _stage: None
        self._rename_at_hook: Callable[[Path, Path], None] = lambda _source, _target: (
            None
        )
        self._promotion_boundary_hook: Callable[[str], None] = lambda _stage: None

    @property
    def market_root(self) -> Path:
        return self.data_root / "market-timeseries"

    @property
    def operations_root(self) -> Path:
        return self.data_root / "operations" / "market-v4-cutover"

    @property
    def backups_root(self) -> Path:
        return self.operations_root / "backups"

    def _managed_path(self, path: Path) -> Path:
        path = _managed_root.lexical_absolute(path)
        try:
            path.relative_to(self.data_root)
        except ValueError as exc:
            raise _managed_root.CutoverSafetyError(
                "Managed operation path escapes the data root"
            ) from exc
        return path

    def _managed_relative(self, path: Path) -> Path:
        path = self._managed_path(path)
        relative = path.relative_to(self.data_root)
        if not relative.parts:
            raise _managed_root.CutoverSafetyError(
                "Managed operation must be below the data root"
            )
        return relative

    def _assert_managed_directory(self, path: Path) -> Path:
        path = self._managed_path(path)
        if path == self.data_root:
            os.fstat(self._managed().fd)
            return path
        fd = self._managed().open_dir(self._managed_relative(path))
        os.close(fd)
        return path

    def _prepare_managed_directory(
        self,
        path: Path,
        *,
        exist_ok: bool,
    ) -> Path:
        path = self._managed_path(path)
        try:
            fd = self._managed().open_dir(
                self._managed_relative(path),
                create=True,
                exclusive_leaf=not exist_ok,
            )
        except FileExistsError as exc:
            raise _managed_root.CutoverSafetyError(
                "Managed operation destination already exists"
            ) from exc
        os.close(fd)
        return path

    def _assert_managed_target_absent(self, path: Path) -> Path:
        path = self._managed_path(path)
        parent, name = self._managed().open_parent(self._managed_relative(path))
        try:
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            os.close(parent)
            return path
        os.close(parent)
        raise _managed_root.CutoverSafetyError(
            "Managed operation destination already exists"
        )

    def _secure_rename(self, source: Path, target: Path) -> None:
        source_parent, source_name = self._managed().open_parent(
            self._managed_relative(source)
        )
        target_parent, target_name = self._managed().open_parent(
            self._managed_relative(target)
        )
        try:
            self._rename_at_hook(source, target)
            current_source_parent, _ = self._managed().open_parent(
                self._managed_relative(source)
            )
            try:
                current_target_parent, _ = self._managed().open_parent(
                    self._managed_relative(target)
                )
                try:
                    for retained, current in (
                        (source_parent, current_source_parent),
                        (target_parent, current_target_parent),
                    ):
                        retained_stat = os.fstat(retained)
                        current_stat = os.fstat(current)
                        if (retained_stat.st_dev, retained_stat.st_ino) != (
                            current_stat.st_dev,
                            current_stat.st_ino,
                        ):
                            raise _managed_root.CutoverSafetyError(
                                "Managed rename parent identity changed"
                            )
                finally:
                    os.close(current_target_parent)
            finally:
                os.close(current_source_parent)
            filesystem._rename_exclusive_at(
                source_parent,
                source_name,
                target_parent,
                target_name,
            )
            os.fsync(source_parent)
            if target_parent != source_parent:
                os.fsync(target_parent)
        finally:
            os.close(source_parent)
            os.close(target_parent)

    def _require_code_identity(self) -> str:
        identity = self.code_version().strip()
        if (
            not identity
            or identity == "unknown"
            or identity.endswith("-dirty")
            or re.fullmatch(r"[0-9a-f]{7,64}", identity) is None
        ):
            raise _managed_root.CutoverSafetyError(
                "A clean immutable git code identity is required"
            )
        return identity

    def _require_unchanged_code_identity(self, expected: str) -> None:
        if self._require_code_identity() != expected:
            raise _managed_root.CutoverSafetyError(
                "Code identity changed during operation"
            )

    def _validate_active_roots(self) -> None:
        _managed_root.assert_market_managed_root_safe(self.data_root, self.market_root)

    def _assert_current_data_root_identity(self) -> None:
        retained = os.fstat(self._managed().fd)
        try:
            current = self.data_root.lstat()
        except FileNotFoundError as exc:
            raise _managed_root.CutoverSafetyError(
                "Active data root pathname disappeared"
            ) from exc
        if stat.S_ISLNK(current.st_mode) or (
            retained.st_dev,
            retained.st_ino,
        ) != (current.st_dev, current.st_ino):
            raise _managed_root.CutoverSafetyError(
                "Active data root pathname identity changed"
            )

    def _assert_managed_directory_identity(
        self,
        path: Path,
        expected: os.stat_result,
    ) -> None:
        current_fd = self._managed().open_dir(self._managed_relative(path))
        try:
            current = os.fstat(current_fd)
        finally:
            os.close(current_fd)
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
            raise _managed_root.CutoverSafetyError(
                "Managed directory pathname identity changed"
            )

    @staticmethod
    def _remove_market_runtime(market_fd: int, runtime_name: str) -> None:
        with _managed_root.ManagedRootFd(Path("."), os.dup(market_fd)) as market:
            market.remove_tree(Path(runtime_name))

    def _activate_staged_market(self, staged_market: Path, operation_id: str) -> Path:
        self._assert_current_data_root_identity()
        self._validate_active_roots()
        self._assert_managed_directory(staged_market)
        quarantine_root = self.operations_root / "quarantine"
        self._prepare_managed_directory(quarantine_root, exist_ok=True)
        quarantine = quarantine_root / (
            f"pre-cutover-{operation_id}-{time.time_ns()}-{secrets.token_hex(4)}"
        )
        self._assert_managed_target_absent(quarantine)
        self._secure_rename(self.market_root, quarantine)
        try:
            self._secure_rename(staged_market, self.market_root)
        except Exception as exc:
            try:
                self._secure_rename(quarantine, self.market_root)
            except Exception as rollback_exc:
                raise _managed_root.CutoverSafetyError(
                    "Staged Market activation and active-tree rollback failed"
                ) from rollback_exc
            raise _managed_root.CutoverSafetyError(
                "Staged Market activation failed; active tree was rolled back"
            ) from exc
        return quarantine

    @staticmethod
    def _validate_id(value: str | None, *, label: str) -> str:
        if not value:
            raise _managed_root.CutoverSafetyError(
                f"An explicit {label} ID is required"
            )
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
            raise _managed_root.CutoverSafetyError(f"Invalid {label} ID")
        return value

    @staticmethod
    def _wal_path(db_path: Path) -> Path:
        return Path(f"{db_path}.wal")

    @contextmanager
    def _market_identity_guard(self) -> Iterator[int]:
        market_relative = Path("market-timeseries")
        retained = self._managed().open_dir(market_relative)
        try:
            retained_stat = os.fstat(retained)
            database_stat = os.stat(
                "market.duckdb",
                dir_fd=retained,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(database_stat.st_mode):
                raise _managed_root.CutoverSafetyError(
                    "market.duckdb must be a regular file"
                )
            yield retained
            current = self._managed().open_dir(market_relative)
            try:
                current_stat = os.fstat(current)
                current_database_stat = os.stat(
                    "market.duckdb",
                    dir_fd=current,
                    follow_symlinks=False,
                )
                if (retained_stat.st_dev, retained_stat.st_ino) != (
                    current_stat.st_dev,
                    current_stat.st_ino,
                ) or (database_stat.st_dev, database_stat.st_ino) != (
                    current_database_stat.st_dev,
                    current_database_stat.st_ino,
                ):
                    raise _managed_root.CutoverSafetyError(
                        "Market path identity changed during DuckDB operation"
                    )
            finally:
                os.close(current)
        finally:
            os.close(retained)

    def _source_files(self, root: Path) -> list[Path]:
        if self._managed_root_fd is not None:
            try:
                root_relative = self._managed_relative(root)
            except _managed_root.CutoverSafetyError:
                pass
            else:
                files: list[Path] = []
                for relative, entry_stat in self._managed().regular_files(
                    root_relative
                ):
                    if (
                        relative.as_posix() == "market.duckdb.wal"
                        and entry_stat.st_size == 0
                    ):
                        continue
                    files.append(root / relative)
                if root / "market.duckdb" not in files:
                    raise _managed_root.CutoverSafetyError("market.duckdb is missing")
                return files
        if not root.is_dir():
            raise _managed_root.CutoverSafetyError(
                "Market time-series directory is missing"
            )
        files: list[Path] = []
        for path in sorted(root.rglob("*")):
            if path == self._wal_path(root / "market.duckdb"):
                if path.is_file() and path.stat().st_size == 0:
                    continue
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise _managed_root.CutoverSafetyError(
                    f"Backup source contains symlink: {path.name}"
                )
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                raise _managed_root.CutoverSafetyError(
                    f"Backup source contains special file: {path.name}"
                )
            files.append(path)
        db_path = root / "market.duckdb"
        if db_path not in files:
            raise _managed_root.CutoverSafetyError("market.duckdb is missing")
        return files

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _write_all(fd: int, payload: bytes) -> None:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]

    def _write_managed_file_create(self, target: Path, payload: bytes) -> None:
        relative = self._managed_relative(target)
        fd = self._managed().open_regular(
            relative,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        try:
            self._write_all(fd, payload)
            os.fsync(fd)
        except Exception:
            os.close(fd)
            self._managed().unlink(relative, missing_ok=True)
            raise
        os.close(fd)

    def _copy_regular_to_managed(self, source: Path, target: Path) -> tuple[int, str]:
        try:
            source_relative = self._managed_relative(source)
        except _managed_root.CutoverSafetyError:
            source_fd = os.open(source, os.O_RDONLY | _FILE_NOFOLLOW)
            if not stat.S_ISREG(os.fstat(source_fd).st_mode):
                os.close(source_fd)
                raise _managed_root.CutoverSafetyError(
                    "Copy source is not a regular file"
                )
        else:
            source_fd = self._managed().open_regular(source_relative, os.O_RDONLY)
        target_relative = self._managed_relative(target)
        digest = hashlib.sha256()
        total_bytes = 0
        try:
            if not stat.S_ISREG(os.fstat(source_fd).st_mode):
                raise _managed_root.CutoverSafetyError(
                    "Backup source is not a regular file"
                )
            self._managed_mutation_hook("copy")
            target_fd = self._managed().open_regular(
                target_relative,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            try:
                while chunk := os.read(source_fd, 1024 * 1024):
                    digest.update(chunk)
                    total_bytes += len(chunk)
                    self._write_all(target_fd, chunk)
                os.fsync(target_fd)
            except Exception:
                os.close(target_fd)
                self._managed().unlink(target_relative, missing_ok=True)
                raise
            os.close(target_fd)
        finally:
            os.close(source_fd)
        return total_bytes, digest.hexdigest()

    def _checkpoint(self, *, guard_lease_fd: int) -> MarketSourceMetadata:
        try:
            with self._market_identity_guard() as market_fd:
                metadata = self.duckdb.checkpoint_exclusive(
                    market_fd,
                    "market.duckdb",
                    guard_lease_fd=guard_lease_fd,
                )
        except WorkerShutdownError:
            raise
        except Exception as exc:
            raise _managed_root.CutoverSafetyError(
                "Could not prove an exclusive writable DuckDB checkpoint"
            ) from exc
        try:
            wal_stat = self._managed().stat(Path("market-timeseries/market.duckdb.wal"))
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(wal_stat.st_mode) or wal_stat.st_size > 0:
                raise _managed_root.CutoverSafetyError(
                    "Nonempty or invalid DuckDB WAL remains after checkpoint"
                )
        return metadata
