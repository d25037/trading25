"""Gated Market v4 cutover workflow."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import ctypes
import errno
from enum import StrEnum
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import socket
import stat
import subprocess
import sys
import time
from typing import BinaryIO, Callable, cast, ContextManager, Iterator, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from src.infrastructure.db.market.valuation_queries import (
    get_adjusted_metrics_snapshot,
    get_adjusted_metrics_source_diagnostics,
)


class CutoverSafetyError(RuntimeError):
    """A fail-closed cutover safety gate rejected the operation."""


class RuntimeStopError(CutoverSafetyError):
    """An owned runtime stop failed, with an explicit join verdict."""

    def __init__(self, message: str, *, process_joined: bool) -> None:
        super().__init__(message)
        self.process_joined = process_joined


class WorkerShutdownError(CutoverSafetyError):
    """A directory-bound helper cleanup failed, with an explicit join verdict."""

    def __init__(self, message: str, *, process_joined: bool) -> None:
        super().__init__(message)
        self.process_joined = process_joined


class RetainedMarketMutationError(CutoverSafetyError):
    """The retained Market DB or Parquet identity changed during smoke."""


class PromotionState(StrEnum):
    """Durable states in the retained Market promotion transaction."""

    VALIDATED = "validated"
    RUNTIMES_DETACHED = "runtimes_detached"
    PREPARED = "prepared"
    EXCHANGED = "exchanged"
    QUARANTINED = "quarantined"
    ACTIVE_SMOKE_PASSED = "active_smoke_passed"
    CLEANUP_STAGED = "cleanup_staged"
    REPORT_PERSISTED = "report_persisted"
    COMMITTED = "committed"
    EXCHANGED_BACK = "exchanged_back"
    ROLLED_BACK = "rolled_back"
    ROLLBACK_DEFERRED = "rollback_deferred_with_lease_held"


@dataclass(frozen=True)
class PromotionIdentityEvidence:
    """Exact immutable and current-location identities for one journal state."""

    active_before_directory: dict[str, int]
    active_before_payload: dict[str, object]
    retained_v4_directory: dict[str, int]
    retained_v4_payload: dict[str, object]
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    active_current: dict[str, object] | None
    retained_current: dict[str, object] | None
    quarantine_current: dict[str, object] | None
    holding_current: dict[str, object] | None
    detached_runtime_names: tuple[str, ...]
    detached_artifacts: tuple[dict[str, object], ...] = ()
    rollback_mode: str | None = None
    promotion_report_sha256: str | None = None


@dataclass(frozen=True)
class PromotionJournalRecord:
    """Typed representation of one validated durable journal record."""

    sequence: int
    state: PromotionState
    operation_id: str
    identities: PromotionIdentityEvidence
    created_at: str


class PromotionAppendStatus(StrEnum):
    """Exact durability outcome for one promotion journal append attempt."""

    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True)
class PromotionAppendResult:
    """Result of a promotion journal append or same-ID recovery."""

    status: PromotionAppendStatus
    record: PromotionJournalRecord | None
    attempt_id: str


@dataclass(frozen=True)
class _PromotionJournalAuthorization:
    secret: object
    operation_id: str
    attempt_id: str
    sequence: int
    candidate_sha256: str
    resolution_sha256: str
    record_directory: tuple[int, int]
    control_directory: tuple[int, int]
    record_files: tuple[tuple[str, int, int, int, str], ...]
    control_files: tuple[tuple[str, int, int, int, str], ...]


_DIR_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def _safe_relative_parts(relative: Path) -> tuple[str, ...]:
    if relative.is_absolute() or not relative.parts:
        raise CutoverSafetyError("Managed path must be a non-empty relative path")
    if any(part in {"", ".", ".."} or "/" in part for part in relative.parts):
        raise CutoverSafetyError("Managed path contains an unsafe component")
    return relative.parts


@dataclass
class ManagedRootFd:
    """A retained data-root descriptor used for symlink-safe relative mutation."""

    path: Path
    fd: int

    @classmethod
    def open(cls, path: Path) -> ManagedRootFd:
        path = _lexical_absolute(path)
        _assert_safe_directory_chain(path)
        fd = os.open("/", _DIR_OPEN_FLAGS)
        try:
            for component in path.parts[1:]:
                child = os.open(component, _DIR_OPEN_FLAGS, dir_fd=fd)
                os.close(fd)
                fd = child
            fd_stat = os.fstat(fd)
            path_stat = path.lstat()
            if (
                not stat.S_ISDIR(fd_stat.st_mode)
                or stat.S_ISLNK(path_stat.st_mode)
                or (fd_stat.st_dev, fd_stat.st_ino)
                != (path_stat.st_dev, path_stat.st_ino)
            ):
                raise CutoverSafetyError("Managed data-root descriptor identity mismatch")
        except Exception:
            os.close(fd)
            raise
        return cls(path, fd)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self) -> ManagedRootFd:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def open_dir(
        self,
        relative: Path,
        *,
        create: bool = False,
        exclusive_leaf: bool = False,
    ) -> int:
        parts = _safe_relative_parts(relative)
        current = os.dup(self.fd)
        try:
            for index, part in enumerate(parts):
                leaf = index == len(parts) - 1
                try:
                    child = os.open(part, _DIR_OPEN_FLAGS, dir_fd=current)
                    if leaf and exclusive_leaf:
                        os.close(child)
                        raise FileExistsError(errno.EEXIST, "directory exists", part)
                except FileNotFoundError:
                    if not create:
                        raise
                    os.mkdir(part, 0o700, dir_fd=current)
                    child = os.open(part, _DIR_OPEN_FLAGS, dir_fd=current)
                child_stat = os.fstat(child)
                if not stat.S_ISDIR(child_stat.st_mode):
                    os.close(child)
                    raise CutoverSafetyError("Managed component is not a directory")
                os.close(current)
                current = child
            return current
        except OSError as exc:
            os.close(current)
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise CutoverSafetyError(
                    "Managed directory traversal encountered a symlink"
                ) from exc
            raise
        except Exception:
            os.close(current)
            raise

    def open_parent(self, relative: Path, *, create: bool = False) -> tuple[int, str]:
        parts = _safe_relative_parts(relative)
        if len(parts) == 1:
            return os.dup(self.fd), parts[0]
        return self.open_dir(Path(*parts[:-1]), create=create), parts[-1]

    def open_regular(self, relative: Path, flags: int, mode: int = 0o600) -> int:
        parent, name = self.open_parent(relative)
        try:
            fd = os.open(name, flags | _FILE_NOFOLLOW, mode, dir_fd=parent)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                os.close(fd)
                raise CutoverSafetyError("Managed file is not regular")
            return fd
        finally:
            os.close(parent)

    def stat(self, relative: Path) -> os.stat_result:
        parent, name = self.open_parent(relative)
        try:
            return os.stat(name, dir_fd=parent, follow_symlinks=False)
        finally:
            os.close(parent)

    def unlink(self, relative: Path, *, missing_ok: bool = False) -> None:
        parent, name = self.open_parent(relative)
        try:
            try:
                os.unlink(name, dir_fd=parent)
            except FileNotFoundError:
                if not missing_ok:
                    raise
        finally:
            os.close(parent)

    def fsync_dir(self, relative: Path) -> None:
        fd = self.open_dir(relative)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def remove_tree(self, relative: Path, *, missing_ok: bool = False) -> None:
        parent, name = self.open_parent(relative)
        try:
            try:
                directory = os.open(name, _DIR_OPEN_FLAGS, dir_fd=parent)
            except FileNotFoundError:
                if missing_ok:
                    return
                raise

            def remove_contents(directory_fd: int) -> None:
                for child_name in os.listdir(directory_fd):
                    child_stat = os.stat(
                        child_name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                    if stat.S_ISDIR(child_stat.st_mode):
                        child_fd = os.open(
                            child_name,
                            _DIR_OPEN_FLAGS,
                            dir_fd=directory_fd,
                        )
                        try:
                            remove_contents(child_fd)
                        finally:
                            os.close(child_fd)
                        os.rmdir(child_name, dir_fd=directory_fd)
                    elif stat.S_ISREG(child_stat.st_mode):
                        os.unlink(child_name, dir_fd=directory_fd)
                    else:
                        raise CutoverSafetyError(
                            "Managed cleanup encountered a symlink or special file"
                        )

            try:
                remove_contents(directory)
            finally:
                os.close(directory)
            os.rmdir(name, dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(parent)

    def chmod_tree(self, relative: Path, *, directory_mode: int, file_mode: int) -> None:
        root = self.open_dir(relative)

        def chmod_contents(directory_fd: int) -> None:
            for child_name in os.listdir(directory_fd):
                child_stat = os.stat(
                    child_name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                if stat.S_ISDIR(child_stat.st_mode):
                    child_fd = os.open(child_name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                    try:
                        chmod_contents(child_fd)
                        os.fchmod(child_fd, directory_mode)
                    finally:
                        os.close(child_fd)
                elif stat.S_ISREG(child_stat.st_mode):
                    os.chmod(
                        child_name,
                        file_mode,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                else:
                    raise CutoverSafetyError(
                        "Managed chmod encountered a symlink or special file"
                    )

        try:
            chmod_contents(root)
            os.fchmod(root, directory_mode)
        finally:
            os.close(root)

    def regular_files(self, relative: Path) -> list[tuple[Path, os.stat_result]]:
        root = self.open_dir(relative)
        files: list[tuple[Path, os.stat_result]] = []

        def walk(directory_fd: int, prefix: Path) -> None:
            for name in sorted(os.listdir(directory_fd)):
                entry_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                child_relative = prefix / name
                if stat.S_ISDIR(entry_stat.st_mode):
                    child_fd = os.open(name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                    try:
                        walk(child_fd, child_relative)
                    finally:
                        os.close(child_fd)
                elif stat.S_ISREG(entry_stat.st_mode):
                    files.append((child_relative, entry_stat))
                else:
                    raise CutoverSafetyError(
                        "Managed tree contains a symlink or special file"
                    )

        try:
            walk(root, Path())
        finally:
            os.close(root)
        return files

    def sha256(self, relative: Path) -> str:
        fd = self.open_regular(relative, os.O_RDONLY)
        digest = hashlib.sha256()
        try:
            while chunk := os.read(fd, 1024 * 1024):
                digest.update(chunk)
        finally:
            os.close(fd)
        return digest.hexdigest()

    def read_bytes(self, relative: Path) -> bytes:
        fd = self.open_regular(relative, os.O_RDONLY)
        chunks: list[bytes] = []
        try:
            while chunk := os.read(fd, 1024 * 1024):
                chunks.append(chunk)
        finally:
            os.close(fd)
        return b"".join(chunks)

    def copy_tree_create(self, source: Path, target: Path) -> None:
        source_fd = self.open_dir(source)
        try:
            target_fd = self.open_dir(target, create=True, exclusive_leaf=True)
        except Exception:
            os.close(source_fd)
            raise

        def copy_contents(source_dir_fd: int, target_dir_fd: int) -> None:
            for name in sorted(os.listdir(source_dir_fd)):
                entry_stat = os.stat(
                    name,
                    dir_fd=source_dir_fd,
                    follow_symlinks=False,
                )
                if stat.S_ISDIR(entry_stat.st_mode):
                    os.mkdir(name, 0o700, dir_fd=target_dir_fd)
                    source_child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=source_dir_fd)
                    try:
                        target_child = os.open(
                            name,
                            _DIR_OPEN_FLAGS,
                            dir_fd=target_dir_fd,
                        )
                    except Exception:
                        os.close(source_child)
                        raise
                    try:
                        copy_contents(source_child, target_child)
                        os.fsync(target_child)
                    finally:
                        os.close(source_child)
                        os.close(target_child)
                elif stat.S_ISREG(entry_stat.st_mode):
                    source_file = os.open(
                        name,
                        os.O_RDONLY | _FILE_NOFOLLOW,
                        dir_fd=source_dir_fd,
                    )
                    try:
                        target_file = os.open(
                            name,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW,
                            entry_stat.st_mode & 0o777,
                            dir_fd=target_dir_fd,
                        )
                    except Exception:
                        os.close(source_file)
                        raise
                    try:
                        while chunk := os.read(source_file, 1024 * 1024):
                            view = memoryview(chunk)
                            while view:
                                written = os.write(target_file, view)
                                view = view[written:]
                        os.fsync(target_file)
                    finally:
                        os.close(source_file)
                        os.close(target_file)
                else:
                    raise CutoverSafetyError(
                        "Managed copy encountered a symlink or special file"
                    )

        try:
            copy_contents(source_fd, target_fd)
            os.fsync(target_fd)
        except Exception:
            os.close(source_fd)
            os.close(target_fd)
            self.remove_tree(target, missing_ok=True)
            raise
        os.close(source_fd)
        os.close(target_fd)

    def fsync_tree(self, relative: Path) -> None:
        root = self.open_dir(relative)

        def sync_contents(directory_fd: int) -> None:
            for name in os.listdir(directory_fd):
                entry_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISDIR(entry_stat.st_mode):
                    child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
                    try:
                        sync_contents(child)
                        os.fsync(child)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(entry_stat.st_mode):
                    child = os.open(
                        name,
                        os.O_RDONLY | _FILE_NOFOLLOW,
                        dir_fd=directory_fd,
                    )
                    try:
                        os.fsync(child)
                    finally:
                        os.close(child)
                else:
                    raise CutoverSafetyError(
                        "Managed fsync encountered a symlink or special file"
                    )

        try:
            sync_contents(root)
            os.fsync(root)
        finally:
            os.close(root)


class PromotionJournal:
    """Descriptor-confined, append-only promotion state journal."""

    _SCHEMA_VERSION = 1
    _RECORD_NAME = re.compile(r"[0-9]{8}\.json")
    _RECORD_KEYS = {
        "schema_version",
        "operation_id",
        "sequence",
        "state",
        "created_at",
        "identities",
        "previous_record_sha256",
    }
    _CONTROL_NAME = re.compile(r"[0-9]{8}\.(?:intent|resolution)\.json")
    _CONTROL_COMMON_KEYS = {
        "schema_version",
        "control_sequence",
        "kind",
        "operation_id",
        "attempt_id",
        "created_at",
        "previous_control_sha256",
    }
    _INTENT_KEYS = _CONTROL_COMMON_KEYS | {
        "target_sequence",
        "target_name",
        "payload_sha256",
        "previous_record_sha256",
        "state",
        "identities",
    }
    _RESOLUTION_KEYS = _CONTROL_COMMON_KEYS | {
        "target_sequence",
        "target_name",
        "payload_sha256",
        "outcome",
    }
    _IDENTITY_KEYS = {
        "active_before_directory",
        "active_before_payload",
        "retained_v4_directory",
        "retained_v4_payload",
        "backup_manifest_sha256",
        "backup_file_set_sha256",
        "active_current",
        "retained_current",
        "quarantine_current",
        "holding_current",
        "detached_runtime_names",
        "detached_artifacts",
        "rollback_mode",
        "promotion_report_sha256",
    }
    _TRANSITIONS: dict[PromotionState | None, frozenset[PromotionState]] = {
        None: frozenset({PromotionState.VALIDATED}),
        PromotionState.VALIDATED: frozenset(
            {
                PromotionState.RUNTIMES_DETACHED,
                PromotionState.ROLLED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.RUNTIMES_DETACHED: frozenset(
            {
                PromotionState.PREPARED,
                PromotionState.ROLLED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.PREPARED: frozenset(
            {
                PromotionState.EXCHANGED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
                PromotionState.ROLLED_BACK,
            }
        ),
        PromotionState.EXCHANGED: frozenset(
            {
                PromotionState.QUARANTINED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.QUARANTINED: frozenset(
            {
                PromotionState.ACTIVE_SMOKE_PASSED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.ACTIVE_SMOKE_PASSED: frozenset(
            {
                PromotionState.CLEANUP_STAGED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.CLEANUP_STAGED: frozenset(
            {
                PromotionState.REPORT_PERSISTED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.REPORT_PERSISTED: frozenset(
            {
                PromotionState.COMMITTED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.COMMITTED: frozenset(),
        PromotionState.EXCHANGED_BACK: frozenset({PromotionState.ROLLED_BACK}),
        PromotionState.ROLLED_BACK: frozenset(),
        PromotionState.ROLLBACK_DEFERRED: frozenset(
            {PromotionState.EXCHANGED_BACK}
        ),
    }
    _LOCATION_REQUIREMENTS: dict[
        PromotionState, tuple[bool, bool | None, bool | None, bool | None]
    ] = {
        PromotionState.VALIDATED: (True, True, False, False),
        PromotionState.RUNTIMES_DETACHED: (True, True, False, True),
        PromotionState.PREPARED: (True, True, False, True),
        PromotionState.EXCHANGED: (True, True, False, True),
        PromotionState.QUARANTINED: (True, False, True, True),
        PromotionState.ACTIVE_SMOKE_PASSED: (True, False, True, True),
        PromotionState.CLEANUP_STAGED: (True, False, True, True),
        PromotionState.REPORT_PERSISTED: (True, False, True, True),
        PromotionState.COMMITTED: (True, False, True, True),
        PromotionState.EXCHANGED_BACK: (True, True, None, None),
        PromotionState.ROLLED_BACK: (True, True, None, False),
        # A deferred rollback may still have v3 at either rollback location.
        PromotionState.ROLLBACK_DEFERRED: (True, None, None, None),
    }

    def __init__(
        self,
        managed_root: ManagedRootFd,
        operation_id: str,
        *,
        now: Callable[[], str],
        file_fsync: Callable[[int], None] = os.fsync,
        directory_fsync: Callable[[int], None] = os.fsync,
        boundary_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._managed_root = managed_root
        self.operation_id = MarketV4CutoverService._validate_id(
            operation_id, label="operation"
        )
        self._now = now
        self._file_fsync = file_fsync
        self._directory_fsync = directory_fsync
        self._boundary_hook = boundary_hook or (lambda _stage: None)
        self._authorization_secret = object()
        self._authorization: _PromotionJournalAuthorization | None = None
        self._recovery_fence_attempt: str | None = None
        self._relative = (
            Path("operations")
            / "market-v4-cutover"
            / "journals"
            / self.operation_id
        )
        self._control_relative = (
            Path("operations")
            / "market-v4-cutover"
            / "journal-controls"
            / self.operation_id
        )
        self._staging_relative = self._control_relative / "staging"
        self._lock_relative = (
            Path("operations")
            / "market-v4-cutover"
            / "journal-locks"
            / f"{self.operation_id}.lock"
        )

    @staticmethod
    def _canonical_json(value: object) -> bytes:
        try:
            return (
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode()
        except (TypeError, ValueError) as exc:
            raise CutoverSafetyError("Promotion journal record is not JSON-safe") from exc

    @staticmethod
    def _sha256_valid(value: object) -> bool:
        return (
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
        )

    @staticmethod
    def _directory_valid(value: object) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"device", "inode"}
            and all(type(value[key]) is int and value[key] >= 0 for key in value)
        )

    @classmethod
    def _file_valid(cls, candidate: object) -> bool:
        return (
            isinstance(candidate, dict)
            and set(candidate) == {"device", "inode", "size", "sha256"}
            and all(
                type(candidate[key]) is int and candidate[key] >= 0
                for key in ("device", "inode", "size")
            )
            and cls._sha256_valid(candidate["sha256"])
        )

    @classmethod
    def _payload_valid(cls, value: object) -> bool:
        if not isinstance(value, dict) or set(value) != {
            "marketDuckdb",
            "parquetSha256",
        }:
            return False

        parquet = value["parquetSha256"]
        return (
            cls._file_valid(value["marketDuckdb"])
            and isinstance(parquet, dict)
            and bool(parquet)
            and all(
                isinstance(path, str)
                and bool(path)
                and not Path(path).is_absolute()
                and ".." not in Path(path).parts
                and Path(path).as_posix() == path
                and all(part not in {"", "."} for part in path.split("/"))
                and cls._file_valid(identity)
                for path, identity in parquet.items()
            )
        )

    @classmethod
    def _location_valid(cls, value: object) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"directory", "payload"}
            and cls._directory_valid(value["directory"])
            and cls._payload_valid(value["payload"])
        )

    @classmethod
    def _identity_mapping_valid(
        cls,
        value: object,
        state: PromotionState,
    ) -> bool:
        if not isinstance(value, dict) or set(value) != cls._IDENTITY_KEYS:
            return False
        if not (
            cls._directory_valid(value["active_before_directory"])
            and cls._payload_valid(value["active_before_payload"])
            and cls._directory_valid(value["retained_v4_directory"])
            and cls._payload_valid(value["retained_v4_payload"])
            and cls._sha256_valid(value["backup_manifest_sha256"])
            and cls._sha256_valid(value["backup_file_set_sha256"])
        ):
            return False
        detached = value["detached_runtime_names"]
        if not (
            isinstance(detached, list)
            and all(
                isinstance(name, str)
                and bool(name)
                and "/" not in name
                and name not in {".", ".."}
                for name in detached
            )
            and len(set(detached)) == len(detached)
        ):
            return False
        artifacts = value["detached_artifacts"]
        if not isinstance(artifacts, list):
            return False
        artifact_names: list[str] = []
        for artifact in artifacts:
            if not isinstance(artifact, dict) or set(artifact) != {
                "name",
                "kind",
                "identity",
                "directories",
                "files",
            }:
                return False
            name = artifact["name"]
            kind = artifact["kind"]
            directories = artifact["directories"]
            files = artifact["files"]
            if not (
                isinstance(name, str)
                and name in {*detached, "duckdb-tmp", "market.duckdb.wal"}
                and isinstance(kind, str)
                and kind in {"directory", "regular"}
                and isinstance(directories, dict)
                and isinstance(files, dict)
                and all(
                    isinstance(path, str) and cls._directory_valid(identity)
                    for path, identity in directories.items()
                )
                and all(
                    isinstance(path, str) and cls._file_valid(identity)
                    for path, identity in files.items()
                )
                and (
                    cls._directory_valid(artifact["identity"])
                    if kind == "directory"
                    else cls._file_valid(artifact["identity"])
                )
            ):
                return False
            artifact_names.append(name)
        if len(set(artifact_names)) != len(artifact_names):
            return False
        if value["rollback_mode"] not in {
            None,
            "atomic_exchange",
            "backup_restore",
        }:
            return False
        report_sha256 = value["promotion_report_sha256"]
        if state is PromotionState.COMMITTED:
            if not cls._sha256_valid(report_sha256):
                return False
        elif report_sha256 is not None:
            return False
        active, retained, quarantine, holding = cls._LOCATION_REQUIREMENTS[state]
        locations = (
            ("active_current", active),
            ("retained_current", retained),
            ("quarantine_current", quarantine),
            ("holding_current", holding),
        )
        for key, required in locations:
            if required is True and not cls._location_valid(value[key]):
                return False
            if required is False and value[key] is not None:
                return False
        if state is PromotionState.ROLLBACK_DEFERRED:
            retained = value["retained_current"]
            quarantine = value["quarantine_current"]
            if not (
                (cls._location_valid(retained) and quarantine is None)
                or (retained is None and cls._location_valid(quarantine))
            ):
                return False
        if state in {PromotionState.EXCHANGED_BACK, PromotionState.ROLLBACK_DEFERRED}:
            holding = value["holding_current"]
            if holding is not None and not cls._location_valid(holding):
                return False
        if state in {PromotionState.EXCHANGED_BACK, PromotionState.ROLLED_BACK}:
            rollback_mode = value["rollback_mode"]
            quarantine = value["quarantine_current"]
            if rollback_mode == "atomic_exchange" and quarantine is not None:
                return False
            if rollback_mode == "backup_restore" and not cls._location_valid(
                quarantine
            ):
                return False
        return True

    @classmethod
    def _identity_to_mapping(
        cls, identities: PromotionIdentityEvidence
    ) -> dict[str, object]:
        return {
            "active_before_directory": identities.active_before_directory,
            "active_before_payload": identities.active_before_payload,
            "retained_v4_directory": identities.retained_v4_directory,
            "retained_v4_payload": identities.retained_v4_payload,
            "backup_manifest_sha256": identities.backup_manifest_sha256,
            "backup_file_set_sha256": identities.backup_file_set_sha256,
            "active_current": identities.active_current,
            "retained_current": identities.retained_current,
            "quarantine_current": identities.quarantine_current,
            "holding_current": identities.holding_current,
            "detached_runtime_names": list(identities.detached_runtime_names),
            "detached_artifacts": list(identities.detached_artifacts),
            "rollback_mode": identities.rollback_mode,
            "promotion_report_sha256": identities.promotion_report_sha256,
        }

    @classmethod
    def _identity_from_mapping(
        cls,
        value: dict[str, object],
        state: PromotionState,
    ) -> PromotionIdentityEvidence:
        if not cls._identity_mapping_valid(value, state):
            raise CutoverSafetyError("Promotion journal identity schema is invalid")
        return PromotionIdentityEvidence(
            active_before_directory=cast(
                dict[str, int], value["active_before_directory"]
            ),
            active_before_payload=cast(
                dict[str, object], value["active_before_payload"]
            ),
            retained_v4_directory=cast(
                dict[str, int], value["retained_v4_directory"]
            ),
            retained_v4_payload=cast(
                dict[str, object], value["retained_v4_payload"]
            ),
            backup_manifest_sha256=cast(str, value["backup_manifest_sha256"]),
            backup_file_set_sha256=cast(str, value["backup_file_set_sha256"]),
            active_current=cast(
                dict[str, object] | None, value["active_current"]
            ),
            retained_current=cast(
                dict[str, object] | None, value["retained_current"]
            ),
            quarantine_current=cast(
                dict[str, object] | None, value["quarantine_current"]
            ),
            holding_current=cast(
                dict[str, object] | None, value["holding_current"]
            ),
            detached_runtime_names=tuple(
                cast(list[str], value["detached_runtime_names"])
            ),
            detached_artifacts=tuple(
                cast(list[dict[str, object]], value["detached_artifacts"])
            ),
            rollback_mode=cast(str | None, value["rollback_mode"]),
            promotion_report_sha256=cast(
                str | None, value["promotion_report_sha256"]
            ),
        )

    @staticmethod
    def _immutable_identity(identities: PromotionIdentityEvidence) -> tuple[object, ...]:
        return (
            identities.active_before_directory,
            identities.active_before_payload,
            identities.retained_v4_directory,
            identities.retained_v4_payload,
            identities.backup_manifest_sha256,
            identities.backup_file_set_sha256,
        )

    @classmethod
    def _validate_transition(
        cls,
        previous: PromotionState | None,
        current: PromotionState,
    ) -> None:
        if current not in cls._TRANSITIONS[previous]:
            raise CutoverSafetyError(
                f"Invalid promotion journal state transition: {previous!s} -> {current}"
            )

    def _ensure_durable_directory(self, relative: Path) -> int:
        current = os.dup(self._managed_root.fd)
        prefix = Path()
        try:
            for part in _safe_relative_parts(relative):
                prefix /= part
                created = False
                try:
                    child = os.open(part, _DIR_OPEN_FLAGS, dir_fd=current)
                except FileNotFoundError:
                    try:
                        os.mkdir(part, 0o700, dir_fd=current)
                        created = True
                    except FileExistsError:
                        pass
                    child = os.open(part, _DIR_OPEN_FLAGS, dir_fd=current)
                if created:
                    self._boundary_hook(f"ancestor_child_fsync_before:{prefix}")
                    self._directory_fsync(child)
                    self._boundary_hook(f"ancestor_child_fsynced:{prefix}")
                    self._boundary_hook(f"ancestor_parent_fsync_before:{prefix}")
                    self._directory_fsync(current)
                    self._boundary_hook(f"ancestor_parent_fsynced:{prefix}")
                os.close(current)
                current = child
            return current
        except Exception:
            os.close(current)
            raise

    def _ensure_layout(self) -> None:
        for relative in (
            self._relative,
            self._control_relative,
            self._staging_relative,
            self._lock_relative.parent,
        ):
            fd = self._ensure_durable_directory(relative)
            os.close(fd)

    @contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[None]:
        self._ensure_layout()
        parent = self._ensure_durable_directory(self._lock_relative.parent)
        name = self._lock_relative.name
        created = False
        try:
            try:
                lock_fd = os.open(
                    name,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | _FILE_NOFOLLOW,
                    0o600,
                    dir_fd=parent,
                )
                created = True
            except FileExistsError:
                lock_fd = os.open(
                    name,
                    os.O_RDWR | _FILE_NOFOLLOW,
                    dir_fd=parent,
                )
            if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
                os.close(lock_fd)
                raise CutoverSafetyError("Promotion journal lock must be regular")
            if created:
                self._file_fsync(lock_fd)
                self._directory_fsync(parent)
            fcntl.flock(lock_fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
        finally:
            os.close(parent)

    @staticmethod
    def _read_regular(directory_fd: int, name: str, *, label: str) -> bytes:
        try:
            fd = os.open(name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd)
        except OSError as exc:
            raise CutoverSafetyError(f"{label} is not a confined regular file") from exc
        chunks: list[bytes] = []
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise CutoverSafetyError(f"{label} must be a regular file")
            while chunk := os.read(fd, 1024 * 1024):
                chunks.append(chunk)
        finally:
            os.close(fd)
        return b"".join(chunks)

    def _read_control_entries(self) -> list[tuple[str, bytes]]:
        directory_fd = self._managed_root.open_dir(self._control_relative)
        entries: list[tuple[str, bytes]] = []
        try:
            for name in sorted(os.listdir(directory_fd)):
                if name == "staging":
                    entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                    if not stat.S_ISDIR(entry.st_mode):
                        raise CutoverSafetyError(
                            "Promotion journal staging entry must be a directory"
                        )
                    continue
                if self._CONTROL_NAME.fullmatch(name) is None:
                    raise CutoverSafetyError(
                        f"Promotion journal has unknown control entry: {name}"
                    )
                entries.append(
                    (
                        name,
                        self._read_regular(
                            directory_fd,
                            name,
                            label="Promotion journal control record",
                        ),
                    )
                )
        finally:
            os.close(directory_fd)
        return entries

    def _directory_snapshot(
        self,
        relative: Path,
        *,
        skip_staging: bool,
    ) -> tuple[tuple[int, int], tuple[tuple[str, int, int, int, str], ...]]:
        directory_fd = self._managed_root.open_dir(relative)
        files: list[tuple[str, int, int, int, str]] = []
        try:
            directory_stat = os.fstat(directory_fd)
            for name in sorted(os.listdir(directory_fd)):
                if skip_staging and name == "staging":
                    continue
                fd = os.open(
                    name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd
                )
                digest = hashlib.sha256()
                try:
                    before = os.fstat(fd)
                    if not stat.S_ISREG(before.st_mode):
                        raise CutoverSafetyError(
                            "Promotion journal authorization identity is invalid"
                        )
                    while chunk := os.read(fd, 1024 * 1024):
                        digest.update(chunk)
                    after = os.fstat(fd)
                    current = os.stat(
                        name, dir_fd=directory_fd, follow_symlinks=False
                    )
                    identity = (before.st_dev, before.st_ino, before.st_size)
                    if identity != (
                        after.st_dev,
                        after.st_ino,
                        after.st_size,
                    ) or identity != (
                        current.st_dev,
                        current.st_ino,
                        current.st_size,
                    ):
                        raise CutoverSafetyError(
                            "Promotion journal authorization identity changed"
                        )
                    files.append((name, *identity, digest.hexdigest()))
                finally:
                    os.close(fd)
        finally:
            os.close(directory_fd)
        return (directory_stat.st_dev, directory_stat.st_ino), tuple(files)

    def _resolution_sha256(self, attempt_id: str) -> str:
        for _name, raw in self._read_control_entries():
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if (
                isinstance(value, dict)
                and value.get("kind") == "resolution"
                and value.get("attempt_id") == attempt_id
            ):
                return hashlib.sha256(raw).hexdigest()
        raise CutoverSafetyError("Promotion journal resolution is unavailable")

    def _grant_authorization(
        self,
        *,
        attempt_id: str,
        sequence: int,
        candidate_sha256: str,
    ) -> None:
        record_directory, record_files = self._directory_snapshot(
            self._relative, skip_staging=False
        )
        control_directory, control_files = self._directory_snapshot(
            self._control_relative, skip_staging=True
        )
        self._authorization = _PromotionJournalAuthorization(
            secret=self._authorization_secret,
            operation_id=self.operation_id,
            attempt_id=attempt_id,
            sequence=sequence,
            candidate_sha256=candidate_sha256,
            resolution_sha256=self._resolution_sha256(attempt_id),
            record_directory=record_directory,
            control_directory=control_directory,
            record_files=record_files,
            control_files=control_files,
        )
        self._recovery_fence_attempt = None

    def _require_authorization(
        self,
        *,
        intents: dict[str, dict[str, object]],
        resolutions: dict[str, dict[str, object]],
    ) -> None:
        if self._recovery_fence_attempt is not None:
            raise CutoverSafetyError(
                "Promotion journal is fenced pending same-attempt recovery"
            )
        if not intents and not resolutions:
            return
        authorization = self._authorization
        if (
            authorization is None
            or authorization.secret is not self._authorization_secret
            or authorization.operation_id != self.operation_id
        ):
            raise CutoverSafetyError(
                "Promotion journal live recovery authorization is required"
            )
        if not resolutions or next(reversed(resolutions)) != authorization.attempt_id:
            self._authorization = None
            raise CutoverSafetyError(
                "Promotion journal authorization attempt is not current"
            )
        resolution = resolutions.get(authorization.attempt_id)
        intent = intents.get(authorization.attempt_id)
        if (
            resolution is None
            or intent is None
            or intent["target_sequence"] != authorization.sequence
            or intent["payload_sha256"] != authorization.candidate_sha256
            or self._resolution_sha256(authorization.attempt_id)
            != authorization.resolution_sha256
        ):
            self._authorization = None
            raise CutoverSafetyError(
                "Promotion journal authorization identity mismatch"
            )
        record_directory, record_files = self._directory_snapshot(
            self._relative, skip_staging=False
        )
        control_directory, control_files = self._directory_snapshot(
            self._control_relative, skip_staging=True
        )
        if (
            record_directory != authorization.record_directory
            or control_directory != authorization.control_directory
            or record_files != authorization.record_files
            or control_files != authorization.control_files
        ):
            self._authorization = None
            raise CutoverSafetyError(
                "Promotion journal authorization identity drifted"
            )

    def _control_state(
        self,
    ) -> tuple[
        list[dict[str, object]],
        dict[str, dict[str, object]],
        dict[str, dict[str, object]],
    ]:
        controls: list[dict[str, object]] = []
        intents: dict[str, dict[str, object]] = {}
        resolutions: dict[str, dict[str, object]] = {}
        target_names: set[str] = set()
        previous_raw: bytes | None = None
        expected_kind = "intent"
        for sequence, (name, raw) in enumerate(self._read_control_entries(), start=1):
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CutoverSafetyError("Promotion journal control record is invalid") from exc
            if not isinstance(value, dict):
                raise CutoverSafetyError("Promotion journal control schema is invalid")
            kind = value.get("kind")
            expected_keys = (
                self._INTENT_KEYS if kind == "intent" else self._RESOLUTION_KEYS
            )
            if kind not in {"intent", "resolution"} or set(value) != expected_keys:
                raise CutoverSafetyError("Promotion journal control schema is invalid")
            expected_name = f"{sequence:08d}.{kind}.json"
            if name != expected_name or kind != expected_kind:
                raise CutoverSafetyError("Promotion journal control sequence is invalid")
            if raw != self._canonical_json(value):
                raise CutoverSafetyError("Promotion journal control is not canonical JSON")
            if type(value["schema_version"]) is not int or value["schema_version"] != 1:
                raise CutoverSafetyError("Promotion journal control schema version is unknown")
            if type(value["control_sequence"]) is not int or value["control_sequence"] != sequence:
                raise CutoverSafetyError("Promotion journal control sequence is invalid")
            if value["operation_id"] != self.operation_id:
                raise CutoverSafetyError("Promotion journal control operation mismatch")
            attempt_id = value["attempt_id"]
            if not isinstance(attempt_id, str):
                raise CutoverSafetyError("Promotion journal control attempt is invalid")
            MarketV4CutoverService._validate_id(attempt_id, label="attempt")
            expected_previous = (
                hashlib.sha256(previous_raw).hexdigest() if previous_raw is not None else None
            )
            if value["previous_control_sha256"] != expected_previous:
                raise CutoverSafetyError("Promotion journal control SHA chain mismatch")
            if type(value["target_sequence"]) is not int or value["target_sequence"] <= 0:
                raise CutoverSafetyError("Promotion journal control target is invalid")
            if value["target_name"] != f'{value["target_sequence"]:08d}.json':
                raise CutoverSafetyError("Promotion journal control target is invalid")
            if not self._sha256_valid(value["payload_sha256"]):
                raise CutoverSafetyError("Promotion journal control payload SHA is invalid")
            if kind == "intent":
                if attempt_id in intents:
                    raise CutoverSafetyError("Promotion journal attempt is duplicated")
                target_name = cast(str, value["target_name"])
                if target_name in target_names:
                    raise CutoverSafetyError(
                        "Promotion journal numbered target path was reused"
                    )
                target_names.add(target_name)
                try:
                    state = PromotionState(value["state"])
                except (TypeError, ValueError) as exc:
                    raise CutoverSafetyError("Promotion journal control state is invalid") from exc
                identities = value["identities"]
                if not self._identity_mapping_valid(identities, state):
                    raise CutoverSafetyError("Promotion journal identity schema is invalid")
                previous_record = value["previous_record_sha256"]
                if previous_record is not None and not self._sha256_valid(previous_record):
                    raise CutoverSafetyError("Promotion journal previous-record SHA is invalid")
                intents[attempt_id] = value
                expected_kind = "resolution"
            else:
                intent = intents.get(attempt_id)
                if intent is None or any(
                    value[key] != intent[key]
                    for key in ("target_sequence", "target_name", "payload_sha256")
                ):
                    raise CutoverSafetyError("Promotion journal resolution mismatch")
                if value["outcome"] not in {"accepted", "rejected"}:
                    raise CutoverSafetyError("Promotion journal resolution outcome is invalid")
                resolutions[attempt_id] = value
                expected_kind = "intent"
            if not isinstance(value["created_at"], str) or not value["created_at"]:
                raise CutoverSafetyError("Promotion journal control timestamp is invalid")
            controls.append(value)
            previous_raw = raw
        return controls, intents, resolutions

    def _append_control(self, value: dict[str, object], *, stage: str) -> None:
        entries = self._read_control_entries()
        sequence = len(entries) + 1
        value = {
            **value,
            "schema_version": self._SCHEMA_VERSION,
            "control_sequence": sequence,
            "created_at": self._now(),
            "previous_control_sha256": (
                hashlib.sha256(entries[-1][1]).hexdigest() if entries else None
            ),
        }
        kind = value.get("kind")
        expected_keys = (
            self._INTENT_KEYS if kind == "intent" else self._RESOLUTION_KEYS
        )
        if kind not in {"intent", "resolution"} or set(value) != expected_keys:
            raise CutoverSafetyError("Promotion journal control schema is invalid")
        if (
            type(value["schema_version"]) is not int
            or value["schema_version"] != self._SCHEMA_VERSION
            or type(value["control_sequence"]) is not int
            or value["control_sequence"] != sequence
            or not isinstance(value["created_at"], str)
            or not value["created_at"]
            or value["operation_id"] != self.operation_id
            or not isinstance(value["attempt_id"], str)
            or type(value["target_sequence"]) is not int
            or value["target_sequence"] <= 0
            or value["target_name"] != f'{value["target_sequence"]:08d}.json'
            or not self._sha256_valid(value["payload_sha256"])
        ):
            raise CutoverSafetyError("Promotion journal control schema is invalid")
        MarketV4CutoverService._validate_id(
            cast(str, value["attempt_id"]), label="attempt"
        )
        previous_control = value["previous_control_sha256"]
        if previous_control is not None and not self._sha256_valid(previous_control):
            raise CutoverSafetyError("Promotion journal control schema is invalid")
        if kind == "intent":
            try:
                state = PromotionState(value["state"])
            except (TypeError, ValueError) as exc:
                raise CutoverSafetyError(
                    "Promotion journal control schema is invalid"
                ) from exc
            if (
                not self._identity_mapping_valid(value["identities"], state)
                or (
                    value["previous_record_sha256"] is not None
                    and not self._sha256_valid(value["previous_record_sha256"])
                )
            ):
                raise CutoverSafetyError("Promotion journal control schema is invalid")
        elif value["outcome"] not in {"accepted", "rejected"}:
            raise CutoverSafetyError("Promotion journal control schema is invalid")
        payload = self._canonical_json(value)
        directory_fd = self._managed_root.open_dir(self._control_relative)
        staging_fd = self._managed_root.open_dir(self._staging_relative)
        name = f'{sequence:08d}.{value["kind"]}.json'
        staged_name = f"{value['attempt_id']}.{sequence:08d}.{value['kind']}.control"
        published = False
        try:
            fd = os.open(
                staged_name,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW,
                0o600,
                dir_fd=staging_fd,
            )
            try:
                MarketV4CutoverService._write_all(fd, payload)
                self._boundary_hook(f"{stage}_file_fsync_before")
                self._file_fsync(fd)
                self._boundary_hook(f"{stage}_file_fsynced")
            finally:
                os.close(fd)
            self._boundary_hook(f"{stage}_parent_fsync_before")
            self._directory_fsync(staging_fd)
            self._boundary_hook(f"{stage}_parent_fsynced")
            self._boundary_hook(f"{stage}_control_publication_before")
            _rename_exclusive_at(staging_fd, staged_name, directory_fd, name)
            published = True
            self._boundary_hook(f"{stage}_control_publication_after")
            self._boundary_hook(f"{stage}_source_parent_fsync_before")
            self._directory_fsync(staging_fd)
            self._boundary_hook(f"{stage}_source_parent_fsynced")
            self._boundary_hook(f"{stage}_destination_parent_fsync_before")
            self._directory_fsync(directory_fd)
            self._boundary_hook(f"{stage}_destination_parent_fsynced")
        except Exception:
            if published:
                try:
                    os.unlink(name, dir_fd=directory_fd)
                    os.fsync(directory_fd)
                    os.fsync(staging_fd)
                except OSError:
                    pass
            raise
        finally:
            os.close(staging_fd)
            os.close(directory_fd)

    def _open_journal_dir(self, *, create: bool) -> int | None:
        try:
            return self._managed_root.open_dir(self._relative, create=create)
        except FileNotFoundError:
            if not create:
                return None
            raise

    def _read_entries(self) -> list[tuple[str, bytes]]:
        directory_fd = self._open_journal_dir(create=False)
        if directory_fd is None:
            return []
        entries: list[tuple[str, bytes]] = []
        try:
            names = sorted(os.listdir(directory_fd))
            for name in names:
                if self._RECORD_NAME.fullmatch(name) is None:
                    raise CutoverSafetyError(
                        f"Promotion journal has unknown journal entry: {name}"
                    )
                try:
                    record_fd = os.open(
                        name,
                        os.O_RDONLY | _FILE_NOFOLLOW,
                        dir_fd=directory_fd,
                    )
                except OSError as exc:
                    raise CutoverSafetyError(
                        "Promotion journal record is not a confined regular file"
                    ) from exc
                chunks: list[bytes] = []
                try:
                    if not stat.S_ISREG(os.fstat(record_fd).st_mode):
                        raise CutoverSafetyError(
                            "Promotion journal record must be a regular file"
                        )
                    while chunk := os.read(record_fd, 1024 * 1024):
                        chunks.append(chunk)
                finally:
                    os.close(record_fd)
                entries.append((name, b"".join(chunks)))
        finally:
            os.close(directory_fd)
        return entries

    def _read_records_validated(self) -> tuple[PromotionJournalRecord, ...]:
        records: list[PromotionJournalRecord] = []
        previous_bytes: bytes | None = None
        immutable_identity: tuple[object, ...] | None = None
        for expected_sequence, (name, raw) in enumerate(
            self._read_entries(), start=1
        ):
            expected_name = f"{expected_sequence:08d}.json"
            if name != expected_name:
                raise CutoverSafetyError("Promotion journal sequence is not contiguous")
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CutoverSafetyError("Promotion journal record is torn or invalid") from exc
            if not isinstance(value, dict) or set(value) != self._RECORD_KEYS:
                raise CutoverSafetyError("Promotion journal record schema is invalid")
            if raw != self._canonical_json(value):
                raise CutoverSafetyError("Promotion journal record is not canonical JSON")
            if (
                type(value["schema_version"]) is not int
                or value["schema_version"] != self._SCHEMA_VERSION
            ):
                raise CutoverSafetyError("Promotion journal schema version is unknown")
            if value["operation_id"] != self.operation_id:
                raise CutoverSafetyError("Promotion journal operation ID mismatch")
            if type(value["sequence"]) is not int or value["sequence"] != expected_sequence:
                raise CutoverSafetyError("Promotion journal record sequence mismatch")
            if not isinstance(value["created_at"], str) or not value["created_at"]:
                raise CutoverSafetyError("Promotion journal record timestamp is invalid")
            try:
                state = PromotionState(value["state"])
            except (TypeError, ValueError) as exc:
                raise CutoverSafetyError("Promotion journal record state is unknown") from exc
            self._validate_transition(records[-1].state if records else None, state)
            expected_previous_sha = (
                None
                if previous_bytes is None
                else hashlib.sha256(previous_bytes).hexdigest()
            )
            if value["previous_record_sha256"] != expected_previous_sha:
                raise CutoverSafetyError("Promotion journal previous-record SHA mismatch")
            identities_value = value["identities"]
            if not isinstance(identities_value, dict):
                raise CutoverSafetyError("Promotion journal identity schema is invalid")
            identities = self._identity_from_mapping(identities_value, state)
            current_immutable_identity = self._immutable_identity(identities)
            if immutable_identity is None:
                immutable_identity = current_immutable_identity
            elif current_immutable_identity != immutable_identity:
                raise CutoverSafetyError("Promotion journal immutable identity mismatch")
            records.append(
                PromotionJournalRecord(
                    sequence=expected_sequence,
                    state=state,
                    operation_id=self.operation_id,
                    identities=identities,
                    created_at=value["created_at"],
                )
            )
            previous_bytes = raw
        return tuple(records)

    def _read_validated_locked(
        self,
        *,
        require_authorization: bool = True,
    ) -> tuple[PromotionJournalRecord, ...]:
        _controls, intents, resolutions = self._control_state()
        unresolved = set(intents) - set(resolutions)
        if unresolved:
            raise CutoverSafetyError(
                "Promotion journal has unresolved intent; dedicated recovery is required"
            )
        records = self._read_records_validated()
        accepted = [
            (intent, resolutions[attempt_id])
            for attempt_id, intent in intents.items()
            if resolutions[attempt_id]["outcome"] == "accepted"
        ]
        if len(records) != len(accepted):
            raise CutoverSafetyError("Promotion journal accepted resolution mismatch")
        raw_entries = self._read_entries()
        for index, ((intent, _resolution), (name, raw)) in enumerate(
            zip(accepted, raw_entries, strict=True), start=1
        ):
            if (
                intent["target_sequence"] != index
                or intent["target_name"] != name
                or intent["payload_sha256"] != hashlib.sha256(raw).hexdigest()
            ):
                raise CutoverSafetyError("Promotion journal accepted candidate mismatch")
        if require_authorization:
            self._require_authorization(intents=intents, resolutions=resolutions)
        return records

    def read_validated(self) -> tuple[PromotionJournalRecord, ...]:
        with self._locked(exclusive=False):
            return self._read_validated_locked()

    def recovery_attempt_id(self) -> str:
        """Return the sole exact live attempt which may authorize this instance."""

        with self._locked(exclusive=False):
            _controls, intents, resolutions = self._control_state()
            if not intents:
                raise CutoverSafetyError("Promotion journal recovery intent is missing")
            unresolved = tuple(attempt for attempt in intents if attempt not in resolutions)
            if len(unresolved) > 1:
                raise CutoverSafetyError("Promotion journal recovery intent is ambiguous")
            attempt_id = unresolved[0] if unresolved else next(reversed(intents))
            if resolutions and not unresolved and next(reversed(resolutions)) != attempt_id:
                raise CutoverSafetyError(
                    "Promotion journal recovery attempt is not current"
                )
            return attempt_id

    def append(
        self,
        state: PromotionState,
        *,
        identities: PromotionIdentityEvidence,
    ) -> PromotionAppendResult:
        attempt_id = f"attempt-{secrets.token_hex(12)}"
        last_result: PromotionAppendResult | None = None
        candidate_published = False

        def finish(
            status: PromotionAppendStatus,
            record: PromotionJournalRecord | None = None,
        ) -> PromotionAppendResult:
            nonlocal last_result
            if status is PromotionAppendStatus.INDETERMINATE:
                self._authorization = None
                self._recovery_fence_attempt = attempt_id
            last_result = PromotionAppendResult(status, record, attempt_id)
            return last_result

        if type(state) is not PromotionState:
            raise CutoverSafetyError("Promotion journal state is unknown")
        try:
            with self._locked(exclusive=True):
                existing = self._read_validated_locked()
                previous_state = existing[-1].state if existing else None
                self._validate_transition(previous_state, state)
                identity_mapping = self._identity_to_mapping(identities)
                if not self._identity_mapping_valid(identity_mapping, state):
                    raise CutoverSafetyError(
                        "Promotion journal identity schema is invalid"
                    )
                if existing and self._immutable_identity(
                    identities
                ) != self._immutable_identity(existing[0].identities):
                    raise CutoverSafetyError(
                        "Promotion journal immutable identity mismatch"
                    )
                created_at = self._now()
                if not isinstance(created_at, str) or not created_at:
                    raise CutoverSafetyError("Promotion journal timestamp is invalid")
                entries = self._read_entries()
                sequence = len(existing) + 1
                name = f"{sequence:08d}.json"
                _controls, prior_intents, _resolutions = self._control_state()
                if any(intent["target_name"] == name for intent in prior_intents.values()):
                    raise CutoverSafetyError(
                        "Promotion journal numbered target path cannot be reused"
                    )
                previous_sha256 = (
                    hashlib.sha256(entries[-1][1]).hexdigest()
                    if entries
                    else None
                )
                payload = self._canonical_json(
                    {
                        "schema_version": self._SCHEMA_VERSION,
                        "operation_id": self.operation_id,
                        "sequence": sequence,
                        "state": state.value,
                        "created_at": created_at,
                        "identities": identity_mapping,
                        "previous_record_sha256": previous_sha256,
                    }
                )
                payload_sha256 = hashlib.sha256(payload).hexdigest()
                intent = {
                    "kind": "intent",
                    "operation_id": self.operation_id,
                    "attempt_id": attempt_id,
                    "target_sequence": sequence,
                    "target_name": name,
                    "payload_sha256": payload_sha256,
                    "previous_record_sha256": previous_sha256,
                    "state": state.value,
                    "identities": identity_mapping,
                }
                try:
                    self._append_control(intent, stage="intent")
                except Exception:
                    return finish(PromotionAppendStatus.NOT_COMMITTED)

                resolution_base = {
                    "kind": "resolution",
                    "operation_id": self.operation_id,
                    "attempt_id": attempt_id,
                    "target_sequence": sequence,
                    "target_name": name,
                    "payload_sha256": payload_sha256,
                }
                staging_fd = self._managed_root.open_dir(self._staging_relative)
                staged_name = f"{attempt_id}.candidate"
                staged_created = False
                try:
                    staged_fd = os.open(
                        staged_name,
                        os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW,
                        0o600,
                        dir_fd=staging_fd,
                    )
                    staged_created = True
                    try:
                        MarketV4CutoverService._write_all(staged_fd, payload)
                        self._boundary_hook("candidate_file_fsync_before")
                        self._file_fsync(staged_fd)
                        self._boundary_hook("candidate_file_fsynced")
                    finally:
                        os.close(staged_fd)
                    self._boundary_hook("candidate_parent_fsync_before")
                    self._directory_fsync(staging_fd)
                    self._boundary_hook("candidate_parent_fsynced")
                except Exception:
                    if staged_created:
                        try:
                            self._boundary_hook("cleanup_unlink_before")
                            os.unlink(staged_name, dir_fd=staging_fd)
                            self._boundary_hook("cleanup_parent_fsync_before")
                            self._directory_fsync(staging_fd)
                        except OSError:
                            pass
                    try:
                        self._append_control(
                            {**resolution_base, "outcome": "rejected"},
                            stage="resolution",
                        )
                    except Exception:
                        pass
                    return finish(PromotionAppendStatus.NOT_COMMITTED)
                finally:
                    os.close(staging_fd)

                journal_fd = self._managed_root.open_dir(self._relative)
                staging_fd = self._managed_root.open_dir(self._staging_relative)
                published = False
                try:
                    self._boundary_hook("publication_before")
                    _rename_exclusive_at(
                        staging_fd,
                        staged_name,
                        journal_fd,
                        name,
                    )
                    published = True
                    candidate_published = True
                    self._boundary_hook("publication_after")
                    self._directory_fsync(staging_fd)
                    self._boundary_hook("journal_parent_fsync_before")
                    self._directory_fsync(journal_fd)
                    self._boundary_hook("journal_parent_fsynced")
                except Exception:
                    cleanup_proven = False
                    if published:
                        try:
                            self._boundary_hook("cleanup_unlink_before")
                            os.unlink(name, dir_fd=journal_fd)
                            self._boundary_hook("cleanup_parent_fsync_before")
                            self._directory_fsync(journal_fd)
                            cleanup_proven = True
                        except OSError:
                            cleanup_proven = False
                    if cleanup_proven:
                        try:
                            self._append_control(
                                {**resolution_base, "outcome": "rejected"},
                                stage="resolution",
                            )
                        except Exception:
                            return finish(PromotionAppendStatus.INDETERMINATE)
                        return finish(PromotionAppendStatus.NOT_COMMITTED)
                    return finish(PromotionAppendStatus.INDETERMINATE)
                finally:
                    os.close(staging_fd)
                    os.close(journal_fd)

                try:
                    self._append_control(
                        {**resolution_base, "outcome": "accepted"},
                        stage="resolution",
                    )
                    self._grant_authorization(
                        attempt_id=attempt_id,
                        sequence=sequence,
                        candidate_sha256=payload_sha256,
                    )
                except Exception:
                    return finish(PromotionAppendStatus.INDETERMINATE)
                record = PromotionJournalRecord(
                    sequence=sequence,
                    state=state,
                    operation_id=self.operation_id,
                    identities=identities,
                    created_at=created_at,
                )
                return finish(PromotionAppendStatus.COMMITTED, record)
        except OSError:
            if last_result is not None:
                return last_result
            return finish(
                PromotionAppendStatus.INDETERMINATE
                if candidate_published
                else PromotionAppendStatus.NOT_COMMITTED
            )

    def recover(self, attempt_id: str) -> PromotionAppendResult:
        attempt_id = MarketV4CutoverService._validate_id(
            attempt_id, label="attempt"
        )
        last_result: PromotionAppendResult | None = None

        def finish(
            status: PromotionAppendStatus,
            record: PromotionJournalRecord | None = None,
        ) -> PromotionAppendResult:
            nonlocal last_result
            if status is PromotionAppendStatus.INDETERMINATE:
                self._authorization = None
                self._recovery_fence_attempt = attempt_id
            last_result = PromotionAppendResult(status, record, attempt_id)
            return last_result

        try:
            with self._locked(exclusive=True):
                _controls, intents, resolutions = self._control_state()
                intent = intents.get(attempt_id)
                if intent is None:
                    raise CutoverSafetyError(
                        "Promotion journal recovery intent is missing"
                    )
                resolution = resolutions.get(attempt_id)
                if resolution is not None and next(reversed(resolutions)) != attempt_id:
                    raise CutoverSafetyError(
                        "Promotion journal recovery attempt is not current"
                    )
                unresolved = set(intents) - set(resolutions)
                if resolution is None and unresolved != {attempt_id}:
                    raise CutoverSafetyError(
                        "Promotion journal recovery intent is not exact"
                    )
                if resolution is not None and unresolved:
                    raise CutoverSafetyError(
                        "Promotion journal recovery ledger has another unresolved intent"
                    )
                journal_fd = self._managed_root.open_dir(self._relative)
                name = cast(str, intent["target_name"])
                try:
                    try:
                        candidate_stat = os.stat(
                            name, dir_fd=journal_fd, follow_symlinks=False
                        )
                    except FileNotFoundError:
                        self._directory_fsync(journal_fd)
                        if resolution is not None and resolution["outcome"] != "rejected":
                            raise CutoverSafetyError(
                                "Promotion journal accepted candidate is missing"
                            )
                        if resolution is None:
                            try:
                                self._append_control(
                                    {
                                        "kind": "resolution",
                                        "operation_id": self.operation_id,
                                        "attempt_id": attempt_id,
                                        "target_sequence": intent["target_sequence"],
                                        "target_name": name,
                                        "payload_sha256": intent["payload_sha256"],
                                        "outcome": "rejected",
                                    },
                                    stage="resolution",
                                )
                            except OSError:
                                return finish(PromotionAppendStatus.INDETERMINATE)
                        control_fd = self._managed_root.open_dir(self._control_relative)
                        staging_fd = self._managed_root.open_dir(self._staging_relative)
                        try:
                            self._directory_fsync(staging_fd)
                            self._directory_fsync(control_fd)
                        finally:
                            os.close(staging_fd)
                            os.close(control_fd)
                        self._grant_authorization(
                            attempt_id=attempt_id,
                            sequence=cast(int, intent["target_sequence"]),
                            candidate_sha256=cast(str, intent["payload_sha256"]),
                        )
                        return finish(PromotionAppendStatus.NOT_COMMITTED)
                    if resolution is not None and resolution["outcome"] != "accepted":
                        raise CutoverSafetyError(
                            "Promotion journal rejected candidate is still visible"
                        )
                    if not stat.S_ISREG(candidate_stat.st_mode):
                        raise CutoverSafetyError(
                            "Promotion journal recovery candidate is suspicious"
                        )
                    raw = self._read_regular(
                        journal_fd,
                        name,
                        label="Promotion journal recovery candidate",
                    )
                    if hashlib.sha256(raw).hexdigest() != intent["payload_sha256"]:
                        raise CutoverSafetyError(
                            "Promotion journal recovery candidate payload mismatch"
                        )
                    candidate_fd = os.open(
                        name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=journal_fd
                    )
                    try:
                        self._file_fsync(candidate_fd)
                    finally:
                        os.close(candidate_fd)
                    self._directory_fsync(journal_fd)
                finally:
                    os.close(journal_fd)
                records = self._read_records_validated()
                sequence = cast(int, intent["target_sequence"])
                if len(records) != sequence:
                    raise CutoverSafetyError(
                        "Promotion journal recovery candidate sequence mismatch"
                    )
                record = records[-1]
                if (
                    record.state.value != intent["state"]
                    or self._identity_to_mapping(record.identities)
                    != intent["identities"]
                ):
                    raise CutoverSafetyError(
                        "Promotion journal recovery candidate identity mismatch"
                    )
                if resolution is None:
                    try:
                        self._append_control(
                            {
                                "kind": "resolution",
                                "operation_id": self.operation_id,
                                "attempt_id": attempt_id,
                                "target_sequence": sequence,
                                "target_name": name,
                                "payload_sha256": intent["payload_sha256"],
                                "outcome": "accepted",
                            },
                            stage="resolution",
                        )
                    except OSError:
                        return finish(PromotionAppendStatus.INDETERMINATE)
                control_fd = self._managed_root.open_dir(self._control_relative)
                staging_fd = self._managed_root.open_dir(self._staging_relative)
                try:
                    self._directory_fsync(staging_fd)
                    self._directory_fsync(control_fd)
                finally:
                    os.close(staging_fd)
                    os.close(control_fd)
                self._grant_authorization(
                    attempt_id=attempt_id,
                    sequence=sequence,
                    candidate_sha256=cast(str, intent["payload_sha256"]),
                )
                return finish(PromotionAppendStatus.COMMITTED, record)
        except OSError:
            return last_result or finish(PromotionAppendStatus.INDETERMINATE)


def _rename_exclusive_at(
    source_dir_fd: int,
    source_name: str,
    target_dir_fd: int,
    target_name: str,
) -> None:
    """Atomically rename without replacing an existing target."""
    libc = ctypes.CDLL(None, use_errno=True)
    source = os.fsencode(source_name)
    target = os.fsencode(target_name)
    if sys.platform == "darwin" and hasattr(libc, "renameatx_np"):
        function = libc.renameatx_np
        function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(source_dir_fd, source, target_dir_fd, target, 0x00000004)
    elif hasattr(libc, "renameat2"):
        function = libc.renameat2
        function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(source_dir_fd, source, target_dir_fd, target, 1)
    else:
        raise CutoverSafetyError("Atomic no-replace directory rename is unavailable")
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), target_name)


def _lexical_absolute(path: Path) -> Path:
    """Return an absolute path without resolving symlinks."""
    return Path(os.path.abspath(os.path.expanduser(os.fspath(path))))


def _assert_real_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise CutoverSafetyError(f"{label} is missing") from exc
    if stat.S_ISLNK(mode):
        raise CutoverSafetyError(f"{label} must not be a symlink")
    if not stat.S_ISDIR(mode):
        raise CutoverSafetyError(f"{label} must be a real directory")


def _assert_safe_directory_chain(path: Path, *, target_may_be_missing: bool = False) -> None:
    """Reject every existing symlink/non-directory component in a lexical path."""
    path = _lexical_absolute(path)
    chain = tuple(reversed((path, *path.parents)))
    for component in chain:
        try:
            mode = component.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise CutoverSafetyError(
                f"Managed path component must not be a symlink: {component.name or '/'}"
            )
        if not stat.S_ISDIR(mode):
            raise CutoverSafetyError("Managed path component must be a directory")
    if not target_may_be_missing and not path.exists():
        raise CutoverSafetyError("Managed directory is missing")


def _mkdir_safe_directory_chain(path: Path) -> None:
    """Create a lexical directory chain one component at a time, failing closed."""
    path = _lexical_absolute(path)
    _assert_safe_directory_chain(path, target_may_be_missing=True)
    current = os.open("/", _DIR_OPEN_FLAGS)
    try:
        for component in path.parts[1:]:
            try:
                child = os.open(component, _DIR_OPEN_FLAGS, dir_fd=current)
            except FileNotFoundError:
                os.mkdir(component, 0o700, dir_fd=current)
                child = os.open(component, _DIR_OPEN_FLAGS, dir_fd=current)
            os.close(current)
            current = child
    finally:
        os.close(current)
    _assert_safe_directory_chain(path)


def assert_market_managed_root_safe(data_root: Path, market_root: Path) -> None:
    """Reject managed roots that can redirect mutation outside the selected root."""
    data_root = _lexical_absolute(data_root)
    market_root = _lexical_absolute(market_root)
    _assert_safe_directory_chain(data_root)
    if market_root.parent != data_root:
        raise CutoverSafetyError("Market root must be a direct child of the data root")
    _assert_safe_directory_chain(market_root)
    _assert_real_directory(market_root, "Market time-series root")


def prepare_market_managed_root(data_root: Path, market_root: Path) -> None:
    """Create missing managed roots without traversing a symlink ancestor."""
    data_root = _lexical_absolute(data_root)
    market_root = _lexical_absolute(market_root)
    if market_root.parent != data_root:
        raise CutoverSafetyError("Market root must be a direct child of the data root")
    _mkdir_safe_directory_chain(data_root)
    _assert_safe_directory_chain(data_root)
    if market_root.exists() or market_root.is_symlink():
        _assert_safe_directory_chain(market_root)
    else:
        with ManagedRootFd.open(data_root) as managed:
            os.mkdir(market_root.name, 0o700, dir_fd=managed.fd)
    assert_market_managed_root_safe(data_root, market_root)


@dataclass
class MarketOperationLease:
    """Crash-safe cooperative flock shared by servers, writers, and cutover."""

    data_root: Path
    path: Path
    fd: int
    exclusive: bool
    root_fd: int = -1
    owns_fd: bool = True
    unlock_on_release: bool = True

    @classmethod
    def acquire(
        cls,
        data_root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
    ) -> MarketOperationLease:
        data_root = _lexical_absolute(data_root)
        managed_root = ManagedRootFd.open(data_root)
        path = data_root / ".market-timeseries.operation.lock"
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(
                ".market-timeseries.operation.lock",
                flags,
                0o600,
                dir_fd=managed_root.fd,
            )
        except OSError as exc:
            managed_root.close()
            raise CutoverSafetyError("Could not open Market operation lock") from exc
        try:
            fd_stat = os.fstat(descriptor)
            path_stat = path.lstat()
            if (
                not stat.S_ISREG(fd_stat.st_mode)
                or stat.S_ISLNK(path_stat.st_mode)
                or (fd_stat.st_dev, fd_stat.st_ino)
                != (path_stat.st_dev, path_stat.st_ino)
            ):
                raise CutoverSafetyError("Market operation lock must be a regular file")
            operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            if not blocking:
                operation |= fcntl.LOCK_NB
            fcntl.flock(descriptor, operation)
            os.fchmod(descriptor, 0o600)
        except BlockingIOError as exc:
            os.close(descriptor)
            managed_root.close()
            raise CutoverSafetyError("Market operation lease is held by another process") from exc
        except Exception:
            os.close(descriptor)
            managed_root.close()
            raise
        return cls(
            data_root=data_root,
            path=path,
            fd=descriptor,
            exclusive=exclusive,
            root_fd=managed_root.fd,
        )

    @classmethod
    def acquire_existing(
        cls,
        data_root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
    ) -> MarketOperationLease:
        """Acquire an existing lease without creating or changing lock metadata."""

        data_root = _lexical_absolute(data_root)
        managed_root = ManagedRootFd.open(data_root)
        path = data_root / ".market-timeseries.operation.lock"
        flags = os.O_RDONLY | _FILE_NOFOLLOW
        try:
            descriptor = os.open(
                ".market-timeseries.operation.lock",
                flags,
                dir_fd=managed_root.fd,
            )
        except OSError as exc:
            managed_root.close()
            raise CutoverSafetyError(
                "An existing Market operation lock is required"
            ) from exc
        try:
            fd_stat = os.fstat(descriptor)
            path_stat = os.stat(
                ".market-timeseries.operation.lock",
                dir_fd=managed_root.fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(fd_stat.st_mode)
                or stat.S_ISLNK(path_stat.st_mode)
                or (fd_stat.st_dev, fd_stat.st_ino)
                != (path_stat.st_dev, path_stat.st_ino)
            ):
                raise CutoverSafetyError(
                    "Existing Market operation lock must be a regular file"
                )
            operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            if not blocking:
                operation |= fcntl.LOCK_NB
            fcntl.flock(descriptor, operation)
            current_path_stat = os.stat(
                ".market-timeseries.operation.lock",
                dir_fd=managed_root.fd,
                follow_symlinks=False,
            )
            if (
                stat.S_ISLNK(current_path_stat.st_mode)
                or not stat.S_ISREG(current_path_stat.st_mode)
                or (fd_stat.st_dev, fd_stat.st_ino)
                != (current_path_stat.st_dev, current_path_stat.st_ino)
            ):
                raise CutoverSafetyError(
                    "Existing Market operation lock identity changed"
                )
        except BlockingIOError as exc:
            os.close(descriptor)
            managed_root.close()
            raise CutoverSafetyError(
                "Market operation lease is held by another process"
            ) from exc
        except Exception:
            os.close(descriptor)
            managed_root.close()
            raise
        return cls(
            data_root=data_root,
            path=path,
            fd=descriptor,
            exclusive=exclusive,
            root_fd=managed_root.fd,
        )

    @classmethod
    def adopt_inherited(
        cls,
        data_root: Path,
        fd: int,
        *,
        root_fd: int | None = None,
    ) -> MarketOperationLease:
        data_root = _lexical_absolute(data_root)
        if root_fd is None:
            managed_root = ManagedRootFd.open(data_root)
        else:
            try:
                root_stat = os.fstat(root_fd)
            except OSError as exc:
                raise CutoverSafetyError(
                    "Inherited Market data-root descriptor is invalid"
                ) from exc
            if not stat.S_ISDIR(root_stat.st_mode):
                raise CutoverSafetyError(
                    "Inherited Market data-root descriptor is not a directory"
                )
            managed_root = ManagedRootFd(data_root, root_fd)
        path = data_root / ".market-timeseries.operation.lock"
        try:
            fd_stat = os.fstat(fd)
            path_stat = os.stat(
                ".market-timeseries.operation.lock",
                dir_fd=managed_root.fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            managed_root.close()
            raise CutoverSafetyError("Inherited Market operation lease is invalid") from exc
        if (
            not stat.S_ISREG(fd_stat.st_mode)
            or stat.S_ISLNK(path_stat.st_mode)
            or (fd_stat.st_dev, fd_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino)
        ):
            managed_root.close()
            raise CutoverSafetyError("Inherited Market operation lease identity mismatch")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            managed_root.close()
            raise CutoverSafetyError(
                "Inherited Market operation lease does not carry the exclusive lock"
            ) from exc
        probe_flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            probe_flags |= os.O_NOFOLLOW
        try:
            probe = os.open(
                ".market-timeseries.operation.lock",
                probe_flags,
                dir_fd=managed_root.fd,
            )
            try:
                try:
                    fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    fcntl.flock(probe, fcntl.LOCK_UN)
                    raise CutoverSafetyError(
                        "Inherited Market operation lease did not establish exclusivity"
                    )
            finally:
                os.close(probe)
        except Exception:
            managed_root.close()
            raise
        return cls(
            data_root=data_root,
            path=path,
            fd=fd,
            exclusive=True,
            root_fd=managed_root.fd,
            owns_fd=True,
            unlock_on_release=False,
        )

    def __enter__(self) -> MarketOperationLease:
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def __del__(self) -> None:
        if getattr(self, "fd", -1) >= 0:
            try:
                self.release()
            except Exception:
                pass

    def release(self) -> None:
        if self.fd < 0:
            return
        try:
            if self.unlock_on_release:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            if self.owns_fd:
                os.close(self.fd)
            if self.root_fd >= 0:
                os.close(self.root_fd)
                self.root_fd = -1
            self.fd = -1


@dataclass(frozen=True)
class MarketSourceMetadata:
    schema_version: int | None
    adjustment_mode: str | None
    adjusted_metrics_ready: bool = True


@dataclass(frozen=True)
class BackupResult:
    backup_id: str


@dataclass(frozen=True)
class RestoreResult:
    backup_id: str
    quarantine_path: str | None


@dataclass(frozen=True)
class SmokeConfig:
    symbol: str
    strategy: str
    dataset_preset: str


@dataclass(frozen=True)
class SmokeResult:
    schema_version: int
    adjustment_mode: str
    checks: tuple[str, ...]
    api_paths: tuple[str, ...]
    lineage: dict[str, int]


@dataclass(frozen=True)
class OperationResult:
    report_id: str
    report_path: str


@dataclass(frozen=True)
class RetainedPromotionEligibility:
    """Immutable evidence gathered before retained promotion may mutate disk."""

    retained_report_id: str
    retained_report_sha256: str
    source_report_id: str
    source_report_sha256: str
    retained_root: Path
    source_market_identity: dict[str, object]
    active_market_identity: dict[str, object]
    target_root_fingerprint: str
    configuration_fingerprint: str


@dataclass(frozen=True)
class DetachedArtifactEvidence:
    """Exact descriptor-derived identity for one held promotion artifact."""

    name: str
    kind: str
    identity: dict[str, object]
    directories: dict[str, dict[str, int]]
    files: dict[str, dict[str, object]]

    def to_mapping(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": self.kind,
            "identity": self.identity,
            "directories": self.directories,
            "files": self.files,
        }


@dataclass(frozen=True)
class RetainedPromotionPreparation:
    """Durable evidence produced before a retained Market exchange."""

    eligibility: RetainedPromotionEligibility
    backup_id: str
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    backup_payload_identity: dict[str, object]
    holding_root: Path
    holding_directory_identity: dict[str, int]
    detached_runtime_names: tuple[str, ...]
    detached_artifacts: tuple[DetachedArtifactEvidence, ...]


@dataclass(frozen=True)
class RetainedPromotionContext:
    """Live exact evidence needed to unwind one retained promotion attempt."""

    preparation: RetainedPromotionPreparation
    journal: PromotionJournal


@dataclass(frozen=True)
class RetainedPromotionReportExpectation:
    """Authoritative, independently constructed promotion report contract."""

    report_id: str
    created_at: str
    code_version: str
    retained_report: dict[str, object]
    source_report: dict[str, object]
    fingerprints: dict[str, object]
    payload_identities: dict[str, object]
    filesystem_evidence: dict[str, object]
    backup_id: str
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    backup_evidence: dict[str, object]
    journal: dict[str, object]
    quarantine_path: str
    runtime_cleanup: dict[str, object]
    no_sync: bool
    no_jquants: bool
    api_checks: tuple[str, ...]
    server_process_joined: bool
    worker_process_joined: bool
    semantic_smoke: dict[str, object]
    source_consumed: dict[str, object]
    rollback_instructions: str

    def to_report(self) -> dict[str, object]:
        report = {
            "schemaVersion": 1,
            "reportId": self.report_id,
            "phase": "promotion",
            "status": "passed",
            "activationMode": "retained_atomic_exchange",
            "createdAt": self.created_at,
            "codeVersion": self.code_version,
            "retainedReport": self.retained_report,
            "sourceReport": self.source_report,
            "fingerprints": self.fingerprints,
            "payloadIdentities": self.payload_identities,
            "filesystemEvidence": self.filesystem_evidence,
            "backupId": self.backup_id,
            "backupManifestSha256": self.backup_manifest_sha256,
            "backupFileSetSha256": self.backup_file_set_sha256,
            "backupEvidence": self.backup_evidence,
            "journal": self.journal,
            "quarantinePath": self.quarantine_path,
            "runtimeCleanup": self.runtime_cleanup,
            "noSync": self.no_sync,
            "noJQuants": self.no_jquants,
            "apiChecks": list(self.api_checks),
            "serverProcessJoined": self.server_process_joined,
            "workerProcessJoined": self.worker_process_joined,
            "semanticSmoke": self.semantic_smoke,
            "sourceConsumed": self.source_consumed,
            "rollbackInstructions": self.rollback_instructions,
        }
        # The report candidate must never share mutable nested mappings with the
        # independently assembled expectation used to validate it.
        return cast(
            dict[str, object],
            json.loads(PromotionJournal._canonical_json(report)),
        )


class AtomicExchange(Protocol):
    """Capability for atomically exchanging two managed directories."""

    def exchange(
        self,
        managed_root: ManagedRootFd,
        left: Path,
        right: Path,
    ) -> None: ...


class DarwinAtomicExchange:
    """Darwin descriptor-relative directory exchange with fail-closed guards."""

    RENAME_SWAP = 0x2

    @staticmethod
    def require_capability() -> object:
        if sys.platform != "darwin":
            raise CutoverSafetyError("Atomic directory exchange is unavailable")
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            rename_swap = libc.renameatx_np
        except AttributeError as exc:
            raise CutoverSafetyError("Atomic directory exchange is unavailable") from exc
        rename_swap.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename_swap.restype = ctypes.c_int
        return rename_swap

    @staticmethod
    def _directory_identity(directory_fd: int) -> tuple[int, int]:
        directory_stat = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory_stat.st_mode):
            raise CutoverSafetyError("Atomic exchange parent must be a real directory")
        return directory_stat.st_dev, directory_stat.st_ino

    @staticmethod
    def _leaf_stat(parent_fd: int, name: str) -> os.stat_result:
        try:
            leaf_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise CutoverSafetyError("Atomic exchange leaf is unavailable") from exc
        if stat.S_ISLNK(leaf_stat.st_mode) or not stat.S_ISDIR(leaf_stat.st_mode):
            raise CutoverSafetyError("Atomic exchange leaf must be a real directory")
        return leaf_stat

    @staticmethod
    def _open_leaf(parent_fd: int, name: str) -> int:
        try:
            leaf_fd = os.open(name, _DIR_OPEN_FLAGS, dir_fd=parent_fd)
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise CutoverSafetyError(
                    "Atomic exchange leaf must be a real directory"
                ) from exc
            raise CutoverSafetyError("Atomic exchange leaf is unavailable") from exc
        if not stat.S_ISDIR(os.fstat(leaf_fd).st_mode):
            os.close(leaf_fd)
            raise CutoverSafetyError("Atomic exchange leaf must be a real directory")
        return leaf_fd

    @classmethod
    def _assert_parent_identity(
        cls,
        managed_root: ManagedRootFd,
        relative: Path,
        retained_fd: int,
    ) -> None:
        current_fd, _ = managed_root.open_parent(relative)
        try:
            if cls._directory_identity(current_fd) != cls._directory_identity(
                retained_fd
            ):
                raise CutoverSafetyError("Atomic exchange parent identity changed")
        finally:
            os.close(current_fd)

    @classmethod
    def _assert_leaf_identity(
        cls,
        parent_fd: int,
        name: str,
        retained_fd: int,
    ) -> os.stat_result:
        current = cls._leaf_stat(parent_fd, name)
        retained = os.fstat(retained_fd)
        if (current.st_dev, current.st_ino) != (
            retained.st_dev,
            retained.st_ino,
        ):
            raise CutoverSafetyError("Atomic exchange leaf identity changed")
        return retained

    @staticmethod
    def _fsync_parents(left_parent: int, right_parent: int) -> None:
        failures: list[OSError] = []
        for parent in (left_parent, right_parent):
            try:
                os.fsync(parent)
            except OSError as exc:
                failures.append(exc)
        if failures:
            primary = failures[0]
            for additional in failures[1:]:
                primary.add_note(f"Additional parent fsync failure: {additional}")
            raise primary

    def exchange(
        self,
        managed_root: ManagedRootFd,
        left: Path,
        right: Path,
    ) -> None:
        rename_swap = self.require_capability()

        left_parent, left_name = managed_root.open_parent(left)
        try:
            right_parent, right_name = managed_root.open_parent(right)
        except Exception:
            os.close(left_parent)
            raise
        left_leaf_fd = -1
        right_leaf_fd = -1
        try:
            left_leaf_fd = self._open_leaf(left_parent, left_name)
            right_leaf_fd = self._open_leaf(right_parent, right_name)
            left_parent_identity = self._directory_identity(left_parent)
            right_parent_identity = self._directory_identity(right_parent)
            self._assert_parent_identity(managed_root, left, left_parent)
            self._assert_parent_identity(managed_root, right, right_parent)
            left_leaf = self._assert_leaf_identity(
                left_parent, left_name, left_leaf_fd
            )
            right_leaf = self._assert_leaf_identity(
                right_parent, right_name, right_leaf_fd
            )
            devices = {
                left_parent_identity[0],
                right_parent_identity[0],
                left_leaf.st_dev,
                right_leaf.st_dev,
            }
            if len(devices) != 1:
                raise CutoverSafetyError(
                    "Atomic exchange directories must be on the same device"
                )

            result = rename_swap(
                left_parent,
                os.fsencode(left_name),
                right_parent,
                os.fsencode(right_name),
                self.RENAME_SWAP,
            )
            if result != 0:
                error = ctypes.get_errno()
                if error in {
                    errno.ENOSYS,
                    errno.ENOTSUP,
                    errno.EOPNOTSUPP,
                    errno.EXDEV,
                }:
                    raise CutoverSafetyError(
                        "Atomic directory exchange is unsupported"
                    )
                raise OSError(error, os.strerror(error))

            try:
                self._assert_parent_identity(managed_root, left, left_parent)
                self._assert_parent_identity(managed_root, right, right_parent)
                self._assert_leaf_identity(left_parent, left_name, right_leaf_fd)
                self._assert_leaf_identity(right_parent, right_name, left_leaf_fd)
            except Exception as validation_error:
                try:
                    self._fsync_parents(left_parent, right_parent)
                except OSError as sync_error:
                    validation_error.add_note(
                        f"Parent fsync also failed after committed swap: {sync_error}"
                    )
                raise
            self._fsync_parents(left_parent, right_parent)
        finally:
            if left_leaf_fd >= 0:
                os.close(left_leaf_fd)
            if right_leaf_fd >= 0:
                os.close(right_leaf_fd)
            os.close(left_parent)
            os.close(right_parent)


class DuckDbAdapter(Protocol):
    """Exclusive DuckDB operations used by the workflow."""

    def checkpoint_exclusive(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata: ...

    def checkpoint_snapshot(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> ContextManager[MarketSourceMetadata]: ...

    def inspect(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata: ...


class RuntimeAdapter(Protocol):
    """Owned server process and HTTP operations used by the workflow."""

    def assert_quiescent(self, data_root: Path) -> None: ...

    def start(
        self,
        *,
        root_fd: int,
        market_fd: int,
        lease_fd: int,
        retained_lease_fd: int | None = None,
        environment: dict[str, str],
        log_path: Path,
        log_fd: int,
    ) -> ApiAdapter: ...

    def cancel_owned_work(self, api: ApiAdapter) -> None: ...

    def stop(self, api: ApiAdapter) -> None: ...


class ApiAdapter(Protocol):
    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]: ...


class DefaultDuckDbAdapter:
    """Raw DuckDB adapter bound to an inherited directory descriptor."""

    _WORKER_EXIT_TIMEOUT_SECONDS = 30.0
    _WORKER_STOP_TIMEOUT_SECONDS = 5.0
    _MAX_METADATA_BYTES = 64 * 1024

    @staticmethod
    def _metadata(connection: object) -> MarketSourceMetadata:
        execute = getattr(connection, "execute")

        def table_exists(table_name: str) -> bool:
            row = execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return bool(row and int(row[0]) > 0)

        def count_rows(table_name: str) -> int:
            if not table_exists(table_name):
                return 0
            escaped = '"' + table_name.replace('"', '""') + '"'
            row = execute(f"SELECT COUNT(*) FROM {escaped}").fetchone()
            return int(row[0] or 0) if row else 0

        def fetchone(sql: str, params: object = None) -> object:
            return execute(sql, params or []).fetchone()

        try:
            schema_row = execute(
                "SELECT MAX(version) FROM market_schema_version"
            ).fetchone()
            schema_version = schema_row[0] if schema_row else None
        except Exception:
            schema_version = None
        try:
            mode_row = execute(
                "SELECT value FROM sync_metadata "
                "WHERE key = 'stock_price_adjustment_mode'"
            ).fetchone()
            adjustment_mode = mode_row[0] if mode_row else None
        except Exception:
            adjustment_mode = None
        try:
            snapshot = get_adjusted_metrics_snapshot(
                table_exists,
                count_rows,
                fetchone,
            )
            diagnostics = get_adjusted_metrics_source_diagnostics(
                table_exists,
                fetchone,
            )
            positive_keys = (
                "statementRows",
                "dailyValuationRows",
                "readyBasisCount",
            )
            positive_diagnostic_keys = (
                "sourceStatementKeyCount",
                "expectedAdjustedStatementRows",
            )
            zero_snapshot_keys = (
                "invalidBasisCount",
                "underCoveredActiveBasisCount",
                "overlappingBasisCount",
                "orphanAdjustedStatementRows",
                "orphanDailyValuationRows",
            )
            zero_diagnostic_keys = (
                "missingAdjustedStatementRows",
                "extraAdjustedStatementRows",
                "staleAdjustedStatementRows",
                "wrongBasisAdjustedStatementRows",
                "missingDailyValuationRows",
                "extraDailyValuationRows",
                "wrongBasisDailyValuationRows",
            )
            adjusted_metrics_ready = (
                all(int(snapshot.get(key, 0) or 0) > 0 for key in positive_keys)
                and all(
                    int(diagnostics.get(key, 0) or 0) > 0
                    for key in positive_diagnostic_keys
                )
                and all(int(snapshot.get(key, 0) or 0) == 0 for key in zero_snapshot_keys)
                and all(
                    int(diagnostics.get(key, 0) or 0) == 0
                    for key in zero_diagnostic_keys
                )
            )
        except Exception:
            adjusted_metrics_ready = False
        return MarketSourceMetadata(
            schema_version=(int(schema_version) if schema_version is not None else None),
            adjustment_mode=(str(adjustment_mode) if adjustment_mode is not None else None),
            adjusted_metrics_ready=adjusted_metrics_ready,
        )

    @staticmethod
    def _validate_target(directory_fd: int, filename: str) -> None:
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
            raise CutoverSafetyError("DuckDB parent descriptor must be a directory")
        if filename in {"", ".", ".."} or Path(filename).name != filename:
            raise CutoverSafetyError("DuckDB filename must be a safe leaf name")

    @staticmethod
    def _worker_argv(
        operation: str,
        directory_fd: int,
        guard_lease_fd: int,
        filename: str,
    ) -> list[str]:
        return [
            sys.executable,
            "-c",
            (
                "from src.application.services.market_v4_cutover import "
                "directory_bound_duckdb_worker as worker; worker()"
            ),
            operation,
            str(directory_fd),
            str(guard_lease_fd),
            filename,
        ]

    @classmethod
    def _start_worker(
        cls,
        operation: str,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> subprocess.Popen[bytes]:
        cls._validate_target(directory_fd, filename)
        if not stat.S_ISREG(os.fstat(guard_lease_fd).st_mode):
            raise CutoverSafetyError("DuckDB worker guard lease must be a regular file")
        return subprocess.Popen(
            cls._worker_argv(operation, directory_fd, guard_lease_fd, filename),
            cwd=Path(__file__).resolve().parents[3],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(directory_fd, guard_lease_fd),
        )

    @classmethod
    def _read_metadata(cls, process: subprocess.Popen[bytes]) -> MarketSourceMetadata:
        assert process.stdout is not None
        stdout_fd = process.stdout.fileno()
        was_blocking = os.get_blocking(stdout_fd)
        deadline = time.monotonic() + cls._WORKER_EXIT_TIMEOUT_SECONDS
        buffer = bytearray()
        os.set_blocking(stdout_fd, False)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(stdout_fd, selectors.EVENT_READ)
                while b"\n" not in buffer:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise CutoverSafetyError(
                            "Directory-bound DuckDB worker metadata timed out"
                        )
                    chunk = os.read(stdout_fd, 4096)
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    if len(buffer) > cls._MAX_METADATA_BYTES:
                        raise CutoverSafetyError(
                            "Directory-bound DuckDB worker metadata is oversized"
                        )
        finally:
            os.set_blocking(stdout_fd, was_blocking)
        line = bytes(buffer).partition(b"\n")[0]
        if not line:
            raise CutoverSafetyError("Directory-bound DuckDB worker returned no metadata")
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CutoverSafetyError(
                "Directory-bound DuckDB worker returned invalid metadata"
            ) from exc
        return MarketSourceMetadata(
            schema_version=payload.get("schemaVersion"),
            adjustment_mode=payload.get("adjustmentMode"),
            adjusted_metrics_ready=payload.get("adjustedMetricsReady") is True,
        )

    @classmethod
    def _shutdown_worker(
        cls,
        process: subprocess.Popen[bytes],
        *,
        release_checkpoint: bool,
    ) -> WorkerShutdownError | None:
        """Release, stop, and reap a worker without leaking pipes or processes."""
        cleanup_error: Exception | None = None
        stdin = process.stdin
        if stdin is not None:
            try:
                if release_checkpoint:
                    stdin.write(b"x")
            except (BrokenPipeError, OSError) as exc:
                cleanup_error = exc
            finally:
                try:
                    stdin.close()
                except OSError as exc:
                    cleanup_error = cleanup_error or exc
                # Popen.communicate() flushes a non-None stdin even when it has
                # already been closed. Detach it before draining the other pipes.
                process.stdin = None

        return_code: int | None = None
        forced_stop = False
        process_joined = False
        try:
            return_code = process.wait(timeout=cls._WORKER_EXIT_TIMEOUT_SECONDS)
            process_joined = True
        except subprocess.TimeoutExpired:
            forced_stop = True
            try:
                process.terminate()
            except Exception as exc:
                cleanup_error = cleanup_error or exc
            try:
                return_code = process.wait(timeout=cls._WORKER_STOP_TIMEOUT_SECONDS)
                process_joined = True
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
                try:
                    return_code = process.wait(timeout=cls._WORKER_STOP_TIMEOUT_SECONDS)
                    process_joined = True
                except subprocess.TimeoutExpired as exc:
                    cleanup_error = cleanup_error or exc
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
            except Exception as exc:
                cleanup_error = cleanup_error or exc
        except Exception as exc:
            cleanup_error = cleanup_error or exc

        stderr = b""
        try:
            _stdout, stderr = process.communicate(
                timeout=cls._WORKER_STOP_TIMEOUT_SECONDS
            )
            process_joined = True
        except subprocess.TimeoutExpired as exc:
            cleanup_error = cleanup_error or exc
            try:
                process.kill()
            except Exception as kill_exc:
                cleanup_error = cleanup_error or kill_exc
            try:
                _stdout, stderr = process.communicate(
                    timeout=cls._WORKER_STOP_TIMEOUT_SECONDS
                )
                process_joined = True
            except Exception as final_exc:
                cleanup_error = cleanup_error or final_exc
        except Exception as exc:
            cleanup_error = cleanup_error or exc

        if cleanup_error is not None:
            action = "release" if release_checkpoint else "shutdown"
            return WorkerShutdownError(
                f"Directory-bound DuckDB worker {action} failed: {cleanup_error}",
                process_joined=process_joined,
            )
        if forced_stop:
            return WorkerShutdownError(
                "Directory-bound DuckDB worker timed out",
                process_joined=process_joined,
            )
        if return_code != 0:
            return WorkerShutdownError(
                "Directory-bound DuckDB worker failed: "
                + stderr.decode(errors="replace")[-500:],
                process_joined=process_joined,
            )
        return None

    def checkpoint_exclusive(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        with self.checkpoint_snapshot(
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        ) as metadata:
            return metadata

    @contextmanager
    def checkpoint_snapshot(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> Iterator[MarketSourceMetadata]:
        process = self._start_worker(
            "checkpoint",
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        )
        primary_error = False
        try:
            metadata = self._read_metadata(process)
            yield metadata
        except BaseException:
            primary_error = True
            raise
        finally:
            cleanup_error = self._shutdown_worker(
                process,
                release_checkpoint=True,
            )
            if cleanup_error is not None and (
                not primary_error or not cleanup_error.process_joined
            ):
                raise cleanup_error

    def inspect(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        process = self._start_worker(
            "inspect",
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        )
        primary_error = False
        try:
            return self._read_metadata(process)
        except BaseException:
            primary_error = True
            raise
        finally:
            cleanup_error = self._shutdown_worker(
                process,
                release_checkpoint=False,
            )
            if cleanup_error is not None and (
                not primary_error or not cleanup_error.process_joined
            ):
                raise cleanup_error


def directory_bound_duckdb_worker() -> None:
    """Run one raw DuckDB operation after anchoring cwd to an inherited fd."""
    import duckdb

    operation, raw_fd, raw_guard_fd, filename = sys.argv[-4:]
    directory_fd = int(raw_fd)
    guard_lease_fd = int(raw_guard_fd)
    if operation not in {"checkpoint", "inspect"}:
        raise SystemExit("Unsupported DuckDB worker operation")
    if filename in {"", ".", ".."} or Path(filename).name != filename:
        raise SystemExit("Unsafe DuckDB filename")
    if not stat.S_ISREG(os.fstat(guard_lease_fd).st_mode):
        raise SystemExit("Invalid DuckDB worker guard lease")
    os.fchdir(directory_fd)
    connection = duckdb.connect(filename, read_only=operation == "inspect")
    try:
        metadata = DefaultDuckDbAdapter._metadata(connection)
        if operation == "checkpoint":
            connection.execute("CHECKPOINT")
        print(
            json.dumps(
                {
                    "schemaVersion": metadata.schema_version,
                    "adjustmentMode": metadata.adjustment_mode,
                    "adjustedMetricsReady": metadata.adjusted_metrics_ready,
                }
            ),
            flush=True,
        )
        if operation == "checkpoint":
            sys.stdin.buffer.read(1)
    finally:
        connection.close()


_CREATE_JOB_RESPONSE_FIELDS: dict[str, tuple[str, str]] = {
    "/api/db/sync": ("sync", "jobId"),
    "/api/db/adjusted-metrics/materialize": ("materialize", "jobId"),
    "/api/analytics/screening/jobs": ("screening", "job_id"),
    "/api/dataset": ("dataset", "jobId"),
}


class HttpApiAdapter:
    """Small synchronous JSON client for one owned cutover server."""

    def __init__(self, base_url: str, *, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.owned_jobs: dict[str, str] = {}

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        body = None
        headers = {"accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode()
            headers["content-type"] = "application/json"
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise CutoverSafetyError(
                f"API {method} {path} failed with HTTP {exc.code}: {detail[:500]}"
            ) from exc
        except (OSError, URLError) as exc:
            raise CutoverSafetyError(f"API {method} {path} failed") from exc
        try:
            value = json.loads(raw or b"{}")
        except json.JSONDecodeError as exc:
            raise CutoverSafetyError(f"API {method} {path} returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise CutoverSafetyError(f"API {method} {path} returned a non-object")
        job_response = _CREATE_JOB_RESPONSE_FIELDS.get(path)
        if job_response is not None:
            job_kind, job_id_field = job_response
            job_id = value.get(job_id_field)
            if isinstance(job_id, str) and job_id:
                self.owned_jobs[job_kind] = job_id
        return value


@dataclass
class _OwnedProcess:
    process: subprocess.Popen[bytes]
    log_handle: object
    log_path: Path
    log_fd: int
    environment: dict[str, str]


class SubprocessRuntimeAdapter:
    """Owns an isolated uvicorn process from start through joined shutdown."""

    def __init__(
        self,
        *,
        startup_timeout_seconds: float = 60.0,
    ) -> None:
        self.startup_timeout_seconds = startup_timeout_seconds
        self._owned: dict[int, _OwnedProcess] = {}

    def assert_quiescent(self, data_root: Path) -> None:
        # The root-scoped exclusive flock acquired by the caller is the
        # authoritative writer-quiescence proof. A fixed TCP port can belong to
        # another data root and must never accept or reject this operation.
        del data_root

    @staticmethod
    def server_argv(port: int, *, market_fd: int) -> list[str]:
        project_root = Path(__file__).resolve().parents[3]
        bootstrap = (
            "import os,runpy,sys;"
            "market_fd=int(sys.argv[1]);"
            "sys.path.insert(0,sys.argv[2]);"
            "os.fchdir(market_fd);"
            "sys.argv=sys.argv[3:];"
            "runpy.run_module('uvicorn',run_name='__main__')"
        )
        return [
            sys.executable,
            "-c",
            bootstrap,
            str(market_fd),
            str(project_root),
            "uvicorn",
            "src.entrypoints.http.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]

    @staticmethod
    def _reserve_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def start(
        self,
        *,
        root_fd: int,
        market_fd: int,
        lease_fd: int,
        retained_lease_fd: int | None = None,
        environment: dict[str, str],
        log_path: Path,
        log_fd: int,
    ) -> ApiAdapter:
        port = self._reserve_port()
        child_environment = dict(environment)
        child_environment.update(
            {
                "TRADING25_DATA_ROOT_FD": str(root_fd),
                "TRADING25_MARKET_OPERATION_LOCK_FD": str(lease_fd),
            }
        )
        pass_fds = [root_fd, market_fd, lease_fd]
        if retained_lease_fd is not None:
            os.fstat(retained_lease_fd)
            child_environment["TRADING25_RETAINED_MARKET_OPERATION_LOCK_FD"] = str(
                retained_lease_fd
            )
            pass_fds.append(retained_lease_fd)
        log_handle: BinaryIO | None = None
        retained_log_fd = -1
        try:
            log_handle = os.fdopen(os.dup(log_fd), "wb", buffering=0)
            retained_log_fd = os.dup(log_fd)
            process = subprocess.Popen(
                self.server_argv(port, market_fd=market_fd),
                cwd=Path(__file__).resolve().parents[3],
                env=child_environment,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                pass_fds=tuple(dict.fromkeys(pass_fds)),
            )
        except Exception:
            if log_handle is not None:
                log_handle.close()
            if retained_log_fd >= 0:
                os.close(retained_log_fd)
            raise
        api = HttpApiAdapter(f"http://127.0.0.1:{port}")
        self._owned[id(api)] = _OwnedProcess(
            process=process,
            log_handle=log_handle,
            log_path=log_path,
            log_fd=retained_log_fd,
            environment=child_environment,
        )
        deadline = time.monotonic() + self.startup_timeout_seconds
        try:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.stop(api)
                    raise CutoverSafetyError(
                        "Owned FastAPI server exited during startup"
                    )
                try:
                    health = api.request("GET", "/api/health")
                except CutoverSafetyError:
                    time.sleep(0.1)
                    continue
                if health.get("status") == "healthy":
                    return api
                time.sleep(0.1)
            self.stop(api)
            raise CutoverSafetyError("Owned FastAPI server startup timed out")
        except RuntimeStopError:
            raise
        except Exception as exc:
            try:
                self.stop(api)
            except RuntimeStopError as stop_error:
                raise stop_error from exc
            except Exception as stop_error:
                raise RuntimeStopError(
                    "Owned FastAPI startup cleanup has no join verdict",
                    process_joined=False,
                ) from stop_error
            raise

    def cancel_owned_work(self, api: ApiAdapter) -> None:
        if not isinstance(api, HttpApiAdapter):
            return
        routes = {
            "sync": (
                "DELETE",
                "/api/db/sync/jobs/{job_id}",
                "/api/db/sync/jobs/{job_id}",
            ),
            "materialize": (
                "DELETE",
                "/api/db/adjusted-metrics/materialize/jobs/{job_id}",
                "/api/db/adjusted-metrics/materialize/jobs/{job_id}",
            ),
            "screening": (
                "POST",
                "/api/analytics/screening/jobs/{job_id}/cancel",
                "/api/analytics/screening/jobs/{job_id}",
            ),
            "dataset": (
                "DELETE",
                "/api/dataset/jobs/{job_id}",
                "/api/dataset/jobs/{job_id}",
            ),
        }
        for kind, job_id in list(api.owned_jobs.items()):
            cancel_method, cancel_template, status_template = routes[kind]
            encoded = quote(job_id, safe="")
            try:
                api.request(cancel_method, cancel_template.format(job_id=encoded))
            except CutoverSafetyError:
                pass
            terminal = False
            for _ in range(3_600):
                try:
                    job = api.request("GET", status_template.format(job_id=encoded))
                except CutoverSafetyError:
                    break
                if job.get("status") in {"completed", "failed", "cancelled"}:
                    terminal = True
                    break
                time.sleep(0.5)
            if not terminal:
                raise CutoverSafetyError(
                    f"Owned {kind} job did not reach a terminal state after cancellation"
                )

    def stop(self, api: ApiAdapter) -> None:
        owned = self._owned.get(id(api))
        if owned is None:
            return
        process = owned.process
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=600)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=30)
        except Exception as exc:
            raise RuntimeStopError(
                "Owned FastAPI process could not be proven stopped",
                process_joined=False,
            ) from exc
        self._owned.pop(id(api), None)
        try:
            try:
                close = getattr(owned.log_handle, "close")
                close()
            finally:
                try:
                    self.redact_log_fd(owned.log_fd, owned.environment)
                finally:
                    os.close(owned.log_fd)
        except Exception as exc:
            raise RuntimeStopError(
                "Owned FastAPI process stopped but log cleanup failed",
                process_joined=True,
            ) from exc

    @staticmethod
    def redact_log_fd(log_fd: int, environment: dict[str, str]) -> None:
        if not stat.S_ISREG(os.fstat(log_fd).st_mode):
            raise CutoverSafetyError("Owned server log must be a regular file")
        os.lseek(log_fd, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        while chunk := os.read(log_fd, 1024 * 1024):
            chunks.append(chunk)
        text = b"".join(chunks).decode("utf-8", errors="replace")
        path_keys = {
            "XDG_DATA_HOME",
            "TRADING25_DATA_DIR",
            "MARKET_TIMESERIES_DIR",
            "MARKET_DB_PATH",
            "DATASET_BASE_PATH",
            "PORTFOLIO_DB_PATH",
            "TRADING25_STRATEGIES_DIR",
            "TRADING25_BACKTEST_DIR",
            "TRADING25_DEFAULT_CONFIG_PATH",
        }
        for key, value in environment.items():
            if not value:
                continue
            upper = key.upper()
            if key in path_keys:
                if Path(value).is_absolute():
                    text = text.replace(value, f"<{key.lower()}>")
            elif any(token in upper for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")):
                text = text.replace(value, "<redacted-secret>")
        text = text.replace(str(Path.home()), "<home>")
        payload = text.encode()
        os.lseek(log_fd, 0, os.SEEK_SET)
        os.ftruncate(log_fd, 0)
        view = memoryview(payload)
        while view:
            written = os.write(log_fd, view)
            view = view[written:]
        os.fchmod(log_fd, 0o600)
        os.fsync(log_fd)


class MarketV4CutoverService:
    """Coordinates explicit, gated Market v4 maintenance phases."""

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
        self.data_root = _lexical_absolute(data_root)
        self.duckdb = duckdb
        self.runtime = runtime
        self.disk_free_bytes = disk_free_bytes
        self.now = now
        self.code_version = code_version
        self.atomic_exchange = (
            DarwinAtomicExchange() if atomic_exchange is None else atomic_exchange
        )
        self._active_lease: MarketOperationLease | None = None
        self._retained_lease: MarketOperationLease | None = None
        self._active_code_version: str | None = None
        self._managed_root_fd: ManagedRootFd | None = None
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
        path = _lexical_absolute(path)
        try:
            path.relative_to(self.data_root)
        except ValueError as exc:
            raise CutoverSafetyError("Managed operation path escapes the data root") from exc
        return path

    def _managed_relative(self, path: Path) -> Path:
        path = self._managed_path(path)
        relative = path.relative_to(self.data_root)
        if not relative.parts:
            raise CutoverSafetyError("Managed operation must be below the data root")
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
            raise CutoverSafetyError("Managed operation destination already exists") from exc
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
        raise CutoverSafetyError("Managed operation destination already exists")

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
                            raise CutoverSafetyError(
                                "Managed rename parent identity changed"
                            )
                finally:
                    os.close(current_target_parent)
            finally:
                os.close(current_source_parent)
            _rename_exclusive_at(
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
            raise CutoverSafetyError("A clean immutable git code identity is required")
        return identity

    def _require_unchanged_code_identity(self, expected: str) -> None:
        if self._require_code_identity() != expected:
            raise CutoverSafetyError("Code identity changed during operation")

    @contextmanager
    def _managed_root_scope(self) -> Iterator[ManagedRootFd]:
        if self._managed_root_fd is not None:
            yield self._managed_root_fd
            return
        with ManagedRootFd.open(self.data_root) as managed:
            self._managed_root_fd = managed
            try:
                yield managed
            finally:
                self._managed_root_fd = None

    def _managed(self) -> ManagedRootFd:
        if self._managed_root_fd is None:
            raise CutoverSafetyError("Managed data-root descriptor is not retained")
        return self._managed_root_fd

    def _active_lease_fd(self) -> int:
        if self._active_lease is None:
            raise CutoverSafetyError("An active Market operation lease is required")
        return self._active_lease.fd

    @contextmanager
    def _exclusive_operation(self) -> Iterator[str]:
        if self._active_lease is not None:
            if self._active_code_version is None:
                raise CutoverSafetyError("Operation code identity is unavailable")
            yield self._active_code_version
            return
        code_version = self._require_code_identity()
        self._validate_active_roots()
        with MarketOperationLease.acquire(self.data_root, exclusive=True) as lease:
            with self._managed_root_scope():
                self._active_lease = lease
                self._active_code_version = code_version
                try:
                    try:
                        yield code_version
                    except (RuntimeStopError, WorkerShutdownError) as exc:
                        if not exc.process_joined:
                            lease.unlock_on_release = False
                        raise
                finally:
                    self._active_code_version = None
                    self._active_lease = None

    @contextmanager
    def _existing_exclusive_operation(self) -> Iterator[str]:
        if self._active_lease is not None:
            raise CutoverSafetyError(
                "Promotion eligibility requires a newly acquired active lease"
            )
        code_version = self._require_code_identity()
        self._validate_active_roots()
        with MarketOperationLease.acquire_existing(
            self.data_root, exclusive=True
        ) as lease:
            with self._managed_root_scope():
                self._active_lease = lease
                self._active_code_version = code_version
                try:
                    yield code_version
                finally:
                    self._active_code_version = None
                    self._active_lease = None

    def _validate_active_roots(self) -> None:
        assert_market_managed_root_safe(self.data_root, self.market_root)

    def _assert_current_data_root_identity(self) -> None:
        retained = os.fstat(self._managed().fd)
        try:
            current = self.data_root.lstat()
        except FileNotFoundError as exc:
            raise CutoverSafetyError("Active data root pathname disappeared") from exc
        if stat.S_ISLNK(current.st_mode) or (
            retained.st_dev,
            retained.st_ino,
        ) != (current.st_dev, current.st_ino):
            raise CutoverSafetyError("Active data root pathname identity changed")

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
            raise CutoverSafetyError("Managed directory pathname identity changed")

    @staticmethod
    def _remove_market_runtime(market_fd: int, runtime_name: str) -> None:
        with ManagedRootFd(Path("."), os.dup(market_fd)) as market:
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
                raise CutoverSafetyError(
                    "Staged Market activation and active-tree rollback failed"
                ) from rollback_exc
            raise CutoverSafetyError(
                "Staged Market activation failed; active tree was rolled back"
            ) from exc
        return quarantine

    @staticmethod
    def _validate_id(value: str | None, *, label: str) -> str:
        if not value:
            raise CutoverSafetyError(f"An explicit {label} ID is required")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
            raise CutoverSafetyError(f"Invalid {label} ID")
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
                raise CutoverSafetyError("market.duckdb must be a regular file")
            yield retained
            current = self._managed().open_dir(market_relative)
            try:
                current_stat = os.fstat(current)
                current_database_stat = os.stat(
                    "market.duckdb",
                    dir_fd=current,
                    follow_symlinks=False,
                )
                if (
                    (retained_stat.st_dev, retained_stat.st_ino)
                    != (current_stat.st_dev, current_stat.st_ino)
                    or (database_stat.st_dev, database_stat.st_ino)
                    != (current_database_stat.st_dev, current_database_stat.st_ino)
                ):
                    raise CutoverSafetyError(
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
            except CutoverSafetyError:
                pass
            else:
                files: list[Path] = []
                for relative, entry_stat in self._managed().regular_files(root_relative):
                    if relative.as_posix() == "market.duckdb.wal" and entry_stat.st_size == 0:
                        continue
                    files.append(root / relative)
                if root / "market.duckdb" not in files:
                    raise CutoverSafetyError("market.duckdb is missing")
                return files
        if not root.is_dir():
            raise CutoverSafetyError("Market time-series directory is missing")
        files: list[Path] = []
        for path in sorted(root.rglob("*")):
            if path == self._wal_path(root / "market.duckdb"):
                if path.is_file() and path.stat().st_size == 0:
                    continue
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise CutoverSafetyError(f"Backup source contains symlink: {path.name}")
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                raise CutoverSafetyError(f"Backup source contains special file: {path.name}")
            files.append(path)
        db_path = root / "market.duckdb"
        if db_path not in files:
            raise CutoverSafetyError("market.duckdb is missing")
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
        except CutoverSafetyError:
            source_fd = os.open(source, os.O_RDONLY | _FILE_NOFOLLOW)
            if not stat.S_ISREG(os.fstat(source_fd).st_mode):
                os.close(source_fd)
                raise CutoverSafetyError("Copy source is not a regular file")
        else:
            source_fd = self._managed().open_regular(source_relative, os.O_RDONLY)
        target_relative = self._managed_relative(target)
        digest = hashlib.sha256()
        total_bytes = 0
        try:
            if not stat.S_ISREG(os.fstat(source_fd).st_mode):
                raise CutoverSafetyError("Backup source is not a regular file")
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
            raise CutoverSafetyError(
                "Could not prove an exclusive writable DuckDB checkpoint"
            ) from exc
        try:
            wal_stat = self._managed().stat(
                Path("market-timeseries/market.duckdb.wal")
            )
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(wal_stat.st_mode) or wal_stat.st_size > 0:
                raise CutoverSafetyError("Nonempty or invalid DuckDB WAL remains after checkpoint")
        return metadata

    def preflight(self) -> MarketSourceMetadata:
        with self._exclusive_operation():
            return self._preflight_under_lease()

    def _preflight_under_lease(self) -> MarketSourceMetadata:
        self._validate_active_roots()
        self.runtime.assert_quiescent(self.data_root)
        metadata = self._checkpoint(guard_lease_fd=self._active_lease_fd())
        source_bytes = sum(
            self._managed().stat(self._managed_relative(path)).st_size
            for path in self._source_files(self.market_root)
        )
        required_bytes = max(source_bytes * 4, 1)
        if self.disk_free_bytes(self.data_root) < required_bytes:
            raise CutoverSafetyError(
                f"Insufficient free space: require at least {required_bytes} bytes"
            )
        return metadata

    def backup(self, backup_id: str) -> BackupResult:
        with self._exclusive_operation() as code_version:
            return self._backup_under_lease(backup_id, code_version=code_version)

    def _backup_under_lease(
        self,
        backup_id: str,
        *,
        code_version: str,
    ) -> BackupResult:
        backup_id = self._validate_id(backup_id, label="backup")
        self._preflight_under_lease()
        with self._market_identity_guard() as market_fd:
            with self.duckdb.checkpoint_snapshot(
                market_fd,
                "market.duckdb",
                guard_lease_fd=self._active_lease_fd(),
            ) as metadata:
                try:
                    wal_stat = self._managed().stat(
                        Path("market-timeseries/market.duckdb.wal")
                    )
                except FileNotFoundError:
                    pass
                else:
                    if not stat.S_ISREG(wal_stat.st_mode) or wal_stat.st_size > 0:
                        raise CutoverSafetyError(
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
        self._prepare_managed_directory(self.backups_root, exist_ok=True)
        destination = self.backups_root / backup_id
        if destination.exists() or destination.is_symlink():
            raise CutoverSafetyError(f"Backup destination already exists: {backup_id}")
        self._managed_mutation_hook("mkdir")
        self._prepare_managed_directory(destination, exist_ok=False)
        payload = destination / "payload"
        self._prepare_managed_directory(payload, exist_ok=False)
        entries: list[dict[str, object]] = []
        try:
            for source in self._source_files(self.market_root):
                self._assert_managed_directory(payload)
                relative = source.relative_to(self.market_root)
                target = payload / relative
                self._prepare_managed_directory(target.parent, exist_ok=True)
                self._assert_managed_target_absent(target)
                copied_bytes, copied_sha256 = self._copy_regular_to_managed(
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
                "createdAt": self.now(),
                "codeVersion": code_version,
                "sourceRootFingerprint": self.root_fingerprint(self.data_root),
                "source": {
                    "schemaVersion": metadata.schema_version,
                    "stockPriceAdjustmentMode": metadata.adjustment_mode,
                },
                "files": entries,
            }
            manifest_path = destination / "manifest.json"
            self._assert_managed_target_absent(manifest_path)
            self._write_managed_file_create(
                manifest_path,
                (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode(),
            )
            self._managed().fsync_tree(self._managed_relative(payload))
            self._managed().fsync_dir(self._managed_relative(destination))
            self.verify_backup(backup_id)
            self._managed().chmod_tree(
                self._managed_relative(destination),
                directory_mode=0o500,
                file_mode=0o400,
            )
            self._managed().fsync_dir(self._managed_relative(self.backups_root))
        except Exception:
            try:
                self._managed().chmod_tree(
                    self._managed_relative(destination),
                    directory_mode=0o700,
                    file_mode=0o600,
                )
                self._managed().remove_tree(
                    self._managed_relative(destination),
                    missing_ok=True,
                )
            except FileNotFoundError:
                pass
            raise
        return BackupResult(backup_id)

    def verify_backup(self, backup_id: str) -> BackupResult:
        with self._managed_root_scope():
            return self._verify_backup_managed(backup_id)

    def _verify_backup_managed(
        self,
        backup_id: str,
        *,
        require_current_root: bool = True,
    ) -> BackupResult:
        backup_id = self._validate_id(backup_id, label="backup")
        destination = self.backups_root / backup_id
        self._assert_managed_directory(destination)
        self._assert_managed_directory(destination / "payload")
        manifest_path = destination / "manifest.json"
        try:
            manifest_mode = self._managed().stat(
                self._managed_relative(manifest_path)
            ).st_mode
        except FileNotFoundError:
            raise CutoverSafetyError(f"Backup manifest is missing: {backup_id}")
        if stat.S_ISLNK(manifest_mode) or not stat.S_ISREG(manifest_mode):
            raise CutoverSafetyError(f"Backup manifest is invalid: {backup_id}")
        try:
            manifest = json.loads(
                self._managed()
                .read_bytes(self._managed_relative(manifest_path))
                .decode("utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError("Backup manifest is unreadable") from exc
        if manifest.get("backupId") != backup_id:
            raise CutoverSafetyError("Backup manifest ID mismatch")
        if require_current_root and manifest.get(
            "sourceRootFingerprint"
        ) != self.root_fingerprint(self.data_root):
            raise CutoverSafetyError("Backup source root fingerprint mismatch")
        entries = manifest.get("files")
        if not isinstance(entries, list) or not entries:
            raise CutoverSafetyError("Backup manifest has no files")
        expected_paths: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                raise CutoverSafetyError("Backup manifest file entry is invalid")
            relative = Path(entry["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise CutoverSafetyError("Backup manifest contains unsafe path")
            expected_paths.add(relative.as_posix())
            target = destination / "payload" / relative
            target_relative = self._managed_relative(target)
            try:
                target_stat = self._managed().stat(target_relative)
            except FileNotFoundError:
                raise CutoverSafetyError(f"Backup file is missing: {relative.as_posix()}")
            if not stat.S_ISREG(target_stat.st_mode):
                raise CutoverSafetyError(f"Backup file is invalid: {relative.as_posix()}")
            if target_stat.st_size != entry.get("bytes"):
                raise CutoverSafetyError(f"Backup size mismatch: {relative.as_posix()}")
            if self._managed().sha256(target_relative) != entry.get("sha256"):
                raise CutoverSafetyError(f"Backup checksum mismatch: {relative.as_posix()}")
        actual_paths = {
            path.relative_to(destination / "payload").as_posix()
            for path in self._source_files(destination / "payload")
        }
        if actual_paths != expected_paths:
            raise CutoverSafetyError("Backup file set mismatch")
        return BackupResult(backup_id)

    def restore(self, backup_id: str | None) -> RestoreResult:
        with self._exclusive_operation():
            return self._restore_under_lease(backup_id)

    def _restore_under_lease(self, backup_id: str | None) -> RestoreResult:
        backup_id = self._validate_id(backup_id, label="backup")
        self.runtime.assert_quiescent(self.data_root)
        try:
            active_database_stat = self._managed().stat(
                Path("market-timeseries/market.duckdb")
            )
        except FileNotFoundError:
            active_database_stat = None
        if active_database_stat is not None:
            if not stat.S_ISREG(active_database_stat.st_mode):
                raise CutoverSafetyError("Active market.duckdb is invalid")
            self._checkpoint(guard_lease_fd=self._active_lease_fd())
        try:
            wal_stat = self._managed().stat(
                Path("market-timeseries/market.duckdb.wal")
            )
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(wal_stat.st_mode) or wal_stat.st_size > 0:
                raise CutoverSafetyError("Cannot restore over a nonempty or invalid DuckDB WAL")
        self._verify_backup_managed(backup_id, require_current_root=False)
        backup_payload = self.backups_root / backup_id / "payload"
        self._assert_managed_directory(backup_payload)
        stage = self.data_root / f"market-timeseries.restore-{backup_id}"
        try:
            self._managed().stat(self._managed_relative(stage))
        except FileNotFoundError:
            pass
        else:
            raise CutoverSafetyError("Restore staging destination already exists")
        self._assert_managed_target_absent(stage)
        self._managed().copy_tree_create(
            self._managed_relative(backup_payload),
            self._managed_relative(stage),
        )
        self._assert_managed_directory(stage)
        self._managed().chmod_tree(
            self._managed_relative(stage),
            directory_mode=0o700,
            file_mode=0o600,
        )
        self._verify_tree_copy(backup_payload, stage)
        self._managed().fsync_tree(self._managed_relative(stage))
        quarantine_relative: str | None = None
        quarantine: Path | None = None
        try:
            self._managed().stat(self._managed_relative(self.market_root))
            market_exists = True
        except FileNotFoundError:
            market_exists = False
        if market_exists:
            quarantine_root = self.operations_root / "quarantine"
            self._prepare_managed_directory(quarantine_root, exist_ok=True)
            quarantine = quarantine_root / (
                f"failed-{backup_id}-{time.time_ns()}-{secrets.token_hex(4)}"
            )
            self._assert_managed_target_absent(quarantine)
            self._validate_active_roots()
            self._assert_managed_directory(quarantine_root)
            self._secure_rename(self.market_root, quarantine)
            quarantine_relative = quarantine.relative_to(self.data_root).as_posix()
        try:
            self._assert_managed_directory(stage)
            self._secure_rename(stage, self.market_root)
        except Exception as exc:
            try:
                try:
                    self._managed().stat(self._managed_relative(self.market_root))
                    failed_market_exists = True
                except FileNotFoundError:
                    failed_market_exists = False
                if failed_market_exists:
                    failed_stage = self.operations_root / "quarantine" / (
                        f"restore-stage-failed-{backup_id}-{time.time_ns()}"
                    )
                    self._assert_managed_target_absent(failed_stage)
                    self._validate_active_roots()
                    self._secure_rename(self.market_root, failed_stage)
                if quarantine is not None:
                    self._assert_managed_directory(quarantine.parent)
                    self._assert_managed_directory(quarantine)
                    self._secure_rename(quarantine, self.market_root)
            except Exception as rollback_exc:
                raise CutoverSafetyError(
                    "Restore activation failed and quarantine rollback failed"
                ) from rollback_exc
            raise CutoverSafetyError(
                "Restore activation failed; original active tree was rolled back"
            ) from exc
        self._verify_backup_managed(backup_id, require_current_root=False)
        return RestoreResult(backup_id, quarantine_relative)

    def _verify_tree_copy(self, source: Path, target: Path) -> None:
        source_files = self._source_files(source)
        target_files = self._source_files(target)
        source_relatives = {path.relative_to(source) for path in source_files}
        target_relatives = {path.relative_to(target) for path in target_files}
        if source_relatives != target_relatives:
            raise CutoverSafetyError("Restore staging file set mismatch")
        for relative in source_relatives:
            source_file = source / relative
            target_file = target / relative
            source_relative = self._managed_relative(source_file)
            target_relative = self._managed_relative(target_file)
            source_stat = self._managed().stat(source_relative)
            target_stat = self._managed().stat(target_relative)
            if (
                source_stat.st_size != target_stat.st_size
                or self._managed().sha256(source_relative)
                != self._managed().sha256(target_relative)
            ):
                raise CutoverSafetyError(
                    f"Restore staging checksum mismatch: {relative.as_posix()}"
                )

    def smoke(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        *,
        operation_id: str,
        market_root: Path | None = None,
        market_directory_fd: int | None = None,
        guard_lease_fd: int | None = None,
    ) -> SmokeResult:
        if guard_lease_fd is None:
            with MarketOperationLease.acquire(
                self.data_root,
                exclusive=False,
            ) as smoke_lease:
                try:
                    return self.smoke(
                        api,
                        config,
                        operation_id=operation_id,
                        market_root=market_root,
                        market_directory_fd=market_directory_fd,
                        guard_lease_fd=smoke_lease.fd,
                    )
                except WorkerShutdownError as exc:
                    if not exc.process_joined:
                        smoke_lease.unlock_on_release = False
                    raise
        operation_id = self._validate_id(operation_id, label="operation")
        inspected_root = market_root or self.market_root
        if market_directory_fd is not None:
            inspected_fd = os.dup(market_directory_fd)
            try:
                metadata = self.duckdb.inspect(
                    inspected_fd,
                    "market.duckdb",
                    guard_lease_fd=guard_lease_fd,
                )
            finally:
                os.close(inspected_fd)
        else:
            with self._managed_root_scope():
                inspected_fd = self._managed().open_dir(
                    self._managed_relative(inspected_root)
                )
                try:
                    metadata = self.duckdb.inspect(
                        inspected_fd,
                        "market.duckdb",
                        guard_lease_fd=guard_lease_fd,
                    )
                finally:
                    os.close(inspected_fd)
        if metadata.schema_version != 4:
            raise CutoverSafetyError(
                f"Market schema must be exactly 4, got {metadata.schema_version!r}"
            )
        if metadata.adjustment_mode != "local_projection_v2_event_time":
            raise CutoverSafetyError(
                "Market adjustment mode must be local_projection_v2_event_time"
            )

        stats = api.request("GET", "/api/db/stats")
        schema = stats.get("schema")
        if not isinstance(schema, dict) or schema != {
            "version": 4,
            "requiredVersion": 4,
            "current": True,
        }:
            raise CutoverSafetyError("Market stats schema v4 gate failed")
        stats_adjusted = stats.get("adjustedMetrics")
        if (
            not isinstance(stats_adjusted, dict)
            or stats_adjusted.get("status") != "ready"
            or not isinstance(stats_adjusted.get("statementRows"), int)
            or int(stats_adjusted["statementRows"]) <= 0
            or not isinstance(stats_adjusted.get("dailyValuationRows"), int)
            or int(stats_adjusted["dailyValuationRows"]) <= 0
            or not isinstance(stats_adjusted.get("readyBasisCount"), int)
            or int(stats_adjusted["readyBasisCount"]) <= 0
        ):
            raise CutoverSafetyError("Market adjusted-metric coverage is not ready")

        validation = api.request("GET", "/api/db/validate")
        if validation.get("status") != "healthy":
            raise CutoverSafetyError("Market validation did not report healthy")
        adjusted = validation.get("adjustedMetrics")
        if not isinstance(adjusted, dict):
            raise CutoverSafetyError("Validation omitted adjusted-metric lineage")
        zero_counters = (
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "missingDailyValuationRows",
            "extraDailyValuationRows",
            "wrongBasisDailyValuationRows",
        )
        if (
            adjusted.get("status") != "ready"
            or not isinstance(adjusted.get("sourceStatementKeyCount"), int)
            or int(adjusted["sourceStatementKeyCount"]) <= 0
            or not isinstance(adjusted.get("expectedAdjustedStatementRows"), int)
            or int(adjusted["expectedAdjustedStatementRows"]) <= 0
            or any(adjusted.get(counter) != 0 for counter in zero_counters)
        ):
            raise CutoverSafetyError("Exact adjusted-metric lineage validation failed")

        symbol = quote(config.symbol, safe="")
        get_fundamentals = api.request(
            "GET", f"/api/analytics/fundamentals/{symbol}"
        )
        post_fundamentals = api.request(
            "POST", "/api/fundamentals/compute", {"symbol": config.symbol}
        )
        semantic_keys = ("asOfDate", "data", "latestMetrics")
        if any(
            get_fundamentals.get(key) != post_fundamentals.get(key)
            for key in semantic_keys
        ):
            raise CutoverSafetyError("Fundamentals GET/POST parity failed")
        if not get_fundamentals.get("data"):
            raise CutoverSafetyError("Fundamentals smoke returned no data")

        screening = api.request(
            "POST",
            "/api/analytics/screening/jobs",
            {
                "strategies": config.strategy,
                "recentDays": 10,
                "sortBy": "matchedDate",
                "order": "desc",
            },
        )
        screening_job_id = self._require_job_id(
            screening,
            "/api/analytics/screening/jobs",
        )
        self._poll_api_job(
            api,
            f"/api/analytics/screening/jobs/{quote(screening_job_id, safe='')}",
            "screening",
        )
        screening_result = api.request(
            "GET",
            f"/api/analytics/screening/result/{quote(screening_job_id, safe='')}",
        )
        if not isinstance(screening_result.get("results"), list):
            raise CutoverSafetyError("Screening result payload is invalid")

        ranking = api.request("GET", "/api/analytics/fundamental-ranking")
        if not isinstance(ranking.get("rankings"), dict):
            raise CutoverSafetyError("Fundamental ranking payload is invalid")

        dataset_name = f"cutover-smoke-{operation_id.replace('.', '-')}"
        dataset = api.request(
            "POST",
            "/api/dataset",
            {
                "name": dataset_name,
                "preset": config.dataset_preset,
                "overwrite": False,
            },
        )
        dataset_job_id = self._require_job_id(dataset, "/api/dataset")
        self._poll_api_job(
            api,
            f"/api/dataset/jobs/{quote(dataset_job_id, safe='')}",
            "dataset",
        )
        dataset_info = api.request("GET", f"/api/dataset/{dataset_name}/info")
        snapshot = dataset_info.get("snapshot")
        dataset_validation = dataset_info.get("validation")
        if not isinstance(snapshot, dict) or snapshot != {
            **snapshot,
            "schemaVersion": 3,
            "sourceMarketSchemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        }:
            raise CutoverSafetyError("Dataset event-time lineage gate failed")
        if not isinstance(dataset_validation, dict) or dataset_validation.get("isValid") is not True:
            raise CutoverSafetyError("Dataset validation failed")
        opened = api.request("GET", f"/api/dataset/{dataset_name}/sample?count=1")
        if not isinstance(opened.get("codes"), list) or not opened["codes"]:
            raise CutoverSafetyError("Dataset sample smoke returned no codes")

        return SmokeResult(
            schema_version=metadata.schema_version,
            adjustment_mode=metadata.adjustment_mode,
            checks=(
                "market_metadata",
                "adjusted_metrics_lineage",
                "fundamentals_parity",
                "screening",
                "fundamental_ranking",
                "dataset_create_info_open",
            ),
            api_paths=(
                "/api/db/stats",
                "/api/db/validate",
                f"/api/analytics/fundamentals/{symbol}",
                "/api/fundamentals/compute",
                "/api/analytics/screening/jobs",
                f"/api/analytics/screening/jobs/{screening_job_id}",
                f"/api/analytics/screening/result/{screening_job_id}",
                "/api/analytics/fundamental-ranking",
                "/api/dataset",
                f"/api/dataset/jobs/{dataset_job_id}",
                f"/api/dataset/{dataset_name}/info",
                f"/api/dataset/{dataset_name}/sample?count=1",
            ),
            lineage={
                **{
                    key: int(adjusted[key])
                    for key in (
                        "sourceStatementKeyCount",
                        "expectedAdjustedStatementRows",
                        *zero_counters,
                    )
                },
                "statementRows": int(stats_adjusted["statementRows"]),
                "dailyValuationRows": int(stats_adjusted["dailyValuationRows"]),
                "readyBasisCount": int(stats_adjusted["readyBasisCount"]),
            },
        )

    @staticmethod
    def _require_job_id(payload: dict[str, object], endpoint: str) -> str:
        try:
            label, job_id_field = _CREATE_JOB_RESPONSE_FIELDS[endpoint]
        except KeyError as exc:
            raise CutoverSafetyError(
                f"Unsupported job-creating endpoint: {endpoint}"
            ) from exc
        job_id = payload.get(job_id_field)
        if not isinstance(job_id, str) or not job_id:
            raise CutoverSafetyError(f"{label} did not return a job ID")
        return job_id

    @staticmethod
    def _poll_api_job(
        api: ApiAdapter,
        path: str,
        label: str,
        *,
        attempts: int = 21_600,
        poll_interval_seconds: float = 2.0,
    ) -> dict[str, object]:
        for _ in range(attempts):
            job = api.request("GET", path)
            status = job.get("status")
            if status == "completed":
                return job
            if status in {"failed", "cancelled"}:
                progress = job.get("progress")
                result = job.get("result")
                details: list[str] = []
                if isinstance(progress, dict):
                    for key in ("stage", "message"):
                        value = progress.get(key)
                        if isinstance(value, str) and value:
                            details.append(f"{key}={value}")
                if isinstance(result, dict):
                    errors = result.get("errors")
                    if isinstance(errors, list):
                        result_errors = [
                            value for value in errors if isinstance(value, str) and value
                        ]
                        if result_errors:
                            details.append(f"errors={' | '.join(result_errors)}")
                error = job.get("error")
                if isinstance(error, str) and error:
                    details.append(f"error={error}")
                suffix = f"; {'; '.join(details)}" if details else ""
                raise CutoverSafetyError(
                    f"{label} job ended with status {status}{suffix}"
                )
            time.sleep(poll_interval_seconds)
        raise CutoverSafetyError(f"{label} job polling timed out")

    def configuration_fingerprint(self, root: Path) -> str:
        root = _lexical_absolute(root)
        if self._managed_root_fd is not None:
            try:
                root_relative = root.relative_to(self.data_root)
            except ValueError:
                pass
            else:
                digest = hashlib.sha256()
                config_relative = root_relative / "config" / "default.yaml"
                try:
                    config_stat = self._managed().stat(config_relative)
                except FileNotFoundError:
                    repository_config = self._repository_default_config_path()
                    config_sha = self._sha256(repository_config)
                else:
                    if not stat.S_ISREG(config_stat.st_mode):
                        raise CutoverSafetyError("Fingerprint config is not regular")
                    config_sha = self._managed().sha256(config_relative)
                digest.update(b"config/default.yaml\0")
                digest.update(config_sha.encode())
                digest.update(b"\n")
                strategies_relative = root_relative / "strategies"
                try:
                    strategy_files = self._managed().regular_files(
                        strategies_relative
                    )
                except FileNotFoundError:
                    strategy_files = []
                for relative, _entry_stat in strategy_files:
                    label = f"strategies/{relative.as_posix()}"
                    digest.update(label.encode())
                    digest.update(b"\0")
                    digest.update(
                        self._managed()
                        .sha256(strategies_relative / relative)
                        .encode()
                    )
                    digest.update(b"\n")
                return digest.hexdigest()
        _assert_real_directory(root, "Fingerprint root")
        _assert_safe_directory_chain(root)
        digest = hashlib.sha256()
        candidates: list[tuple[str, Path]] = []
        config = root / "config" / "default.yaml"
        if not config.is_file():
            config = self._repository_default_config_path()
        candidates.append(("config/default.yaml", config))
        strategies = root / "strategies"
        if strategies.exists():
            _assert_real_directory(strategies, "Strategies root")
            for path in sorted(strategies.rglob("*")):
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise CutoverSafetyError("Strategy fingerprint source contains symlink")
                if stat.S_ISDIR(mode):
                    continue
                if not stat.S_ISREG(mode):
                    raise CutoverSafetyError("Strategy fingerprint source contains special file")
                candidates.append(
                    (f"strategies/{path.relative_to(strategies).as_posix()}", path)
                )
        for label, path in candidates:
            if path.is_symlink() or not path.is_file():
                raise CutoverSafetyError(f"Fingerprint source is invalid: {label}")
            digest.update(label.encode())
            digest.update(b"\0")
            digest.update(self._sha256(path).encode())
            digest.update(b"\n")
        return digest.hexdigest()

    @staticmethod
    def _repository_default_config_path() -> Path:
        config = Path(__file__).resolve().parents[3] / "config" / "default.yaml"
        try:
            config_stat = config.lstat()
        except FileNotFoundError as exc:
            raise CutoverSafetyError(
                "Repository default configuration is missing"
            ) from exc
        if stat.S_ISLNK(config_stat.st_mode) or not stat.S_ISREG(config_stat.st_mode):
            raise CutoverSafetyError(
                "Repository default configuration must be a regular file"
            )
        return config

    def root_fingerprint(self, root: Path) -> str:
        root = _lexical_absolute(root)
        if self._managed_root_fd is not None:
            try:
                relative = root.relative_to(self.data_root)
            except ValueError:
                relative = None
            if relative is not None:
                root_fd = (
                    os.dup(self._managed().fd)
                    if not relative.parts
                    else self._managed().open_dir(relative)
                )
                try:
                    root_stat = os.fstat(root_fd)
                finally:
                    os.close(root_fd)
            else:
                _assert_safe_directory_chain(root)
                _assert_real_directory(root, "Fingerprint root")
                root_stat = root.lstat()
        else:
            _assert_safe_directory_chain(root)
            _assert_real_directory(root, "Fingerprint root")
            root_stat = root.lstat()
        digest = hashlib.sha256(
            f"dev={root_stat.st_dev};ino={root_stat.st_ino}\n".encode()
        )
        digest.update(self.configuration_fingerprint(root).encode())
        digest.update(b"\n")
        return digest.hexdigest()

    def rehearse(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        with self._managed_root_scope():
            return self._rehearse_managed(
                report_id,
                config,
                inherited_environment=inherited_environment,
            )

    def rehearse_retained(
        self,
        report_id: str,
        *,
        source_rehearsal_report_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        with self._managed_root_scope():
            return self._rehearse_retained_managed(
                report_id,
                source_rehearsal_report_id=source_rehearsal_report_id,
                config=config,
                inherited_environment=inherited_environment,
            )

    def _rehearse_retained_managed(
        self,
        report_id: str,
        *,
        source_rehearsal_report_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        report_id = self._validate_id(report_id, label="report")
        source_rehearsal_report_id = self._validate_id(
            source_rehearsal_report_id,
            label="source rehearsal report",
        )
        if report_id == source_rehearsal_report_id:
            raise CutoverSafetyError("Retained rehearsal requires a new report ID")
        code_version = self._require_code_identity()
        self._validate_active_roots()
        target_root_fingerprint = self.root_fingerprint(self.data_root)
        source_report = self._read_report(source_rehearsal_report_id)
        expected_smoke_config = {
            "symbol": config.symbol,
            "strategy": config.strategy,
            "datasetPreset": config.dataset_preset,
        }
        source_code_version = source_report.get("codeVersion")
        if not (
            source_report.get("reportId") == source_rehearsal_report_id
            and source_report.get("phase") == "rehearsal"
            and source_report.get("status") in {"passed", "failed"}
            and source_report.get("smokeConfig") == expected_smoke_config
            and source_report.get("targetRootFingerprint")
            == target_root_fingerprint
            and source_report.get("serverProcessJoined") is True
            and source_report.get("workerProcessJoined") is True
            and isinstance(source_code_version, str)
            and bool(source_code_version)
        ):
            raise CutoverSafetyError("An eligible retained rehearsal report is required")
        retained_root = self._retained_rehearsal_root(source_rehearsal_report_id)
        if self.configuration_fingerprint(
            retained_root
        ) != self.configuration_fingerprint(self.data_root):
            raise CutoverSafetyError(
                "Retained rehearsal configuration differs from active configuration"
            )
        source_retained_root_fingerprint = self.root_fingerprint(retained_root)
        with MarketOperationLease.acquire(retained_root, exclusive=True) as lease:
            self._assert_retained_root_identity(retained_root, lease.root_fd)
            leased_root_fingerprint = self._root_fingerprint_at(lease.root_fd)
            if leased_root_fingerprint != source_retained_root_fingerprint:
                raise CutoverSafetyError(
                    "Retained rehearsal root changed before lease acquisition"
                )
            return self._rehearse_retained_under_lease(
                report_id,
                source_rehearsal_report_id=source_rehearsal_report_id,
                source_rehearsal_code_version=source_code_version,
                source_retained_root_fingerprint=leased_root_fingerprint,
                retained_root=retained_root,
                config=config,
                inherited_environment=inherited_environment,
                lease=lease,
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
            )

    def _rehearse_retained_under_lease(
        self,
        report_id: str,
        *,
        source_rehearsal_report_id: str,
        source_rehearsal_code_version: str,
        source_retained_root_fingerprint: str,
        retained_root: Path,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        lease: MarketOperationLease,
        target_root_fingerprint: str,
        code_version: str,
    ) -> OperationResult:
        runtime_name = f".cutover-runtime-{report_id}"
        report_dir = self.operations_root / "reports" / report_id
        log_path = report_dir / "server.log"
        started = time.monotonic()
        api: ApiAdapter | None = None
        market_fd: int | None = None
        source_market_identity_before: dict[str, object] | None = None
        source_market_identity_after: dict[str, object] | None = None
        completed_api_checks: tuple[str, ...] = ()
        completed_phases: tuple[dict[str, object], ...] = ()
        completed_evidence: dict[str, object] | None = None
        report_reserved = False
        runtime_reserved = False
        runtime_start_attempted = False

        def mark_runtime_reserved() -> None:
            nonlocal runtime_reserved
            runtime_reserved = True

        try:
            try:
                self._managed().stat(self._managed_relative(report_dir))
            except FileNotFoundError:
                pass
            else:
                raise CutoverSafetyError("Retained report destination already exists")
            market_fd = os.open(
                "market-timeseries",
                _DIR_OPEN_FLAGS,
                dir_fd=lease.root_fd,
            )
            metadata = self.duckdb.inspect(
                market_fd,
                "market.duckdb",
                guard_lease_fd=lease.fd,
            )
            if metadata.schema_version != 4:
                raise CutoverSafetyError("Retained Market schema v4 is required")
            if metadata.adjustment_mode != "local_projection_v2_event_time":
                raise CutoverSafetyError("Retained Market adjustment mode is incompatible")
            if metadata.adjusted_metrics_ready is not True:
                raise CutoverSafetyError(
                    "Retained adjusted-metric event-time lineage is not ready"
                )
            source_market_identity_before = self._market_tree_identity(lease.root_fd)
            try:
                self._prepare_retained_runtime(
                    retained_root,
                    runtime_name=runtime_name,
                    root_fd=lease.root_fd,
                    on_reserved=mark_runtime_reserved,
                )
            except Exception:
                if runtime_reserved:
                    with ManagedRootFd(Path("."), os.dup(lease.root_fd)) as retained:
                        retained.remove_tree(
                            Path("market-timeseries") / runtime_name,
                            missing_ok=True,
                        )
                    runtime_reserved = False
                raise
            self._prepare_managed_directory(report_dir.parent, exist_ok=True)
            self._prepare_managed_directory(report_dir, exist_ok=False)
            report_reserved = True
            self._assert_retained_root_identity(retained_root, lease.root_fd)
            if self.root_fingerprint(self.data_root) != target_root_fingerprint:
                raise CutoverSafetyError(
                    "Active configuration changed before retained runtime start"
                )
            if self._configuration_fingerprint_at(
                lease.root_fd
            ) != self.configuration_fingerprint(self.data_root):
                raise CutoverSafetyError(
                    "Retained rehearsal configuration changed before runtime start"
                )
            self._require_unchanged_code_identity(code_version)
            environment = self._isolated_environment(
                inherited_environment,
                lease_fd=lease.fd,
                root_fd=lease.root_fd,
                runtime_name=runtime_name,
            )
            environment["TRADING25_RUNTIME_CAPABILITY"] = "retained_market_smoke"
            log_fd = self._managed().open_regular(
                self._managed_relative(log_path),
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
            )
            try:
                runtime_start_attempted = True
                api = self.runtime.start(
                    root_fd=lease.root_fd,
                    market_fd=market_fd,
                    lease_fd=lease.fd,
                    environment=environment,
                    log_path=log_path,
                    log_fd=log_fd,
                )
            finally:
                os.close(log_fd)
            smoke_started = time.monotonic()
            smoke_result = self.smoke(
                api,
                config,
                operation_id=report_id,
                market_root=retained_root / "market-timeseries",
                market_directory_fd=market_fd,
                guard_lease_fd=lease.fd,
            )
            phases = (
                {
                    "name": "retained_market_smoke",
                    "status": "passed",
                    "durationSeconds": round(time.monotonic() - smoke_started, 6),
                },
            )
            completed_api_checks = smoke_result.api_paths
            completed_phases = phases
            completed_evidence = {
                "schemaVersion": smoke_result.schema_version,
                "stockPriceAdjustmentMode": smoke_result.adjustment_mode,
                "adjustedMetrics": smoke_result.lineage,
            }
            self.runtime.stop(api)
            api = None
            source_market_identity_after = self._market_tree_identity(lease.root_fd)
            if source_market_identity_before != source_market_identity_after:
                raise RetainedMarketMutationError(
                    "retained Market tree changed during smoke"
                )
            if self.root_fingerprint(self.data_root) != target_root_fingerprint:
                raise CutoverSafetyError(
                    "Active configuration changed during retained rehearsal"
                )
            self._require_unchanged_code_identity(code_version)
            self._assert_retained_root_identity(retained_root, lease.root_fd)
            if self._root_fingerprint_at(lease.root_fd) != source_retained_root_fingerprint:
                raise CutoverSafetyError(
                    "Retained configuration changed during semantic smoke"
                )
            report = self._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status="passed",
                duration_seconds=time.monotonic() - started,
                api_checks=smoke_result.api_paths,
                server_log=f"reports/{report_id}/server.log",
                evidence=completed_evidence,
                phases=phases,
                config=config,
                code_version=code_version,
                rehearsal_mode="retained_market_smoke",
                source_rehearsal_report_id=source_rehearsal_report_id,
                source_rehearsal_code_version=source_rehearsal_code_version,
                source_retained_root_fingerprint=source_retained_root_fingerprint,
                source_market_identity_before=source_market_identity_before,
                source_market_identity_after=source_market_identity_after,
                server_process_joined=True,
                worker_process_joined=True,
                target_root_fingerprint=target_root_fingerprint,
            )
            def final_validator() -> None:
                self._require_unchanged_code_identity(code_version)
                if self.root_fingerprint(self.data_root) != target_root_fingerprint:
                    raise CutoverSafetyError("Active configuration changed at report publication")
                self._assert_retained_root_identity(retained_root, lease.root_fd)
                if self._root_fingerprint_at(lease.root_fd) != source_retained_root_fingerprint:
                    raise CutoverSafetyError("Retained root changed at report publication")
                if self._market_tree_identity(lease.root_fd) != source_market_identity_after:
                    raise RetainedMarketMutationError(
                        "Retained Market changed at report publication"
                    )

            report_path = self._write_report(
                report_id,
                report,
                expected_root_fingerprint=target_root_fingerprint,
                final_validator=final_validator,
            )
        except Exception as exc:
            if not runtime_start_attempted:
                if market_fd is not None and runtime_reserved:
                    try:
                        self._remove_market_runtime(market_fd, runtime_name)
                    except FileNotFoundError:
                        pass
                    runtime_reserved = False
                if report_reserved:
                    self._managed().remove_tree(
                        self._managed_relative(report_dir),
                        missing_ok=True,
                    )
                    report_reserved = False
            cleanup_error: Exception | None = None
            stop_error: Exception | None = (
                exc if isinstance(exc, RuntimeStopError) else None
            )
            server_process_joined = (
                exc.process_joined if isinstance(exc, RuntimeStopError) else api is None
            )
            worker_process_joined = not (
                isinstance(exc, WorkerShutdownError) and not exc.process_joined
            )
            if api is not None:
                try:
                    self.runtime.cancel_owned_work(api)
                except Exception as runtime_cleanup_error:
                    cleanup_error = runtime_cleanup_error
                try:
                    self.runtime.stop(api)
                except RuntimeStopError as runtime_stop_error:
                    stop_error = runtime_stop_error
                    server_process_joined = runtime_stop_error.process_joined
                except Exception as runtime_stop_error:
                    stop_error = runtime_stop_error
                    server_process_joined = False
                else:
                    server_process_joined = True
            if source_market_identity_before is not None:
                try:
                    source_market_identity_after = self._market_tree_identity(
                        lease.root_fd
                    )
                except Exception:
                    source_market_identity_after = None
            if not server_process_joined or not worker_process_joined:
                lease.unlock_on_release = False
            failure_report = self._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status=(
                    "failed"
                    if server_process_joined and worker_process_joined
                    else "stop_failed_cleanup_deferred"
                ),
                duration_seconds=time.monotonic() - started,
                api_checks=completed_api_checks,
                server_log=f"reports/{report_id}/server.log",
                evidence=completed_evidence,
                phases=completed_phases,
                config=config,
                code_version=code_version,
                rehearsal_mode="retained_market_smoke",
                source_rehearsal_report_id=source_rehearsal_report_id,
                source_rehearsal_code_version=source_rehearsal_code_version,
                source_retained_root_fingerprint=source_retained_root_fingerprint,
                source_market_identity_before=source_market_identity_before,
                source_market_identity_after=source_market_identity_after,
                error=type(exc).__name__,
                error_message=self._redact_diagnostic(
                    str(exc), inherited_environment
                ),
                cleanup_error=(type(cleanup_error).__name__ if cleanup_error else None),
                stop_error=type(stop_error).__name__ if stop_error else None,
                server_process_joined=server_process_joined,
                worker_process_joined=worker_process_joined,
                target_root_fingerprint=target_root_fingerprint,
            )
            if report_reserved and report_dir.exists() and not report_dir.is_symlink():
                self._try_write_report(report_id, failure_report)
            if isinstance(exc, RetainedMarketMutationError):
                raise CutoverSafetyError(str(exc)) from exc
            raise CutoverSafetyError("Retained Market rehearsal failed") from exc
        finally:
            if market_fd is not None:
                os.close(market_fd)
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    def _rehearse_managed(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        report_id = self._validate_id(report_id, label="report")
        code_version = self._require_code_identity()
        self._validate_active_roots()
        target_root_fingerprint = self.root_fingerprint(self.data_root)
        source_configuration_fingerprint = self.configuration_fingerprint(
            self.data_root
        )
        rehearsal_dir = self.operations_root / "rehearsals" / report_id
        self._prepare_managed_directory(rehearsal_dir.parent, exist_ok=True)
        if rehearsal_dir.exists() or rehearsal_dir.is_symlink():
            raise CutoverSafetyError("Rehearsal destination already exists")
        self._prepare_managed_directory(rehearsal_dir, exist_ok=False)
        rehearsal_root = rehearsal_dir / "root"
        runtime_name = f".cutover-runtime-{report_id}"
        self._prepare_isolated_root(rehearsal_root, runtime_name=runtime_name)
        if (
            self.configuration_fingerprint(rehearsal_root)
            != source_configuration_fingerprint
        ):
            raise CutoverSafetyError("Rehearsal configuration snapshot mismatch")
        with MarketOperationLease.acquire(
            rehearsal_root,
            exclusive=True,
        ) as lease:
            return self._rehearse_under_lease(
                report_id,
                config,
                inherited_environment=inherited_environment,
                rehearsal_dir=rehearsal_dir,
                rehearsal_root=rehearsal_root,
                lease=lease,
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
            )

    def _rehearse_under_lease(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
        rehearsal_dir: Path,
        rehearsal_root: Path,
        lease: MarketOperationLease,
        target_root_fingerprint: str,
        code_version: str,
    ) -> OperationResult:
        environment = self._isolated_environment(
            inherited_environment,
            lease_fd=lease.fd,
            root_fd=lease.root_fd,
            runtime_name=f".cutover-runtime-{report_id}",
        )
        started = time.monotonic()
        api: ApiAdapter | None = None
        market_fd: int | None = None
        log_path = rehearsal_dir / "server.log"
        try:
            market_fd = os.open(
                "market-timeseries",
                _DIR_OPEN_FLAGS,
                dir_fd=lease.root_fd,
            )
            log_fd = self._managed().open_regular(
                self._managed_relative(log_path),
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
            )
            try:
                api = self.runtime.start(
                    root_fd=lease.root_fd,
                    market_fd=market_fd,
                    lease_fd=lease.fd,
                    environment=environment,
                    log_path=log_path,
                    log_fd=log_fd,
                )
            finally:
                os.close(log_fd)
            checks, evidence, phases, _market_identity = self._run_rebuild(
                api,
                config,
                rehearsal_root,
                report_id,
                market_directory_fd=market_fd,
                guard_lease_fd=lease.fd,
            )
            self.runtime.stop(api)
            api = None
            os.close(market_fd)
            market_fd = None
            if self.root_fingerprint(self.data_root) != target_root_fingerprint:
                raise CutoverSafetyError(
                    "Active configuration changed during isolated rehearsal"
                )
            self._require_unchanged_code_identity(code_version)
        except Exception as exc:
            if market_fd is not None:
                os.close(market_fd)
            cleanup_error: Exception | None = None
            stop_error: Exception | None = (
                exc if isinstance(exc, RuntimeStopError) else None
            )
            server_process_joined = (
                exc.process_joined if isinstance(exc, RuntimeStopError) else api is None
            )
            worker_process_joined = not (
                isinstance(exc, WorkerShutdownError) and not exc.process_joined
            )
            if api is not None:
                try:
                    self.runtime.cancel_owned_work(api)
                except Exception as runtime_cleanup_error:
                    cleanup_error = runtime_cleanup_error
                try:
                    self.runtime.stop(api)
                except RuntimeStopError as runtime_stop_error:
                    stop_error = runtime_stop_error
                    server_process_joined = runtime_stop_error.process_joined
                except Exception as runtime_stop_error:
                    stop_error = runtime_stop_error
                    server_process_joined = False
                else:
                    server_process_joined = True
            if not server_process_joined or not worker_process_joined:
                lease.unlock_on_release = False
            report = self._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status=(
                    "failed"
                    if server_process_joined and worker_process_joined
                    else "stop_failed_cleanup_deferred"
                ),
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log="rehearsals/{}/server.log".format(report_id),
                evidence=None,
                phases=(),
                config=config,
                error=type(exc).__name__,
                error_message=self._redact_diagnostic(
                    str(exc), inherited_environment
                ),
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
                rehearsal_mode="full_rebuild",
                cleanup_error=(
                    type(cleanup_error).__name__ if cleanup_error else None
                ),
                stop_error=type(stop_error).__name__ if stop_error else None,
                server_process_joined=server_process_joined,
                worker_process_joined=worker_process_joined,
            )
            self._write_report(report_id, report)
            raise CutoverSafetyError("Isolated Market v4 rehearsal failed") from exc
        report = self._operation_report(
            report_id=report_id,
            phase="rehearsal",
            status="passed",
            duration_seconds=time.monotonic() - started,
            api_checks=checks,
            server_log="rehearsals/{}/server.log".format(report_id),
            evidence=evidence,
            phases=phases,
            config=config,
            target_root_fingerprint=target_root_fingerprint,
            code_version=code_version,
            rehearsal_mode="full_rebuild",
            server_process_joined=True,
            worker_process_joined=True,
        )
        try:
            report_path = self._write_report(
                report_id,
                report,
                expected_root_fingerprint=target_root_fingerprint,
            )
        except Exception as exc:
            failure_report = self._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status="failed",
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log=f"rehearsals/{report_id}/server.log",
                evidence=None,
                phases=(),
                config=config,
                error=type(exc).__name__,
                error_message=self._redact_diagnostic(
                    str(exc), inherited_environment
                ),
                target_root_fingerprint=target_root_fingerprint,
                code_version=code_version,
                rehearsal_mode="full_rebuild",
                server_process_joined=True,
                worker_process_joined=True,
            )
            self._try_write_report(report_id, failure_report)
            raise CutoverSafetyError("Isolated Market v4 rehearsal failed") from exc
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    def cutover(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        with self._exclusive_operation() as code_version:
            return self._cutover_under_lease(
                report_id,
                rehearsal_report_id=rehearsal_report_id,
                backup_id=backup_id,
                config=config,
                inherited_environment=inherited_environment,
                code_version=code_version,
            )

    @staticmethod
    def _market_identity_evidence_valid(value: object) -> bool:
        if not isinstance(value, dict) or set(value) != {
            "marketDuckdb",
            "parquetSha256",
        }:
            return False

        def file_identity_valid(identity: object) -> bool:
            return (
                isinstance(identity, dict)
                and set(identity) == {"device", "inode", "size", "sha256"}
                and all(
                    isinstance(identity.get(key), int)
                    and int(identity[key]) >= 0
                    for key in ("device", "inode", "size")
                )
                and isinstance(identity.get("sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", identity["sha256"]) is not None
            )

        parquet = value.get("parquetSha256")
        return (
            file_identity_valid(value.get("marketDuckdb"))
            and isinstance(parquet, dict)
            and bool(parquet)
            and all(
                isinstance(path, str)
                and bool(path)
                and not Path(path).is_absolute()
                and ".." not in Path(path).parts
                and file_identity_valid(identity)
                for path, identity in parquet.items()
            )
        )

    @staticmethod
    def _retained_report_contract_valid(
        report: dict[str, object],
        *,
        report_id: str,
        config: SmokeConfig,
    ) -> bool:
        api_checks = report.get("apiChecks")
        required_api_checks = {
            "/api/db/stats",
            "/api/db/validate",
            f"/api/analytics/fundamentals/{quote(config.symbol, safe='')}",
            "/api/fundamentals/compute",
            "/api/analytics/screening/jobs",
            "/api/analytics/fundamental-ranking",
            "/api/dataset",
        }
        if (
            not isinstance(api_checks, list)
            or not all(isinstance(path, str) for path in api_checks)
            or not required_api_checks.issubset(set(api_checks))
            or not any(
                path.startswith("/api/analytics/screening/jobs/")
                for path in api_checks
            )
            or not any("/api/analytics/screening/result/" in path for path in api_checks)
            or not any(path.startswith("/api/dataset/jobs/") for path in api_checks)
            or not any(path.endswith("/info") for path in api_checks)
            or not any("/sample?count=1" in path for path in api_checks)
            or any(
                forbidden in path
                for path in api_checks
                for forbidden in (
                    "/api/db/sync",
                    "materialize",
                    "stocks/refresh",
                    "intraday/sync",
                )
            )
        ):
            return False
        coverage = report.get("schemaCoverage")
        lineage_keys = {
            "sourceStatementKeyCount",
            "expectedAdjustedStatementRows",
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "missingDailyValuationRows",
            "extraDailyValuationRows",
            "wrongBasisDailyValuationRows",
            "statementRows",
            "dailyValuationRows",
            "readyBasisCount",
        }
        if not isinstance(coverage, dict) or set(coverage) != {
            "schemaVersion",
            "stockPriceAdjustmentMode",
            "adjustedMetrics",
        }:
            return False
        lineage = coverage.get("adjustedMetrics")
        if (
            coverage.get("schemaVersion") != 4
            or coverage.get("stockPriceAdjustmentMode")
            != "local_projection_v2_event_time"
            or not isinstance(lineage, dict)
            or set(lineage) != lineage_keys
            or any(not isinstance(value, int) for value in lineage.values())
            or any(
                lineage[key] <= 0
                for key in (
                    "sourceStatementKeyCount",
                    "expectedAdjustedStatementRows",
                    "statementRows",
                    "dailyValuationRows",
                    "readyBasisCount",
                )
            )
            or any(
                lineage[key] != 0
                for key in lineage_keys
                - {
                    "sourceStatementKeyCount",
                    "expectedAdjustedStatementRows",
                    "statementRows",
                    "dailyValuationRows",
                    "readyBasisCount",
                }
            )
        ):
            return False
        phases = report.get("phases")
        if not isinstance(phases, list) or not any(
            isinstance(phase, dict)
            and phase.get("name") == "retained_market_smoke"
            and phase.get("status") == "passed"
            and isinstance(phase.get("durationSeconds"), (int, float))
            and float(phase["durationSeconds"]) >= 0
            for phase in phases
        ):
            return False
        before = report.get("sourceMarketIdentityBefore")
        return (
            report.get("reportId") == report_id
            and MarketV4CutoverService._market_identity_evidence_valid(before)
            and before == report.get("sourceMarketIdentityAfter")
        )

    @staticmethod
    def _full_rebuild_report_contract_valid(
        report: dict[str, object],
        *,
        config: SmokeConfig,
    ) -> bool:
        api_checks = report.get("apiChecks")
        if (
            not isinstance(api_checks, list)
            or not all(isinstance(path, str) for path in api_checks)
            or "/api/db/sync" not in api_checks
            or not any(path.startswith("/api/db/sync/jobs/") for path in api_checks)
        ):
            return False
        synthetic_identity = {
            "marketDuckdb": {
                "device": 0,
                "inode": 0,
                "size": 0,
                "sha256": "0" * 64,
            },
            "parquetSha256": {
                "evidence.parquet": {
                    "device": 0,
                    "inode": 0,
                    "size": 0,
                    "sha256": "0" * 64,
                }
            },
        }
        semantic_report = {
            **report,
            "apiChecks": [
                path for path in api_checks if not path.startswith("/api/db/sync")
            ],
            "phases": [
                {
                    "name": "retained_market_smoke",
                    "status": "passed",
                    "durationSeconds": 0,
                }
            ],
            "sourceMarketIdentityBefore": synthetic_identity,
            "sourceMarketIdentityAfter": synthetic_identity,
        }
        if not MarketV4CutoverService._retained_report_contract_valid(
            semantic_report,
            report_id=str(report.get("reportId", "")),
            config=config,
        ):
            return False
        phases = report.get("phases")
        required_phases = {
            "initial_sync_and_adjusted_metrics_pit",
            "semantic_smoke",
        }
        return isinstance(phases, list) and required_phases == {
            str(phase.get("name"))
            for phase in phases
            if isinstance(phase, dict)
            and phase.get("status") == "passed"
            and isinstance(phase.get("durationSeconds"), (int, float))
            and float(phase["durationSeconds"]) >= 0
        }

    def _promotion_report_snapshot(
        self,
        report_id: str,
    ) -> tuple[dict[str, object], str, tuple[int, int, int, int, int]]:
        report_id = self._validate_id(report_id, label="rehearsal report")
        relative = Path("operations/market-v4-cutover/reports") / report_id / (
            "report.json"
        )
        try:
            metadata, digest = self._regular_file_identity(self._managed(), relative)
            payload = self._managed().read_bytes(relative)
            current, current_digest = self._regular_file_identity(
                self._managed(), relative
            )
        except FileNotFoundError as exc:
            raise CutoverSafetyError(
                "An exact passing retained rehearsal report is required"
            ) from exc
        identity = (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        )
        if identity != (
            current.st_dev,
            current.st_ino,
            current.st_size,
            current.st_mtime_ns,
            current.st_ctime_ns,
        ) or digest != current_digest or hashlib.sha256(payload).hexdigest() != digest:
            raise CutoverSafetyError("Promotion report changed during validation")
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError("Promotion report is unreadable") from exc
        if not isinstance(value, dict):
            raise CutoverSafetyError("Promotion report is invalid")
        return value, digest, identity

    def _assert_promotion_destination_absent(self, relative: Path) -> None:
        try:
            self._managed().stat(relative)
        except FileNotFoundError:
            return
        raise CutoverSafetyError("Promotion destination already exists")

    @staticmethod
    def _assert_empty_directory(directory_fd: int, name: str) -> None:
        child = os.open(name, _DIR_OPEN_FLAGS, dir_fd=directory_fd)
        try:
            if os.listdir(child):
                raise CutoverSafetyError(
                    "Retained Market temporary artifact is not empty"
                )
        finally:
            os.close(child)

    def _validate_retained_market_allowlist(
        self,
        root_fd: int,
        *,
        proven_runtime_names: tuple[str, ...],
    ) -> None:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            allowed_runtime_names = set(proven_runtime_names)
            for name in os.listdir(market_fd):
                entry = os.stat(name, dir_fd=market_fd, follow_symlinks=False)
                if name in {"market.duckdb", "parquet"}:
                    continue
                if name == "market.duckdb.wal":
                    if not stat.S_ISREG(entry.st_mode) or entry.st_size != 0:
                        raise CutoverSafetyError(
                            "Nonempty or invalid retained DuckDB WAL"
                        )
                    continue
                if name == "duckdb-tmp" or name in allowed_runtime_names:
                    if not stat.S_ISDIR(entry.st_mode) or stat.S_ISLNK(entry.st_mode):
                        raise CutoverSafetyError(
                            "Retained Market artifact must be a real directory"
                        )
                    if name == "duckdb-tmp":
                        MarketV4CutoverService._assert_empty_directory(
                            market_fd, name
                        )
                    continue
                raise CutoverSafetyError(
                    "Retained Market contains an unexpected artifact"
                )
        finally:
            os.close(market_fd)

    def _proven_retained_runtime_names(
        self,
        root_fd: int,
        *,
        source_report_id: str,
        retained_report_id: str,
        source_report_code_version: str,
        source_market_identity: dict[str, object],
        retained_root_fingerprint: str,
        target_root_fingerprint: str,
    ) -> tuple[str, ...]:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            runtime_names = sorted(
                name
                for name in os.listdir(market_fd)
                if name.startswith(".cutover-runtime-")
            )
        finally:
            os.close(market_fd)
        source_runtime = f".cutover-runtime-{source_report_id}"
        selected_runtime = f".cutover-runtime-{retained_report_id}"
        proven = {source_runtime, selected_runtime}
        prefix = ".cutover-runtime-"
        for runtime_name in runtime_names:
            if runtime_name in proven:
                continue
            report_id = runtime_name.removeprefix(prefix)
            try:
                report_id = self._validate_id(report_id, label="retained report")
                report, _sha256, _identity = self._promotion_report_snapshot(report_id)
            except CutoverSafetyError:
                continue
            if (
                report.get("reportId") == report_id
                and report.get("phase") == "rehearsal"
                and report.get("status") in {"passed", "failed"}
                and report.get("rehearsalMode") == "retained_market_smoke"
                and report.get("sourceRehearsalReportId") == source_report_id
                and report.get("sourceRehearsalCodeVersion")
                == source_report_code_version
                and report.get("sourceRetainedRootFingerprint")
                == retained_root_fingerprint
                and report.get("targetRootFingerprint") == target_root_fingerprint
                and report.get("serverProcessJoined") is True
                and report.get("workerProcessJoined") is True
                and report.get("sourceMarketIdentityBefore") == source_market_identity
                and report.get("sourceMarketIdentityAfter") == source_market_identity
            ):
                proven.add(runtime_name)
        ordered = [source_runtime]
        ordered.extend(sorted(proven - {source_runtime, selected_runtime}))
        if selected_runtime != source_runtime:
            ordered.append(selected_runtime)
        return tuple(ordered)

    def _assert_promotion_exchange_capability(
        self,
        retained_lease: MarketOperationLease,
    ) -> None:
        active_market = self._managed().open_dir(Path("market-timeseries"))
        retained_market = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_lease.root_fd
        )
        try:
            devices = {
                os.fstat(self._managed().fd).st_dev,
                os.fstat(active_market).st_dev,
                os.fstat(retained_lease.root_fd).st_dev,
                os.fstat(retained_market).st_dev,
            }
            if len(devices) != 1:
                raise CutoverSafetyError(
                    "Atomic exchange directories must be on the same device"
                )
        finally:
            os.close(active_market)
            os.close(retained_market)
        if isinstance(self.atomic_exchange, DarwinAtomicExchange):
            self.atomic_exchange.require_capability()
        else:
            capability = getattr(self.atomic_exchange, "require_capability", None)
            if capability is not None:
                capability()

    def _validate_retained_promotion_eligibility_under_leases(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        code_version: str,
        retained_lease: MarketOperationLease,
    ) -> RetainedPromotionEligibility:
        report_id = self._validate_id(report_id, label="report")
        retained_report_id = self._validate_id(
            retained_report_id, label="retained report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        expected_smoke_config = {
            "symbol": config.symbol,
            "strategy": config.strategy,
            "datasetPreset": config.dataset_preset,
        }
        retained, retained_sha256, retained_stat = self._promotion_report_snapshot(
            retained_report_id
        )
        source_report_value = retained.get("sourceRehearsalReportId")
        if not isinstance(source_report_value, str):
            raise CutoverSafetyError("Retained report provenance is invalid")
        source_report_id = self._validate_id(
            source_report_value, label="source rehearsal report"
        )
        source, source_sha256, source_stat = self._promotion_report_snapshot(
            source_report_id
        )
        retained_again, retained_sha256_again, retained_stat_again = (
            self._promotion_report_snapshot(retained_report_id)
        )
        source_again, source_sha256_again, source_stat_again = (
            self._promotion_report_snapshot(source_report_id)
        )
        if (
            retained != retained_again
            or retained_sha256 != retained_sha256_again
            or retained_stat != retained_stat_again
            or source != source_again
            or source_sha256 != source_sha256_again
            or source_stat != source_stat_again
        ):
            raise CutoverSafetyError("Promotion report provenance changed")

        target_root_fingerprint = self.root_fingerprint(self.data_root)
        retained_valid = (
            retained.get("reportId") == retained_report_id
            and retained.get("phase") == "rehearsal"
            and retained.get("status") == "passed"
            and retained.get("rehearsalMode") == "retained_market_smoke"
            and retained.get("serverProcessJoined") is True
            and retained.get("workerProcessJoined") is True
            and retained.get("smokeConfig") == expected_smoke_config
            and retained.get("targetRootFingerprint") == target_root_fingerprint
            and self._retained_report_contract_valid(
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
            and source.get("codeVersion")
            == retained.get("sourceRehearsalCodeVersion")
        )
        if not retained_valid or not source_valid:
            raise CutoverSafetyError(
                "An exact passing retained rehearsal report is required"
            )

        retained_root = self._retained_rehearsal_root(source_report_id)
        self._assert_retained_root_identity(retained_root, retained_lease.root_fd)
        retained_root_fingerprint = self._root_fingerprint_at(retained_lease.root_fd)
        if retained_root_fingerprint != retained.get("sourceRetainedRootFingerprint"):
            raise CutoverSafetyError("Retained root fingerprint mismatch")
        configuration_fingerprint = self.configuration_fingerprint(self.data_root)
        if (
            self._configuration_fingerprint_at(retained_lease.root_fd)
            != configuration_fingerprint
        ):
            raise CutoverSafetyError("Retained configuration differs from active")

        source_market_identity = self._market_tree_identity(retained_lease.root_fd)
        if source_market_identity != retained.get(
            "sourceMarketIdentityBefore"
        ) or source_market_identity != retained.get("sourceMarketIdentityAfter"):
            raise CutoverSafetyError("Retained Market payload identity mismatch")
        source_code_version = source.get("codeVersion")
        if not isinstance(source_code_version, str):
            raise CutoverSafetyError("Promotion source report code is invalid")
        proven_runtime_names = self._proven_retained_runtime_names(
            retained_lease.root_fd,
            source_report_id=source_report_id,
            retained_report_id=retained_report_id,
            source_report_code_version=source_code_version,
            source_market_identity=source_market_identity,
            retained_root_fingerprint=retained_root_fingerprint,
            target_root_fingerprint=target_root_fingerprint,
        )
        self._validate_retained_market_allowlist(
            retained_lease.root_fd,
            proven_runtime_names=proven_runtime_names,
        )
        retained_market_fd = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_lease.root_fd
        )
        try:
            metadata = self.duckdb.inspect(
                retained_market_fd,
                "market.duckdb",
                guard_lease_fd=retained_lease.fd,
            )
        finally:
            os.close(retained_market_fd)
        if metadata.schema_version != 4:
            raise CutoverSafetyError("Retained Market schema v4 is required")
        if metadata.adjustment_mode != "local_projection_v2_event_time":
            raise CutoverSafetyError("Retained Market adjustment mode is incompatible")
        if not metadata.adjusted_metrics_ready:
            raise CutoverSafetyError(
                "Retained adjusted-metric event-time lineage is not ready"
            )

        self.runtime.assert_quiescent(self.data_root)
        try:
            active_wal = self._managed().stat(
                Path("market-timeseries/market.duckdb.wal")
            )
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(active_wal.st_mode) or active_wal.st_size != 0:
                raise CutoverSafetyError("Nonempty or invalid active DuckDB WAL")
        active_market_identity = self._market_tree_identity(self._active_lease_fd_root())

        for destination in (
            Path("operations/market-v4-cutover/reports") / report_id,
            Path("operations/market-v4-cutover/journals") / report_id,
            Path("operations/market-v4-cutover/journal-controls") / report_id,
            Path("operations/market-v4-cutover/journal-locks") / f"{report_id}.lock",
            Path("operations/market-v4-cutover/holding") / report_id,
            Path("operations/market-v4-cutover/cleanup-staging") / report_id,
            Path("operations/market-v4-cutover/cleanup-controls")
            / f"{report_id}.json",
            Path("operations/market-v4-cutover/cleanup-results")
            / f"{report_id}.json",
            Path("operations/market-v4-cutover/quarantine") / report_id,
            Path("operations/market-v4-cutover/backups") / backup_id,
            Path("operations/market-v4-cutover/consumed")
            / f"{retained_report_id}.json",
        ):
            self._assert_promotion_destination_absent(destination)
        self._assert_promotion_exchange_capability(retained_lease)
        self._require_unchanged_code_identity(code_version)
        self._assert_retained_root_identity(retained_root, retained_lease.root_fd)
        if self.root_fingerprint(self.data_root) != target_root_fingerprint:
            raise CutoverSafetyError("Active root changed during promotion validation")
        if self._market_tree_identity(retained_lease.root_fd) != source_market_identity:
            raise CutoverSafetyError(
                "Retained Market changed during promotion validation"
            )
        if (
            self._proven_retained_runtime_names(
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
            raise CutoverSafetyError("Retained runtime provenance changed")
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
        if self._active_lease is None:
            raise CutoverSafetyError("An active Market operation lease is required")
        return self._active_lease.root_fd

    def _retained_lease_fd_root(self) -> int:
        if self._retained_lease is None:
            raise CutoverSafetyError("A retained Market operation lease is required")
        return self._retained_lease.root_fd

    @staticmethod
    def _directory_identity_evidence(directory_fd: int) -> dict[str, int]:
        directory = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory.st_mode):
            raise CutoverSafetyError("Promotion location must be a real directory")
        return {"device": directory.st_dev, "inode": directory.st_ino}

    @classmethod
    def _market_location_identity(cls, root_fd: int) -> dict[str, object]:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            directory = cls._directory_identity_evidence(market_fd)
        finally:
            os.close(market_fd)
        return {
            "directory": directory,
            "payload": cls._market_tree_identity(root_fd),
        }

    @staticmethod
    def _manifest_file_set_sha256(manifest: dict[str, object]) -> str:
        entries = manifest.get("files")
        if not isinstance(entries, list):
            raise CutoverSafetyError("Backup manifest file set is invalid")
        return hashlib.sha256(PromotionJournal._canonical_json(entries)).hexdigest()

    @staticmethod
    def _payload_manifest_entries(
        identity: dict[str, object],
    ) -> dict[str, tuple[int, str]]:
        database = identity.get("marketDuckdb")
        parquet = identity.get("parquetSha256")
        if not isinstance(database, dict) or not isinstance(parquet, dict):
            raise CutoverSafetyError("Promotion payload identity is invalid")
        entries: dict[str, tuple[int, str]] = {}

        def add(path: str, value: object) -> None:
            if (
                not isinstance(value, dict)
                or type(value.get("size")) is not int
                or not isinstance(value.get("sha256"), str)
            ):
                raise CutoverSafetyError("Promotion payload identity is invalid")
            entries[path] = (cast(int, value["size"]), cast(str, value["sha256"]))

        add("market.duckdb", database)
        for path, value in parquet.items():
            if not isinstance(path, str):
                raise CutoverSafetyError("Promotion payload identity is invalid")
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
        payload_fd = self._managed().open_dir(
            self._managed_relative(self.backups_root / backup_id / "payload")
        )
        try:
            return self._market_payload_identity(payload_fd)
        finally:
            os.close(payload_fd)

    def _held_artifact_evidence(
        self,
        holding_fd: int,
        name: str,
    ) -> DetachedArtifactEvidence:
        if not name or "/" in name or name in {".", ".."}:
            raise CutoverSafetyError("Promotion held artifact name is invalid")
        entry = os.stat(name, dir_fd=holding_fd, follow_symlinks=False)
        if stat.S_ISREG(entry.st_mode):
            with ManagedRootFd(Path("."), os.dup(holding_fd)) as holding:
                file_stat, digest = self._regular_file_identity(holding, Path(name))
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
            raise CutoverSafetyError("Promotion held artifact must be regular or directory")
        artifact_fd = os.open(name, _DIR_OPEN_FLAGS, dir_fd=holding_fd)
        directories: dict[str, dict[str, int]] = {}
        try:
            with ManagedRootFd(Path("."), os.dup(artifact_fd)) as artifact:
                files: dict[str, dict[str, object]] = {}

                def walk(directory_fd: int, relative: Path) -> None:
                    directory = self._directory_identity_evidence(directory_fd)
                    directories[relative.as_posix() if relative.parts else "."] = directory
                    for child_name in sorted(os.listdir(directory_fd)):
                        child = os.stat(
                            child_name,
                            dir_fd=directory_fd,
                            follow_symlinks=False,
                        )
                        child_relative = relative / child_name
                        if stat.S_ISDIR(child.st_mode) and not stat.S_ISLNK(child.st_mode):
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
                            file_stat, digest = self._regular_file_identity(
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
                            raise CutoverSafetyError(
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
        manifest_path = self.backups_root / backup_id / "manifest.json"
        manifest_bytes = self._managed().read_bytes(
            self._managed_relative(manifest_path)
        )
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError("Backup manifest is unreadable") from exc
        if not isinstance(manifest, dict):
            raise CutoverSafetyError("Backup manifest is invalid")
        entries = manifest.get("files")
        if not isinstance(entries, list):
            raise CutoverSafetyError("Backup manifest file set is invalid")
        actual: dict[str, tuple[int, str]] = {}
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("path"), str)
                or type(entry.get("bytes")) is not int
                or not isinstance(entry.get("sha256"), str)
            ):
                raise CutoverSafetyError("Backup manifest file entry is invalid")
            path = cast(str, entry["path"])
            if path in actual:
                raise CutoverSafetyError("Backup manifest contains a duplicate path")
            actual[path] = (
                cast(int, entry["bytes"]),
                cast(str, entry["sha256"]),
            )
        if actual != self._payload_manifest_entries(expected_payload):
            raise CutoverSafetyError("Backup payload identity mismatch")
        backup_payload_identity = self._backup_payload_identity(backup_id)
        if self._payload_manifest_entries(backup_payload_identity) != actual:
            raise CutoverSafetyError("Backup physical payload content mismatch")
        if not self._payload_physical_identity_distinct(
            backup_payload_identity,
            expected_payload,
        ):
            raise CutoverSafetyError("Backup physical payload identity is not distinct")
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
            for lease in (self._active_lease, self._retained_lease):
                if lease is not None:
                    lease.unlock_on_release = False
                    lease.owns_fd = False
            raise CutoverSafetyError(
                f"Promotion journal append is indeterminate: {result.attempt_id}"
            )
        raise CutoverSafetyError(
            f"Promotion journal append was not committed: {result.attempt_id}"
        )

    @staticmethod
    def _validate_canonical_market_payload(market_fd: int) -> None:
        if set(os.listdir(market_fd)) != {"market.duckdb", "parquet"}:
            raise CutoverSafetyError("Retained Market payload is not canonical")
        database = os.stat("market.duckdb", dir_fd=market_fd, follow_symlinks=False)
        parquet = os.stat("parquet", dir_fd=market_fd, follow_symlinks=False)
        if not stat.S_ISREG(database.st_mode) or not stat.S_ISDIR(parquet.st_mode):
            raise CutoverSafetyError("Retained Market payload is not canonical")

    def _retained_artifact_detachment_plan(
        self,
        eligibility: RetainedPromotionEligibility,
    ) -> tuple[tuple[str, ...], tuple[DetachedArtifactEvidence, ...]]:
        """Snapshot every allowed artifact before the first detach mutation."""

        retained_root_fd = self._retained_lease_fd_root()
        self._assert_retained_root_identity(eligibility.retained_root, retained_root_fd)
        market_fd = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_root_fd
        )
        try:
            source_report, source_sha256, _source_stat = (
                self._promotion_report_snapshot(eligibility.source_report_id)
            )
            source_code_version = source_report.get("codeVersion")
            if source_sha256 != eligibility.source_report_sha256 or not isinstance(
                source_code_version, str
            ):
                raise CutoverSafetyError(
                    "Promotion source report changed before detach"
                )
            proven_runtimes = self._proven_retained_runtime_names(
                retained_root_fd,
                source_report_id=eligibility.source_report_id,
                retained_report_id=eligibility.retained_report_id,
                source_report_code_version=source_code_version,
                source_market_identity=eligibility.source_market_identity,
                retained_root_fingerprint=self._root_fingerprint_at(retained_root_fd),
                target_root_fingerprint=eligibility.target_root_fingerprint,
            )
            self._validate_retained_market_allowlist(
                retained_root_fd,
                proven_runtime_names=proven_runtimes,
            )
            artifacts: list[DetachedArtifactEvidence] = []
            for name in (*proven_runtimes, "duckdb-tmp", "market.duckdb.wal"):
                try:
                    entry = os.stat(name, dir_fd=market_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if name in proven_runtimes:
                    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                        raise CutoverSafetyError(
                            "Proven retained runtime must be a real directory"
                        )
                elif name == "duckdb-tmp":
                    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                        raise CutoverSafetyError(
                            "Retained Market temporary artifact is invalid"
                        )
                    self._assert_empty_directory(market_fd, name)
                elif not stat.S_ISREG(entry.st_mode) or entry.st_size != 0:
                    raise CutoverSafetyError("Retained Market WAL artifact is invalid")
                artifacts.append(self._held_artifact_evidence(market_fd, name))
            present_runtime_names = tuple(
                artifact.name
                for artifact in artifacts
                if artifact.name in proven_runtimes
            )
            return present_runtime_names, tuple(artifacts)
        finally:
            os.close(market_fd)

    def _detach_retained_artifacts(
        self,
        eligibility: RetainedPromotionEligibility,
        *,
        holding_root: Path,
        planned_artifacts: tuple[DetachedArtifactEvidence, ...],
    ) -> None:
        retained_root_fd = self._retained_lease_fd_root()
        self._assert_retained_root_identity(eligibility.retained_root, retained_root_fd)
        market_fd = os.open(
            "market-timeseries", _DIR_OPEN_FLAGS, dir_fd=retained_root_fd
        )
        holding_fd = self._managed().open_dir(self._managed_relative(holding_root))
        retained_market_identity = self._directory_identity_evidence(market_fd)
        holding_identity = self._directory_identity_evidence(holding_fd)
        try:
            for artifact in planned_artifacts:
                name = artifact.name
                if self._held_artifact_evidence(market_fd, name) != artifact:
                    raise CutoverSafetyError(
                        "Planned promotion artifact identity changed"
                    )
                self._rename_at_hook(
                    eligibility.retained_root / "market-timeseries" / name,
                    holding_root / name,
                )
                _rename_exclusive_at(market_fd, name, holding_fd, name)
                if self._held_artifact_evidence(holding_fd, name) != artifact:
                    raise CutoverSafetyError(
                        "Detached promotion artifact identity changed"
                    )
                self._promotion_boundary_hook(f"detach_artifact_{name}:moved")
                os.fsync(market_fd)
                self._promotion_boundary_hook(
                    f"detach_artifact_{name}:source_fsynced"
                )
                os.fsync(holding_fd)
                self._promotion_boundary_hook(
                    f"detach_artifact_{name}:holding_fsynced"
                )
            if retained_market_identity != self._directory_identity_evidence(market_fd):
                raise CutoverSafetyError("Retained Market directory identity changed")
            if holding_identity != self._directory_identity_evidence(holding_fd):
                raise CutoverSafetyError("Promotion holding directory identity changed")
            self._validate_canonical_market_payload(market_fd)
        finally:
            os.close(holding_fd)
            os.close(market_fd)
        if (
            self._market_tree_identity(retained_root_fd)
            != eligibility.source_market_identity
        ):
            raise CutoverSafetyError(
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

        backup_id = self._validate_id(backup_id, label="backup")
        if self._active_lease is None or self._retained_lease is None:
            raise CutoverSafetyError(
                "Active and retained Market operation leases are required"
            )
        if self._market_tree_identity(self._active_lease_fd_root()) != (
            eligibility.active_market_identity
        ):
            raise CutoverSafetyError("Active Market payload identity changed")
        if self._market_tree_identity(self._retained_lease_fd_root()) != (
            eligibility.source_market_identity
        ):
            raise CutoverSafetyError("Retained Market payload identity changed")

        source_bytes = sum(
            self._managed().stat(self._managed_relative(path)).st_size
            for path in self._source_files(self.market_root)
        )
        required_bytes = source_bytes + max(source_bytes // 20, 1)
        if self.disk_free_bytes(self.data_root) < required_bytes:
            raise CutoverSafetyError(
                f"Insufficient free space: require at least {required_bytes} bytes"
            )

        active_market_fd = os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=self._active_lease_fd_root(),
        )
        try:
            metadata = self.duckdb.inspect(
                active_market_fd,
                "market.duckdb",
                guard_lease_fd=self._active_lease_fd(),
            )
        finally:
            os.close(active_market_fd)
        code_version = self._active_code_version
        if code_version is None:
            raise CutoverSafetyError("Operation code identity is unavailable")
        self._copy_backup_under_snapshot(
            backup_id,
            metadata,
            code_version=code_version,
        )
        (
            backup_manifest_sha256,
            backup_file_set_sha256,
            backup_payload_identity,
        ) = self._verified_backup_evidence(
            backup_id,
            expected_payload=eligibility.active_market_identity,
        )
        if self._market_tree_identity(self._active_lease_fd_root()) != (
            eligibility.active_market_identity
        ):
            raise CutoverSafetyError(
                "Active Market payload identity changed after backup"
            )
        if self._backup_payload_identity(backup_id) != backup_payload_identity:
            raise CutoverSafetyError("Backup physical payload identity changed")

        active_location = self._market_location_identity(self._active_lease_fd_root())
        retained_location = self._market_location_identity(
            self._retained_lease_fd_root()
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
        self._append_preparation_state(journal, PromotionState.VALIDATED, validated)

        holding_parent = self.operations_root / "holding"
        self._prepare_managed_directory(holding_parent, exist_ok=True)
        self._managed().fsync_dir(self._managed_relative(self.operations_root))
        holding_root = holding_parent / journal.operation_id
        self._prepare_managed_directory(holding_root, exist_ok=False)
        self._managed().fsync_dir(self._managed_relative(holding_parent))
        holding_fd = self._managed().open_dir(self._managed_relative(holding_root))
        try:
            os.fsync(holding_fd)
            holding_directory = self._directory_identity_evidence(holding_fd)
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
            os.fsync(self._retained_lease_fd_root())
            retained_location = self._market_location_identity(
                self._retained_lease_fd_root()
            )
            holding_fd = self._managed().open_dir(
                self._managed_relative(holding_root)
            )
            try:
                detached_artifacts = self._held_artifacts_evidence(holding_fd)
            finally:
                os.close(holding_fd)
            if detached_artifacts != planned_artifacts:
                raise CutoverSafetyError("Detached runtime evidence is incomplete")
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
            self._append_preparation_state(
                journal,
                PromotionState.RUNTIMES_DETACHED,
                detached,
            )
            self._append_preparation_state(
                journal, PromotionState.PREPARED, detached
            )
            return preparation
        except Exception as exc:
            if "journal append is indeterminate" in str(exc):
                raise
            try:
                self._rollback_retained_promotion(
                    RetainedPromotionContext(preparation, journal),
                    processes_joined=True,
                )
            except Exception as rollback_error:
                deferred_journal_error: Exception | None = None
                try:
                    last = journal.read_validated()[-1]
                    holding_current: dict[str, object] | None = None
                    try:
                        unresolved_holding_fd = self._managed().open_dir(
                            self._managed_relative(holding_root)
                        )
                    except FileNotFoundError:
                        pass
                    else:
                        try:
                            holding_current = {
                                "directory": self._directory_identity_evidence(
                                    unresolved_holding_fd
                                ),
                                "payload": eligibility.source_market_identity,
                            }
                        finally:
                            os.close(unresolved_holding_fd)
                    deferred = self._promotion_identities(
                        last.identities,
                        active_current=self._market_location_identity(
                            self._active_lease_fd_root()
                        ),
                        retained_current=self._market_location_identity(
                            self._retained_lease_fd_root()
                        ),
                        quarantine_current=None,
                        holding_current=holding_current,
                    )
                    self._append_preparation_state(
                        journal,
                        PromotionState.ROLLBACK_DEFERRED,
                        deferred,
                    )
                except Exception as journal_error:
                    deferred_journal_error = journal_error
                for lease in (self._active_lease, self._retained_lease):
                    if lease is not None:
                        lease.unlock_on_release = False
                        lease.owns_fd = False
                raise CutoverSafetyError(
                    "Retained promotion preparation rollback deferred with both leases held"
                ) from (deferred_journal_error or rollback_error)
            raise exc

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
        "missingAdjustedStatementRows",
        "extraAdjustedStatementRows",
        "staleAdjustedStatementRows",
        "wrongBasisAdjustedStatementRows",
        "missingDailyValuationRows",
        "extraDailyValuationRows",
        "wrongBasisDailyValuationRows",
    )
    _PROMOTION_POSITIVE_LINEAGE_KEYS = (
        "sourceStatementKeyCount",
        "expectedAdjustedStatementRows",
        "statementRows",
        "dailyValuationRows",
        "readyBasisCount",
    )

    @classmethod
    def _retained_promotion_report_contract_valid(
        cls,
        report: object,
        *,
        expectation: RetainedPromotionReportExpectation | None = None,
    ) -> bool:
        if not isinstance(report, dict) or set(report) != cls._PROMOTION_REPORT_KEYS:
            return False
        if expectation is None or report != expectation.to_report():
            return False

        def exact_mapping(
            value: object,
            keys: set[str],
        ) -> dict[str, object] | None:
            return value if isinstance(value, dict) and set(value) == keys else None

        retained = exact_mapping(
            report["retainedReport"],
            {"reportId", "codeVersion", "reportSha256"},
        )
        source = exact_mapping(
            report["sourceReport"],
            {"reportId", "codeVersion", "reportSha256"},
        )
        fingerprints = exact_mapping(
            report["fingerprints"],
            {"targetRoot", "retainedRoot", "configuration"},
        )
        payloads = exact_mapping(
            report["payloadIdentities"],
            {"activeBefore", "backup", "retainedSource", "activated", "activeAfter"},
        )
        filesystem = exact_mapping(
            report["filesystemEvidence"],
            {
                "sameDevice",
                "atomicExchange",
                "activeBeforeDirectory",
                "retainedSourceDirectory",
                "activatedDirectory",
                "activeAfterDirectory",
                "quarantineDirectory",
            },
        )
        journal = exact_mapping(report["journal"], {"operationId", "finalState"})
        cleanup = exact_mapping(
            report["runtimeCleanup"],
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
        )
        backup_evidence = exact_mapping(
            report["backupEvidence"],
            {
                "manifestSha256",
                "fileSetSha256",
                "contentEquivalentToActiveBefore",
                "physicalIdentityDistinct",
            },
        )
        semantic = exact_mapping(
            report["semanticSmoke"],
            {
                "schemaVersion",
                "stockPriceAdjustmentMode",
                "checks",
                "adjustedMetrics",
            },
        )
        consumed = exact_mapping(
            report["sourceConsumed"],
            {"retainedReportId", "markerPath"},
        )
        mappings = (
            retained,
            source,
            fingerprints,
            payloads,
            filesystem,
            journal,
            cleanup,
            backup_evidence,
            semantic,
            consumed,
        )
        if any(value is None for value in mappings):
            return False
        assert retained is not None
        assert source is not None
        assert fingerprints is not None
        assert payloads is not None
        assert filesystem is not None
        assert journal is not None
        assert cleanup is not None
        assert backup_evidence is not None
        assert semantic is not None
        assert consumed is not None
        lineage = semantic["adjustedMetrics"]
        exact_lineage = bool(
            isinstance(lineage, dict)
            and set(lineage)
            == set(cls._PROMOTION_ZERO_LINEAGE_KEYS)
            | set(cls._PROMOTION_POSITIVE_LINEAGE_KEYS)
            and all(lineage.get(key) == 0 for key in cls._PROMOTION_ZERO_LINEAGE_KEYS)
            and all(
                type(lineage.get(key)) is int and cast(int, lineage[key]) > 0
                for key in cls._PROMOTION_POSITIVE_LINEAGE_KEYS
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
        api_checks = report["apiChecks"]
        report_id = report["reportId"]
        exact_api_checks = False
        if (
            isinstance(report_id, str)
            and isinstance(api_checks, list)
            and len(api_checks) == 12
            and all(isinstance(path, str) for path in api_checks)
        ):
            screening_job = cast(str, api_checks[5]).removeprefix(
                "/api/analytics/screening/jobs/"
            )
            dataset_job = cast(str, api_checks[9]).removeprefix("/api/dataset/jobs/")
            dataset_name = f"cutover-smoke-{report_id.replace('.', '-')}-active"
            exact_api_checks = bool(
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
            and all(PromotionJournal._payload_valid(value) for value in identity_values)
            and all(
                PromotionJournal._directory_valid(value) for value in directory_values
            )
            and cls._payload_manifest_entries(
                cast(dict[str, object], payloads["activeBefore"])
            )
            == cls._payload_manifest_entries(
                cast(dict[str, object], payloads["backup"])
            )
            and cls._payload_physical_identity_distinct(
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
            and PromotionJournal._directory_valid(cleanup["holdingDirectory"])
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
            and backup_evidence["manifestSha256"]
            == report["backupManifestSha256"]
            and backup_evidence["fileSetSha256"]
            == report["backupFileSetSha256"]
            and backup_evidence["contentEquivalentToActiveBefore"] is True
            and backup_evidence["physicalIdentityDistinct"] is True
            and exact_api_checks
            and report["serverProcessJoined"] is True
            and report["workerProcessJoined"] is True
            and semantic["schemaVersion"] == 4
            and semantic["stockPriceAdjustmentMode"] == "local_projection_v2_event_time"
            and semantic["checks"] == list(cls._PROMOTION_SMOKE_CHECKS)
            and exact_lineage
            and consumed["retainedReportId"] == retained["reportId"]
            and consumed["markerPath"]
            == (f"operations/market-v4-cutover/consumed/{retained['reportId']}.json")
            and report["quarantinePath"]
            == (f"operations/market-v4-cutover/quarantine/{report['reportId']}")
        )

    @classmethod
    def _promotion_runtime_environment(
        cls,
        inherited: dict[str, str],
        *,
        lease_fd: int,
        root_fd: int,
        runtime_name: str,
    ) -> dict[str, str]:
        allowed = {
            key: value
            for key, value in inherited.items()
            if key in cls._PROMOTION_ENVIRONMENT_ALLOWLIST
            and not any(
                token in key.upper() for token in cls._PROMOTION_CREDENTIAL_KEY_TOKENS
            )
        }
        environment = cls._isolated_environment(
            allowed,
            lease_fd=lease_fd,
            root_fd=root_fd,
            runtime_name=runtime_name,
        )
        environment["TRADING25_RUNTIME_CAPABILITY"] = "retained_market_smoke"
        if any(
            token in key.upper()
            for key in environment
            for token in cls._PROMOTION_CREDENTIAL_KEY_TOKENS
        ):
            raise CutoverSafetyError(
                "Promotion runtime environment contains a credential capability"
            )
        return environment

    @classmethod
    def _build_retained_promotion_report(
        cls,
        expectation: RetainedPromotionReportExpectation,
    ) -> dict[str, object]:
        report = expectation.to_report()
        if not cls._retained_promotion_report_contract_valid(
            report,
            expectation=expectation,
        ):
            raise CutoverSafetyError("Promotion report contract is invalid")
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
            self._promotion_report_snapshot(eligibility.retained_report_id)
        )
        source_report, source_sha256, _source_stat = self._promotion_report_snapshot(
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
            raise CutoverSafetyError("Promotion report provenance changed")
        current_backup_identity = self._backup_payload_identity(preparation.backup_id)
        if current_backup_identity != preparation.backup_payload_identity:
            raise CutoverSafetyError("Promotion backup physical identity changed")
        if self._payload_manifest_entries(current_backup_identity) != (
            self._payload_manifest_entries(base.active_before_payload)
        ):
            raise CutoverSafetyError("Promotion backup content identity changed")
        if not self._payload_physical_identity_distinct(
            current_backup_identity,
            base.active_before_payload,
        ):
            raise CutoverSafetyError("Promotion backup is not physically independent")
        if verify_cleanup_staging:
            holding_fd = self._managed().open_dir(
                self._managed_relative(self._cleanup_staging_root(operation_id))
            )
            try:
                if (
                    self._directory_identity_evidence(holding_fd)
                    != preparation.holding_directory_identity
                    or self._held_artifacts_evidence(holding_fd)
                    != preparation.detached_artifacts
                ):
                    raise CutoverSafetyError(
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
                "retainedRoot": self._root_fingerprint_at(
                    self._retained_lease_fd_root()
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
            quarantine_path=self._managed_relative(quarantine).as_posix(),
            runtime_cleanup={
                "holdingDirectory": preparation.holding_directory_identity,
                "detachedRuntimeNames": list(preparation.detached_runtime_names),
                "detachedArtifacts": list(artifact_mappings),
                "removedArtifacts": [],
                "cleanupStagingPath": self._managed_relative(
                    self._cleanup_staging_root(operation_id)
                ).as_posix(),
                "cleanupControlPath": self._managed_relative(
                    self._cleanup_control_path(operation_id)
                ).as_posix(),
                "cleanupResultPath": self._managed_relative(
                    self._cleanup_result_path(operation_id)
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
                "adjustedMetrics": smoke_result.lineage,
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
        market_fd = self._managed().open_dir(self._managed_relative(market_path))
        try:
            return {
                "directory": self._directory_identity_evidence(market_fd),
                "payload": self._market_payload_identity(market_fd),
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

    def _delete_held_promotion_artifacts(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        artifact_root: Path | None = None,
    ) -> None:
        holding_relative = self._managed_relative(
            artifact_root or preparation.holding_root
        )
        holding_fd = self._managed().open_dir(holding_relative)
        try:
            if (
                self._directory_identity_evidence(holding_fd)
                != preparation.holding_directory_identity
            ):
                raise CutoverSafetyError("Promotion holding directory identity changed")
            current_artifacts = self._held_artifacts_evidence(holding_fd)
            if current_artifacts != preparation.detached_artifacts:
                raise CutoverSafetyError("Promotion held artifact identity changed")
            with ManagedRootFd(Path("."), os.dup(holding_fd)) as holding:
                for artifact in preparation.detached_artifacts:
                    if (
                        self._held_artifact_evidence(holding_fd, artifact.name)
                        != artifact
                    ):
                        raise CutoverSafetyError(
                            "Promotion held artifact identity changed before deletion"
                        )
                    if artifact.kind == "directory":
                        holding.remove_tree(Path(artifact.name))
                    elif artifact.kind == "regular":
                        os.unlink(artifact.name, dir_fd=holding_fd)
                    else:
                        raise CutoverSafetyError("Promotion held artifact kind is invalid")
            os.fsync(holding_fd)
            if os.listdir(holding_fd):
                raise CutoverSafetyError("Promotion holding cleanup is incomplete")
        finally:
            os.close(holding_fd)

    def _cleanup_staging_root(self, operation_id: str) -> Path:
        return self.operations_root / "cleanup-staging" / operation_id

    def _cleanup_result_path(self, operation_id: str) -> Path:
        return self.operations_root / "cleanup-results" / f"{operation_id}.json"

    def _cleanup_control_path(self, operation_id: str) -> Path:
        return self.operations_root / "cleanup-controls" / f"{operation_id}.json"

    def _stage_held_promotion_artifacts(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        operation_id: str,
    ) -> Path:
        staging = self._cleanup_staging_root(operation_id)
        self._prepare_managed_directory(staging.parent, exist_ok=True)
        self._assert_managed_target_absent(staging)
        self._secure_rename(preparation.holding_root, staging)
        staging_fd = self._managed().open_dir(self._managed_relative(staging))
        try:
            if (
                self._directory_identity_evidence(staging_fd)
                != preparation.holding_directory_identity
                or self._held_artifacts_evidence(staging_fd)
                != preparation.detached_artifacts
            ):
                raise CutoverSafetyError("Promotion cleanup staging identity mismatch")
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
            raw = self._managed().read_bytes(self._managed_relative(result))
        except FileNotFoundError:
            raw = None
        if raw is not None:
            try:
                actual = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise CutoverSafetyError("Promotion cleanup result is invalid") from exc
            if actual != expected:
                raise CutoverSafetyError("Promotion cleanup result identity mismatch")
            try:
                self._managed().stat(self._managed_relative(staging))
            except FileNotFoundError:
                return
            raise CutoverSafetyError(
                "Promotion cleanup result exists while staging remains"
            )
        control = self._cleanup_control_path(operation_id)
        control_payload = {
            **expected,
            "kind": "cleanup_intent",
        }
        try:
            control_raw = self._managed().read_bytes(
                self._managed_relative(control)
            )
        except FileNotFoundError:
            staging_fd = self._managed().open_dir(
                self._managed_relative(staging)
            )
            try:
                if (
                    self._directory_identity_evidence(staging_fd)
                    != preparation.holding_directory_identity
                    or self._held_artifacts_evidence(staging_fd)
                    != preparation.detached_artifacts
                ):
                    raise CutoverSafetyError(
                        "Promotion cleanup staging identity mismatch"
                    )
            finally:
                os.close(staging_fd)
            control_root = control.parent
            self._prepare_managed_directory(control_root, exist_ok=True)
            control_fd = self._managed().open_regular(
                self._managed_relative(control),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            try:
                self._write_all(
                    control_fd,
                    PromotionJournal._canonical_json(control_payload),
                )
                os.fsync(control_fd)
            finally:
                os.close(control_fd)
            self._managed().fsync_dir(self._managed_relative(control_root))
            self._promotion_boundary_hook("cleanup_intent_fsynced")
        else:
            try:
                actual_control = json.loads(control_raw)
            except json.JSONDecodeError as exc:
                raise CutoverSafetyError(
                    "Promotion cleanup control is invalid"
                ) from exc
            if actual_control != control_payload:
                raise CutoverSafetyError("Promotion cleanup control identity mismatch")
        try:
            staging_fd = self._managed().open_dir(
                self._managed_relative(staging)
            )
        except FileNotFoundError:
            staging_fd = None
        if staging_fd is not None:
            try:
                if (
                    self._directory_identity_evidence(staging_fd)
                    != preparation.holding_directory_identity
                ):
                    raise CutoverSafetyError(
                        "Promotion cleanup staging directory changed"
                    )
                expected_by_name = {
                    artifact.name: artifact
                    for artifact in preparation.detached_artifacts
                }
                current = self._held_artifacts_evidence(staging_fd)
                if any(
                    artifact.name not in expected_by_name
                    or artifact != expected_by_name[artifact.name]
                    for artifact in current
                ):
                    raise CutoverSafetyError(
                        "Promotion cleanup staging contains ambiguous artifacts"
                    )
                with ManagedRootFd(Path("."), os.dup(staging_fd)) as managed:
                    for artifact in current:
                        if artifact.kind == "directory":
                            managed.remove_tree(Path(artifact.name))
                        else:
                            os.unlink(artifact.name, dir_fd=staging_fd)
                os.fsync(staging_fd)
            finally:
                os.close(staging_fd)
            parent_fd, name = self._managed().open_parent(
                self._managed_relative(staging)
            )
            try:
                os.rmdir(name, dir_fd=parent_fd)
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        self._promotion_boundary_hook("cleanup_artifacts_deleted")
        result_root = result.parent
        self._prepare_managed_directory(result_root, exist_ok=True)
        fd = self._managed().open_regular(
            self._managed_relative(result),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        try:
            self._write_all(fd, PromotionJournal._canonical_json(expected))
            os.fsync(fd)
        finally:
            os.close(fd)
        self._managed().fsync_dir(self._managed_relative(result_root))

    def _write_source_consumed_marker(
        self,
        *,
        retained_report_id: str,
        operation_id: str,
        promotion_report_sha256: str,
    ) -> Path:
        consumed_root = self.operations_root / "consumed"
        self._prepare_managed_directory(consumed_root, exist_ok=True)
        marker = consumed_root / f"{retained_report_id}.json"
        marker_fd = self._managed().open_regular(
            self._managed_relative(marker),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        try:
            payload = (
                PromotionJournal._canonical_json(
                    {
                        "schemaVersion": 1,
                        "retainedReportId": retained_report_id,
                        "operationId": operation_id,
                        "promotionReportSha256": promotion_report_sha256,
                    }
                )
                + b"\n"
            )
            self._write_all(marker_fd, payload)
            os.fsync(marker_fd)
        finally:
            os.close(marker_fd)
        self._managed().fsync_dir(self._managed_relative(consumed_root))
        return marker

    def _promotion_location_if_present(
        self,
        market_path: Path,
    ) -> dict[str, object] | None:
        try:
            self._managed().stat(self._managed_relative(market_path))
        except FileNotFoundError:
            return None
        return self._payload_location_identity(market_path)

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
    ) -> None:
        candidates = (
            preparation.holding_root,
            self._cleanup_staging_root(
                preparation.holding_root.name
            ),
        )
        opened: list[tuple[Path, int]] = []
        for candidate in candidates:
            try:
                opened.append(
                    (
                        candidate,
                        self._managed().open_dir(
                            self._managed_relative(candidate)
                        ),
                    )
                )
            except FileNotFoundError:
                continue
        if len(opened) > 1:
            for _path, fd in opened:
                os.close(fd)
            raise CutoverSafetyError(
                "Promotion artifact staging identity is ambiguous"
            )
        artifact_root: Path | None
        holding_fd: int | None
        if opened:
            artifact_root, holding_fd = opened[0]
        else:
            artifact_root, holding_fd = None, None
        retained_market = preparation.eligibility.retained_root / "market-timeseries"
        retained_fd = self._managed().open_dir(
            self._managed_relative(retained_market)
        )
        try:
            if holding_fd is not None and (
                self._directory_identity_evidence(holding_fd)
                != preparation.holding_directory_identity
            ):
                raise CutoverSafetyError("Promotion holding directory identity changed")
            staged = (
                self._held_artifacts_evidence(holding_fd)
                if holding_fd is not None
                else ()
            )
            expected_by_name = {
                artifact.name: artifact
                for artifact in preparation.detached_artifacts
            }
            staged_by_name = {artifact.name: artifact for artifact in staged}
            if any(
                artifact.name not in expected_by_name
                or artifact != expected_by_name[artifact.name]
                for artifact in staged
            ):
                raise CutoverSafetyError("Promotion held artifact identity changed")
            retained_names = set(os.listdir(retained_fd))
            canonical_names = {"market.duckdb", "parquet"}
            retained_artifact_names = retained_names - canonical_names
            expected_names = set(expected_by_name)
            staged_names = set(staged_by_name)
            rollback_runtime_name = (
                f".cutover-runtime-{preparation.holding_root.name}"
            )
            rollback_runtime_present = rollback_runtime_name in retained_artifact_names
            if rollback_runtime_present:
                runtime_stat = os.stat(
                    rollback_runtime_name,
                    dir_fd=retained_fd,
                    follow_symlinks=False,
                )
                if not stat.S_ISDIR(runtime_stat.st_mode):
                    raise CutoverSafetyError(
                        "Promotion rollback runtime identity changed"
                    )
            retained_expected_names = retained_artifact_names - {
                rollback_runtime_name
            }
            if (
                canonical_names - retained_names
                or retained_expected_names - expected_names
                or staged_names | retained_expected_names != expected_names
                or staged_names & retained_expected_names
            ):
                raise CutoverSafetyError(
                    "Promotion artifact set is incomplete or ambiguous during restoration"
                )
            restore_from_staging: list[DetachedArtifactEvidence] = []
            for artifact in preparation.detached_artifacts:
                try:
                    retained_identity = self._held_artifact_evidence(
                        retained_fd, artifact.name
                    )
                except FileNotFoundError:
                    retained_identity = None
                staged_identity = staged_by_name.get(artifact.name)
                if (retained_identity is None) == (staged_identity is None):
                    raise CutoverSafetyError(
                        "Promotion artifact is duplicated or missing during restoration"
                    )
                if retained_identity is not None:
                    if retained_identity != artifact:
                        raise CutoverSafetyError(
                            "Promotion restored artifact identity changed"
                        )
                    continue
                assert staged_identity == artifact
                restore_from_staging.append(artifact)
            if rollback_runtime_present:
                self._remove_market_runtime(retained_fd, rollback_runtime_name)
                os.fsync(retained_fd)
            for artifact in restore_from_staging:
                assert holding_fd is not None
                _rename_exclusive_at(
                    holding_fd,
                    artifact.name,
                    retained_fd,
                    artifact.name,
                )
                if self._held_artifact_evidence(retained_fd, artifact.name) != artifact:
                    raise CutoverSafetyError(
                        "Promotion runtime restoration identity changed"
                    )
                os.fsync(holding_fd)
                os.fsync(retained_fd)
                self._promotion_boundary_hook(
                    f"rollback_artifact_moved:{artifact.name}"
                )
            self._promotion_boundary_hook("rollback_artifacts_reconciled")
            if holding_fd is not None and os.listdir(holding_fd):
                raise CutoverSafetyError("Promotion holding restoration is incomplete")
        finally:
            os.close(retained_fd)
            if holding_fd is not None:
                os.close(holding_fd)
        if artifact_root is None:
            return
        parent_fd, name = self._managed().open_parent(
            self._managed_relative(artifact_root)
        )
        try:
            os.rmdir(name, dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    def _remove_incomplete_consumed_marker(
        self,
        *,
        retained_report_id: str,
        operation_id: str,
    ) -> None:
        marker = self.operations_root / "consumed" / f"{retained_report_id}.json"
        try:
            raw = self._managed().read_bytes(self._managed_relative(marker))
        except FileNotFoundError:
            return
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CutoverSafetyError(
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
            raise CutoverSafetyError(
                "Incomplete promotion consumed marker identity mismatch"
            )
        parent_fd, name = self._managed().open_parent(
            self._managed_relative(marker)
        )
        try:
            os.unlink(name, dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)

    def _fence_promotion_leases(self) -> None:
        for lease in (self._active_lease, self._retained_lease):
            if lease is not None:
                lease.unlock_on_release = False
                lease.owns_fd = False

    def _atomic_exchange_parent_identities(
        self,
        left: Path,
        right: Path,
    ) -> tuple[tuple[int, int], tuple[int, int]]:
        identities: list[tuple[int, int]] = []
        for relative in (left, right):
            parent_fd, _name = self._managed().open_parent(relative)
            try:
                parent_stat = os.fstat(parent_fd)
                if not stat.S_ISDIR(parent_stat.st_mode):
                    raise CutoverSafetyError(
                        "Atomic exchange parent is not a directory"
                    )
                identities.append((parent_stat.st_dev, parent_stat.st_ino))
            finally:
                os.close(parent_fd)
        return identities[0], identities[1]

    def _prove_atomic_exchange_parent_durability(
        self,
        left: Path,
        right: Path,
        *,
        expected: tuple[tuple[int, int], tuple[int, int]],
    ) -> None:
        if self._active_lease is None or self._retained_lease is None:
            raise CutoverSafetyError(
                "Promotion exchange durability requires both held leases"
            )
        parent_fds: list[int] = []
        failures: list[Exception] = []
        try:
            for relative, expected_identity in zip((left, right), expected, strict=True):
                try:
                    parent_fd, _name = self._managed().open_parent(relative)
                    parent_fds.append(parent_fd)
                    parent_stat = os.fstat(parent_fd)
                    if (
                        not stat.S_ISDIR(parent_stat.st_mode)
                        or (parent_stat.st_dev, parent_stat.st_ino)
                        != expected_identity
                    ):
                        raise CutoverSafetyError(
                            "Atomic exchange parent identity changed during durability proof"
                        )
                except Exception as exc:
                    failures.append(exc)
            if not failures:
                for parent_fd in parent_fds:
                    try:
                        os.fsync(parent_fd)
                    except OSError as exc:
                        failures.append(exc)
        finally:
            for parent_fd in parent_fds:
                os.close(parent_fd)
        if failures:
            self._fence_promotion_leases()
            raise CutoverSafetyError(
                "Promotion exchange durability could not be proven; both leases remain fenced"
            ) from failures[0]

    def _rollback_retained_promotion(
        self,
        context: RetainedPromotionContext,
        *,
        processes_joined: bool,
    ) -> None:
        """Restore v3 exactly, or durably fence both roots when cleanup is unsafe."""

        if self._active_lease is None or self._retained_lease is None:
            raise CutoverSafetyError("Both promotion leases are required for rollback")
        preparation = context.preparation
        journal = context.journal
        records = journal.read_validated()
        if not records:
            raise CutoverSafetyError("Promotion rollback journal is empty")
        base = records[-1].identities
        if base.detached_artifacts != tuple(
            artifact.to_mapping() for artifact in preparation.detached_artifacts
        ):
            raise CutoverSafetyError(
                "Promotion rollback detached artifact evidence mismatch"
            )
        retained_market = preparation.eligibility.retained_root / "market-timeseries"
        quarantine = self.operations_root / "quarantine" / journal.operation_id
        active = self._market_location_identity(self._active_lease_fd_root())
        retained = self._promotion_location_if_present(retained_market)
        quarantined = self._promotion_location_if_present(quarantine)
        if records[-1].state is PromotionState.VALIDATED:
            if not processes_joined:
                raise CutoverSafetyError(
                    "Validated promotion cannot defer without child ownership"
                )
            if not (
                self._location_matches(
                    active,
                    directory=base.active_before_directory,
                    payload=base.active_before_payload,
                )
                and self._location_matches(
                    retained,
                    directory=base.retained_v4_directory,
                    payload=base.retained_v4_payload,
                )
                and quarantined is None
            ):
                raise CutoverSafetyError(
                    "Validated promotion filesystem identity is ambiguous"
                )
            self._restore_held_promotion_artifacts(preparation)
            retained = self._promotion_location_if_present(retained_market)
            if not self._location_matches(
                retained,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            ):
                raise CutoverSafetyError(
                    "Validated promotion retained restoration is incomplete"
                )
            rolled_back = self._promotion_identities(
                base,
                active_current=active,
                retained_current=retained,
                quarantine_current=None,
                holding_current=None,
            )
            self._append_preparation_state(
                journal, PromotionState.ROLLED_BACK, rolled_back
            )
            return
        staging_candidates = (
            preparation.holding_root,
            self._cleanup_staging_root(journal.operation_id),
        )
        staging_fds: list[int] = []
        for candidate in staging_candidates:
            try:
                staging_fds.append(
                    self._managed().open_dir(self._managed_relative(candidate))
                )
            except FileNotFoundError:
                continue
        if len(staging_fds) > 1:
            for fd in staging_fds:
                os.close(fd)
            raise CutoverSafetyError("Promotion artifact staging is ambiguous")
        if staging_fds:
            holding_fd = staging_fds[0]
            try:
                if (
                    self._directory_identity_evidence(holding_fd)
                    != preparation.holding_directory_identity
                ):
                    raise CutoverSafetyError(
                        "Promotion holding directory identity changed"
                    )
                holding = base.holding_current
            finally:
                os.close(holding_fd)
        else:
            holding = None

        if not processes_joined:
            deferred = self._promotion_identities(
                base,
                active_current=active,
                retained_current=retained,
                quarantine_current=quarantined,
                holding_current=holding,
            )
            self._append_preparation_state(
                journal, PromotionState.ROLLBACK_DEFERRED, deferred
            )
            for lease in (self._active_lease, self._retained_lease):
                lease.unlock_on_release = False
                lease.owns_fd = False
            raise CutoverSafetyError(
                "Retained promotion rollback deferred with both leases held"
            )

        if records[-1].state is PromotionState.EXCHANGED_BACK:
            if base.rollback_mode == "atomic_exchange":
                valid_layout = (
                    self._location_matches(
                        active,
                        directory=base.active_before_directory,
                        payload=base.active_before_payload,
                    )
                    and self._location_matches(
                        retained,
                        directory=base.retained_v4_directory,
                        payload=base.retained_v4_payload,
                    )
                    and quarantined is None
                )
            elif base.rollback_mode == "backup_restore":
                valid_layout = (
                    active == base.active_current
                    and retained == base.retained_current
                    and quarantined == base.quarantine_current
                    and quarantined is not None
                    and quarantined["directory"] == base.active_before_directory
                    and self._payload_manifest_entries(
                        cast(dict[str, object], active["payload"])
                    )
                    == self._payload_manifest_entries(base.active_before_payload)
                    and self._location_matches(
                        retained,
                        directory=base.retained_v4_directory,
                        payload=base.retained_v4_payload,
                    )
                )
            else:
                raise CutoverSafetyError(
                    "EXCHANGED_BACK rollback mode is missing or invalid"
                )
            if not valid_layout:
                raise CutoverSafetyError(
                    "EXCHANGED_BACK promotion filesystem identity mismatch"
                )
            self._restore_held_promotion_artifacts(preparation)
            self._remove_incomplete_consumed_marker(
                retained_report_id=preparation.eligibility.retained_report_id,
                operation_id=journal.operation_id,
            )
            rolled_back = self._promotion_identities(
                base,
                active_current=active,
                retained_current=retained,
                quarantine_current=quarantined,
                holding_current=None,
            )
            self._append_preparation_state(
                journal, PromotionState.ROLLED_BACK, rolled_back
            )
            return

        active_is_v3 = self._location_matches(
            active,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        )
        active_is_v4 = self._location_matches(
            active,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        )
        retained_is_v3 = self._location_matches(
            retained,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        )
        retained_is_v4 = self._location_matches(
            retained,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        )
        quarantine_is_v3 = self._location_matches(
            quarantined,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        )
        layout_exchangeable = active_is_v4 and (
            (retained_is_v3 and quarantined is None)
            or (quarantine_is_v3 and retained is None)
        )
        layout_already_restored = (
            active_is_v3 and retained_is_v4 and quarantined is None
        )
        if not (layout_exchangeable or layout_already_restored):
            raise CutoverSafetyError(
                "Promotion rollback filesystem identity is ambiguous"
            )
        if layout_already_restored and records[-1].state in {
            PromotionState.VALIDATED,
            PromotionState.RUNTIMES_DETACHED,
            PromotionState.PREPARED,
        }:
            self._restore_held_promotion_artifacts(preparation)
            self._remove_incomplete_consumed_marker(
                retained_report_id=preparation.eligibility.retained_report_id,
                operation_id=journal.operation_id,
            )
            rolled_back = self._promotion_identities(
                base,
                active_current=active,
                retained_current=retained,
                quarantine_current=None,
                holding_current=None,
            )
            self._append_preparation_state(
                journal, PromotionState.ROLLED_BACK, rolled_back
            )
            return
        backup_fallback = False
        exchange_error: Exception | None = None
        if layout_exchangeable:
            exchange_target = retained_market if retained_is_v3 else quarantine
            exchange_target_relative = self._managed_relative(exchange_target)
            exchange_parent_identities = self._atomic_exchange_parent_identities(
                Path("market-timeseries"), exchange_target_relative
            )
            try:
                self.atomic_exchange.exchange(
                    self._managed(),
                    Path("market-timeseries"),
                    exchange_target_relative,
                )
            except Exception as exc:
                exchange_error = exc
            active = self._market_location_identity(self._active_lease_fd_root())
            retained = self._promotion_location_if_present(retained_market)
            quarantined = self._promotion_location_if_present(quarantine)
            active_is_v3 = self._location_matches(
                active,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            active_is_v4 = self._location_matches(
                active,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            retained_is_v3 = self._location_matches(
                retained,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            retained_is_v4 = self._location_matches(
                retained,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            quarantine_is_v3 = self._location_matches(
                quarantined,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            )
            quarantine_is_v4 = self._location_matches(
                quarantined,
                directory=base.retained_v4_directory,
                payload=base.retained_v4_payload,
            )
            if active_is_v3 and retained_is_v4 and quarantined is None:
                if exchange_error is not None:
                    self._prove_atomic_exchange_parent_durability(
                        Path("market-timeseries"),
                        exchange_target_relative,
                        expected=exchange_parent_identities,
                    )
                exchange_error = None
            elif active_is_v3 and quarantine_is_v4 and retained is None:
                if exchange_error is not None:
                    self._prove_atomic_exchange_parent_durability(
                        Path("market-timeseries"),
                        exchange_target_relative,
                        expected=exchange_parent_identities,
                    )
                self._secure_rename(quarantine, retained_market)
                exchange_error = None
            elif not (
                exchange_error is not None
                and active_is_v4
                and (
                    (retained_is_v3 and quarantined is None)
                    or (quarantine_is_v3 and retained is None)
                )
            ):
                raise CutoverSafetyError(
                    "Promotion exchange-back result is ambiguous"
                )

        if exchange_error is not None:
            try:
                self._verified_backup_evidence(
                    preparation.backup_id,
                    expected_payload=base.active_before_payload,
                )
                if retained_is_v3:
                    self._prepare_managed_directory(quarantine.parent, exist_ok=True)
                    self._assert_managed_target_absent(quarantine)
                    self._secure_rename(retained_market, quarantine)
                restored = self._restore_under_lease(preparation.backup_id)
                if restored.quarantine_path is None:
                    raise CutoverSafetyError(
                        "Promotion backup fallback did not retain displaced v4"
                    )
                displaced = self.data_root / restored.quarantine_path
                displaced_location = self._payload_location_identity(displaced)
                if displaced_location["payload"] != base.retained_v4_payload:
                    raise CutoverSafetyError(
                        "Promotion backup fallback displaced identity mismatch"
                    )
                if self._promotion_location_if_present(retained_market) is not None:
                    raise CutoverSafetyError(
                        "Promotion backup fallback retained destination is occupied"
                    )
                self._secure_rename(displaced, retained_market)
                backup_fallback = True
            except Exception as restore_error:
                raise CutoverSafetyError(
                    "Terminal promotion recovery failure: exchange-back and "
                    "verified backup restore both failed"
                ) from restore_error

        active = self._market_location_identity(self._active_lease_fd_root())
        retained = self._payload_location_identity(retained_market)
        quarantined = self._promotion_location_if_present(quarantine)
        active_payload_valid = (
            self._payload_manifest_entries(cast(dict[str, object], active["payload"]))
            == self._payload_manifest_entries(base.active_before_payload)
            if backup_fallback
            else active["payload"] == base.active_before_payload
        )
        if not active_payload_valid or not self._location_matches(
            retained,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        ):
            raise CutoverSafetyError("Promotion rollback identity verification failed")
        exchanged_back = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=quarantined if backup_fallback else None,
            holding_current=holding,
            rollback_mode=(
                "backup_restore" if backup_fallback else "atomic_exchange"
            ),
        )
        self._append_preparation_state(
            journal, PromotionState.EXCHANGED_BACK, exchanged_back
        )
        self._promotion_boundary_hook("exchanged_back_journaled")
        self._restore_held_promotion_artifacts(preparation)
        self._remove_incomplete_consumed_marker(
            retained_report_id=preparation.eligibility.retained_report_id,
            operation_id=journal.operation_id,
        )
        retained = self._payload_location_identity(retained_market)
        rolled_back = self._promotion_identities(
            base,
            active_current=active,
            retained_current=retained,
            quarantine_current=quarantined if backup_fallback else None,
            holding_current=None,
            rollback_mode=(
                "backup_restore" if backup_fallback else "atomic_exchange"
            ),
        )
        self._append_preparation_state(
            journal, PromotionState.ROLLED_BACK, rolled_back
        )

    def _validate_committed_promotion_recovery(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        base: PromotionIdentityEvidence,
        preparation: RetainedPromotionPreparation,
    ) -> OperationResult:
        report_path = self.operations_root / "reports" / report_id / "report.json"
        try:
            report = json.loads(
                self._managed().read_bytes(
                    self._managed_relative(report_path)
                )
            )
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError(
                "Committed promotion report is missing or invalid"
            ) from exc
        if not isinstance(report, dict) or set(report) != self._PROMOTION_REPORT_KEYS:
            raise CutoverSafetyError("Committed promotion report contract is invalid")
        quarantine_path = self.operations_root / "quarantine" / report_id
        expected_quarantine_value = self._managed_relative(
            quarantine_path
        ).as_posix()
        if report.get("quarantinePath") != expected_quarantine_value:
            raise CutoverSafetyError("Committed promotion quarantine is invalid")
        report_sha256 = self._sha256(report_path)
        if base.promotion_report_sha256 != report_sha256:
            raise CutoverSafetyError("Committed promotion report SHA mismatch")
        active = self._market_location_identity(self._active_lease_fd_root())
        if not self._location_matches(
            active,
            directory=base.retained_v4_directory,
            payload=base.retained_v4_payload,
        ):
            raise CutoverSafetyError("Committed promotion active identity mismatch")
        quarantine = self._payload_location_identity(quarantine_path)
        if not self._location_matches(
            quarantine,
            directory=base.active_before_directory,
            payload=base.active_before_payload,
        ):
            raise CutoverSafetyError("Committed promotion quarantine identity mismatch")
        try:
            semantic = cast(dict[str, object], report["semanticSmoke"])
            smoke_result = SmokeResult(
                schema_version=cast(int, semantic["schemaVersion"]),
                adjustment_mode=cast(str, semantic["stockPriceAdjustmentMode"]),
                checks=tuple(cast(list[str], semantic["checks"])),
                api_paths=tuple(cast(list[str], report["apiChecks"])),
                lineage=cast(dict[str, int], semantic["adjustedMetrics"]),
            )
            expectation = self._retained_promotion_report_expectation(
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
            raise CutoverSafetyError(
                "Committed promotion report contract is invalid"
            ) from exc
        if not self._retained_promotion_report_contract_valid(
            report,
            expectation=expectation,
        ):
            raise CutoverSafetyError("Committed promotion report contract is invalid")
        consumed = report.get("sourceConsumed")
        if not isinstance(consumed, dict):
            raise CutoverSafetyError("Committed promotion consumed evidence is invalid")
        marker_path = (
            self.operations_root / "consumed" / f"{retained_report_id}.json"
        )
        expected_marker_value = self._managed_relative(marker_path).as_posix()
        if consumed.get("markerPath") != expected_marker_value:
            raise CutoverSafetyError("Committed promotion consumed marker is invalid")
        try:
            marker_bytes = self._managed().read_bytes(
                self._managed_relative(marker_path)
            )
            marker = json.loads(marker_bytes)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError(
                "Committed promotion consumed marker is missing or invalid"
            ) from exc
        if marker != {
            "schemaVersion": 1,
            "retainedReportId": retained_report_id,
            "operationId": report_id,
            "promotionReportSha256": report_sha256,
        }:
            raise CutoverSafetyError("Committed promotion consumed marker mismatch")
        cleanup = report.get("runtimeCleanup")
        if not isinstance(cleanup, dict) or not (
            cleanup.get("cleanupStagingPath")
            == self._managed_relative(
                self._cleanup_staging_root(report_id)
            ).as_posix()
            and cleanup.get("cleanupControlPath")
            == self._managed_relative(
                self._cleanup_control_path(report_id)
            ).as_posix()
            and cleanup.get("cleanupResultPath")
            == self._managed_relative(
                self._cleanup_result_path(report_id)
            ).as_posix()
            and cleanup.get("cleanupDisposition") == "pending_post_commit"
            and cleanup.get("removedArtifacts") == []
        ):
            raise CutoverSafetyError("Committed promotion cleanup evidence mismatch")
        self._complete_committed_promotion_cleanup(
            preparation,
            operation_id=report_id,
            report_sha256=report_sha256,
        )
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    def _validate_rolled_back_promotion_recovery(
        self,
        *,
        report_id: str,
        retained_root: Path,
        base: PromotionIdentityEvidence,
    ) -> None:
        active = self._market_location_identity(self._active_lease_fd_root())
        retained = self._payload_location_identity(
            retained_root / "market-timeseries"
        )
        if active != base.active_current or retained != base.retained_current:
            raise CutoverSafetyError("Rolled-back promotion identity mismatch")
        quarantine_path = self.operations_root / "quarantine" / report_id
        quarantine = self._promotion_location_if_present(quarantine_path)
        if base.rollback_mode == "backup_restore":
            if (
                quarantine != base.quarantine_current
                or quarantine is None
                or quarantine["directory"] != base.active_before_directory
                or self._payload_manifest_entries(
                    cast(dict[str, object], active["payload"])
                )
                != self._payload_manifest_entries(base.active_before_payload)
            ):
                raise CutoverSafetyError(
                    "Rolled-back backup restore evidence mismatch"
                )
        elif base.rollback_mode == "atomic_exchange":
            if quarantine is not None or not self._location_matches(
                active,
                directory=base.active_before_directory,
                payload=base.active_before_payload,
            ):
                raise CutoverSafetyError(
                    "Rolled-back atomic exchange evidence mismatch"
                )
        elif base.rollback_mode is not None:
            raise CutoverSafetyError("Rolled-back promotion mode is invalid")
        for artifact_root in (
            self.operations_root / "holding" / report_id,
            self._cleanup_staging_root(report_id),
        ):
            try:
                self._managed().stat(self._managed_relative(artifact_root))
            except FileNotFoundError:
                continue
            raise CutoverSafetyError("Rolled-back artifact staging still exists")

    def _recover_retained_promotion(
        self,
        report_id: str,
        *,
        retained_report_id: str,
        backup_id: str,
    ) -> OperationResult | None:
        """Authorize and recover only one exact same-ID promotion attempt."""

        report_id = self._validate_id(report_id, label="report")
        retained_report_id = self._validate_id(
            retained_report_id, label="retained report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        with self._existing_exclusive_operation():
            retained_report, retained_sha256, _retained_stat = (
                self._promotion_report_snapshot(retained_report_id)
            )
            source_id_value = retained_report.get("sourceRehearsalReportId")
            if not isinstance(source_id_value, str):
                raise CutoverSafetyError("Retained report identity is invalid")
            source_report_id = self._validate_id(
                source_id_value, label="source rehearsal report"
            )
            retained_root = self._retained_rehearsal_root(source_report_id)
            with MarketOperationLease.acquire_existing(
                retained_root, exclusive=True
            ) as retained_lease:
                self._retained_lease = retained_lease
                try:
                    journal = PromotionJournal(
                        self._managed(), report_id, now=self.now
                    )
                    attempt_id = journal.recovery_attempt_id()
                    recovered = journal.recover(attempt_id)
                    if recovered.status is PromotionAppendStatus.INDETERMINATE:
                        raise CutoverSafetyError(
                            "Promotion journal same-attempt recovery is indeterminate"
                        )
                    records = journal.read_validated()
                    if not records:
                        raise CutoverSafetyError(
                            "Promotion recovery journal has no committed evidence"
                        )
                    last = records[-1]
                    base = last.identities
                    source_report, source_sha256, _source_stat = (
                        self._promotion_report_snapshot(source_report_id)
                    )
                    recorded_source = retained_report.get(
                        "sourceMarketIdentityBefore"
                    )
                    if (
                        retained_report.get("reportId") != retained_report_id
                        or retained_report.get("status") != "passed"
                        or retained_report.get("sourceRehearsalReportId")
                        != source_report_id
                        or recorded_source != base.retained_v4_payload
                        or source_report.get("reportId") != source_report_id
                    ):
                        raise CutoverSafetyError(
                            "Promotion recovery retained report identity mismatch"
                        )
                    (
                        backup_manifest_sha256,
                        backup_file_set_sha256,
                        backup_payload_identity,
                    ) = self._verified_backup_evidence(
                        backup_id,
                        expected_payload=base.active_before_payload,
                    )
                    if (
                        backup_manifest_sha256 != base.backup_manifest_sha256
                        or backup_file_set_sha256 != base.backup_file_set_sha256
                    ):
                        raise CutoverSafetyError(
                            "Promotion recovery backup identity mismatch"
                        )
                    if last.state is PromotionState.ROLLED_BACK:
                        self._validate_rolled_back_promotion_recovery(
                            report_id=report_id,
                            retained_root=retained_root,
                            base=base,
                        )
                        return None
                    holding_root = self.operations_root / "holding" / report_id
                    holding_location = last.identities.holding_current
                    recorded_holding = next(
                        (
                            record.identities.holding_current
                            for record in reversed(records)
                            if record.identities.holding_current is not None
                        ),
                        None,
                    )
                    detached_artifacts = tuple(
                        DetachedArtifactEvidence(
                            name=cast(str, artifact["name"]),
                            kind=cast(str, artifact["kind"]),
                            identity=cast(dict[str, object], artifact["identity"]),
                            directories=cast(
                                dict[str, dict[str, int]], artifact["directories"]
                            ),
                            files=cast(
                                dict[str, dict[str, object]], artifact["files"]
                            ),
                        )
                        for artifact in base.detached_artifacts
                    )
                    if holding_location is None:
                        try:
                            validated_holding_fd = self._managed().open_dir(
                                self._managed_relative(holding_root)
                            )
                        except FileNotFoundError:
                            holding_directory = (
                                cast(dict[str, int], recorded_holding["directory"])
                                if recorded_holding is not None
                                else {"device": 0, "inode": 0}
                            )
                        else:
                            try:
                                current_artifacts = self._held_artifacts_evidence(
                                    validated_holding_fd
                                )
                                expected_by_name = {
                                    artifact.name: artifact
                                    for artifact in detached_artifacts
                                }
                                if any(
                                    artifact.name not in expected_by_name
                                    or artifact != expected_by_name[artifact.name]
                                    for artifact in current_artifacts
                                ):
                                    raise CutoverSafetyError(
                                        "Promotion recovery held artifact identity mismatch"
                                    )
                                holding_directory = (
                                    self._directory_identity_evidence(
                                        validated_holding_fd
                                    )
                                )
                            finally:
                                os.close(validated_holding_fd)
                    else:
                        holding_directory = cast(
                            dict[str, int], holding_location["directory"]
                        )
                        if last.state is not PromotionState.COMMITTED:
                            candidates = (
                                holding_root,
                                self._cleanup_staging_root(report_id),
                            )
                            opened: list[int] = []
                            for candidate in candidates:
                                try:
                                    opened.append(
                                        self._managed().open_dir(
                                            self._managed_relative(candidate)
                                        )
                                    )
                                except FileNotFoundError:
                                    continue
                            partial_recovery_state = last.state in {
                                PromotionState.RUNTIMES_DETACHED,
                                PromotionState.PREPARED,
                                PromotionState.EXCHANGED_BACK,
                                PromotionState.ROLLBACK_DEFERRED,
                            }
                            if len(opened) > 1 or (
                                not opened and not partial_recovery_state
                            ):
                                for fd in opened:
                                    os.close(fd)
                                raise CutoverSafetyError(
                                    "Promotion recovery artifact staging is missing or ambiguous"
                                )
                            if opened:
                                holding_fd = opened[0]
                                try:
                                    current_artifacts = (
                                        self._held_artifacts_evidence(holding_fd)
                                    )
                                    expected_by_name = {
                                        artifact.name: artifact
                                        for artifact in detached_artifacts
                                    }
                                    if (
                                        self._directory_identity_evidence(holding_fd)
                                        != holding_directory
                                        or any(
                                            artifact.name not in expected_by_name
                                            or artifact
                                            != expected_by_name[artifact.name]
                                            for artifact in current_artifacts
                                        )
                                        or (
                                            not partial_recovery_state
                                            and current_artifacts
                                            != detached_artifacts
                                        )
                                    ):
                                        raise CutoverSafetyError(
                                            "Promotion recovery held artifact identity mismatch"
                                        )
                                finally:
                                    os.close(holding_fd)
                    eligibility = RetainedPromotionEligibility(
                        retained_report_id=retained_report_id,
                        retained_report_sha256=retained_sha256,
                        source_report_id=source_report_id,
                        source_report_sha256=source_sha256,
                        retained_root=retained_root,
                        source_market_identity=base.retained_v4_payload,
                        active_market_identity=base.active_before_payload,
                        target_root_fingerprint=self.root_fingerprint(self.data_root),
                        configuration_fingerprint=self.configuration_fingerprint(
                            self.data_root
                        ),
                    )
                    preparation = RetainedPromotionPreparation(
                        eligibility=eligibility,
                        backup_id=backup_id,
                        backup_manifest_sha256=backup_manifest_sha256,
                        backup_file_set_sha256=backup_file_set_sha256,
                        backup_payload_identity=backup_payload_identity,
                        holding_root=holding_root,
                        holding_directory_identity=holding_directory,
                        detached_runtime_names=base.detached_runtime_names,
                        detached_artifacts=detached_artifacts,
                    )
                    if last.state is PromotionState.COMMITTED:
                        return self._validate_committed_promotion_recovery(
                            report_id=report_id,
                            retained_report_id=retained_report_id,
                            backup_id=backup_id,
                            base=base,
                            preparation=preparation,
                        )
                    self._rollback_retained_promotion(
                        RetainedPromotionContext(preparation, journal),
                        processes_joined=True,
                    )
                    return None
                finally:
                    self._retained_lease = None

    def _retained_promotion_attempt_exists(self, report_id: str) -> bool:
        """Return whether durable same-ID evidence requires recovery."""

        candidates = (
            Path("operations/market-v4-cutover/journals") / report_id,
            Path("operations/market-v4-cutover/journal-controls") / report_id,
            Path("operations/market-v4-cutover/reports") / report_id / "report.json",
        )
        with self._managed_root_scope():
            for candidate in candidates:
                try:
                    self._managed().stat(candidate)
                except FileNotFoundError:
                    continue
                return True
        return False

    def promote_retained(
        self,
        report_id: str,
        *,
        retained_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str] | None = None,
    ) -> OperationResult:
        """Promote one exact retained rehearsal through the recovery-safe path."""

        report_id = self._validate_id(report_id, label="report")
        retained_report_id = self._validate_id(
            retained_report_id, label="retained report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        if self._retained_promotion_attempt_exists(report_id):
            recovered = self._recover_retained_promotion(
                report_id,
                retained_report_id=retained_report_id,
                backup_id=backup_id,
            )
            if recovered is None:
                raise CutoverSafetyError(
                    "Existing promotion attempt was rolled back; use a new report ID"
                )
            return recovered

        with self._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id=retained_report_id,
            backup_id=backup_id,
            config=config,
        ) as eligibility:
            journal = PromotionJournal(self._managed(), report_id, now=self.now)
            preparation = self._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id=backup_id,
                journal=journal,
            )
            return self._promote_retained_under_leases(
                preparation,
                journal=journal,
                config=config,
                inherited_environment=inherited_environment or {},
            )

    def _promote_retained_under_leases(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        journal: PromotionJournal,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        try:
            return self._promote_retained_under_leases_unchecked(
                preparation,
                journal=journal,
                config=config,
                inherited_environment=inherited_environment,
            )
        except Exception as exc:
            try:
                current_state = journal.read_validated()[-1].state
            except (CutoverSafetyError, IndexError):
                current_state = None
            if current_state is PromotionState.COMMITTED:
                raise CutoverSafetyError(
                    "Committed promotion cleanup incomplete; same-ID recovery required"
                ) from exc
            joined = not (
                isinstance(exc, (RuntimeStopError, WorkerShutdownError))
                and not exc.process_joined
            )
            try:
                self._rollback_retained_promotion(
                    RetainedPromotionContext(preparation, journal),
                    processes_joined=joined,
                )
            except CutoverSafetyError as rollback_error:
                if not joined and "deferred" in str(rollback_error):
                    raise rollback_error from exc
                raise CutoverSafetyError(
                    "Retained promotion failed and rollback recovery failed"
                ) from rollback_error
            if isinstance(exc, CutoverSafetyError):
                raise exc
            raise CutoverSafetyError(
                "Retained promotion failed and was rolled back"
            ) from exc

    def _promote_retained_under_leases_unchecked(
        self,
        preparation: RetainedPromotionPreparation,
        *,
        journal: PromotionJournal,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        if self._active_lease is None or self._retained_lease is None:
            raise CutoverSafetyError(
                "Active and retained Market operation leases are required"
            )
        operation_id = journal.operation_id
        eligibility = preparation.eligibility
        records = journal.read_validated()
        if not records or records[-1].state is not PromotionState.PREPARED:
            raise CutoverSafetyError("Promotion journal must be prepared")
        base = records[-1].identities
        active_relative = Path("market-timeseries")
        retained_market = eligibility.retained_root / "market-timeseries"
        retained_relative = self._managed_relative(retained_market)
        quarantine = self.operations_root / "quarantine" / operation_id
        runtime_name = f".cutover-runtime-{operation_id}"
        report_dir = self.operations_root / "reports" / operation_id
        log_path = report_dir / "active-smoke.log"
        code_version = self._active_code_version
        if code_version is None:
            raise CutoverSafetyError("Operation code identity is unavailable")

        self.atomic_exchange.exchange(
            self._managed(),
            active_relative,
            retained_relative,
        )
        self._promotion_boundary_hook("exchange_fsynced")
        active_location = self._market_location_identity(self._active_lease_fd_root())
        retained_location = self._market_location_identity(
            self._retained_lease_fd_root()
        )
        if (
            active_location["directory"] != base.retained_v4_directory
            or active_location["payload"] != base.retained_v4_payload
            or retained_location["directory"] != base.active_before_directory
            or retained_location["payload"] != base.active_before_payload
        ):
            raise CutoverSafetyError("Atomic promotion exchange identity mismatch")
        exchanged = self._promotion_identities(
            base,
            active_current=active_location,
            retained_current=retained_location,
            quarantine_current=None,
            holding_current=base.holding_current,
        )
        self._append_preparation_state(journal, PromotionState.EXCHANGED, exchanged)
        self._promotion_boundary_hook("exchanged_journaled")

        self._prepare_managed_directory(quarantine.parent, exist_ok=True)
        self._secure_rename(retained_market, quarantine)
        self._promotion_boundary_hook("quarantine_fsynced")
        quarantine_location = self._payload_location_identity(quarantine)
        if (
            quarantine_location["directory"] != base.active_before_directory
            or quarantine_location["payload"] != base.active_before_payload
        ):
            raise CutoverSafetyError("Promotion quarantine identity mismatch")
        quarantined = self._promotion_identities(
            base,
            active_current=active_location,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=base.holding_current,
        )
        self._append_preparation_state(
            journal,
            PromotionState.QUARANTINED,
            quarantined,
        )
        self._promotion_boundary_hook("quarantined_journaled")

        self._prepare_retained_runtime(
            self.data_root,
            runtime_name=runtime_name,
            root_fd=self._active_lease.root_fd,
        )
        self._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._prepare_managed_directory(report_dir, exist_ok=False)
        self._require_unchanged_code_identity(code_version)
        environment = self._promotion_runtime_environment(
            inherited_environment,
            lease_fd=self._active_lease.fd,
            root_fd=self._active_lease.root_fd,
            runtime_name=runtime_name,
        )
        market_fd = os.open(
            "market-timeseries",
            _DIR_OPEN_FLAGS,
            dir_fd=self._active_lease.root_fd,
        )
        api: ApiAdapter | None = None
        log_fd = self._managed().open_regular(
            self._managed_relative(log_path),
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
        )
        try:
            api = self.runtime.start(
                root_fd=self._active_lease.root_fd,
                market_fd=market_fd,
                lease_fd=self._active_lease.fd,
                retained_lease_fd=self._retained_lease.fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
        finally:
            os.close(log_fd)
        try:
            try:
                smoke_result = self.smoke(
                    api,
                    config,
                    operation_id=f"{operation_id}.active",
                    market_directory_fd=market_fd,
                    guard_lease_fd=self._active_lease.fd,
                )
            except Exception as smoke_error:
                try:
                    self.runtime.cancel_owned_work(api)
                finally:
                    try:
                        self.runtime.stop(api)
                    except RuntimeStopError as stop_error:
                        raise stop_error from smoke_error
                api = None
                raise
            self.runtime.stop(api)
            api = None
        finally:
            os.close(market_fd)
        self._promotion_boundary_hook("smoke_joined")
        active_runtime_market_fd = self._managed().open_dir(Path("market-timeseries"))
        try:
            self._remove_market_runtime(active_runtime_market_fd, runtime_name)
        finally:
            os.close(active_runtime_market_fd)
        log_bytes = self._managed().read_bytes(self._managed_relative(log_path))
        if b"jquants_fetch" in log_bytes.lower():
            raise CutoverSafetyError("Promotion smoke observed a J-Quants fetch")
        forbidden_paths = (
            "/api/db/sync",
            "/api/db/adjusted-metrics/materialize",
            "/api/db/stocks/refresh",
            "/api/db/intraday/sync",
        )
        if any(
            path.startswith(forbidden)
            for path in smoke_result.api_paths
            for forbidden in forbidden_paths
        ):
            raise CutoverSafetyError("Promotion smoke used a forbidden mutation API")
        active_after = self._market_location_identity(self._active_lease_fd_root())
        if active_after["payload"] != eligibility.source_market_identity:
            raise RetainedMarketMutationError(
                "Active retained payload changed during promotion smoke"
            )
        smoke_passed = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=base.holding_current,
        )
        self._append_preparation_state(
            journal,
            PromotionState.ACTIVE_SMOKE_PASSED,
            smoke_passed,
        )
        self._promotion_boundary_hook("smoke_journaled")

        self._require_unchanged_code_identity(code_version)
        cleanup_staging = self._stage_held_promotion_artifacts(
            preparation,
            operation_id=operation_id,
        )
        self._promotion_boundary_hook("held_cleanup_fsynced")
        cleanup_location = {
            "directory": preparation.holding_directory_identity,
            "payload": base.retained_v4_payload,
        }
        cleanup_staged = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=cleanup_location,
        )
        self._append_preparation_state(
            journal,
            PromotionState.CLEANUP_STAGED,
            cleanup_staged,
        )
        active_market_fd = self._managed().open_dir(Path("market-timeseries"))
        try:
            self._validate_canonical_market_payload(active_market_fd)
        finally:
            os.close(active_market_fd)

        created_at = self.now()
        expectation = self._retained_promotion_report_expectation(
            operation_id=operation_id,
            created_at=created_at,
            code_version=code_version,
            preparation=preparation,
            base=base,
            active_location=active_location,
            active_after=active_after,
            quarantine=quarantine,
            quarantine_location=quarantine_location,
            smoke_result=smoke_result,
        )
        report = self._build_retained_promotion_report(expectation)

        def final_validator() -> None:
            current_active = self._market_location_identity(
                self._active_lease_fd_root()
            )
            current_quarantine = self._payload_location_identity(quarantine)
            current_expectation = self._retained_promotion_report_expectation(
                operation_id=operation_id,
                created_at=created_at,
                code_version=code_version,
                preparation=preparation,
                base=base,
                active_location=active_location,
                active_after=current_active,
                quarantine=quarantine,
                quarantine_location=current_quarantine,
                smoke_result=smoke_result,
            )
            if not self._retained_promotion_report_contract_valid(
                report,
                expectation=current_expectation,
            ):
                raise CutoverSafetyError("Promotion report contract changed")
            self._require_unchanged_code_identity(code_version)
            if current_active != active_after:
                raise RetainedMarketMutationError(
                    "Active retained payload changed during report publication"
                )
            if current_quarantine != quarantine_location:
                raise CutoverSafetyError(
                    "Promotion quarantine changed during report publication"
                )
            staging_fd = self._managed().open_dir(
                self._managed_relative(cleanup_staging)
            )
            try:
                if (
                    self._directory_identity_evidence(staging_fd)
                    != preparation.holding_directory_identity
                    or self._held_artifacts_evidence(staging_fd)
                    != preparation.detached_artifacts
                ):
                    raise CutoverSafetyError(
                        "Promotion cleanup staging changed during report publication"
                    )
            finally:
                os.close(staging_fd)

        report_path = self._write_report(
            operation_id,
            report,
            final_validator=final_validator,
        )
        self._promotion_boundary_hook("report_fsynced")
        persisted = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=cleanup_location,
        )
        self._append_preparation_state(
            journal,
            PromotionState.REPORT_PERSISTED,
            persisted,
        )
        self._promotion_boundary_hook("report_journaled")
        self._write_source_consumed_marker(
            retained_report_id=eligibility.retained_report_id,
            operation_id=operation_id,
            promotion_report_sha256=self._sha256(report_path),
        )
        self._promotion_boundary_hook("consumed_marker_fsynced")
        committed = self._promotion_identities(
            base,
            active_current=active_after,
            retained_current=None,
            quarantine_current=quarantine_location,
            holding_current=cleanup_location,
            promotion_report_sha256=self._sha256(report_path),
        )
        self._append_preparation_state(
            journal,
            PromotionState.COMMITTED,
            committed,
        )
        self._promotion_boundary_hook("committed_journaled")
        self._complete_committed_promotion_cleanup(
            preparation,
            operation_id=operation_id,
            report_sha256=self._sha256(report_path),
        )
        return OperationResult(
            operation_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    @contextmanager
    def _retained_promotion_eligibility_scope(
        self,
        *,
        report_id: str,
        retained_report_id: str,
        backup_id: str,
        config: SmokeConfig,
    ) -> Iterator[RetainedPromotionEligibility]:
        with self._existing_exclusive_operation() as code_version:
            retained, retained_sha256, retained_stat = self._promotion_report_snapshot(
                retained_report_id
            )
            source_report_value = retained.get("sourceRehearsalReportId")
            if not isinstance(source_report_value, str):
                raise CutoverSafetyError("Retained report provenance is invalid")
            source_report_id = self._validate_id(
                source_report_value, label="source rehearsal report"
            )
            retained_root = self._retained_rehearsal_root(source_report_id)
            with MarketOperationLease.acquire_existing(
                retained_root, exclusive=True
            ) as retained_lease:
                self._retained_lease = retained_lease
                try:
                    eligibility = (
                        self._validate_retained_promotion_eligibility_under_leases(
                            report_id=report_id,
                            retained_report_id=retained_report_id,
                            backup_id=backup_id,
                            config=config,
                            code_version=code_version,
                            retained_lease=retained_lease,
                        )
                    )
                    final_retained, final_retained_sha256, final_retained_stat = (
                        self._promotion_report_snapshot(retained_report_id)
                    )
                    final_source, final_source_sha256, _final_source_stat = (
                        self._promotion_report_snapshot(
                            eligibility.source_report_id
                        )
                    )
                    if (
                        eligibility.retained_report_sha256 != retained_sha256
                        or final_retained_sha256
                        != eligibility.retained_report_sha256
                        or final_retained_stat != retained_stat
                        or final_retained.get("reportId") != retained_report_id
                        or final_source_sha256
                        != eligibility.source_report_sha256
                        or final_source.get("reportId")
                        != eligibility.source_report_id
                    ):
                        raise CutoverSafetyError(
                            "Retained promotion report changed"
                        )
                    if (
                        self._market_tree_identity(self._active_lease_fd_root())
                        != eligibility.active_market_identity
                    ):
                        raise CutoverSafetyError(
                            "Active Market payload identity changed during validation"
                        )
                    if (
                        self._market_tree_identity(retained_lease.root_fd)
                        != eligibility.source_market_identity
                    ):
                        raise CutoverSafetyError(
                            "Retained Market payload identity changed during validation"
                        )
                    self._require_unchanged_code_identity(code_version)
                    self._assert_retained_root_identity(
                        eligibility.retained_root, retained_lease.root_fd
                    )
                    yield eligibility
                finally:
                    self._retained_lease = None

    def _cutover_under_lease(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
        code_version: str,
    ) -> OperationResult:
        report_id = self._validate_id(report_id, label="report")
        rehearsal_report_id = self._validate_id(
            rehearsal_report_id, label="rehearsal report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        rehearsal = self._read_report(rehearsal_report_id)
        target_root_fingerprint = rehearsal.get("targetRootFingerprint")
        expected_root_fingerprint = (
            target_root_fingerprint
            if isinstance(target_root_fingerprint, str)
            else None
        )
        expected_smoke_config = {
            "symbol": config.symbol,
            "strategy": config.strategy,
            "datasetPreset": config.dataset_preset,
        }
        mode = rehearsal.get("rehearsalMode")
        common_valid = (
            rehearsal.get("phase") == "rehearsal"
            and rehearsal.get("status") == "passed"
            and rehearsal.get("reportId") == rehearsal_report_id
            and mode in {"full_rebuild", "retained_market_smoke"}
            and rehearsal.get("serverProcessJoined") is True
            and rehearsal.get("workerProcessJoined") is True
            and expected_root_fingerprint == self.root_fingerprint(self.data_root)
            and rehearsal.get("codeVersion") == code_version
            and rehearsal.get("smokeConfig") == expected_smoke_config
        )
        retained_valid = True
        if mode == "full_rebuild":
            retained_valid = self._full_rebuild_report_contract_valid(
                rehearsal,
                config=config,
            )
        elif mode == "retained_market_smoke":
            source_market_identity_before = rehearsal.get(
                "sourceMarketIdentityBefore"
            )
            retained_valid = (
                isinstance(rehearsal.get("sourceRehearsalReportId"), str)
                and bool(rehearsal["sourceRehearsalReportId"])
                and isinstance(rehearsal.get("sourceRehearsalCodeVersion"), str)
                and bool(rehearsal["sourceRehearsalCodeVersion"])
                and isinstance(
                    rehearsal.get("sourceRetainedRootFingerprint"), str
                )
                and bool(rehearsal["sourceRetainedRootFingerprint"])
                and isinstance(source_market_identity_before, dict)
                and source_market_identity_before
                == rehearsal.get("sourceMarketIdentityAfter")
                and self._retained_report_contract_valid(
                    rehearsal,
                    report_id=rehearsal_report_id,
                    config=config,
                )
            )
            if retained_valid:
                try:
                    source_report_id_value = rehearsal.get(
                        "sourceRehearsalReportId"
                    )
                    if not isinstance(source_report_id_value, str):
                        raise CutoverSafetyError(
                            "Retained rehearsal source report ID is invalid"
                        )
                    source_report_id = self._validate_id(
                        source_report_id_value,
                        label="source rehearsal report",
                    )
                    source_report = self._read_report(source_report_id)
                    retained_root = self._retained_rehearsal_root(source_report_id)
                    with MarketOperationLease.acquire(
                        retained_root,
                        exclusive=True,
                    ) as source_lease:
                        self._assert_retained_root_identity(
                            retained_root,
                            source_lease.root_fd,
                        )
                        source_report = self._read_report(source_report_id)
                        current_market_identity = self._market_tree_identity(
                            source_lease.root_fd
                        )
                        retained_valid = (
                            source_report.get("reportId") == source_report_id
                            and source_report.get("phase") == "rehearsal"
                            and source_report.get("status") in {"passed", "failed"}
                            and source_report.get("targetRootFingerprint")
                            == expected_root_fingerprint
                            and source_report.get("smokeConfig")
                            == rehearsal.get("smokeConfig")
                            and source_report.get("codeVersion")
                            == rehearsal.get("sourceRehearsalCodeVersion")
                            and source_report.get("serverProcessJoined") is True
                            and source_report.get("workerProcessJoined") is True
                            and self._root_fingerprint_at(source_lease.root_fd)
                            == rehearsal.get("sourceRetainedRootFingerprint")
                            and current_market_identity
                            == rehearsal.get("sourceMarketIdentityBefore")
                        )
                except (CutoverSafetyError, TypeError):
                    retained_valid = False
        if not common_valid or not retained_valid:
            raise CutoverSafetyError("An exact passing rehearsal report is required")
        self.verify_backup(backup_id)
        self._preflight_under_lease()
        assert self._active_lease is not None
        started = time.monotonic()
        api: ApiAdapter | None = None
        activated = False
        activation_attempted = False
        staging_lease: MarketOperationLease | None = None
        staged_market_fd: int | None = None
        active_market_fd: int | None = None
        report_dir = self.operations_root / "reports" / report_id
        self._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._prepare_managed_directory(report_dir, exist_ok=False)
        log_path = report_dir / "server.log"
        self._assert_managed_target_absent(log_path)
        try:
            staging_dir = self.operations_root / "staging" / report_id
            self._prepare_managed_directory(staging_dir.parent, exist_ok=True)
            self._prepare_managed_directory(staging_dir, exist_ok=False)
            staging_root = staging_dir / "root"
            runtime_name = f".cutover-runtime-{report_id}"
            runtime_template = staging_root / f"runtime-template-{report_id}"
            self._prepare_isolated_root(staging_root, runtime_name=runtime_name)
            if self.configuration_fingerprint(
                staging_root
            ) != self.configuration_fingerprint(self.data_root):
                raise CutoverSafetyError(
                    "Cutover staging configuration snapshot mismatch"
                )
            if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
                raise CutoverSafetyError("Active inputs changed before owned server start")
            staging_lease = MarketOperationLease.acquire(
                staging_root,
                exclusive=True,
            )
            if staging_lease is not None:
                staging_root_identity = os.fstat(staging_lease.root_fd)
                staged_market_fd = os.open(
                    "market-timeseries",
                    _DIR_OPEN_FLAGS,
                    dir_fd=staging_lease.root_fd,
                )
                staged_market_identity = os.fstat(staged_market_fd)
                environment = self._isolated_environment(
                    inherited_environment,
                    lease_fd=staging_lease.fd,
                    root_fd=staging_lease.root_fd,
                    runtime_name=runtime_name,
                )
                log_fd = self._managed().open_regular(
                    self._managed_relative(log_path),
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                )
                try:
                    api = self.runtime.start(
                        root_fd=staging_lease.root_fd,
                        market_fd=staged_market_fd,
                        lease_fd=staging_lease.fd,
                        environment=environment,
                        log_path=log_path,
                        log_fd=log_fd,
                    )
                finally:
                    os.close(log_fd)
                if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
                    raise CutoverSafetyError(
                        "Active inputs changed during staged server start"
                    )
                (
                    checks,
                    evidence,
                    phases,
                    _verified_market_identity,
                ) = self._run_rebuild(
                    api,
                    config,
                    staging_root,
                    report_id,
                    market_directory_fd=staged_market_fd,
                    guard_lease_fd=staging_lease.fd,
                )
                if (
                    _verified_market_identity.st_dev,
                    _verified_market_identity.st_ino,
                ) != (staged_market_identity.st_dev, staged_market_identity.st_ino):
                    raise CutoverSafetyError("Staged Market identity changed during rebuild")
                self.runtime.stop(api)
                api = None
                os.close(staged_market_fd)
                staged_market_fd = None
                staging_lease.release()
                staging_lease = None
            self._secure_rename(
                staging_root / "market-timeseries" / runtime_name,
                runtime_template,
            )
            if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
                raise CutoverSafetyError("Active inputs changed during staged rebuild")
            self._assert_current_data_root_identity()
            self._validate_active_roots()
            self._assert_managed_directory_identity(
                staging_root,
                staging_root_identity,
            )
            self._assert_managed_directory_identity(
                staging_root / "market-timeseries",
                staged_market_identity,
            )
            activation_attempted = True
            self._activate_staged_market(
                staging_root / "market-timeseries",
                report_id,
            )
            activated = True
            self._secure_rename(
                runtime_template,
                self.market_root / runtime_name,
            )

            active_environment = self._isolated_environment(
                inherited_environment,
                lease_fd=self._active_lease.fd,
                root_fd=self._active_lease.root_fd,
                runtime_name=runtime_name,
            )
            active_log_path = report_dir / "active-smoke.log"
            active_log_fd = self._managed().open_regular(
                self._managed_relative(active_log_path),
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
            )
            active_market_fd = os.open(
                "market-timeseries",
                _DIR_OPEN_FLAGS,
                dir_fd=self._active_lease.root_fd,
            )
            try:
                api = self.runtime.start(
                    root_fd=self._active_lease.root_fd,
                    market_fd=active_market_fd,
                    lease_fd=self._active_lease.fd,
                    environment=active_environment,
                    log_path=active_log_path,
                    log_fd=active_log_fd,
                )
            finally:
                os.close(active_log_fd)
            active_smoke_started = time.monotonic()
            active_smoke = self.smoke(
                api,
                config,
                operation_id=f"{report_id}.active",
                market_directory_fd=active_market_fd,
                guard_lease_fd=self._active_lease.fd,
            )
            active_smoke_duration = time.monotonic() - active_smoke_started
            self.runtime.stop(api)
            api = None
            self._remove_market_runtime(active_market_fd, runtime_name)
            os.close(active_market_fd)
            active_market_fd = None
            checks = (*checks, *active_smoke.api_paths)
            phases = (
                *phases,
                {
                    "name": "activated_market_smoke",
                    "status": "passed",
                    "durationSeconds": round(active_smoke_duration, 6),
                },
            )
            if self.root_fingerprint(self.data_root) != expected_root_fingerprint:
                raise CutoverSafetyError("Active inputs changed before report persistence")
            self._assert_current_data_root_identity()
            self._require_unchanged_code_identity(code_version)
            report = self._operation_report(
                report_id=report_id,
                phase="cutover",
                status="passed",
                duration_seconds=time.monotonic() - started,
                api_checks=checks,
                server_log=f"reports/{report_id}/server.log",
                evidence=evidence,
                phases=phases,
                config=config,
                backup_id=backup_id,
                rehearsal_report_id=rehearsal_report_id,
                target_root_fingerprint=expected_root_fingerprint,
                code_version=code_version,
            )
            report_path = self._write_report(
                report_id,
                report,
                expected_root_fingerprint=expected_root_fingerprint,
            )
            return OperationResult(
                report_id,
                report_path.relative_to(self.data_root).as_posix(),
            )
        except Exception as exc:
            if staged_market_fd is not None:
                os.close(staged_market_fd)
            if active_market_fd is not None:
                os.close(active_market_fd)
            stop_error: Exception | None = (
                exc if isinstance(exc, RuntimeStopError) else None
            )
            server_stopped = api is None
            if isinstance(exc, RuntimeStopError):
                server_stopped = exc.process_joined
            worker_stopped = not (
                isinstance(exc, WorkerShutdownError) and not exc.process_joined
            )
            if api is not None:
                try:
                    self.runtime.cancel_owned_work(api)
                except Exception:
                    pass
                try:
                    self.runtime.stop(api)
                except RuntimeStopError as runtime_stop_error:
                    server_stopped = runtime_stop_error.process_joined
                    stop_error = runtime_stop_error
                except Exception as runtime_stop_error:
                    stop_error = runtime_stop_error
                else:
                    server_stopped = True
            if staging_lease is not None:
                if not server_stopped or not worker_stopped:
                    staging_lease.unlock_on_release = False
                staging_lease.release()
                staging_lease = None
            if not server_stopped or not worker_stopped:
                assert self._active_lease is not None
                self._active_lease.unlock_on_release = False
                report = self._operation_report(
                    report_id=report_id,
                    phase="cutover",
                    status="stop_failed_restore_deferred",
                    duration_seconds=time.monotonic() - started,
                    api_checks=(),
                    server_log=f"reports/{report_id}/server.log",
                    evidence=None,
                    phases=(),
                    config=config,
                    backup_id=backup_id,
                    rehearsal_report_id=rehearsal_report_id,
                    error=type(exc).__name__,
                    error_message=self._redact_diagnostic(
                        str(exc), inherited_environment
                    ),
                    target_root_fingerprint=expected_root_fingerprint,
                    code_version=code_version,
                    stop_error=(
                        type(stop_error).__name__ if stop_error else None
                    ),
                    server_process_joined=server_stopped,
                    worker_process_joined=worker_stopped,
                )
                self._try_write_report(report_id, report)
                raise CutoverSafetyError(
                    "Owned process stop was not proven; restore is deferred"
                ) from (stop_error or exc)
            if not activated and not activation_attempted:
                report = self._operation_report(
                    report_id=report_id,
                    phase="cutover",
                    status="failed_active_untouched",
                    duration_seconds=time.monotonic() - started,
                    api_checks=(),
                    server_log=f"reports/{report_id}/server.log",
                    evidence=None,
                    phases=(),
                    config=config,
                    backup_id=backup_id,
                    rehearsal_report_id=rehearsal_report_id,
                    error=type(exc).__name__,
                    error_message=self._redact_diagnostic(
                        str(exc), inherited_environment
                    ),
                    target_root_fingerprint=expected_root_fingerprint,
                    code_version=code_version,
                )
                self._try_write_report(report_id, report)
                raise CutoverSafetyError(
                    "Staged cutover failed before activation; active market is unchanged"
                ) from exc
            try:
                self.restore(backup_id)
            except Exception as restore_exc:
                report = self._operation_report(
                    report_id=report_id,
                    phase="cutover",
                    status="restore_failed",
                    duration_seconds=time.monotonic() - started,
                    api_checks=(),
                    server_log=f"reports/{report_id}/server.log",
                    evidence=None,
                    phases=(),
                    config=config,
                    backup_id=backup_id,
                    rehearsal_report_id=rehearsal_report_id,
                    error=type(exc).__name__,
                    error_message=self._redact_diagnostic(
                        str(exc), inherited_environment
                    ),
                    target_root_fingerprint=expected_root_fingerprint,
                    code_version=code_version,
                    restore_error=type(restore_exc).__name__,
                )
                self._try_write_report(report_id, report)
                raise CutoverSafetyError(
                    "Active cutover failed and explicit restore also failed"
                ) from restore_exc
            report = self._operation_report(
                report_id=report_id,
                phase="cutover",
                status="failed_restored",
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log=f"reports/{report_id}/server.log",
                evidence=None,
                phases=(),
                config=config,
                backup_id=backup_id,
                rehearsal_report_id=rehearsal_report_id,
                error=type(exc).__name__,
                error_message=self._redact_diagnostic(
                    str(exc), inherited_environment
                ),
                target_root_fingerprint=expected_root_fingerprint,
                code_version=code_version,
            )
            self._try_write_report(report_id, report)
            raise CutoverSafetyError(
                f"Active cutover failed; restored backup {backup_id}"
            ) from exc

    def _require_confined_real_directory(self, root: Path, base: Path) -> None:
        root = self._managed_path(root)
        base = self._managed_path(base)
        try:
            root.relative_to(base)
        except ValueError as exc:
            raise CutoverSafetyError("Managed directory escapes its required root") from exc
        try:
            base_fd = self._managed().open_dir(self._managed_relative(base))
            os.close(base_fd)
            root_fd = self._managed().open_dir(self._managed_relative(root))
            os.close(root_fd)
        except (FileNotFoundError, CutoverSafetyError) as exc:
            raise CutoverSafetyError("Managed directory is unavailable") from exc

    def _retained_rehearsal_root(self, source_report_id: str) -> Path:
        source_report_id = self._validate_id(
            source_report_id,
            label="source rehearsal report",
        )
        rehearsals_root = self.operations_root / "rehearsals"
        root = rehearsals_root / source_report_id / "root"
        self._require_confined_real_directory(root, rehearsals_root)
        return root

    def _assert_retained_root_identity(
        self,
        retained_root: Path,
        root_fd: int,
    ) -> None:
        retained = os.fstat(root_fd)
        try:
            current_fd = (
                os.dup(self._managed().fd)
                if self._managed_path(retained_root) == self.data_root
                else self._managed().open_dir(self._managed_relative(retained_root))
            )
        except (FileNotFoundError, CutoverSafetyError) as exc:
            raise CutoverSafetyError(
                "Retained rehearsal root identity changed"
            ) from exc
        try:
            current = os.fstat(current_fd)
        finally:
            os.close(current_fd)
        if not stat.S_ISDIR(current.st_mode) or (retained.st_dev, retained.st_ino) != (
            current.st_dev,
            current.st_ino,
        ):
            raise CutoverSafetyError("Retained rehearsal root identity changed")

    @staticmethod
    def _regular_file_identity(
        managed: ManagedRootFd,
        relative: Path,
    ) -> tuple[os.stat_result, str]:
        canonical_relative = Path(*_safe_relative_parts(relative)).as_posix()

        def metadata(value: os.stat_result) -> dict[str, int | bool]:
            return {
                "device": value.st_dev,
                "inode": value.st_ino,
                "size": value.st_size,
                "mtimeNs": value.st_mtime_ns,
                "ctimeNs": value.st_ctime_ns,
                "regular": stat.S_ISREG(value.st_mode),
            }

        def delta(
            before: os.stat_result,
            observed: os.stat_result,
            *,
            observed_label: str,
        ) -> dict[str, dict[str, int | bool]]:
            before_metadata = metadata(before)
            observed_metadata = metadata(observed)
            return {
                key: {"before": before_value, observed_label: observed_metadata[key]}
                for key, before_value in before_metadata.items()
                if observed_metadata[key] != before_value
            }

        def failure(
            failure_class: str,
            *,
            before: os.stat_result | None = None,
            after: os.stat_result | None = None,
            current: os.stat_result | None = None,
            current_missing: bool = False,
            error_number: int | None = None,
        ) -> CutoverSafetyError:
            diagnostic: dict[str, object] = {
                "before": metadata(before) if before is not None else None,
                "afterDelta": (
                    delta(before, after, observed_label="after")
                    if before is not None and after is not None
                    else None
                ),
                "currentDelta": (
                    delta(before, current, observed_label="current")
                    if before is not None and current is not None
                    else (
                        {"missing": {"before": False, "current": True}}
                        if before is not None and current_missing
                        else None
                    )
                ),
            }
            if error_number is not None:
                diagnostic["errno"] = error_number
            return CutoverSafetyError(
                "Retained Market file changed during identity hashing: "
                f"path={canonical_relative}; failure={failure_class}; "
                f"metadata={json.dumps(diagnostic, sort_keys=True, separators=(',', ':'))}"
            )

        try:
            file_fd = managed.open_regular(relative, os.O_RDONLY)
        except (FileNotFoundError, OSError, CutoverSafetyError) as exc:
            raise failure(
                "open_failed",
                error_number=exc.errno if isinstance(exc, OSError) else None,
            ) from exc
        digest = hashlib.sha256()
        try:
            before = os.fstat(file_fd)
            if not stat.S_ISREG(before.st_mode):
                raise failure("not_regular", before=before)
            try:
                while chunk := os.read(file_fd, 1024 * 1024):
                    digest.update(chunk)
                after = os.fstat(file_fd)
            except OSError as exc:
                raise failure(
                    "read_or_fstat_failed",
                    before=before,
                    error_number=exc.errno,
                ) from exc
            try:
                current = managed.stat(relative)
            except FileNotFoundError as exc:
                raise failure(
                    "path_missing_after_hash",
                    before=before,
                    after=after,
                    current_missing=True,
                    error_number=exc.errno,
                ) from exc
            except OSError as exc:
                raise failure(
                    "path_stat_failed_after_hash",
                    before=before,
                    after=after,
                    error_number=exc.errno,
                ) from exc
            stable_metadata = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            if (
                stable_metadata
                != (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                    after.st_ctime_ns,
                )
                or stable_metadata
                != (
                    current.st_dev,
                    current.st_ino,
                    current.st_size,
                    current.st_mtime_ns,
                    current.st_ctime_ns,
                )
                or not stat.S_ISREG(current.st_mode)
            ):
                raise failure(
                    "metadata_changed",
                    before=before,
                    after=after,
                    current=current,
                )
            return before, digest.hexdigest()
        finally:
            os.close(file_fd)

    @staticmethod
    def _market_payload_identity(market_fd: int) -> dict[str, object]:
        with ManagedRootFd(Path("."), os.dup(market_fd)) as retained:
            database_relative = Path("market.duckdb")
            database_stat, database_sha256 = (
                MarketV4CutoverService._regular_file_identity(
                    retained,
                    database_relative,
                )
            )
            parquet_relative = Path("parquet")
            try:
                parquet_files = retained.regular_files(parquet_relative)
            except FileNotFoundError as exc:
                raise CutoverSafetyError(
                    "Retained Market parquet tree is missing"
                ) from exc
            parquet_paths = tuple(relative for relative, _entry in parquet_files)
            parquet_sha256: dict[str, dict[str, object]] = {}
            for relative in parquet_paths:
                parquet_stat, parquet_digest = (
                    MarketV4CutoverService._regular_file_identity(
                        retained,
                        parquet_relative / relative,
                    )
                )
                parquet_sha256[relative.as_posix()] = {
                    "device": parquet_stat.st_dev,
                    "inode": parquet_stat.st_ino,
                    "size": parquet_stat.st_size,
                    "sha256": parquet_digest,
                }
            try:
                current_parquet_paths = tuple(
                    relative
                    for relative, _entry in retained.regular_files(parquet_relative)
                )
            except FileNotFoundError as exc:
                raise CutoverSafetyError(
                    "Retained Market parquet tree changed during identity hashing"
                ) from exc
            if current_parquet_paths != parquet_paths:
                raise CutoverSafetyError(
                    "Retained Market parquet tree changed during identity hashing"
                )
            return {
                "marketDuckdb": {
                    "device": database_stat.st_dev,
                    "inode": database_stat.st_ino,
                    "size": database_stat.st_size,
                    "sha256": database_sha256,
                },
                "parquetSha256": parquet_sha256,
            }

    @staticmethod
    def _market_tree_identity(root_fd: int) -> dict[str, object]:
        market_fd = os.open("market-timeseries", _DIR_OPEN_FLAGS, dir_fd=root_fd)
        try:
            return MarketV4CutoverService._market_payload_identity(market_fd)
        finally:
            os.close(market_fd)

    @staticmethod
    def _configuration_fingerprint_at(root_fd: int) -> str:
        with ManagedRootFd(Path("."), os.dup(root_fd)) as retained:
            digest = hashlib.sha256()
            config_relative = Path("config/default.yaml")
            config_stat, config_sha256 = MarketV4CutoverService._regular_file_identity(
                retained,
                config_relative,
            )
            del config_stat
            digest.update(b"config/default.yaml\0")
            digest.update(config_sha256.encode())
            digest.update(b"\n")
            try:
                strategy_files = retained.regular_files(Path("strategies"))
            except FileNotFoundError:
                strategy_files = []
            strategy_paths = tuple(relative for relative, _entry in strategy_files)
            for relative in strategy_paths:
                _metadata, strategy_sha256 = (
                    MarketV4CutoverService._regular_file_identity(
                        retained,
                        Path("strategies") / relative,
                    )
                )
                digest.update(f"strategies/{relative.as_posix()}".encode())
                digest.update(b"\0")
                digest.update(strategy_sha256.encode())
                digest.update(b"\n")
            current_strategy_paths = tuple(
                relative
                for relative, _entry in retained.regular_files(Path("strategies"))
            )
            if current_strategy_paths != strategy_paths:
                raise CutoverSafetyError(
                    "Retained strategies changed during fingerprinting"
                )
            return digest.hexdigest()

    @staticmethod
    def _root_fingerprint_at(root_fd: int) -> str:
        root_stat = os.fstat(root_fd)
        if not stat.S_ISDIR(root_stat.st_mode):
            raise CutoverSafetyError("Retained rehearsal root is not a directory")
        digest = hashlib.sha256(
            f"dev={root_stat.st_dev};ino={root_stat.st_ino}\n".encode()
        )
        digest.update(
            MarketV4CutoverService._configuration_fingerprint_at(root_fd).encode()
        )
        digest.update(b"\n")
        return digest.hexdigest()

    def _prepare_retained_runtime(
        self,
        retained_root: Path,
        *,
        runtime_name: str,
        root_fd: int | None = None,
        on_reserved: Callable[[], None] | None = None,
    ) -> None:
        retained_root = self._managed_path(retained_root)
        if root_fd is None:
            with ManagedRootFd.open(retained_root) as retained:
                self._prepare_retained_runtime(
                    retained_root,
                    runtime_name=runtime_name,
                    root_fd=retained.fd,
                    on_reserved=on_reserved,
                )
            return
        self._assert_retained_root_identity(retained_root, root_fd)
        with ManagedRootFd(Path("."), os.dup(root_fd)) as retained:
            active_source = retained_root == self.data_root

            def source_fingerprint() -> str:
                if active_source:
                    return self.configuration_fingerprint(self.data_root)
                return self._configuration_fingerprint_at(root_fd)

            repository_config: Path | None = None
            if active_source:
                try:
                    retained.stat(Path("config/default.yaml"))
                except FileNotFoundError:
                    repository_config = self._repository_default_config_path()
                    if self._active_code_version is None:
                        raise CutoverSafetyError(
                            "Operation code identity is unavailable"
                        )
                    self._require_unchanged_code_identity(
                        self._active_code_version
                    )
            source_configuration_fingerprint = source_fingerprint()
            runtime_relative = Path("market-timeseries") / runtime_name
            try:
                runtime_fd = retained.open_dir(
                    runtime_relative,
                    create=True,
                    exclusive_leaf=True,
                )
            except FileExistsError as exc:
                raise CutoverSafetyError(
                    "Managed operation destination already exists"
                ) from exc
            os.close(runtime_fd)
            if on_reserved is not None:
                on_reserved()
            for relative in ("datasets", "backtest", "config"):
                child_fd = retained.open_dir(
                    runtime_relative / relative,
                    create=True,
                    exclusive_leaf=True,
                )
                os.close(child_fd)
            runtime_config = retained_root / runtime_relative / "config/default.yaml"
            if repository_config is not None:
                self._copy_regular_to_managed(repository_config, runtime_config)
                assert self._active_code_version is not None
                self._require_unchanged_code_identity(self._active_code_version)
            else:
                _config_metadata, config_payload_sha256 = (
                    self._regular_file_identity(
                        retained,
                        Path("config/default.yaml"),
                    )
                )
                config_payload = retained.read_bytes(Path("config/default.yaml"))
                if hashlib.sha256(config_payload).hexdigest() != config_payload_sha256:
                    raise CutoverSafetyError(
                        "Retained configuration changed before runtime copy"
                    )
                config_fd = retained.open_regular(
                    runtime_relative / "config/default.yaml",
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
                try:
                    self._write_all(config_fd, config_payload)
                    os.fsync(config_fd)
                finally:
                    os.close(config_fd)
            retained.copy_tree_create(
                Path("strategies"),
                runtime_relative / "strategies",
            )
            if source_fingerprint() != source_configuration_fingerprint:
                raise CutoverSafetyError(
                    "Retained configuration changed during runtime snapshot"
                )
            if repository_config is not None:
                assert self._active_code_version is not None
                self._require_unchanged_code_identity(self._active_code_version)
            runtime_fd = retained.open_dir(runtime_relative)
            try:
                runtime_configuration_fingerprint = (
                    self._configuration_fingerprint_at(runtime_fd)
                )
            finally:
                os.close(runtime_fd)
            if runtime_configuration_fingerprint != source_configuration_fingerprint:
                raise CutoverSafetyError(
                    "Retained runtime configuration snapshot is incoherent"
                )
        self._assert_retained_root_identity(retained_root, root_fd)

    def _prepare_isolated_root(self, root: Path, *, runtime_name: str) -> None:
        self._prepare_managed_directory(root, exist_ok=False)
        for relative in (
            "market-timeseries",
            "datasets",
            "backtest",
            "config",
        ):
            self._prepare_managed_directory(root / relative, exist_ok=False)
        source_config = self.data_root / "config" / "default.yaml"
        if not source_config.is_file():
            source_config = Path(__file__).resolve().parents[3] / "config" / "default.yaml"
        if not source_config.is_file():
            raise CutoverSafetyError("Repository default configuration is missing")
        config_target = root / "config" / "default.yaml"
        self._assert_managed_target_absent(config_target)
        self._copy_regular_to_managed(source_config, config_target)
        try:
            active_strategies_fd = self._managed().open_dir(Path("strategies"))
        except FileNotFoundError:
            self._prepare_managed_directory(root / "strategies", exist_ok=False)
        else:
            os.close(active_strategies_fd)
            self._managed().copy_tree_create(
                Path("strategies"),
                self._managed_relative(root / "strategies"),
            )
        runtime_root = root / "market-timeseries" / runtime_name
        self._prepare_managed_directory(runtime_root, exist_ok=False)
        for relative in ("datasets", "backtest", "config"):
            self._prepare_managed_directory(runtime_root / relative, exist_ok=False)
        runtime_config = runtime_root / "config" / "default.yaml"
        self._assert_managed_target_absent(runtime_config)
        self._copy_regular_to_managed(config_target, runtime_config)
        self._managed().copy_tree_create(
            self._managed_relative(root / "strategies"),
            self._managed_relative(runtime_root / "strategies"),
        )

    @staticmethod
    def _isolated_environment(
        inherited: dict[str, str],
        *,
        lease_fd: int,
        root_fd: int,
        runtime_name: str,
    ) -> dict[str, str]:
        environment = dict(inherited)
        environment.pop("TRADING25_RUNTIME_CAPABILITY", None)
        overrides = {
            "XDG_DATA_HOME": f"{runtime_name}/xdg-data-home",
            "TRADING25_DATA_DIR": runtime_name,
            "MARKET_TIMESERIES_DIR": ".",
            "MARKET_DB_PATH": "market.duckdb",
            "DATASET_BASE_PATH": f"{runtime_name}/datasets",
            "PORTFOLIO_DB_PATH": f"{runtime_name}/portfolio.db",
            "TRADING25_STRATEGIES_DIR": f"{runtime_name}/strategies",
            "TRADING25_BACKTEST_DIR": f"{runtime_name}/backtest",
            "TRADING25_DEFAULT_CONFIG_PATH": f"{runtime_name}/config/default.yaml",
            "TRADING25_MARKET_OPERATION_LOCK_FD": str(lease_fd),
            "TRADING25_DATA_ROOT_FD": str(root_fd),
        }
        environment.update(overrides)
        return environment

    def _run_rebuild(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        root: Path,
        operation_id: str,
        *,
        market_directory_fd: int,
        guard_lease_fd: int,
    ) -> tuple[
        tuple[str, ...],
        dict[str, object],
        tuple[dict[str, object], ...],
        os.stat_result,
    ]:
        sync_started = time.monotonic()
        sync = api.request(
            "POST",
            "/api/db/sync",
            {
                "mode": "initial",
                "resetBeforeSync": False,
                "enforceBulkForStockData": True,
            },
        )
        job_id = self._require_job_id(sync, "/api/db/sync")
        self._poll_api_job(
            api,
            f"/api/db/sync/jobs/{quote(job_id, safe='')}",
            "sync",
        )
        sync_duration = time.monotonic() - sync_started
        smoke_started = time.monotonic()
        market_identity = os.fstat(market_directory_fd)
        result = self.smoke(
            api,
            config,
            operation_id=operation_id,
            market_root=root / "market-timeseries",
            market_directory_fd=market_directory_fd,
            guard_lease_fd=guard_lease_fd,
        )
        smoke_duration = time.monotonic() - smoke_started
        return (
            (
                "/api/db/sync",
                f"/api/db/sync/jobs/{job_id}",
                *result.api_paths,
            ),
            {
                "schemaVersion": result.schema_version,
                "stockPriceAdjustmentMode": result.adjustment_mode,
                "adjustedMetrics": result.lineage,
            },
            (
                {
                    "name": "initial_sync_and_adjusted_metrics_pit",
                    "status": "passed",
                    "durationSeconds": round(sync_duration, 6),
                },
                {
                    "name": "semantic_smoke",
                    "status": "passed",
                    "durationSeconds": round(smoke_duration, 6),
                },
            ),
            market_identity,
        )

    def _operation_report(
        self,
        *,
        report_id: str,
        phase: str,
        status: str,
        duration_seconds: float,
        api_checks: tuple[str, ...],
        server_log: str,
        evidence: dict[str, object] | None,
        phases: tuple[dict[str, object], ...],
        config: SmokeConfig,
        code_version: str,
        backup_id: str | None = None,
        rehearsal_report_id: str | None = None,
        rehearsal_mode: str | None = None,
        source_rehearsal_report_id: str | None = None,
        source_rehearsal_code_version: str | None = None,
        source_retained_root_fingerprint: str | None = None,
        source_market_identity_before: dict[str, object] | None = None,
        source_market_identity_after: dict[str, object] | None = None,
        error: str | None = None,
        error_message: str | None = None,
        cleanup_error: str | None = None,
        stop_error: str | None = None,
        restore_error: str | None = None,
        server_process_joined: bool | None = None,
        worker_process_joined: bool | None = None,
        target_root_fingerprint: str | None = None,
    ) -> dict[str, object]:
        report: dict[str, object] = {
            "reportId": report_id,
            "phase": phase,
            "status": status,
            "createdAt": self.now(),
            "durationSeconds": round(duration_seconds, 6),
            "codeVersion": code_version,
            "targetRootFingerprint": (
                target_root_fingerprint
                if target_root_fingerprint is not None
                else self.root_fingerprint(self.data_root)
            ),
            "command": [
                "python",
                "-m",
                "uvicorn",
                "src.entrypoints.http.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                "<reserved>",
            ],
            "apiChecks": list(api_checks),
            "serverLog": server_log,
            "schemaCoverage": evidence,
            "phases": list(phases),
            "smokeConfig": {
                "symbol": config.symbol,
                "strategy": config.strategy,
                "datasetPreset": config.dataset_preset,
            },
        }
        if backup_id is not None:
            report["backupId"] = backup_id
            report["backupManifest"] = f"backups/{backup_id}/manifest.json"
        if rehearsal_report_id is not None:
            report["rehearsalReportId"] = rehearsal_report_id
        if rehearsal_mode is not None:
            report["rehearsalMode"] = rehearsal_mode
        if source_rehearsal_report_id is not None:
            report["sourceRehearsalReportId"] = source_rehearsal_report_id
        if source_rehearsal_code_version is not None:
            report["sourceRehearsalCodeVersion"] = source_rehearsal_code_version
        if source_retained_root_fingerprint is not None:
            report["sourceRetainedRootFingerprint"] = (
                source_retained_root_fingerprint
            )
        if source_market_identity_before is not None:
            report["sourceMarketIdentityBefore"] = source_market_identity_before
        if source_market_identity_after is not None:
            report["sourceMarketIdentityAfter"] = source_market_identity_after
        if error is not None:
            report["errorType"] = error
        if error_message is not None:
            report["errorMessage"] = error_message
        if cleanup_error is not None:
            report["cleanupErrorType"] = cleanup_error
        if stop_error is not None:
            report["stopErrorType"] = stop_error
        if restore_error is not None:
            report["restoreErrorType"] = restore_error
        if server_process_joined is not None:
            report["serverProcessJoined"] = server_process_joined
        if worker_process_joined is not None:
            report["workerProcessJoined"] = worker_process_joined
        return report

    def _redact_diagnostic(
        self,
        message: str,
        environment: dict[str, str],
        *,
        max_chars: int = 1_024,
    ) -> str:
        redacted = message.replace(str(self.data_root), "<data-root>")
        path_keys = {
            "XDG_DATA_HOME",
            "TRADING25_DATA_DIR",
            "MARKET_TIMESERIES_DIR",
            "MARKET_DB_PATH",
            "DATASET_BASE_PATH",
            "PORTFOLIO_DB_PATH",
            "TRADING25_STRATEGIES_DIR",
            "TRADING25_BACKTEST_DIR",
            "TRADING25_DEFAULT_CONFIG_PATH",
        }
        for key, value in environment.items():
            if not value:
                continue
            upper = key.upper()
            if key in path_keys:
                if Path(value).is_absolute():
                    redacted = redacted.replace(value, f"<{key.lower()}>")
            elif any(
                token in upper for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")
            ):
                redacted = redacted.replace(value, "<redacted-secret>")
        redacted = redacted.replace(str(Path.home()), "<home>")
        if len(redacted) > max_chars:
            return redacted[: max_chars - 3] + "..."
        return redacted

    def _write_report(
        self,
        report_id: str,
        report: dict[str, object],
        *,
        expected_root_fingerprint: str | None = None,
        final_validator: Callable[[], None] | None = None,
    ) -> Path:
        if (
            expected_root_fingerprint is not None
            and self.root_fingerprint(self.data_root) != expected_root_fingerprint
        ):
            raise CutoverSafetyError("Active configuration changed before report write")
        report_dir = self.operations_root / "reports" / report_id
        self._prepare_managed_directory(report_dir.parent, exist_ok=True)
        self._prepare_managed_directory(report_dir, exist_ok=True)
        report_path = report_dir / "report.json"
        self._assert_managed_target_absent(report_path)
        report_relative = self._managed_relative(report_path)
        report_dir_relative = report_relative.parent
        report_dir_fd = self._managed().open_dir(report_dir_relative)
        temporary_name = f".report.json.{secrets.token_hex(8)}.tmp"
        published = False
        temporary_created = False
        try:
            self._managed_mutation_hook("write")
            path_report_dir_fd = self._managed().open_dir(report_dir_relative)
            try:
                retained_stat = os.fstat(report_dir_fd)
                path_stat = os.fstat(path_report_dir_fd)
                if (retained_stat.st_dev, retained_stat.st_ino) != (
                    path_stat.st_dev,
                    path_stat.st_ino,
                ):
                    raise CutoverSafetyError("Report directory identity changed")
            finally:
                os.close(path_report_dir_fd)
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | _FILE_NOFOLLOW
            temporary_fd = os.open(temporary_name, flags, 0o600, dir_fd=report_dir_fd)
            temporary_created = True
            try:
                payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
                view = memoryview(payload)
                while view:
                    written = os.write(temporary_fd, view)
                    view = view[written:]
                os.fsync(temporary_fd)
            finally:
                os.close(temporary_fd)
            self._report_publish_hook("after_temp_fsync")
            if final_validator is not None:
                final_validator()
            os.link(
                temporary_name,
                "report.json",
                src_dir_fd=report_dir_fd,
                dst_dir_fd=report_dir_fd,
                follow_symlinks=False,
            )
            published = True
            self._report_publish_hook("after_publish")
            os.fsync(report_dir_fd)
            if final_validator is not None:
                final_validator()
            if (
                expected_root_fingerprint is not None
                and self.root_fingerprint(self.data_root) != expected_root_fingerprint
            ):
                raise CutoverSafetyError("Active configuration changed during report write")
            os.unlink(temporary_name, dir_fd=report_dir_fd)
            temporary_created = False
            os.fsync(report_dir_fd)
        except Exception:
            if published:
                try:
                    os.unlink("report.json", dir_fd=report_dir_fd)
                except FileNotFoundError:
                    pass
            if temporary_created:
                try:
                    os.unlink(temporary_name, dir_fd=report_dir_fd)
                except FileNotFoundError:
                    pass
            os.fsync(report_dir_fd)
            raise
        finally:
            os.close(report_dir_fd)
        return report_path

    def _try_write_report(self, report_id: str, report: dict[str, object]) -> None:
        try:
            self._write_report(report_id, report)
        except Exception:
            pass

    def _read_report(self, report_id: str) -> dict[str, object]:
        path = self.operations_root / "reports" / report_id / "report.json"
        relative = self._managed_relative(path)
        try:
            report_mode = self._managed().stat(relative).st_mode
        except FileNotFoundError:
            raise CutoverSafetyError("An exact passing rehearsal report is required")
        if stat.S_ISLNK(report_mode) or not stat.S_ISREG(report_mode):
            raise CutoverSafetyError("Rehearsal report is invalid")
        try:
            value = json.loads(self._managed().read_bytes(relative).decode("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError("Rehearsal report is unreadable") from exc
        if not isinstance(value, dict):
            raise CutoverSafetyError("Rehearsal report is invalid")
        return value
