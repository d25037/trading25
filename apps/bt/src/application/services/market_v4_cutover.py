"""Gated Market v4 cutover workflow."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import ctypes
import errno
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
from typing import BinaryIO, Callable, ContextManager, Iterator, Protocol
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
                pass_fds=(root_fd, market_fd, lease_fd),
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
    ) -> None:
        self.data_root = _lexical_absolute(data_root)
        self.duckdb = duckdb
        self.runtime = runtime
        self.disk_free_bytes = disk_free_bytes
        self.now = now
        self.code_version = code_version
        self._active_lease: MarketOperationLease | None = None
        self._active_code_version: str | None = None
        self._managed_root_fd: ManagedRootFd | None = None
        self._report_publish_hook: Callable[[str], None] = lambda _stage: None
        self._managed_mutation_hook: Callable[[str], None] = lambda _stage: None
        self._rename_at_hook: Callable[[Path, Path], None] = lambda _source, _target: None

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
                    repository_config = (
                        Path(__file__).resolve().parents[3] / "config" / "default.yaml"
                    )
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
            config = Path(__file__).resolve().parents[3] / "config" / "default.yaml"
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
        base_fd = self._managed().open_dir(self._managed_relative(base))
        os.close(base_fd)
        root_fd = self._managed().open_dir(self._managed_relative(root))
        os.close(root_fd)

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
            current_fd = self._managed().open_dir(
                self._managed_relative(retained_root)
            )
        except (FileNotFoundError, CutoverSafetyError) as exc:
            raise CutoverSafetyError(
                "Retained rehearsal root identity changed"
            ) from exc
        try:
            current = os.fstat(current_fd)
        finally:
            os.close(current_fd)
        if (
            not stat.S_ISDIR(current.st_mode)
            or (retained.st_dev, retained.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise CutoverSafetyError("Retained rehearsal root identity changed")

    @staticmethod
    def _regular_file_identity(
        managed: ManagedRootFd,
        relative: Path,
    ) -> tuple[os.stat_result, str]:
        try:
            file_fd = managed.open_regular(relative, os.O_RDONLY)
        except (FileNotFoundError, OSError) as exc:
            raise CutoverSafetyError(
                "Retained Market file changed during identity hashing"
            ) from exc
        digest = hashlib.sha256()
        try:
            before = os.fstat(file_fd)
            if not stat.S_ISREG(before.st_mode):
                raise CutoverSafetyError("Retained Market file must be regular")
            while chunk := os.read(file_fd, 1024 * 1024):
                digest.update(chunk)
            after = os.fstat(file_fd)
            try:
                current = managed.stat(relative)
            except FileNotFoundError as exc:
                raise CutoverSafetyError(
                    "Retained Market file changed during identity hashing"
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
                raise CutoverSafetyError(
                    "Retained Market file changed during identity hashing"
                )
            return before, digest.hexdigest()
        finally:
            os.close(file_fd)

    @staticmethod
    def _market_tree_identity(root_fd: int) -> dict[str, object]:
        with ManagedRootFd(Path("."), os.dup(root_fd)) as retained:
            database_relative = Path("market-timeseries/market.duckdb")
            database_stat, database_sha256 = (
                MarketV4CutoverService._regular_file_identity(
                    retained,
                    database_relative,
                )
            )
            parquet_relative = Path("market-timeseries/parquet")
            try:
                parquet_files = retained.regular_files(parquet_relative)
            except FileNotFoundError as exc:
                raise CutoverSafetyError("Retained Market parquet tree is missing") from exc
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
            source_configuration_fingerprint = self._configuration_fingerprint_at(
                root_fd
            )
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
            _config_metadata, config_payload_sha256 = self._regular_file_identity(
                retained,
                Path("config/default.yaml"),
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
            if (
                self._configuration_fingerprint_at(root_fd)
                != source_configuration_fingerprint
            ):
                raise CutoverSafetyError(
                    "Retained configuration changed during runtime snapshot"
                )
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
