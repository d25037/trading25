"""Focused Market v5 cutover responsibility module."""

from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import re
import stat
import sys

from src.infrastructure.db.market import managed_root as _managed_root

_DIR_OPEN_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def validate_operation_id(value: str | None, *, label: str) -> str:
    """Validate a caller-owned durable operation identifier."""

    if not value:
        raise _managed_root.CutoverSafetyError(f"An explicit {label} ID is required")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
        raise _managed_root.CutoverSafetyError(f"Invalid {label} ID")
    return value


def write_all(fd: int, payload: bytes) -> None:
    """Write an entire payload to an already-confined descriptor."""

    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _safe_relative_parts(  # pyright: ignore[reportUnusedFunction]
    relative: Path,
) -> tuple[str, ...]:
    if relative.is_absolute() or not relative.parts:
        raise _managed_root.CutoverSafetyError(
            "Managed path must be a non-empty relative path"
        )
    if any(part in {"", ".", ".."} or "/" in part for part in relative.parts):
        raise _managed_root.CutoverSafetyError(
            "Managed path contains an unsafe component"
        )
    return relative.parts


def _rename_exclusive_at(  # pyright: ignore[reportUnusedFunction]
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
        function.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_int
        result = function(source_dir_fd, source, target_dir_fd, target, 0x00000004)
    elif hasattr(libc, "renameat2"):
        function = libc.renameat2
        function.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_int
        result = function(source_dir_fd, source, target_dir_fd, target, 1)
    else:
        raise _managed_root.CutoverSafetyError(
            "Atomic no-replace directory rename is unavailable"
        )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), target_name)


class DarwinAtomicExchange:
    """Darwin descriptor-relative directory exchange with fail-closed guards."""

    RENAME_SWAP = 0x2

    @staticmethod
    def require_capability() -> object:
        if sys.platform != "darwin":
            raise _managed_root.CutoverSafetyError(
                "Atomic directory exchange is unavailable"
            )
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            rename_swap = libc.renameatx_np
        except AttributeError as exc:
            raise _managed_root.CutoverSafetyError(
                "Atomic directory exchange is unavailable"
            ) from exc
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
            raise _managed_root.CutoverSafetyError(
                "Atomic exchange parent must be a real directory"
            )
        return directory_stat.st_dev, directory_stat.st_ino

    @staticmethod
    def _leaf_stat(parent_fd: int, name: str) -> os.stat_result:
        try:
            leaf_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise _managed_root.CutoverSafetyError(
                "Atomic exchange leaf is unavailable"
            ) from exc
        if stat.S_ISLNK(leaf_stat.st_mode) or not stat.S_ISDIR(leaf_stat.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Atomic exchange leaf must be a real directory"
            )
        return leaf_stat

    @staticmethod
    def _open_leaf(parent_fd: int, name: str) -> int:
        try:
            leaf_fd = os.open(name, _DIR_OPEN_FLAGS, dir_fd=parent_fd)
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise _managed_root.CutoverSafetyError(
                    "Atomic exchange leaf must be a real directory"
                ) from exc
            raise _managed_root.CutoverSafetyError(
                "Atomic exchange leaf is unavailable"
            ) from exc
        if not stat.S_ISDIR(os.fstat(leaf_fd).st_mode):
            os.close(leaf_fd)
            raise _managed_root.CutoverSafetyError(
                "Atomic exchange leaf must be a real directory"
            )
        return leaf_fd

    @classmethod
    def _assert_parent_identity(
        cls,
        managed_root: _managed_root.ManagedRootFd,
        relative: Path,
        retained_fd: int,
    ) -> None:
        current_fd, _ = managed_root.open_parent(relative)
        try:
            if cls._directory_identity(current_fd) != cls._directory_identity(
                retained_fd
            ):
                raise _managed_root.CutoverSafetyError(
                    "Atomic exchange parent identity changed"
                )
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
            raise _managed_root.CutoverSafetyError(
                "Atomic exchange leaf identity changed"
            )
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
        managed_root: _managed_root.ManagedRootFd,
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
            left_leaf = self._assert_leaf_identity(left_parent, left_name, left_leaf_fd)
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
                raise _managed_root.CutoverSafetyError(
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
                    raise _managed_root.CutoverSafetyError(
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
