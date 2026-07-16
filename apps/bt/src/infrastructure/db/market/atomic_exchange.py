"""Descriptor-confined atomic exchange for managed directories and regular files."""

from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import stat
import sys
from typing import Literal

from .managed_root import ManagedRootError, ManagedRootFd


_DIR_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)


class PlatformAtomicExchange:
    """Platform atomic exchange with retained descriptor guards."""

    RENAME_SWAP = 0x2
    RENAME_EXCHANGE = 0x2

    @staticmethod
    def require_capability() -> object:
        libc = ctypes.CDLL(None, use_errno=True)
        if sys.platform == "darwin":
            name = "renameatx_np"
        elif sys.platform.startswith("linux"):
            name = "renameat2"
        else:
            raise ManagedRootError("Atomic exchange is unavailable")
        try:
            rename_swap = getattr(libc, name)
        except AttributeError as exc:
            raise ManagedRootError("Atomic exchange is unavailable") from exc
        rename_swap.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename_swap.restype = ctypes.c_int
        return rename_swap

    def exchange_directories(
        self, managed_root: ManagedRootFd, left: Path, right: Path
    ) -> None:
        self._exchange(
            managed_root,
            left,
            right,
            leaf_kind="directory",
            expected_right_parent_identity=None,
        )

    def exchange_regular_files(
        self,
        managed_root: ManagedRootFd,
        left: Path,
        right: Path,
        *,
        expected_right_parent_identity: tuple[int, int] | None = None,
    ) -> None:
        self._exchange(
            managed_root,
            left,
            right,
            leaf_kind="regular file",
            expected_right_parent_identity=expected_right_parent_identity,
        )

    @staticmethod
    def _open_leaf(parent_fd: int, name: str, kind: Literal["directory", "regular file"]) -> int:
        flags = _DIR_FLAGS if kind == "directory" else _FILE_FLAGS
        try:
            leaf_fd = os.open(name, flags, dir_fd=parent_fd)
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                raise ManagedRootError(f"Atomic exchange leaf must be a {kind}") from exc
            raise ManagedRootError("Atomic exchange leaf is unavailable") from exc
        leaf_stat = os.fstat(leaf_fd)
        valid = stat.S_ISDIR(leaf_stat.st_mode) if kind == "directory" else stat.S_ISREG(leaf_stat.st_mode)
        if not valid:
            os.close(leaf_fd)
            raise ManagedRootError(f"Atomic exchange leaf must be a {kind}")
        return leaf_fd

    @staticmethod
    def _parent_identity(parent_fd: int) -> tuple[int, int]:
        parent_stat = os.fstat(parent_fd)
        if not stat.S_ISDIR(parent_stat.st_mode):
            raise ManagedRootError("Atomic exchange parent must be a real directory")
        return parent_stat.st_dev, parent_stat.st_ino

    @classmethod
    def _assert_path_identity(
        cls, managed_root: ManagedRootFd, relative: Path, parent_fd: int
    ) -> None:
        current_fd, _ = managed_root.open_parent(relative)
        try:
            if cls._parent_identity(current_fd) != cls._parent_identity(parent_fd):
                raise ManagedRootError("Atomic exchange parent identity changed")
        finally:
            os.close(current_fd)

    @staticmethod
    def _assert_leaf_identity(parent_fd: int, name: str, expected_fd: int) -> None:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        expected = os.fstat(expected_fd)
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
            raise ManagedRootError("Atomic exchange leaf identity changed")

    @staticmethod
    def _fsync_parents(left_parent: int, right_parent: int) -> None:
        failures: list[OSError] = []
        seen: set[tuple[int, int]] = set()
        for parent in (left_parent, right_parent):
            identity = PlatformAtomicExchange._parent_identity(parent)
            if identity in seen:
                continue
            seen.add(identity)
            try:
                os.fsync(parent)
            except OSError as exc:
                failures.append(exc)
        if failures:
            primary = failures[0]
            for additional in failures[1:]:
                primary.add_note(f"Additional parent fsync failure: {additional}")
            raise primary

    def _exchange(
        self,
        managed_root: ManagedRootFd,
        left: Path,
        right: Path,
        *,
        leaf_kind: Literal["directory", "regular file"],
        expected_right_parent_identity: tuple[int, int] | None,
    ) -> None:
        rename_swap = self.require_capability()
        left_parent, left_name = managed_root.open_parent(left)
        try:
            right_parent, right_name = managed_root.open_parent(right)
        except Exception:
            os.close(left_parent)
            raise
        left_leaf = right_leaf = -1
        try:
            if (
                expected_right_parent_identity is not None
                and self._parent_identity(right_parent) != expected_right_parent_identity
            ):
                raise ManagedRootError("Atomic exchange candidate parent identity changed")
            left_leaf = self._open_leaf(left_parent, left_name, leaf_kind)
            right_leaf = self._open_leaf(right_parent, right_name, leaf_kind)
            self._assert_path_identity(managed_root, left, left_parent)
            self._assert_path_identity(managed_root, right, right_parent)
            self._assert_leaf_identity(left_parent, left_name, left_leaf)
            self._assert_leaf_identity(right_parent, right_name, right_leaf)
            devices = {
                self._parent_identity(left_parent)[0],
                self._parent_identity(right_parent)[0],
                os.fstat(left_leaf).st_dev,
                os.fstat(right_leaf).st_dev,
            }
            if len(devices) != 1:
                raise ManagedRootError("Atomic exchange leaves must be on the same device")
            flags = self.RENAME_SWAP if sys.platform == "darwin" else self.RENAME_EXCHANGE
            result = rename_swap(
                left_parent,
                os.fsencode(left_name),
                right_parent,
                os.fsencode(right_name),
                flags,
            )
            if result != 0:
                error = ctypes.get_errno()
                if error in {errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EXDEV}:
                    raise ManagedRootError("Atomic exchange is unsupported")
                raise OSError(error, os.strerror(error))
            self._assert_path_identity(managed_root, left, left_parent)
            self._assert_path_identity(managed_root, right, right_parent)
            self._assert_leaf_identity(left_parent, left_name, right_leaf)
            self._assert_leaf_identity(right_parent, right_name, left_leaf)
            self._fsync_parents(left_parent, right_parent)
        finally:
            if left_leaf >= 0:
                os.close(left_leaf)
            if right_leaf >= 0:
                os.close(right_leaf)
            os.close(left_parent)
            os.close(right_parent)
