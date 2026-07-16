"""Durable directory creation for the promotion journal."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
from typing import Callable

from src.infrastructure.db.market import managed_root as _managed_root

from .filesystem import _DIR_OPEN_FLAGS, _FILE_NOFOLLOW, _safe_relative_parts


def ensure_durable_directory(
    managed_root: _managed_root.ManagedRootFd,
    relative: Path,
    *,
    directory_fsync: Callable[[int], None],
    boundary_hook: Callable[[str], None],
) -> int:
    """Open a confined directory, durably creating each missing ancestor."""

    current = os.dup(managed_root.fd)
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
                boundary_hook(f"ancestor_child_fsync_before:{prefix}")
                directory_fsync(child)
                boundary_hook(f"ancestor_child_fsynced:{prefix}")
                boundary_hook(f"ancestor_parent_fsync_before:{prefix}")
                directory_fsync(current)
                boundary_hook(f"ancestor_parent_fsynced:{prefix}")
            os.close(current)
            current = child
        return current
    except Exception:
        os.close(current)
        raise


def directory_snapshot(
    managed_root: _managed_root.ManagedRootFd,
    relative: Path,
    *,
    skip_staging: bool,
) -> tuple[tuple[int, int], tuple[tuple[str, int, int, int, str], ...]]:
    """Capture stable directory and regular-file identities for authorization."""

    directory_fd = managed_root.open_dir(relative)
    files: list[tuple[str, int, int, int, str]] = []
    try:
        directory_stat = os.fstat(directory_fd)
        for name in sorted(os.listdir(directory_fd)):
            if skip_staging and name == "staging":
                continue
            fd = os.open(name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd)
            digest = hashlib.sha256()
            try:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode):
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal authorization identity is invalid"
                    )
                while chunk := os.read(fd, 1024 * 1024):
                    digest.update(chunk)
                after = os.fstat(fd)
                current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
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
                    raise _managed_root.CutoverSafetyError(
                        "Promotion journal authorization identity changed"
                    )
                files.append((name, *identity, digest.hexdigest()))
            finally:
                os.close(fd)
    finally:
        os.close(directory_fd)
    return (directory_stat.st_dev, directory_stat.st_ino), tuple(files)


def read_regular(directory_fd: int, name: str, *, label: str) -> bytes:
    """Read one no-follow regular file from an already confined directory."""

    try:
        fd = os.open(name, os.O_RDONLY | _FILE_NOFOLLOW, dir_fd=directory_fd)
    except OSError as exc:
        raise _managed_root.CutoverSafetyError(
            f"{label} is not a confined regular file"
        ) from exc
    chunks: list[bytes] = []
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise _managed_root.CutoverSafetyError(f"{label} must be a regular file")
        while chunk := os.read(fd, 1024 * 1024):
            chunks.append(chunk)
    finally:
        os.close(fd)
    return b"".join(chunks)
