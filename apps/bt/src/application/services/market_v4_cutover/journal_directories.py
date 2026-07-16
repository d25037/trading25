"""Durable directory creation for the promotion journal."""

from __future__ import annotations

import os
from pathlib import Path

from .filesystem import _DIR_OPEN_FLAGS, _safe_relative_parts


class JournalDirectoriesMixin:
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
