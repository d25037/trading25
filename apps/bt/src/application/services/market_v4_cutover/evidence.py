"""Market root and configuration identity evidence."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat

from src.infrastructure.db.market import managed_root as _managed_root

from .project_paths import repository_default_config_path
from .workspace import CutoverWorkspace


class MarketEvidence:
    def __init__(self, workspace: CutoverWorkspace) -> None:
        self._workspace = workspace

    def configuration_fingerprint(self, root: Path) -> str:
        root = _managed_root.lexical_absolute(root)
        if self._workspace._managed_root_fd is not None:
            try:
                root_relative = root.relative_to(self._workspace.data_root)
            except ValueError:
                pass
            else:
                digest = hashlib.sha256()
                config_relative = root_relative / "config" / "default.yaml"
                try:
                    config_stat = self._workspace.managed().stat(config_relative)
                except FileNotFoundError:
                    config_sha = self._workspace._sha256(
                        self._repository_default_config_path()
                    )
                else:
                    if not stat.S_ISREG(config_stat.st_mode):
                        raise _managed_root.CutoverSafetyError(
                            "Fingerprint config is not regular"
                        )
                    config_sha = self._workspace.managed().sha256(config_relative)
                digest.update(b"config/default.yaml\0")
                digest.update(config_sha.encode())
                digest.update(b"\n")
                strategies_relative = root_relative / "strategies"
                try:
                    strategy_files = self._workspace.managed().regular_files(
                        strategies_relative
                    )
                except FileNotFoundError:
                    strategy_files = []
                for relative, _entry_stat in strategy_files:
                    label = f"strategies/{relative.as_posix()}"
                    digest.update(label.encode())
                    digest.update(b"\0")
                    digest.update(
                        self._workspace.managed()
                        .sha256(strategies_relative / relative)
                        .encode()
                    )
                    digest.update(b"\n")
                return digest.hexdigest()
        _managed_root.assert_real_directory(root, "Fingerprint root")
        _managed_root.assert_safe_directory_chain(root)
        digest = hashlib.sha256()
        candidates: list[tuple[str, Path]] = []
        config = root / "config" / "default.yaml"
        if not config.is_file():
            config = self._repository_default_config_path()
        candidates.append(("config/default.yaml", config))
        strategies = root / "strategies"
        if strategies.exists():
            _managed_root.assert_real_directory(strategies, "Strategies root")
            for path in sorted(strategies.rglob("*")):
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise _managed_root.CutoverSafetyError(
                        "Strategy fingerprint source contains symlink"
                    )
                if stat.S_ISDIR(mode):
                    continue
                if not stat.S_ISREG(mode):
                    raise _managed_root.CutoverSafetyError(
                        "Strategy fingerprint source contains special file"
                    )
                candidates.append(
                    (f"strategies/{path.relative_to(strategies).as_posix()}", path)
                )
        for label, path in candidates:
            if path.is_symlink() or not path.is_file():
                raise _managed_root.CutoverSafetyError(
                    f"Fingerprint source is invalid: {label}"
                )
            digest.update(label.encode())
            digest.update(b"\0")
            digest.update(self._workspace._sha256(path).encode())
            digest.update(b"\n")
        return digest.hexdigest()

    @staticmethod
    def _repository_default_config_path() -> Path:
        config = repository_default_config_path()
        try:
            config_stat = config.lstat()
        except FileNotFoundError as exc:
            raise _managed_root.CutoverSafetyError(
                "Repository default configuration is missing"
            ) from exc
        if stat.S_ISLNK(config_stat.st_mode) or not stat.S_ISREG(config_stat.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Repository default configuration must be a regular file"
            )
        return config

    def root_fingerprint(self, root: Path) -> str:
        root = _managed_root.lexical_absolute(root)
        if self._workspace._managed_root_fd is not None:
            try:
                relative = root.relative_to(self._workspace.data_root)
            except ValueError:
                relative = None
            if relative is not None:
                root_fd = (
                    os.dup(self._workspace.managed().fd)
                    if not relative.parts
                    else self._workspace.managed().open_dir(relative)
                )
                try:
                    root_stat = os.fstat(root_fd)
                finally:
                    os.close(root_fd)
            else:
                _managed_root.assert_safe_directory_chain(root)
                _managed_root.assert_real_directory(root, "Fingerprint root")
                root_stat = root.lstat()
        else:
            _managed_root.assert_safe_directory_chain(root)
            _managed_root.assert_real_directory(root, "Fingerprint root")
            root_stat = root.lstat()
        digest = hashlib.sha256(
            f"dev={root_stat.st_dev};ino={root_stat.st_ino}\n".encode()
        )
        digest.update(self.configuration_fingerprint(root).encode())
        digest.update(b"\n")
        return digest.hexdigest()
