"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Callable

from src.infrastructure.db.market import managed_root as _managed_root

from .filesystem import (
    _DIR_OPEN_FLAGS,
    _safe_relative_parts,
)


class DuckDbServiceMixin:
    def _require_confined_real_directory(self, root: Path, base: Path) -> None:
        root = self._managed_path(root)
        base = self._managed_path(base)
        try:
            root.relative_to(base)
        except ValueError as exc:
            raise _managed_root.CutoverSafetyError(
                "Managed directory escapes its required root"
            ) from exc
        try:
            base_fd = self._managed().open_dir(self._managed_relative(base))
            os.close(base_fd)
            root_fd = self._managed().open_dir(self._managed_relative(root))
            os.close(root_fd)
        except (FileNotFoundError, _managed_root.CutoverSafetyError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Managed directory is unavailable"
            ) from exc

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
        except (FileNotFoundError, _managed_root.CutoverSafetyError) as exc:
            raise _managed_root.CutoverSafetyError(
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
            raise _managed_root.CutoverSafetyError(
                "Retained rehearsal root identity changed"
            )

    @staticmethod
    def _regular_file_identity(
        managed: _managed_root.ManagedRootFd,
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
        ) -> _managed_root.CutoverSafetyError:
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
            return _managed_root.CutoverSafetyError(
                "Retained Market file changed during identity hashing: "
                f"path={canonical_relative}; failure={failure_class}; "
                f"metadata={json.dumps(diagnostic, sort_keys=True, separators=(',', ':'))}"
            )

        try:
            file_fd = managed.open_regular(relative, os.O_RDONLY)
        except (FileNotFoundError, OSError, _managed_root.CutoverSafetyError) as exc:
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
        with _managed_root.ManagedRootFd(Path("."), os.dup(market_fd)) as retained:
            database_relative = Path("market.duckdb")
            database_stat, database_sha256 = DuckDbServiceMixin._regular_file_identity(
                retained,
                database_relative,
            )
            parquet_relative = Path("parquet")
            try:
                parquet_files = retained.regular_files(parquet_relative)
            except FileNotFoundError as exc:
                raise _managed_root.CutoverSafetyError(
                    "Retained Market parquet tree is missing"
                ) from exc
            parquet_paths = tuple(relative for relative, _entry in parquet_files)
            parquet_sha256: dict[str, dict[str, object]] = {}
            for relative in parquet_paths:
                parquet_stat, parquet_digest = (
                    DuckDbServiceMixin._regular_file_identity(
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
                raise _managed_root.CutoverSafetyError(
                    "Retained Market parquet tree changed during identity hashing"
                ) from exc
            if current_parquet_paths != parquet_paths:
                raise _managed_root.CutoverSafetyError(
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
            return DuckDbServiceMixin._market_payload_identity(market_fd)
        finally:
            os.close(market_fd)

    @staticmethod
    def _configuration_fingerprint_at(root_fd: int) -> str:
        with _managed_root.ManagedRootFd(Path("."), os.dup(root_fd)) as retained:
            digest = hashlib.sha256()
            config_relative = Path("config/default.yaml")
            config_stat, config_sha256 = DuckDbServiceMixin._regular_file_identity(
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
                _metadata, strategy_sha256 = DuckDbServiceMixin._regular_file_identity(
                    retained,
                    Path("strategies") / relative,
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
                raise _managed_root.CutoverSafetyError(
                    "Retained strategies changed during fingerprinting"
                )
            return digest.hexdigest()

    @staticmethod
    def _root_fingerprint_at(root_fd: int) -> str:
        root_stat = os.fstat(root_fd)
        if not stat.S_ISDIR(root_stat.st_mode):
            raise _managed_root.CutoverSafetyError(
                "Retained rehearsal root is not a directory"
            )
        digest = hashlib.sha256(
            f"dev={root_stat.st_dev};ino={root_stat.st_ino}\n".encode()
        )
        digest.update(
            DuckDbServiceMixin._configuration_fingerprint_at(root_fd).encode()
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
            with _managed_root.ManagedRootFd.open(retained_root) as retained:
                self._prepare_retained_runtime(
                    retained_root,
                    runtime_name=runtime_name,
                    root_fd=retained.fd,
                    on_reserved=on_reserved,
                )
            return
        self._assert_retained_root_identity(retained_root, root_fd)
        with _managed_root.ManagedRootFd(Path("."), os.dup(root_fd)) as retained:
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
                        raise _managed_root.CutoverSafetyError(
                            "Operation code identity is unavailable"
                        )
                    self._require_unchanged_code_identity(self._active_code_version)
            source_configuration_fingerprint = source_fingerprint()
            runtime_relative = Path("market-timeseries") / runtime_name
            try:
                runtime_fd = retained.open_dir(
                    runtime_relative,
                    create=True,
                    exclusive_leaf=True,
                )
            except FileExistsError as exc:
                raise _managed_root.CutoverSafetyError(
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
                _config_metadata, config_payload_sha256 = self._regular_file_identity(
                    retained,
                    Path("config/default.yaml"),
                )
                config_payload = retained.read_bytes(Path("config/default.yaml"))
                if hashlib.sha256(config_payload).hexdigest() != config_payload_sha256:
                    raise _managed_root.CutoverSafetyError(
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
                raise _managed_root.CutoverSafetyError(
                    "Retained configuration changed during runtime snapshot"
                )
            if repository_config is not None:
                assert self._active_code_version is not None
                self._require_unchanged_code_identity(self._active_code_version)
            runtime_fd = retained.open_dir(runtime_relative)
            try:
                runtime_configuration_fingerprint = self._configuration_fingerprint_at(
                    runtime_fd
                )
            finally:
                os.close(runtime_fd)
            if runtime_configuration_fingerprint != source_configuration_fingerprint:
                raise _managed_root.CutoverSafetyError(
                    "Retained runtime configuration snapshot is incoherent"
                )
        self._assert_retained_root_identity(retained_root, root_fd)
