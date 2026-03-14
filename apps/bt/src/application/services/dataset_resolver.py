"""DuckDB-only dataset snapshot resolver."""

from __future__ import annotations

import os
import threading

from src.infrastructure.db.market.dataset_snapshot_reader import DatasetSnapshotReader
from src.shared.utils.snapshot_ids import normalize_dataset_snapshot_name


class DatasetResolver:
    """Resolve dataset snapshots backed by `dataset.duckdb + manifest.v2.json`."""

    def __init__(self, base_path: str) -> None:
        self._base_path = os.path.realpath(base_path)
        self._cache: dict[str, DatasetSnapshotReader] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    @property
    def base_path(self) -> str:
        return self._base_path

    def _validate_name(self, name: str) -> str:
        stem = normalize_dataset_snapshot_name(name)
        if stem is None:
            raise ValueError(f"Invalid dataset name: {name}")
        real = os.path.realpath(os.path.join(self._base_path, stem))
        if not real.startswith(self._base_path + os.sep):
            raise ValueError(f"Path traversal detected: {name}")
        return stem

    def get_snapshot_dir(self, name: str) -> str:
        normalized = self._validate_name(name)
        return os.path.join(self._base_path, normalized)

    def get_duckdb_path(self, name: str) -> str:
        return os.path.join(self.get_snapshot_dir(name), "dataset.duckdb")

    def get_manifest_path(self, name: str) -> str:
        return os.path.join(self.get_snapshot_dir(name), "manifest.v2.json")

    def get_dataset_path(self, name: str) -> str:
        return self.get_snapshot_dir(name)

    def get_legacy_db_path(self, name: str) -> str:
        normalized = self._validate_name(name)
        return os.path.join(self._base_path, f"{normalized}.db")

    def _snapshot_is_supported(self, snapshot_dir: str) -> bool:
        return os.path.exists(os.path.join(snapshot_dir, "dataset.duckdb")) and os.path.exists(
            os.path.join(snapshot_dir, "manifest.v2.json")
        )

    def exists(self, name: str) -> bool:
        return self._snapshot_is_supported(self.get_snapshot_dir(name))

    def get_artifact_paths(self, name: str) -> list[str]:
        normalized = self._validate_name(name)
        snapshot_dir = self.get_snapshot_dir(normalized)
        paths: list[str] = []
        if os.path.isdir(snapshot_dir):
            paths.append(snapshot_dir)

        legacy_db = self.get_legacy_db_path(normalized)
        if os.path.exists(legacy_db):
            paths.append(legacy_db)
        return paths

    def resolve(self, name: str) -> DatasetSnapshotReader | None:
        normalized = self._validate_name(name)
        snapshot_dir = self.get_snapshot_dir(normalized)
        if not self._snapshot_is_supported(snapshot_dir):
            return None
        with self._global_lock:
            if normalized not in self._cache:
                self._cache[normalized] = DatasetSnapshotReader(snapshot_dir)
                self._locks[normalized] = threading.Lock()
        return self._cache[normalized]

    def list_datasets(self) -> list[str]:
        if not os.path.isdir(self._base_path):
            return []
        names: list[str] = []
        for entry in sorted(os.listdir(self._base_path)):
            path = os.path.join(self._base_path, entry)
            if not os.path.isdir(path):
                continue
            try:
                normalized = normalize_dataset_snapshot_name(entry)
            except ValueError:
                normalized = None
            if normalized is None:
                continue
            if self._snapshot_is_supported(path):
                names.append(normalized)
        return names

    def evict(self, name: str) -> None:
        normalized = self._validate_name(name)
        with self._global_lock:
            lock = self._locks.get(normalized)
            db = self._cache.pop(normalized, None)
            if lock:
                self._locks.pop(normalized, None)
        if db and lock:
            with lock:
                db.close()

    def close_all(self) -> None:
        with self._global_lock:
            for db in self._cache.values():
                db.close()
            self._cache.clear()
            self._locks.clear()
