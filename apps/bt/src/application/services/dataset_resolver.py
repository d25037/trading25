"""Dataset snapshot resolver with compatibility fallback."""

from __future__ import annotations

import os
import re
import threading

from src.infrastructure.db.market.dataset_db import DatasetDb
from src.infrastructure.db.market.dataset_snapshot_reader import DatasetSnapshotReader

_DATASET_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


class DatasetResolver:
    """Dataset 名から DatasetDb インスタンスを解決（キャッシュ付き）"""

    def __init__(self, base_path: str) -> None:
        self._base_path = os.path.realpath(base_path)
        self._cache: dict[str, DatasetDb | DatasetSnapshotReader] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    @property
    def base_path(self) -> str:
        return self._base_path

    def _validate_name(self, name: str) -> str:
        """名前検証 + パストラバーサル防御。正規化された dataset 名を返す。"""
        stem = name.removesuffix(".db")
        if not _DATASET_NAME_RE.match(stem):
            raise ValueError(f"Invalid dataset name: {name}")
        real = os.path.realpath(os.path.join(self._base_path, stem))
        if not real.startswith(self._base_path + os.sep):
            raise ValueError(f"Path traversal detected: {name}")
        return stem

    def get_snapshot_dir(self, name: str) -> str:
        normalized = self._validate_name(name)
        return os.path.join(self._base_path, normalized)

    def get_db_path(self, name: str) -> str:
        """新 snapshot layout での compatibility dataset.db パス。"""
        return os.path.join(self.get_snapshot_dir(name), "dataset.db")

    def get_duckdb_path(self, name: str) -> str:
        return os.path.join(self.get_snapshot_dir(name), "dataset.duckdb")

    def get_manifest_path(self, name: str) -> str:
        return os.path.join(self.get_snapshot_dir(name), "manifest.v1.json")

    def get_legacy_db_path(self, name: str) -> str:
        normalized = self._validate_name(name)
        return os.path.join(self._base_path, f"{normalized}.db")

    def _snapshot_has_supported_artifacts(self, snapshot_dir: str) -> bool:
        return os.path.exists(os.path.join(snapshot_dir, "dataset.duckdb")) or os.path.exists(
            os.path.join(snapshot_dir, "dataset.db")
        )

    def get_dataset_path(self, name: str) -> str:
        snapshot_dir = self.get_snapshot_dir(name)
        if self._snapshot_has_supported_artifacts(snapshot_dir):
            return snapshot_dir
        return self.get_legacy_db_path(name)

    def exists(self, name: str) -> bool:
        snapshot_dir = self.get_snapshot_dir(name)
        return self._snapshot_has_supported_artifacts(snapshot_dir) or os.path.exists(
            self.get_legacy_db_path(name)
        )

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

    def resolve(self, name: str) -> DatasetDb | DatasetSnapshotReader | None:
        """名前から snapshot reader を解決。存在しない場合は None。"""
        normalized = self._validate_name(name)
        snapshot_dir = self.get_snapshot_dir(normalized)
        snapshot_duckdb = self.get_duckdb_path(normalized)
        snapshot_db = self.get_db_path(normalized)
        legacy_db = self.get_legacy_db_path(normalized)
        has_snapshot = self._snapshot_has_supported_artifacts(snapshot_dir)
        if not has_snapshot and not os.path.exists(legacy_db):
            return None
        with self._global_lock:
            if normalized not in self._cache:
                if os.path.exists(snapshot_duckdb):
                    self._cache[normalized] = DatasetSnapshotReader(snapshot_dir)
                elif os.path.exists(snapshot_db):
                    self._cache[normalized] = DatasetDb(snapshot_db)
                else:
                    self._cache[normalized] = DatasetDb(legacy_db)
                self._locks[normalized] = threading.Lock()
        return self._cache[normalized]

    def list_datasets(self) -> list[str]:
        """利用可能なデータセット名一覧を返す。snapshot directory を優先する。"""
        if not os.path.isdir(self._base_path):
            return []
        names: list[str] = []
        seen: set[str] = set()
        for entry in sorted(os.listdir(self._base_path)):
            path = os.path.join(self._base_path, entry)
            if os.path.isdir(path) and _DATASET_NAME_RE.match(entry):
                if self._snapshot_has_supported_artifacts(path):
                    names.append(entry)
                    seen.add(entry)
                continue
            if entry.endswith(".db") and _DATASET_NAME_RE.match(entry.removesuffix(".db")):
                stem = entry.removesuffix(".db")
                if stem in seen:
                    continue
                names.append(stem)
        return names

    def evict(self, name: str) -> None:
        """DELETE 時: キャッシュから削除してクローズ。"""
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
        """lifespan shutdown 時: 全キャッシュをクローズ。"""
        with self._global_lock:
            for db in self._cache.values():
                db.close()
            self._cache.clear()
            self._locks.clear()
