"""
Dataset Resolver

Dataset 名から DatasetDb インスタンスを解決するキャッシュ付きリゾルバ。
パストラバーサル防御 + per-dataset ロック付き。
"""

from __future__ import annotations

import os
import re
import threading

from src.infrastructure.db.market.dataset_db import DatasetDb

_DATASET_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


class DatasetResolver:
    """Dataset 名から DatasetDb インスタンスを解決（キャッシュ付き）"""

    def __init__(self, base_path: str) -> None:
        self._base_path = os.path.realpath(base_path)
        self._cache: dict[str, DatasetDb] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    @property
    def base_path(self) -> str:
        return self._base_path

    def _validate_name(self, name: str) -> str:
        """名前検証 + パストラバーサル防御。正規化された .db ファイル名を返す。"""
        stem = name.removesuffix(".db")
        if not _DATASET_NAME_RE.match(stem):
            raise ValueError(f"Invalid dataset name: {name}")
        normalized = f"{stem}.db"
        db_path = os.path.join(self._base_path, normalized)
        real = os.path.realpath(db_path)
        if not real.startswith(self._base_path + os.sep):
            raise ValueError(f"Path traversal detected: {name}")
        return normalized

    def resolve(self, name: str) -> DatasetDb | None:
        """名前から DatasetDb を解決。存在しない場合は None。"""
        normalized = self._validate_name(name)
        db_path = os.path.join(self._base_path, normalized)
        if not os.path.exists(db_path):
            return None
        with self._global_lock:
            if normalized not in self._cache:
                self._cache[normalized] = DatasetDb(db_path)
                self._locks[normalized] = threading.Lock()
        return self._cache[normalized]

    def list_datasets(self) -> list[str]:
        """利用可能なデータセット名一覧（.db 拡張子なし）を返す。"""
        if not os.path.isdir(self._base_path):
            return []
        names: list[str] = []
        for entry in sorted(os.listdir(self._base_path)):
            if entry.endswith(".db") and _DATASET_NAME_RE.match(entry.removesuffix(".db")):
                names.append(entry.removesuffix(".db"))
        return names

    def get_db_path(self, name: str) -> str:
        """バリデーション済みの DB ファイルパスを返す。"""
        normalized = self._validate_name(name)
        return os.path.join(self._base_path, normalized)

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
