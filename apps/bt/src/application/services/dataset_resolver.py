"""DuckDB-only dataset snapshot resolver."""

from __future__ import annotations

import os
import threading

from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetValidationProof,
    DatasetSnapshotReader,
    build_dataset_artifact_fingerprint,
    validate_supported_dataset_snapshot_proof,
)
from src.shared.utils.snapshot_ids import normalize_dataset_snapshot_name


class DatasetResolver:
    """Resolve dataset snapshots backed by `dataset.duckdb + manifest.v2.json`."""

    def __init__(self, base_path: str) -> None:
        self._base_path = os.path.realpath(base_path)
        self._cache: dict[str, DatasetSnapshotReader] = {}
        self._validation_cache: dict[str, DatasetValidationProof] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.RLock()

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

    def _validation_proof(self, snapshot_dir: str) -> DatasetValidationProof | None:
        snapshot_key = os.path.realpath(snapshot_dir)
        with self._global_lock:
            try:
                fingerprint = build_dataset_artifact_fingerprint(snapshot_key)
            except Exception:
                self._invalidate_snapshot_locked(snapshot_key)
                return None
            cached = self._validation_cache.get(snapshot_key)
            if cached is not None and cached.fingerprint == fingerprint:
                return cached
            self._invalidate_snapshot_locked(snapshot_key)
            try:
                proof = validate_supported_dataset_snapshot_proof(snapshot_key)
                if build_dataset_artifact_fingerprint(snapshot_key) != proof.fingerprint:
                    return None
            except Exception:
                return None
            self._validation_cache[snapshot_key] = proof
            return proof

    def _invalidate_snapshot_locked(self, snapshot_key: str) -> None:
        self._validation_cache.pop(snapshot_key, None)
        name = os.path.basename(snapshot_key)
        reader = self._cache.pop(name, None)
        self._locks.pop(name, None)
        if reader is not None:
            reader.close()

    def _snapshot_is_supported(self, snapshot_dir: str) -> bool:
        return self._validation_proof(snapshot_dir) is not None

    def exists(self, name: str) -> bool:
        return self._snapshot_is_supported(self.get_snapshot_dir(name))

    def get_artifact_paths(self, name: str) -> list[str]:
        normalized = self._validate_name(name)
        snapshot_dir = self.get_snapshot_dir(normalized)
        paths: list[str] = []
        if os.path.isdir(snapshot_dir):
            paths.append(snapshot_dir)
        return paths

    def resolve(self, name: str) -> DatasetSnapshotReader | None:
        normalized = self._validate_name(name)
        snapshot_dir = self.get_snapshot_dir(normalized)
        proof = self._validation_proof(snapshot_dir)
        if proof is None:
            return None
        with self._global_lock:
            if normalized not in self._cache:
                try:
                    if build_dataset_artifact_fingerprint(snapshot_dir) != proof.fingerprint:
                        self._invalidate_snapshot_locked(os.path.realpath(snapshot_dir))
                        return None
                except Exception:
                    self._invalidate_snapshot_locked(os.path.realpath(snapshot_dir))
                    return None
                self._cache[normalized] = DatasetSnapshotReader._from_validation_proof(
                    proof
                )
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
            self._validation_cache.pop(
                os.path.realpath(self.get_snapshot_dir(normalized)), None
            )
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
            self._validation_cache.clear()
