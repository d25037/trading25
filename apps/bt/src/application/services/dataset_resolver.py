"""DuckDB-only dataset snapshot resolver."""

from __future__ import annotations

import os
from pathlib import Path
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
        self._validation_cache: dict[tuple[str, str], DatasetValidationProof] = {}
        self._retired: list[tuple[str, DatasetSnapshotReader]] = []
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

    def _validation_cache_key(
        self, normalized: str, snapshot_dir: str
    ) -> tuple[str, str]:
        return (normalized, os.path.realpath(snapshot_dir))

    def _validation_proof(
        self, normalized: str, snapshot_dir: str
    ) -> DatasetValidationProof | None:
        requested_path = str(Path(snapshot_dir).absolute())
        cache_key = self._validation_cache_key(normalized, requested_path)
        with self._global_lock:
            try:
                fingerprint = build_dataset_artifact_fingerprint(requested_path)
            except Exception:
                self._invalidate_snapshot_locked(normalized, cache_key)
                return None
            cached = self._validation_cache.get(cache_key)
            if cached is not None and cached.fingerprint == fingerprint:
                return cached
            self._invalidate_snapshot_locked(normalized, cache_key)
            try:
                proof = validate_supported_dataset_snapshot_proof(requested_path)
                if build_dataset_artifact_fingerprint(requested_path) != proof.fingerprint:
                    return None
            except Exception:
                return None
            self._validation_cache[cache_key] = proof
            return proof

    def _invalidate_snapshot_locked(
        self, normalized: str, cache_key: tuple[str, str]
    ) -> None:
        self._validation_cache.pop(cache_key, None)
        reader = self._cache.pop(normalized, None)
        self._locks.pop(normalized, None)
        if reader is not None:
            self._retired.append((normalized, reader))

    def _snapshot_is_supported(self, normalized: str, snapshot_dir: str) -> bool:
        return self._validation_proof(normalized, snapshot_dir) is not None

    def exists(self, name: str) -> bool:
        normalized = self._validate_name(name)
        return self._snapshot_is_supported(
            normalized, self.get_snapshot_dir(normalized)
        )

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
        proof = self._validation_proof(normalized, snapshot_dir)
        if proof is None:
            return None
        with self._global_lock:
            try:
                if build_dataset_artifact_fingerprint(snapshot_dir) != proof.fingerprint:
                    cache_key = self._validation_cache_key(normalized, snapshot_dir)
                    self._invalidate_snapshot_locked(normalized, cache_key)
                    return self.resolve(normalized)
            except Exception:
                cache_key = self._validation_cache_key(normalized, snapshot_dir)
                self._invalidate_snapshot_locked(normalized, cache_key)
                return None
            if normalized not in self._cache:
                self._cache[normalized] = DatasetSnapshotReader._from_validation_proof(
                    proof
                )
                self._locks[normalized] = threading.Lock()
            reader = self._cache[normalized]
            try:
                if build_dataset_artifact_fingerprint(snapshot_dir) != proof.fingerprint:
                    cache_key = self._validation_cache_key(normalized, snapshot_dir)
                    self._invalidate_snapshot_locked(normalized, cache_key)
                    return self.resolve(normalized)
            except Exception:
                cache_key = self._validation_cache_key(normalized, snapshot_dir)
                self._invalidate_snapshot_locked(normalized, cache_key)
                return None
            return reader

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
            if self._snapshot_is_supported(normalized, path):
                names.append(normalized)
        return names

    def evict(self, name: str) -> None:
        normalized = self._validate_name(name)
        with self._global_lock:
            lock = self._locks.get(normalized)
            db = self._cache.pop(normalized, None)
            cache_key = self._validation_cache_key(
                normalized, self.get_snapshot_dir(normalized)
            )
            self._validation_cache.pop(cache_key, None)
            retired = [reader for name, reader in self._retired if name == normalized]
            self._retired = [item for item in self._retired if item[0] != normalized]
            if lock:
                self._locks.pop(normalized, None)
        if db and lock:
            with lock:
                db.close()
        for reader in retired:
            reader.close()

    def close_all(self) -> None:
        with self._global_lock:
            readers = [*self._cache.values(), *(reader for _, reader in self._retired)]
            self._cache.clear()
            self._retired.clear()
            self._locks.clear()
            self._validation_cache.clear()
        for reader in readers:
            reader.close()
