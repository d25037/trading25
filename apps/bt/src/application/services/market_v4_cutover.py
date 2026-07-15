"""Gated Market v4 cutover workflow."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import sys
import time
from typing import Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class CutoverSafetyError(RuntimeError):
    """A fail-closed cutover safety gate rejected the operation."""


@dataclass(frozen=True)
class MarketSourceMetadata:
    schema_version: int | None
    adjustment_mode: str | None


@dataclass(frozen=True)
class BackupResult:
    backup_id: str


@dataclass(frozen=True)
class RestoreResult:
    backup_id: str
    quarantine_path: str | None


@dataclass(frozen=True)
class SmokeConfig:
    symbol: str
    strategy: str
    dataset_preset: str


@dataclass(frozen=True)
class SmokeResult:
    schema_version: int
    adjustment_mode: str
    checks: tuple[str, ...]
    api_paths: tuple[str, ...]
    lineage: dict[str, int]


@dataclass(frozen=True)
class OperationResult:
    report_id: str
    report_path: str


class DuckDbAdapter(Protocol):
    """Exclusive DuckDB operations used by the workflow."""

    def checkpoint_exclusive(self, db_path: Path) -> MarketSourceMetadata: ...

    def inspect(self, db_path: Path) -> MarketSourceMetadata: ...


class RuntimeAdapter(Protocol):
    """Owned server process and HTTP operations used by the workflow."""

    def assert_quiescent(self, data_root: Path) -> None: ...

    def start(
        self,
        data_root: Path,
        environment: dict[str, str],
        log_path: Path,
    ) -> ApiAdapter: ...

    def cancel_owned_work(self, api: ApiAdapter) -> None: ...

    def stop(self, api: ApiAdapter) -> None: ...


class ApiAdapter(Protocol):
    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]: ...


class DefaultDuckDbAdapter:
    """Raw DuckDB adapter that never constructs the mutating MarketDb wrapper."""

    @staticmethod
    def _metadata(connection: object) -> MarketSourceMetadata:
        execute = getattr(connection, "execute")
        try:
            schema_row = execute(
                "SELECT MAX(version) FROM market_schema_version"
            ).fetchone()
            schema_version = schema_row[0] if schema_row else None
        except Exception:
            schema_version = None
        try:
            mode_row = execute(
                "SELECT value FROM sync_metadata "
                "WHERE key = 'stock_price_adjustment_mode'"
            ).fetchone()
            adjustment_mode = mode_row[0] if mode_row else None
        except Exception:
            adjustment_mode = None
        return MarketSourceMetadata(
            schema_version=(int(schema_version) if schema_version is not None else None),
            adjustment_mode=(str(adjustment_mode) if adjustment_mode is not None else None),
        )

    def checkpoint_exclusive(self, db_path: Path) -> MarketSourceMetadata:
        import duckdb

        connection = duckdb.connect(str(db_path), read_only=False)
        try:
            metadata = self._metadata(connection)
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        return metadata

    def inspect(self, db_path: Path) -> MarketSourceMetadata:
        import duckdb

        connection = duckdb.connect(str(db_path), read_only=True)
        try:
            return self._metadata(connection)
        finally:
            connection.close()


class HttpApiAdapter:
    """Small synchronous JSON client for one owned cutover server."""

    def __init__(self, base_url: str, *, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.owned_jobs: dict[str, str] = {}

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        body = None
        headers = {"accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode()
            headers["content-type"] = "application/json"
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise CutoverSafetyError(
                f"API {method} {path} failed with HTTP {exc.code}: {detail[:500]}"
            ) from exc
        except (OSError, URLError) as exc:
            raise CutoverSafetyError(f"API {method} {path} failed") from exc
        try:
            value = json.loads(raw or b"{}")
        except json.JSONDecodeError as exc:
            raise CutoverSafetyError(f"API {method} {path} returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise CutoverSafetyError(f"API {method} {path} returned a non-object")
        job_id = value.get("jobId")
        if isinstance(job_id, str):
            if path == "/api/db/sync":
                self.owned_jobs["sync"] = job_id
            elif path == "/api/db/adjusted-metrics/materialize":
                self.owned_jobs["materialize"] = job_id
        return value


@dataclass
class _OwnedProcess:
    process: subprocess.Popen[bytes]
    log_handle: object
    log_path: Path
    environment: dict[str, str]


class SubprocessRuntimeAdapter:
    """Owns an isolated uvicorn process from start through joined shutdown."""

    def __init__(
        self,
        *,
        health_url: str = "http://127.0.0.1:3002/api/health",
        startup_timeout_seconds: float = 60.0,
    ) -> None:
        self.health_url = health_url
        self.startup_timeout_seconds = startup_timeout_seconds
        self._owned: dict[int, _OwnedProcess] = {}

    def assert_quiescent(self, data_root: Path) -> None:
        del data_root
        request = Request(self.health_url, method="GET")
        try:
            with urlopen(request, timeout=0.5):
                pass
        except HTTPError:
            pass
        except (OSError, URLError):
            return
        raise CutoverSafetyError(
            "FastAPI process must be fully stopped before raw DuckDB maintenance"
        )

    @staticmethod
    def server_argv(port: int) -> list[str]:
        return [
            sys.executable,
            "-m",
            "uvicorn",
            "src.entrypoints.http.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]

    @staticmethod
    def _reserve_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def start(
        self,
        data_root: Path,
        environment: dict[str, str],
        log_path: Path,
    ) -> ApiAdapter:
        port = self._reserve_port()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        log_handle = os.fdopen(descriptor, "wb", buffering=0)
        process = subprocess.Popen(
            self.server_argv(port),
            cwd=Path(__file__).resolve().parents[3],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        api = HttpApiAdapter(f"http://127.0.0.1:{port}")
        self._owned[id(api)] = _OwnedProcess(
            process=process,
            log_handle=log_handle,
            log_path=log_path,
            environment=dict(environment),
        )
        deadline = time.monotonic() + self.startup_timeout_seconds
        while time.monotonic() < deadline:
            if process.poll() is not None:
                self.stop(api)
                raise CutoverSafetyError("Owned FastAPI server exited during startup")
            try:
                health = api.request("GET", "/api/health")
            except CutoverSafetyError:
                time.sleep(0.1)
                continue
            if health.get("status") == "healthy":
                return api
            time.sleep(0.1)
        self.stop(api)
        raise CutoverSafetyError("Owned FastAPI server startup timed out")

    def cancel_owned_work(self, api: ApiAdapter) -> None:
        if not isinstance(api, HttpApiAdapter):
            return
        routes = {
            "sync": (
                "/api/db/sync/jobs/{job_id}",
                "/api/db/sync/jobs/{job_id}",
            ),
            "materialize": (
                "/api/db/adjusted-metrics/materialize/jobs/{job_id}",
                "/api/db/adjusted-metrics/materialize/jobs/{job_id}",
            ),
        }
        for kind, job_id in list(api.owned_jobs.items()):
            cancel_template, status_template = routes[kind]
            encoded = quote(job_id, safe="")
            try:
                api.request("DELETE", cancel_template.format(job_id=encoded))
            except CutoverSafetyError:
                pass
            terminal = False
            for _ in range(3_600):
                try:
                    job = api.request("GET", status_template.format(job_id=encoded))
                except CutoverSafetyError:
                    break
                if job.get("status") in {"completed", "failed", "cancelled"}:
                    terminal = True
                    break
                time.sleep(0.5)
            if not terminal:
                raise CutoverSafetyError(
                    f"Owned {kind} job did not reach a terminal state after cancellation"
                )

    def stop(self, api: ApiAdapter) -> None:
        owned = self._owned.pop(id(api), None)
        if owned is None:
            return
        process = owned.process
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=600)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=30)
        finally:
            try:
                close = getattr(owned.log_handle, "close")
                close()
            finally:
                self.redact_log_file(owned.log_path, owned.environment)

    @staticmethod
    def redact_log_file(log_path: Path, environment: dict[str, str]) -> None:
        text = log_path.read_text(encoding="utf-8", errors="replace")
        path_keys = {
            "XDG_DATA_HOME",
            "TRADING25_DATA_DIR",
            "MARKET_TIMESERIES_DIR",
            "MARKET_DB_PATH",
            "DATASET_BASE_PATH",
            "PORTFOLIO_DB_PATH",
            "TRADING25_STRATEGIES_DIR",
            "TRADING25_BACKTEST_DIR",
            "TRADING25_DEFAULT_CONFIG_PATH",
        }
        for key, value in environment.items():
            if not value:
                continue
            upper = key.upper()
            if key in path_keys:
                text = text.replace(value, f"<{key.lower()}>")
            elif any(token in upper for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")):
                text = text.replace(value, "<redacted-secret>")
        temporary = log_path.with_suffix(".redacted.tmp")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(log_path)
        log_path.chmod(0o600)
        descriptor = os.open(log_path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class MarketV4CutoverService:
    """Coordinates explicit, gated Market v4 maintenance phases."""

    def __init__(
        self,
        data_root: Path,
        *,
        duckdb: DuckDbAdapter,
        runtime: RuntimeAdapter,
        disk_free_bytes: Callable[[Path], int],
        now: Callable[[], str],
        code_version: Callable[[], str],
    ) -> None:
        self.data_root = data_root.resolve()
        self.duckdb = duckdb
        self.runtime = runtime
        self.disk_free_bytes = disk_free_bytes
        self.now = now
        self.code_version = code_version

    @property
    def market_root(self) -> Path:
        return self.data_root / "market-timeseries"

    @property
    def operations_root(self) -> Path:
        return self.data_root / "operations" / "market-v4-cutover"

    @property
    def backups_root(self) -> Path:
        return self.operations_root / "backups"

    @staticmethod
    def _validate_id(value: str | None, *, label: str) -> str:
        if not value:
            raise CutoverSafetyError(f"An explicit {label} ID is required")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
            raise CutoverSafetyError(f"Invalid {label} ID")
        return value

    @staticmethod
    def _wal_path(db_path: Path) -> Path:
        return Path(f"{db_path}.wal")

    def _source_files(self, root: Path) -> list[Path]:
        if not root.is_dir():
            raise CutoverSafetyError("Market time-series directory is missing")
        files: list[Path] = []
        for path in sorted(root.rglob("*")):
            if path == self._wal_path(root / "market.duckdb"):
                if path.is_file() and path.stat().st_size == 0:
                    continue
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise CutoverSafetyError(f"Backup source contains symlink: {path.name}")
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                raise CutoverSafetyError(f"Backup source contains special file: {path.name}")
            files.append(path)
        db_path = root / "market.duckdb"
        if db_path not in files:
            raise CutoverSafetyError("market.duckdb is missing")
        return files

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _fsync_file(path: Path) -> None:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _checkpoint(self) -> MarketSourceMetadata:
        db_path = self.market_root / "market.duckdb"
        try:
            metadata = self.duckdb.checkpoint_exclusive(db_path)
        except Exception as exc:
            raise CutoverSafetyError(
                "Could not prove an exclusive writable DuckDB checkpoint"
            ) from exc
        wal_path = self._wal_path(db_path)
        if wal_path.exists() and wal_path.stat().st_size > 0:
            raise CutoverSafetyError("Nonempty DuckDB WAL remains after checkpoint")
        return metadata

    def preflight(self) -> MarketSourceMetadata:
        self.runtime.assert_quiescent(self.data_root)
        metadata = self._checkpoint()
        source_bytes = sum(path.stat().st_size for path in self._source_files(self.market_root))
        required_bytes = max(source_bytes * 4, 1)
        if self.disk_free_bytes(self.data_root) < required_bytes:
            raise CutoverSafetyError(
                f"Insufficient free space: require at least {required_bytes} bytes"
            )
        return metadata

    def backup(self, backup_id: str) -> BackupResult:
        backup_id = self._validate_id(backup_id, label="backup")
        metadata = self.preflight()
        destination = self.backups_root / backup_id
        try:
            destination.mkdir(parents=True, exist_ok=False)
        except FileExistsError as exc:
            raise CutoverSafetyError(f"Backup destination already exists: {backup_id}") from exc
        payload = destination / "payload"
        payload.mkdir()
        entries: list[dict[str, object]] = []
        try:
            for source in self._source_files(self.market_root):
                relative = source.relative_to(self.market_root)
                target = payload / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target, follow_symlinks=False)
                self._fsync_file(target)
                entries.append(
                    {
                        "path": relative.as_posix(),
                        "bytes": target.stat().st_size,
                        "sha256": self._sha256(target),
                    }
                )
            manifest = {
                "backupId": backup_id,
                "createdAt": self.now(),
                "codeVersion": self.code_version(),
                "sourceRootFingerprint": self.root_fingerprint(self.data_root),
                "source": {
                    "schemaVersion": metadata.schema_version,
                    "stockPriceAdjustmentMode": metadata.adjustment_mode,
                },
                "files": entries,
            }
            manifest_path = destination / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self._fsync_file(manifest_path)
            self._fsync_tree_dirs(payload)
            self._fsync_dir(destination)
            self.verify_backup(backup_id)
            self._make_tree_read_only(destination)
            self._fsync_dir(self.backups_root)
        except Exception:
            if destination.exists():
                self._make_tree_writable(destination)
                shutil.rmtree(destination)
            raise
        return BackupResult(backup_id)

    def _fsync_tree_dirs(self, root: Path) -> None:
        for directory in sorted(
            (path for path in root.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            self._fsync_dir(directory)
        self._fsync_dir(root)

    @staticmethod
    def _make_tree_read_only(root: Path) -> None:
        for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
            if path.is_dir():
                path.chmod(0o500)
            else:
                path.chmod(0o400)
        root.chmod(0o500)

    @staticmethod
    def _make_tree_writable(root: Path) -> None:
        root.chmod(0o700)
        for path in root.rglob("*"):
            if path.is_dir():
                path.chmod(0o700)
            else:
                path.chmod(0o600)

    def verify_backup(self, backup_id: str) -> BackupResult:
        backup_id = self._validate_id(backup_id, label="backup")
        destination = self.backups_root / backup_id
        manifest_path = destination / "manifest.json"
        if not manifest_path.is_file():
            raise CutoverSafetyError(f"Backup manifest is missing: {backup_id}")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError("Backup manifest is unreadable") from exc
        if manifest.get("backupId") != backup_id:
            raise CutoverSafetyError("Backup manifest ID mismatch")
        if manifest.get("sourceRootFingerprint") != self.root_fingerprint(
            self.data_root
        ):
            raise CutoverSafetyError("Backup source root fingerprint mismatch")
        entries = manifest.get("files")
        if not isinstance(entries, list) or not entries:
            raise CutoverSafetyError("Backup manifest has no files")
        expected_paths: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                raise CutoverSafetyError("Backup manifest file entry is invalid")
            relative = Path(entry["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise CutoverSafetyError("Backup manifest contains unsafe path")
            expected_paths.add(relative.as_posix())
            target = destination / "payload" / relative
            if target.is_symlink() or not target.is_file():
                raise CutoverSafetyError(f"Backup file is missing: {relative.as_posix()}")
            if target.stat().st_size != entry.get("bytes"):
                raise CutoverSafetyError(f"Backup size mismatch: {relative.as_posix()}")
            if self._sha256(target) != entry.get("sha256"):
                raise CutoverSafetyError(f"Backup checksum mismatch: {relative.as_posix()}")
        actual_paths = {
            path.relative_to(destination / "payload").as_posix()
            for path in self._source_files(destination / "payload")
        }
        if actual_paths != expected_paths:
            raise CutoverSafetyError("Backup file set mismatch")
        return BackupResult(backup_id)

    def restore(self, backup_id: str | None) -> RestoreResult:
        backup_id = self._validate_id(backup_id, label="backup")
        self.runtime.assert_quiescent(self.data_root)
        if (self.market_root / "market.duckdb").is_file():
            self._checkpoint()
        wal_path = self._wal_path(self.market_root / "market.duckdb")
        if wal_path.exists() and wal_path.stat().st_size > 0:
            raise CutoverSafetyError("Cannot restore over a nonempty DuckDB WAL")
        self.verify_backup(backup_id)
        backup_payload = self.backups_root / backup_id / "payload"
        stage = self.data_root / f"market-timeseries.restore-{backup_id}"
        if stage.exists():
            raise CutoverSafetyError("Restore staging destination already exists")
        shutil.copytree(backup_payload, stage, symlinks=False)
        self._make_tree_writable(stage)
        self._verify_tree_copy(backup_payload, stage)
        self._fsync_tree_dirs(stage)
        quarantine_relative: str | None = None
        if self.market_root.exists():
            quarantine = self.operations_root / "quarantine" / f"failed-{backup_id}"
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            if quarantine.exists():
                raise CutoverSafetyError("Restore quarantine destination already exists")
            self.market_root.rename(quarantine)
            quarantine_relative = quarantine.relative_to(self.data_root).as_posix()
        stage.rename(self.market_root)
        self._fsync_dir(self.data_root)
        self.verify_backup(backup_id)
        return RestoreResult(backup_id, quarantine_relative)

    def _verify_tree_copy(self, source: Path, target: Path) -> None:
        source_files = self._source_files(source)
        target_files = self._source_files(target)
        source_relatives = {path.relative_to(source) for path in source_files}
        target_relatives = {path.relative_to(target) for path in target_files}
        if source_relatives != target_relatives:
            raise CutoverSafetyError("Restore staging file set mismatch")
        for relative in source_relatives:
            source_file = source / relative
            target_file = target / relative
            if (
                source_file.stat().st_size != target_file.stat().st_size
                or self._sha256(source_file) != self._sha256(target_file)
            ):
                raise CutoverSafetyError(
                    f"Restore staging checksum mismatch: {relative.as_posix()}"
                )

    def smoke(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        *,
        market_root: Path | None = None,
    ) -> SmokeResult:
        inspected_root = market_root or self.market_root
        metadata = self.duckdb.inspect(inspected_root / "market.duckdb")
        if metadata.schema_version != 4:
            raise CutoverSafetyError(
                f"Market schema must be exactly 4, got {metadata.schema_version!r}"
            )
        if metadata.adjustment_mode != "local_projection_v2_event_time":
            raise CutoverSafetyError(
                "Market adjustment mode must be local_projection_v2_event_time"
            )

        stats = api.request("GET", "/api/db/stats")
        schema = stats.get("schema")
        if not isinstance(schema, dict) or schema != {
            "version": 4,
            "requiredVersion": 4,
            "current": True,
        }:
            raise CutoverSafetyError("Market stats schema v4 gate failed")
        stats_adjusted = stats.get("adjustedMetrics")
        if (
            not isinstance(stats_adjusted, dict)
            or stats_adjusted.get("status") != "ready"
            or not isinstance(stats_adjusted.get("statementRows"), int)
            or int(stats_adjusted["statementRows"]) <= 0
            or not isinstance(stats_adjusted.get("dailyValuationRows"), int)
            or int(stats_adjusted["dailyValuationRows"]) <= 0
            or not isinstance(stats_adjusted.get("readyBasisCount"), int)
            or int(stats_adjusted["readyBasisCount"]) <= 0
        ):
            raise CutoverSafetyError("Market adjusted-metric coverage is not ready")

        validation = api.request("GET", "/api/db/validate")
        if validation.get("status") != "healthy":
            raise CutoverSafetyError("Market validation did not report healthy")
        adjusted = validation.get("adjustedMetrics")
        if not isinstance(adjusted, dict):
            raise CutoverSafetyError("Validation omitted adjusted-metric lineage")
        zero_counters = (
            "missingAdjustedStatementRows",
            "extraAdjustedStatementRows",
            "staleAdjustedStatementRows",
            "wrongBasisAdjustedStatementRows",
            "missingDailyValuationRows",
            "extraDailyValuationRows",
            "wrongBasisDailyValuationRows",
        )
        if (
            adjusted.get("status") != "ready"
            or not isinstance(adjusted.get("sourceStatementKeyCount"), int)
            or int(adjusted["sourceStatementKeyCount"]) <= 0
            or not isinstance(adjusted.get("expectedAdjustedStatementRows"), int)
            or int(adjusted["expectedAdjustedStatementRows"]) <= 0
            or any(adjusted.get(counter) != 0 for counter in zero_counters)
        ):
            raise CutoverSafetyError("Exact adjusted-metric lineage validation failed")

        symbol = quote(config.symbol, safe="")
        get_fundamentals = api.request(
            "GET", f"/api/analytics/fundamentals/{symbol}"
        )
        post_fundamentals = api.request(
            "POST", "/api/fundamentals/compute", {"symbol": config.symbol}
        )
        semantic_keys = ("asOfDate", "data", "latestMetrics")
        if any(
            get_fundamentals.get(key) != post_fundamentals.get(key)
            for key in semantic_keys
        ):
            raise CutoverSafetyError("Fundamentals GET/POST parity failed")
        if not get_fundamentals.get("data"):
            raise CutoverSafetyError("Fundamentals smoke returned no data")

        screening = api.request(
            "POST",
            "/api/analytics/screening/jobs",
            {
                "strategies": config.strategy,
                "recentDays": 10,
                "sortBy": "matchedDate",
                "order": "desc",
            },
        )
        screening_job_id = self._require_job_id(screening, "screening")
        self._poll_api_job(
            api,
            f"/api/analytics/screening/jobs/{quote(screening_job_id, safe='')}",
            "screening",
        )
        screening_result = api.request(
            "GET",
            f"/api/analytics/screening/result/{quote(screening_job_id, safe='')}",
        )
        if not isinstance(screening_result.get("results"), list):
            raise CutoverSafetyError("Screening result payload is invalid")

        ranking = api.request("GET", "/api/analytics/fundamental-ranking")
        if not isinstance(ranking.get("rankings"), dict):
            raise CutoverSafetyError("Fundamental ranking payload is invalid")

        dataset_name = "cutover-smoke"
        dataset = api.request(
            "POST",
            "/api/dataset",
            {
                "name": dataset_name,
                "preset": config.dataset_preset,
                "overwrite": True,
            },
        )
        dataset_job_id = self._require_job_id(dataset, "dataset")
        self._poll_api_job(
            api,
            f"/api/dataset/jobs/{quote(dataset_job_id, safe='')}",
            "dataset",
        )
        dataset_info = api.request("GET", f"/api/dataset/{dataset_name}/info")
        snapshot = dataset_info.get("snapshot")
        dataset_validation = dataset_info.get("validation")
        if not isinstance(snapshot, dict) or snapshot != {
            **snapshot,
            "schemaVersion": 3,
            "sourceMarketSchemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        }:
            raise CutoverSafetyError("Dataset event-time lineage gate failed")
        if not isinstance(dataset_validation, dict) or dataset_validation.get("isValid") is not True:
            raise CutoverSafetyError("Dataset validation failed")
        opened = api.request("GET", f"/api/dataset/{dataset_name}/stocks")
        if not opened:
            raise CutoverSafetyError("Dataset open smoke returned an empty payload")

        return SmokeResult(
            schema_version=metadata.schema_version,
            adjustment_mode=metadata.adjustment_mode,
            checks=(
                "market_metadata",
                "adjusted_metrics_lineage",
                "fundamentals_parity",
                "screening",
                "fundamental_ranking",
                "dataset_create_info_open",
            ),
            api_paths=(
                "/api/db/stats",
                "/api/db/validate",
                f"/api/analytics/fundamentals/{symbol}",
                "/api/fundamentals/compute",
                "/api/analytics/screening/jobs",
                f"/api/analytics/screening/jobs/{screening_job_id}",
                f"/api/analytics/screening/result/{screening_job_id}",
                "/api/analytics/fundamental-ranking",
                "/api/dataset",
                f"/api/dataset/jobs/{dataset_job_id}",
                f"/api/dataset/{dataset_name}/info",
                f"/api/dataset/{dataset_name}/stocks",
            ),
            lineage={
                **{
                    key: int(adjusted[key])
                    for key in (
                        "sourceStatementKeyCount",
                        "expectedAdjustedStatementRows",
                        *zero_counters,
                    )
                },
                "statementRows": int(stats_adjusted["statementRows"]),
                "dailyValuationRows": int(stats_adjusted["dailyValuationRows"]),
                "readyBasisCount": int(stats_adjusted["readyBasisCount"]),
            },
        )

    @staticmethod
    def _require_job_id(payload: dict[str, object], label: str) -> str:
        job_id = payload.get("jobId")
        if not isinstance(job_id, str) or not job_id:
            raise CutoverSafetyError(f"{label} did not return a job ID")
        return job_id

    @staticmethod
    def _poll_api_job(
        api: ApiAdapter,
        path: str,
        label: str,
        *,
        attempts: int = 21_600,
        poll_interval_seconds: float = 2.0,
    ) -> dict[str, object]:
        for _ in range(attempts):
            job = api.request("GET", path)
            status = job.get("status")
            if status == "completed":
                return job
            if status in {"failed", "cancelled"}:
                raise CutoverSafetyError(f"{label} job ended with status {status}")
            time.sleep(poll_interval_seconds)
        raise CutoverSafetyError(f"{label} job polling timed out")

    @staticmethod
    def root_fingerprint(root: Path) -> str:
        return hashlib.sha256(str(root.resolve()).encode()).hexdigest()

    def rehearse(
        self,
        report_id: str,
        config: SmokeConfig,
        *,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        report_id = self._validate_id(report_id, label="report")
        rehearsal_dir = self.operations_root / "rehearsals" / report_id
        try:
            rehearsal_dir.mkdir(parents=True, exist_ok=False)
        except FileExistsError as exc:
            raise CutoverSafetyError("Rehearsal destination already exists") from exc
        rehearsal_root = rehearsal_dir / "root"
        self._prepare_isolated_root(rehearsal_root)
        environment = self._isolated_environment(
            inherited_environment,
            rehearsal_root,
        )
        started = time.monotonic()
        api: ApiAdapter | None = None
        log_path = rehearsal_dir / "server.log"
        try:
            api = self.runtime.start(rehearsal_root, environment, log_path)
            checks, evidence, phases = self._run_rebuild(api, config, rehearsal_root)
            self.runtime.stop(api)
            api = None
        except Exception as exc:
            if api is not None:
                try:
                    self.runtime.cancel_owned_work(api)
                except Exception:
                    pass
                try:
                    self.runtime.stop(api)
                except Exception:
                    pass
            report = self._operation_report(
                report_id=report_id,
                phase="rehearsal",
                status="failed",
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log="rehearsals/{}/server.log".format(report_id),
                evidence=None,
                phases=(),
                config=config,
                error=type(exc).__name__,
            )
            self._write_report(report_id, report)
            raise CutoverSafetyError("Isolated Market v4 rehearsal failed") from exc
        report = self._operation_report(
            report_id=report_id,
            phase="rehearsal",
            status="passed",
            duration_seconds=time.monotonic() - started,
            api_checks=checks,
            server_log="rehearsals/{}/server.log".format(report_id),
            evidence=evidence,
            phases=phases,
            config=config,
        )
        report_path = self._write_report(report_id, report)
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    def cutover(
        self,
        report_id: str,
        *,
        rehearsal_report_id: str,
        backup_id: str,
        config: SmokeConfig,
        inherited_environment: dict[str, str],
    ) -> OperationResult:
        report_id = self._validate_id(report_id, label="report")
        rehearsal_report_id = self._validate_id(
            rehearsal_report_id, label="rehearsal report"
        )
        backup_id = self._validate_id(backup_id, label="backup")
        rehearsal = self._read_report(rehearsal_report_id)
        if (
            rehearsal.get("phase") != "rehearsal"
            or rehearsal.get("status") != "passed"
            or rehearsal.get("reportId") != rehearsal_report_id
            or rehearsal.get("targetRootFingerprint")
            != self.root_fingerprint(self.data_root)
            or rehearsal.get("codeVersion") != self.code_version()
            or rehearsal.get("smokeConfig")
            != {
                "symbol": config.symbol,
                "strategy": config.strategy,
                "datasetPreset": config.dataset_preset,
            }
        ):
            raise CutoverSafetyError("An exact passing rehearsal report is required")
        self.verify_backup(backup_id)
        self.preflight()
        environment = self._isolated_environment(inherited_environment, self.data_root)
        started = time.monotonic()
        api: ApiAdapter | None = None
        log_path = self.operations_root / "reports" / report_id / "server.log"
        log_path.parent.mkdir(parents=True, exist_ok=False)
        try:
            api = self.runtime.start(self.data_root, environment, log_path)
            checks, evidence, phases = self._run_rebuild(api, config, self.data_root)
            self.runtime.stop(api)
            api = None
        except Exception as exc:
            if api is not None:
                try:
                    self.runtime.cancel_owned_work(api)
                except Exception:
                    pass
                try:
                    self.runtime.stop(api)
                except Exception:
                    pass
            try:
                self.restore(backup_id)
            except Exception as restore_exc:
                report = self._operation_report(
                    report_id=report_id,
                    phase="cutover",
                    status="restore_failed",
                    duration_seconds=time.monotonic() - started,
                    api_checks=(),
                    server_log=f"reports/{report_id}/server.log",
                    evidence=None,
                    phases=(),
                    config=config,
                    backup_id=backup_id,
                    rehearsal_report_id=rehearsal_report_id,
                    error=type(restore_exc).__name__,
                )
                self._write_report(report_id, report)
                raise CutoverSafetyError(
                    "Active cutover failed and explicit restore also failed"
                ) from restore_exc
            report = self._operation_report(
                report_id=report_id,
                phase="cutover",
                status="failed_restored",
                duration_seconds=time.monotonic() - started,
                api_checks=(),
                server_log=f"reports/{report_id}/server.log",
                evidence=None,
                phases=(),
                config=config,
                backup_id=backup_id,
                rehearsal_report_id=rehearsal_report_id,
                error=type(exc).__name__,
            )
            self._write_report(report_id, report)
            raise CutoverSafetyError(
                f"Active cutover failed; restored backup {backup_id}"
            ) from exc
        report = self._operation_report(
            report_id=report_id,
            phase="cutover",
            status="passed",
            duration_seconds=time.monotonic() - started,
            api_checks=checks,
            server_log=f"reports/{report_id}/server.log",
            evidence=evidence,
            phases=phases,
            config=config,
            backup_id=backup_id,
            rehearsal_report_id=rehearsal_report_id,
        )
        report_path = self._write_report(report_id, report)
        return OperationResult(
            report_id,
            report_path.relative_to(self.data_root).as_posix(),
        )

    def _prepare_isolated_root(self, root: Path) -> None:
        for relative in (
            "market-timeseries",
            "datasets",
            "strategies",
            "backtest",
            "config",
        ):
            (root / relative).mkdir(parents=True, exist_ok=False)
        source_config = self.data_root / "config" / "default.yaml"
        if not source_config.is_file():
            source_config = Path(__file__).resolve().parents[3] / "config" / "default.yaml"
        if not source_config.is_file():
            raise CutoverSafetyError("Repository default configuration is missing")
        shutil.copyfile(source_config, root / "config" / "default.yaml")
        active_strategies = self.data_root / "strategies"
        if active_strategies.is_dir():
            shutil.rmtree(root / "strategies")
            shutil.copytree(active_strategies, root / "strategies", symlinks=False)

    @staticmethod
    def _isolated_environment(
        inherited: dict[str, str],
        root: Path,
    ) -> dict[str, str]:
        environment = dict(inherited)
        overrides = {
            "XDG_DATA_HOME": str(root / "xdg-data-home"),
            "TRADING25_DATA_DIR": str(root),
            "MARKET_TIMESERIES_DIR": str(root / "market-timeseries"),
            "MARKET_DB_PATH": str(root / "market-timeseries" / "market.duckdb"),
            "DATASET_BASE_PATH": str(root / "datasets"),
            "PORTFOLIO_DB_PATH": str(root / "portfolio.db"),
            "TRADING25_STRATEGIES_DIR": str(root / "strategies"),
            "TRADING25_BACKTEST_DIR": str(root / "backtest"),
            "TRADING25_DEFAULT_CONFIG_PATH": str(root / "config" / "default.yaml"),
        }
        environment.update(overrides)
        return environment

    def _run_rebuild(
        self,
        api: ApiAdapter,
        config: SmokeConfig,
        root: Path,
    ) -> tuple[
        tuple[str, ...],
        dict[str, object],
        tuple[dict[str, object], ...],
    ]:
        sync_started = time.monotonic()
        sync = api.request(
            "POST",
            "/api/db/sync",
            {
                "mode": "initial",
                "resetBeforeSync": True,
                "enforceBulkForStockData": True,
            },
        )
        job_id = self._require_job_id(sync, "sync")
        self._poll_api_job(
            api,
            f"/api/db/sync/jobs/{quote(job_id, safe='')}",
            "sync",
        )
        sync_duration = time.monotonic() - sync_started
        smoke_started = time.monotonic()
        result = self.smoke(api, config, market_root=root / "market-timeseries")
        smoke_duration = time.monotonic() - smoke_started
        return (
            (
                "/api/db/sync",
                f"/api/db/sync/jobs/{job_id}",
                *result.api_paths,
            ),
            {
                "schemaVersion": result.schema_version,
                "stockPriceAdjustmentMode": result.adjustment_mode,
                "adjustedMetrics": result.lineage,
            },
            (
                {
                    "name": "initial_sync_and_adjusted_metrics_pit",
                    "status": "passed",
                    "durationSeconds": round(sync_duration, 6),
                },
                {
                    "name": "semantic_smoke",
                    "status": "passed",
                    "durationSeconds": round(smoke_duration, 6),
                },
            ),
        )

    def _operation_report(
        self,
        *,
        report_id: str,
        phase: str,
        status: str,
        duration_seconds: float,
        api_checks: tuple[str, ...],
        server_log: str,
        evidence: dict[str, object] | None,
        phases: tuple[dict[str, object], ...],
        config: SmokeConfig,
        backup_id: str | None = None,
        rehearsal_report_id: str | None = None,
        error: str | None = None,
    ) -> dict[str, object]:
        report: dict[str, object] = {
            "reportId": report_id,
            "phase": phase,
            "status": status,
            "createdAt": self.now(),
            "durationSeconds": round(duration_seconds, 6),
            "codeVersion": self.code_version(),
            "targetRootFingerprint": self.root_fingerprint(self.data_root),
            "command": [
                "python",
                "-m",
                "uvicorn",
                "src.entrypoints.http.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                "<reserved>",
            ],
            "apiChecks": list(api_checks),
            "serverLog": server_log,
            "schemaCoverage": evidence,
            "phases": list(phases),
            "smokeConfig": {
                "symbol": config.symbol,
                "strategy": config.strategy,
                "datasetPreset": config.dataset_preset,
            },
        }
        if backup_id is not None:
            report["backupId"] = backup_id
            report["backupManifest"] = f"backups/{backup_id}/manifest.json"
        if rehearsal_report_id is not None:
            report["rehearsalReportId"] = rehearsal_report_id
        if error is not None:
            report["errorType"] = error
        return report

    def _write_report(self, report_id: str, report: dict[str, object]) -> Path:
        report_dir = self.operations_root / "reports" / report_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "report.json"
        if report_path.exists():
            raise CutoverSafetyError("Operation report already exists")
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self._fsync_file(report_path)
        self._fsync_dir(report_dir)
        self._fsync_dir(report_dir.parent)
        return report_path

    def _read_report(self, report_id: str) -> dict[str, object]:
        path = self.operations_root / "reports" / report_id / "report.json"
        if not path.is_file():
            raise CutoverSafetyError("An exact passing rehearsal report is required")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CutoverSafetyError("Rehearsal report is unreadable") from exc
        if not isinstance(value, dict):
            raise CutoverSafetyError("Rehearsal report is invalid")
        return value
