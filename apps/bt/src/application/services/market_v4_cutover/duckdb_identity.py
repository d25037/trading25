"""Focused Market v4 cutover responsibility module."""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import selectors
import stat
import subprocess
import sys
import time
from typing import Iterator

from src.infrastructure.db.market.valuation_queries import (
    get_adjusted_metrics_snapshot,
    get_adjusted_metrics_source_diagnostics,
    get_provider_vintage_snapshot,
)
from src.infrastructure.db.market import managed_root as _managed_root
from .contracts import MarketSourceMetadata
from .errors import WorkerShutdownError
from .project_paths import bt_project_root


class DefaultDuckDbAdapter:
    """Raw DuckDB adapter bound to an inherited directory descriptor."""

    _WORKER_EXIT_TIMEOUT_SECONDS = 30.0
    _WORKER_STOP_TIMEOUT_SECONDS = 5.0
    _MAX_METADATA_BYTES = 64 * 1024

    @staticmethod
    def _metadata(connection: object) -> MarketSourceMetadata:
        execute = getattr(connection, "execute")

        def table_exists(table_name: str) -> bool:
            row = execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return bool(row and int(row[0]) > 0)

        def count_rows(table_name: str) -> int:
            if not table_exists(table_name):
                return 0
            escaped = '"' + table_name.replace('"', '""') + '"'
            row = execute(f"SELECT COUNT(*) FROM {escaped}").fetchone()
            return int(row[0] or 0) if row else 0

        def fetchone(sql: str, params: object = None) -> object:
            return execute(sql, params or []).fetchone()

        def fetchall_dicts(sql: str, params: object = None) -> list[dict[str, object]]:
            cursor = execute(sql, params or [])
            columns = [str(item[0]) for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

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
        try:
            snapshot = get_adjusted_metrics_snapshot(
                table_exists,
                count_rows,
                fetchone,
            )
            diagnostics = get_adjusted_metrics_source_diagnostics(
                table_exists,
                fetchone,
            )
            provider_vintage = get_provider_vintage_snapshot(
                table_exists,
                fetchall_dicts,
            )
            positive_keys = (
                "currentBasisStatementCount",
                "dailyValuationRows",
                "readyProviderWindowCount",
            )
            positive_diagnostic_keys = (
                "sourceStatementKeyCount",
                "expectedAdjustedStatementRows",
            )
            zero_snapshot_keys = (
                "pendingCurrentBasisCodeCount",
                "orphanAdjustedStatementRows",
                "orphanDailyValuationRows",
            )
            zero_diagnostic_keys = (
                "missingAdjustedStatementRows",
                "extraAdjustedStatementRows",
                "staleAdjustedStatementRows",
                "wrongBasisAdjustedStatementRows",
                "missingDailyValuationRows",
                "extraDailyValuationRows",
                "wrongBasisDailyValuationRows",
            )
            provider_vintage_ready = (
                all(int(snapshot.get(key, 0) or 0) > 0 for key in positive_keys)
                and all(
                    int(diagnostics.get(key, 0) or 0) > 0
                    for key in positive_diagnostic_keys
                )
                and all(
                    int(snapshot.get(key, 0) or 0) == 0 for key in zero_snapshot_keys
                )
                and all(
                    int(diagnostics.get(key, 0) or 0) == 0
                    for key in zero_diagnostic_keys
                )
                and provider_vintage.get("providerWindowCoherent") is True
                and int(
                    provider_vintage.get("providerWindowFingerprintCount", 0) or 0
                )
                == int(snapshot.get("providerWindowCount", 0) or 0)
                and int(
                    provider_vintage.get("invalidProviderWindowCount", 0) or 0
                )
                == 0
                and int(
                    provider_vintage.get("invalidAdjustmentEventCount", 0) or 0
                )
                == 0
            )
        except Exception:
            provider_vintage_ready = False
        return MarketSourceMetadata(
            schema_version=(
                int(schema_version) if schema_version is not None else None
            ),
            adjustment_mode=(
                str(adjustment_mode) if adjustment_mode is not None else None
            ),
            provider_vintage_ready=provider_vintage_ready,
        )

    @staticmethod
    def _validate_target(directory_fd: int, filename: str) -> None:
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
            raise _managed_root.CutoverSafetyError(
                "DuckDB parent descriptor must be a directory"
            )
        if filename in {"", ".", ".."} or Path(filename).name != filename:
            raise _managed_root.CutoverSafetyError(
                "DuckDB filename must be a safe leaf name"
            )

    @staticmethod
    def _worker_argv(
        operation: str,
        directory_fd: int,
        guard_lease_fd: int,
        filename: str,
    ) -> list[str]:
        return [
            sys.executable,
            "-c",
            (
                "from src.application.services.market_v4_cutover.duckdb_identity import "
                "directory_bound_duckdb_worker as worker; worker()"
            ),
            operation,
            str(directory_fd),
            str(guard_lease_fd),
            filename,
        ]

    @classmethod
    def _start_worker(
        cls,
        operation: str,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> subprocess.Popen[bytes]:
        cls._validate_target(directory_fd, filename)
        if not stat.S_ISREG(os.fstat(guard_lease_fd).st_mode):
            raise _managed_root.CutoverSafetyError(
                "DuckDB worker guard lease must be a regular file"
            )
        return subprocess.Popen(
            cls._worker_argv(operation, directory_fd, guard_lease_fd, filename),
            cwd=bt_project_root(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(directory_fd, guard_lease_fd),
        )

    @classmethod
    def _read_metadata(cls, process: subprocess.Popen[bytes]) -> MarketSourceMetadata:
        assert process.stdout is not None
        stdout_fd = process.stdout.fileno()
        was_blocking = os.get_blocking(stdout_fd)
        deadline = time.monotonic() + cls._WORKER_EXIT_TIMEOUT_SECONDS
        buffer = bytearray()
        os.set_blocking(stdout_fd, False)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(stdout_fd, selectors.EVENT_READ)
                while b"\n" not in buffer:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise _managed_root.CutoverSafetyError(
                            "Directory-bound DuckDB worker metadata timed out"
                        )
                    chunk = os.read(stdout_fd, 4096)
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    if len(buffer) > cls._MAX_METADATA_BYTES:
                        raise _managed_root.CutoverSafetyError(
                            "Directory-bound DuckDB worker metadata is oversized"
                        )
        finally:
            os.set_blocking(stdout_fd, was_blocking)
        line = bytes(buffer).partition(b"\n")[0]
        if not line:
            raise _managed_root.CutoverSafetyError(
                "Directory-bound DuckDB worker returned no metadata"
            )
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _managed_root.CutoverSafetyError(
                "Directory-bound DuckDB worker returned invalid metadata"
            ) from exc
        return MarketSourceMetadata(
            schema_version=payload.get("schemaVersion"),
            adjustment_mode=payload.get("adjustmentMode"),
            provider_vintage_ready=payload.get("providerVintageReady") is True,
        )

    @classmethod
    def _shutdown_worker(
        cls,
        process: subprocess.Popen[bytes],
        *,
        release_checkpoint: bool,
    ) -> WorkerShutdownError | None:
        """Release, stop, and reap a worker without leaking pipes or processes."""
        cleanup_error: Exception | None = None
        stdin = process.stdin
        if stdin is not None:
            try:
                if release_checkpoint:
                    stdin.write(b"x")
            except (BrokenPipeError, OSError) as exc:
                cleanup_error = exc
            finally:
                try:
                    stdin.close()
                except OSError as exc:
                    cleanup_error = cleanup_error or exc
                # Popen.communicate() flushes a non-None stdin even when it has
                # already been closed. Detach it before draining the other pipes.
                process.stdin = None

        return_code: int | None = None
        forced_stop = False
        process_joined = False
        try:
            return_code = process.wait(timeout=cls._WORKER_EXIT_TIMEOUT_SECONDS)
            process_joined = True
        except subprocess.TimeoutExpired:
            forced_stop = True
            try:
                process.terminate()
            except Exception as exc:
                cleanup_error = cleanup_error or exc
            try:
                return_code = process.wait(timeout=cls._WORKER_STOP_TIMEOUT_SECONDS)
                process_joined = True
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
                try:
                    return_code = process.wait(timeout=cls._WORKER_STOP_TIMEOUT_SECONDS)
                    process_joined = True
                except subprocess.TimeoutExpired as exc:
                    cleanup_error = cleanup_error or exc
                except Exception as exc:
                    cleanup_error = cleanup_error or exc
            except Exception as exc:
                cleanup_error = cleanup_error or exc
        except Exception as exc:
            cleanup_error = cleanup_error or exc

        stderr = b""
        try:
            _stdout, stderr = process.communicate(
                timeout=cls._WORKER_STOP_TIMEOUT_SECONDS
            )
            process_joined = True
        except subprocess.TimeoutExpired as exc:
            cleanup_error = cleanup_error or exc
            try:
                process.kill()
            except Exception as kill_exc:
                cleanup_error = cleanup_error or kill_exc
            try:
                _stdout, stderr = process.communicate(
                    timeout=cls._WORKER_STOP_TIMEOUT_SECONDS
                )
                process_joined = True
            except Exception as final_exc:
                cleanup_error = cleanup_error or final_exc
        except Exception as exc:
            cleanup_error = cleanup_error or exc

        if cleanup_error is not None:
            action = "release" if release_checkpoint else "shutdown"
            return WorkerShutdownError(
                f"Directory-bound DuckDB worker {action} failed: {cleanup_error}",
                process_joined=process_joined,
            )
        if forced_stop:
            return WorkerShutdownError(
                "Directory-bound DuckDB worker timed out",
                process_joined=process_joined,
            )
        if return_code != 0:
            return WorkerShutdownError(
                "Directory-bound DuckDB worker failed: "
                + stderr.decode(errors="replace")[-500:],
                process_joined=process_joined,
            )
        return None

    def checkpoint_exclusive(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        with self.checkpoint_snapshot(
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        ) as metadata:
            return metadata

    @contextmanager
    def checkpoint_snapshot(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> Iterator[MarketSourceMetadata]:
        process = self._start_worker(
            "checkpoint",
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        )
        primary_error = False
        try:
            metadata = self._read_metadata(process)
            yield metadata
        except BaseException:
            primary_error = True
            raise
        finally:
            cleanup_error = self._shutdown_worker(
                process,
                release_checkpoint=True,
            )
            if cleanup_error is not None and (
                not primary_error or not cleanup_error.process_joined
            ):
                raise cleanup_error

    def inspect(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        process = self._start_worker(
            "inspect",
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        )
        primary_error = False
        try:
            return self._read_metadata(process)
        except BaseException:
            primary_error = True
            raise
        finally:
            cleanup_error = self._shutdown_worker(
                process,
                release_checkpoint=False,
            )
            if cleanup_error is not None and (
                not primary_error or not cleanup_error.process_joined
            ):
                raise cleanup_error


def directory_bound_duckdb_worker() -> None:
    """Run one raw DuckDB operation after anchoring cwd to an inherited fd."""
    import duckdb

    operation, raw_fd, raw_guard_fd, filename = sys.argv[-4:]
    directory_fd = int(raw_fd)
    guard_lease_fd = int(raw_guard_fd)
    if operation not in {"checkpoint", "inspect"}:
        raise SystemExit("Unsupported DuckDB worker operation")
    if filename in {"", ".", ".."} or Path(filename).name != filename:
        raise SystemExit("Unsafe DuckDB filename")
    if not stat.S_ISREG(os.fstat(guard_lease_fd).st_mode):
        raise SystemExit("Invalid DuckDB worker guard lease")
    os.fchdir(directory_fd)
    connection = duckdb.connect(filename, read_only=operation == "inspect")
    try:
        connection.execute("PRAGMA disable_progress_bar")
        metadata = DefaultDuckDbAdapter._metadata(connection)
        if operation == "checkpoint":
            connection.execute("CHECKPOINT")
        print(
            json.dumps(
                {
                    "schemaVersion": metadata.schema_version,
                    "adjustmentMode": metadata.adjustment_mode,
                    "providerVintageReady": metadata.provider_vintage_ready,
                }
            ),
            flush=True,
        )
        if operation == "checkpoint":
            sys.stdin.buffer.read(1)
    finally:
        connection.close()
