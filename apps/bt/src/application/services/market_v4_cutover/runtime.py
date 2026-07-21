"""Focused Market v5 cutover responsibility module."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import time
from typing import BinaryIO
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from src.infrastructure.db.market import managed_root as _managed_root
from .contracts import ApiAdapter
from .errors import RuntimeStopError
from .project_paths import bt_project_root

_CREATE_JOB_RESPONSE_FIELDS: dict[str, tuple[str, str]] = {
    "/api/db/sync": ("sync", "jobId"),
    "/api/analytics/screening/jobs": ("screening", "job_id"),
    "/api/dataset": ("dataset", "jobId"),
}


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
            raise _managed_root.CutoverSafetyError(
                f"API {method} {path} failed with HTTP {exc.code}: {detail[:500]}"
            ) from exc
        except (OSError, URLError) as exc:
            raise _managed_root.CutoverSafetyError(
                f"API {method} {path} failed"
            ) from exc
        try:
            value = json.loads(raw or b"{}")
        except json.JSONDecodeError as exc:
            raise _managed_root.CutoverSafetyError(
                f"API {method} {path} returned invalid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise _managed_root.CutoverSafetyError(
                f"API {method} {path} returned a non-object"
            )
        job_response = _CREATE_JOB_RESPONSE_FIELDS.get(path)
        if job_response is not None:
            job_kind, job_id_field = job_response
            job_id = value.get(job_id_field)
            if isinstance(job_id, str) and job_id:
                self.owned_jobs[job_kind] = job_id
        return value


@dataclass
class _OwnedProcess:
    process: subprocess.Popen[bytes]
    log_handle: object
    log_path: Path
    log_fd: int
    environment: dict[str, str]


class SubprocessRuntimeAdapter:
    """Owns an isolated uvicorn process from start through joined shutdown."""

    def __init__(
        self,
        *,
        startup_timeout_seconds: float = 60.0,
    ) -> None:
        self.startup_timeout_seconds = startup_timeout_seconds
        self._owned: dict[int, _OwnedProcess] = {}

    def assert_quiescent(self, data_root: Path) -> None:
        # The root-scoped exclusive flock acquired by the caller is the
        # authoritative writer-quiescence proof. A fixed TCP port can belong to
        # another data root and must never accept or reject this operation.
        del data_root

    @staticmethod
    def server_argv(port: int, *, market_fd: int) -> list[str]:
        project_root = bt_project_root()
        bootstrap = (
            "import os,runpy,sys;"
            "market_fd=int(sys.argv[1]);"
            "sys.path.insert(0,sys.argv[2]);"
            "os.fchdir(market_fd);"
            "sys.argv=sys.argv[3:];"
            "runpy.run_module('uvicorn',run_name='__main__')"
        )
        return [
            sys.executable,
            "-c",
            bootstrap,
            str(market_fd),
            str(project_root),
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
        *,
        root_fd: int,
        market_fd: int,
        lease_fd: int,
        retained_lease_fd: int | None = None,
        environment: dict[str, str],
        log_path: Path,
        log_fd: int,
    ) -> ApiAdapter:
        port = self._reserve_port()
        child_environment = dict(environment)
        child_environment.update(
            {
                "TRADING25_DATA_ROOT_FD": str(root_fd),
                "TRADING25_MARKET_OPERATION_LOCK_FD": str(lease_fd),
            }
        )
        pass_fds = [root_fd, market_fd, lease_fd]
        if retained_lease_fd is not None:
            os.fstat(retained_lease_fd)
            child_environment["TRADING25_RETAINED_MARKET_OPERATION_LOCK_FD"] = str(
                retained_lease_fd
            )
            pass_fds.append(retained_lease_fd)
        log_handle: BinaryIO | None = None
        retained_log_fd = -1
        try:
            log_handle = os.fdopen(os.dup(log_fd), "wb", buffering=0)
            retained_log_fd = os.dup(log_fd)
            process = subprocess.Popen(
                self.server_argv(port, market_fd=market_fd),
                cwd=bt_project_root(),
                env=child_environment,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                pass_fds=tuple(dict.fromkeys(pass_fds)),
            )
        except Exception:
            if log_handle is not None:
                log_handle.close()
            if retained_log_fd >= 0:
                os.close(retained_log_fd)
            raise
        api = HttpApiAdapter(f"http://127.0.0.1:{port}")
        self._owned[id(api)] = _OwnedProcess(
            process=process,
            log_handle=log_handle,
            log_path=log_path,
            log_fd=retained_log_fd,
            environment=child_environment,
        )
        deadline = time.monotonic() + self.startup_timeout_seconds
        try:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.stop(api)
                    raise _managed_root.CutoverSafetyError(
                        "Owned FastAPI server exited during startup"
                    )
                try:
                    health = api.request("GET", "/api/health")
                except _managed_root.CutoverSafetyError:
                    time.sleep(0.1)
                    continue
                if health.get("status") == "healthy":
                    return api
                time.sleep(0.1)
            self.stop(api)
            raise _managed_root.CutoverSafetyError(
                "Owned FastAPI server startup timed out"
            )
        except RuntimeStopError:
            raise
        except BaseException as exc:
            try:
                self.stop(api)
            except RuntimeStopError as stop_error:
                raise stop_error from exc
            except BaseException as stop_error:
                raise RuntimeStopError(
                    "Owned FastAPI startup cleanup has no join verdict",
                    process_joined=False,
                ) from stop_error
            if not isinstance(exc, Exception):
                raise RuntimeStopError(
                    "Owned FastAPI startup interrupted after child joined",
                    process_joined=True,
                ) from exc
            raise

    def cancel_owned_work(self, api: ApiAdapter) -> None:
        if not isinstance(api, HttpApiAdapter):
            return
        routes = {
            "sync": (
                "DELETE",
                "/api/db/sync/jobs/{job_id}",
                "/api/db/sync/jobs/{job_id}",
            ),
            "screening": (
                "POST",
                "/api/analytics/screening/jobs/{job_id}/cancel",
                "/api/analytics/screening/jobs/{job_id}",
            ),
            "dataset": (
                "DELETE",
                "/api/dataset/jobs/{job_id}",
                "/api/dataset/jobs/{job_id}",
            ),
        }
        for kind, job_id in list(api.owned_jobs.items()):
            cancel_method, cancel_template, status_template = routes[kind]
            encoded = quote(job_id, safe="")
            try:
                api.request(cancel_method, cancel_template.format(job_id=encoded))
            except _managed_root.CutoverSafetyError:
                pass
            terminal = False
            for _ in range(3_600):
                try:
                    job = api.request("GET", status_template.format(job_id=encoded))
                except _managed_root.CutoverSafetyError:
                    break
                if job.get("status") in {"completed", "failed", "cancelled"}:
                    terminal = True
                    break
                time.sleep(0.5)
            if not terminal:
                raise _managed_root.CutoverSafetyError(
                    f"Owned {kind} job did not reach a terminal state after cancellation"
                )

    def stop(self, api: ApiAdapter) -> None:
        owned = self._owned.get(id(api))
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
        except Exception as exc:
            raise RuntimeStopError(
                "Owned FastAPI process could not be proven stopped",
                process_joined=False,
            ) from exc
        self._owned.pop(id(api), None)
        try:
            try:
                close = getattr(owned.log_handle, "close")
                close()
            finally:
                try:
                    self.redact_log_fd(owned.log_fd, owned.environment)
                finally:
                    os.close(owned.log_fd)
        except Exception as exc:
            raise RuntimeStopError(
                "Owned FastAPI process stopped but log cleanup failed",
                process_joined=True,
            ) from exc

    @staticmethod
    def redact_log_fd(log_fd: int, environment: dict[str, str]) -> None:
        if not stat.S_ISREG(os.fstat(log_fd).st_mode):
            raise _managed_root.CutoverSafetyError(
                "Owned server log must be a regular file"
            )
        os.lseek(log_fd, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        while chunk := os.read(log_fd, 1024 * 1024):
            chunks.append(chunk)
        text = b"".join(chunks).decode("utf-8", errors="replace")
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
                if Path(value).is_absolute():
                    text = text.replace(value, f"<{key.lower()}>")
            elif any(
                token in upper for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")
            ):
                text = text.replace(value, "<redacted-secret>")
        text = text.replace(str(Path.home()), "<home>")
        payload = text.encode()
        os.lseek(log_fd, 0, os.SEEK_SET)
        os.ftruncate(log_fd, 0)
        view = memoryview(payload)
        while view:
            written = os.write(log_fd, view)
            view = view[written:]
        os.fchmod(log_fd, 0o600)
        os.fsync(log_fd)
