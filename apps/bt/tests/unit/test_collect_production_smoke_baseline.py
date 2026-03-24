from __future__ import annotations

import importlib.util
import json
import os
import sys
import uuid
from argparse import Namespace
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


def _load_smoke_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[4]
    script_path = repo_root / "scripts" / "collect-production-smoke-baseline.py"
    module_name = f"collect_production_smoke_baseline_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    module.os = SimpleNamespace(environ=dict(os.environ), chdir=os.chdir)
    return module


class _FakeResponse:
    def __init__(self, payload: Any, *, status_code: int = 200, text: str | None = None) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = text or json.dumps(payload)

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}: {self.text}")


class _FakeClient:
    def __init__(
        self,
        *,
        post_responses: dict[str, list[_FakeResponse]] | None = None,
        get_responses: dict[str, list[_FakeResponse]] | None = None,
    ) -> None:
        self._post_responses = {path: list(responses) for path, responses in (post_responses or {}).items()}
        self._get_responses = {path: list(responses) for path, responses in (get_responses or {}).items()}
        self.post_calls: list[tuple[str, Any]] = []
        self.get_calls: list[str] = []

    def _take(self, mapping: dict[str, list[_FakeResponse]], path: str) -> _FakeResponse:
        responses = mapping.get(path)
        if not responses:
            raise AssertionError(f"Unexpected path: {path}")
        return responses.pop(0)

    def post(self, path: str, json: Any = None) -> _FakeResponse:
        self.post_calls.append((path, json))
        return self._take(self._post_responses, path)

    def get(self, path: str) -> _FakeResponse:
        self.get_calls.append(path)
        return self._take(self._get_responses, path)


def test_smoke_baseline_helpers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_smoke_module()
    monkeypatch.setattr(module.Path, "home", lambda: Path("/Users/tester"))

    assert module.p95([]) == 0.0
    assert module.p95([1.0, 2.0, 3.0, 4.0, 5.0]) == 5.0
    assert module.round4(1.23456) == 1.2346

    assert module.redact_local_path(Path("/Users/tester/project/file.txt")) == "$HOME/project/file.txt"
    assert module.redact_local_path(Path("/private/tmp/example/file.txt")) == "$TMPDIR/example/file.txt"
    assert module.redact_local_path(Path("/tmp/example/file.txt")) == "$TMPDIR/example/file.txt"
    assert module.redact_local_path(Path("/opt/data/file.txt")) == "/opt/data/file.txt"
    assert module.redact_local_path_string("relative/path") == "relative/path"

    payload = {
        "homePath": "/Users/tester/project/file.txt",
        "nested": ["/tmp/cache/file.txt", {"other": "/opt/data/file.txt"}],
        "count": 3,
    }
    assert module.redact_local_paths_in_payload(payload) == {
        "homePath": "$HOME/project/file.txt",
        "nested": ["$TMPDIR/cache/file.txt", {"other": "/opt/data/file.txt"}],
        "count": 3,
    }

    runtime_root = tmp_path / "runtime"
    data_root = tmp_path / "data"
    module.prepare_runtime_dirs(runtime_root)
    for rel in ("datasets", "backtest/results", "backtest/attribution", "optimization", "cache"):
        assert (runtime_root / rel).exists()
    source_dataset = data_root / "datasets" / "primeExTopix500"
    source_dataset.mkdir(parents=True, exist_ok=True)
    module.mirror_source_datasets(data_root, runtime_root)
    mirrored_dataset = runtime_root / "datasets" / "primeExTopix500"
    assert mirrored_dataset.is_symlink() is True
    assert mirrored_dataset.resolve() == source_dataset.resolve()

    module.os.environ["UV_CACHE_DIR"] = "/custom/cache"
    module.os.environ["LOG_LEVEL"] = "INFO"
    module.set_runtime_env(data_root, runtime_root)
    assert module.os.environ["MARKET_DB_PATH"] == str(data_root / "market-timeseries" / "market.duckdb")
    assert module.os.environ["DATASET_BASE_PATH"] == str(runtime_root / "datasets")
    assert module.os.environ["MARKET_TIMESERIES_DIR"] == str(data_root / "market-timeseries")
    assert module.os.environ["TRADING25_BACKTEST_DIR"] == str(runtime_root / "backtest")
    assert module.os.environ["UV_CACHE_DIR"] == "/custom/cache"
    assert module.os.environ["LOG_LEVEL"] == "INFO"


def test_parse_args_and_configure_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_smoke_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect-production-smoke-baseline.py",
            "--runs",
            "5",
            "--dataset-preset",
            "primeMarket",
            "--limit",
            "25",
        ],
    )
    args = module.parse_args()
    assert args.runs == 5
    assert args.dataset_preset == "primeMarket"
    assert args.limit == 25

    calls: list[tuple[str, Any]] = []

    class _FakeLogger:
        def remove(self) -> None:
            calls.append(("remove", None))

        def add(self, stream: Any, *, level: str) -> None:
            calls.append(("add", level))

    monkeypatch.setattr(module, "logger", _FakeLogger())
    module.configure_logging()
    assert calls == [("remove", None), ("add", "WARNING")]


def test_query_counts_uses_duckdb_and_handles_missing_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_smoke_module()
    counts = {
        "stocks": (2,),
        "stock_data": (10,),
        "topix_data": (1,),
        "indices_data": None,
        "margin_data": (4,),
        "statements": (3,),
    }

    class _FakeConn:
        def __init__(self) -> None:
            self.current_table = ""
            self.closed = False

        def execute(self, sql: str) -> "_FakeConn":
            self.current_table = sql.split()[-1]
            return self

        def fetchone(self) -> tuple[int] | None:
            return counts[self.current_table]

        def close(self) -> None:
            self.closed = True

    fake_conn = _FakeConn()
    fake_duckdb = type("FakeDuckDb", (), {"connect": staticmethod(lambda path, read_only=True: fake_conn)})
    monkeypatch.setattr(module.importlib, "import_module", lambda name: fake_duckdb)

    result = module.query_counts(tmp_path / "market.duckdb")
    assert result == {
        "stocks": 2,
        "stock_data": 10,
        "topix_data": 1,
        "indices_data": 0,
        "margin_data": 4,
        "statements": 3,
    }
    assert fake_conn.closed is True


def test_poll_job_status_returns_terminal_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_smoke_module()
    client = _FakeClient(
        get_responses={
            "/jobs/1": [
                _FakeResponse({"status": "running"}),
                _FakeResponse({"status": "completed", "result": {"ok": True}}),
            ]
        }
    )
    perf_values = iter([0.0, 0.1])
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(perf_values))
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    result = module.poll_job_status(
        client,
        "/jobs/1",
        terminal_statuses={"completed"},
        sleep_seconds=0.0,
        timeout_seconds=10,
    )
    assert result["status"] == "completed"


def test_poll_job_status_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_smoke_module()
    client = _FakeClient(get_responses={"/jobs/2": [_FakeResponse({"status": "running"})]})
    perf_values = iter([0.0, 2.0])
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(perf_values))
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    with pytest.raises(TimeoutError, match="/jobs/2"):
        module.poll_job_status(
            client,
            "/jobs/2",
            terminal_statuses={"completed"},
            sleep_seconds=0.0,
            timeout_seconds=1,
        )


def test_run_smoke_cycle_completed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_smoke_module()
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    (snapshot_dir / "manifest.v2.json").write_text(
        json.dumps({"counts": {"stock_data": 120, "stocks": 2}}),
        encoding="utf-8",
    )

    client = _FakeClient(
        post_responses={
            "/api/analytics/screening/jobs": [_FakeResponse({"job_id": "screen-job"})],
            "/api/backtest/run": [_FakeResponse({"job_id": "backtest-job"})],
            "/api/dataset": [_FakeResponse({"jobId": "dataset-job"})],
        },
        get_responses={
            "/api/analytics/screening/result/screen-job": [
                _FakeResponse({"summary": {"reportPath": str(snapshot_dir / "screening.json")}})
            ],
            "/api/backtest/result/backtest-job": [
                _FakeResponse(
                    {
                        "summary": {"reportPath": str(snapshot_dir / "backtest.json")},
                        "execution_time": 12.5,
                    }
                )
            ],
            "/api/dataset/smoke-topix100-run-1/info": [
                _FakeResponse({"validation": {"isValid": True}})
            ],
        },
    )
    poll_results = iter(
        [
            {"status": "completed"},
            {"status": "completed"},
            {"status": "completed", "result": {"outputPath": str(snapshot_dir)}},
        ]
    )
    perf_values = iter([0.0, 2.0, 10.0, 15.0, 20.0, 30.0])
    monkeypatch.setattr(module, "poll_job_status", lambda *args, **kwargs: next(poll_results))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(perf_values))

    cycle, failures = module.run_smoke_cycle(
        client=client,
        run_index=1,
        strategy="production/foo",
        screening_strategies="foo",
        markets="prime",
        recent_days=20,
        limit=50,
        dataset_preset="topix100",
        timeout_seconds=10,
    )

    assert failures == []
    assert cycle["screening"]["status"] == "completed"
    assert cycle["backtest"]["executionTime"] == 12.5
    assert cycle["datasetBuild"]["status"] == "completed"
    assert cycle["datasetBuild"]["summary"]["stockDataRows"] == 120
    assert cycle["datasetBuild"]["summary"]["stockDataRowsPerMinute"] == 720.0


def test_run_smoke_cycle_records_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_smoke_module()
    client = _FakeClient(
        post_responses={
            "/api/analytics/screening/jobs": [_FakeResponse({"job_id": "screen-job"})],
            "/api/backtest/run": [_FakeResponse({"job_id": "backtest-job"})],
            "/api/dataset": [_FakeResponse({"jobId": "dataset-job"})],
        }
    )
    poll_results = iter(
        [
            {"status": "failed", "error": "screening failed"},
            {"status": "cancelled"},
            {"status": "failed", "error": "dataset failed"},
        ]
    )
    perf_values = iter([0.0, 2.0, 10.0, 15.0, 20.0, 25.0])
    monkeypatch.setattr(module, "poll_job_status", lambda *args, **kwargs: next(poll_results))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(perf_values))

    cycle, failures = module.run_smoke_cycle(
        client=client,
        run_index=2,
        strategy="production/foo",
        screening_strategies="foo",
        markets="prime",
        recent_days=20,
        limit=50,
        dataset_preset="topix100",
        timeout_seconds=10,
    )

    assert cycle["screening"]["status"] == "failed"
    assert cycle["backtest"]["status"] == "cancelled"
    assert cycle["datasetBuild"]["status"] == "failed"
    assert [failure.workload for failure in failures] == ["screening", "backtest", "dataset_build"]


def test_run_smoke_cycle_completed_dataset_requires_manifest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_smoke_module()
    snapshot_dir = tmp_path / "missing-manifest"
    snapshot_dir.mkdir()
    client = _FakeClient(
        post_responses={
            "/api/analytics/screening/jobs": [_FakeResponse({"job_id": "screen-job"})],
            "/api/backtest/run": [_FakeResponse({"job_id": "backtest-job"})],
            "/api/dataset": [_FakeResponse({"jobId": "dataset-job"})],
        },
        get_responses={
            "/api/analytics/screening/result/screen-job": [_FakeResponse({"summary": {}})],
            "/api/backtest/result/backtest-job": [_FakeResponse({"summary": {}, "execution_time": "n/a"})],
        },
    )
    poll_results = iter(
        [
            {"status": "completed"},
            {"status": "completed"},
            {"status": "completed", "result": {"outputPath": str(snapshot_dir)}},
        ]
    )
    perf_values = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
    monkeypatch.setattr(module, "poll_job_status", lambda *args, **kwargs: next(poll_results))
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(perf_values))

    cycle, failures = module.run_smoke_cycle(
        client=client,
        run_index=3,
        strategy="production/foo",
        screening_strategies="foo",
        markets="prime",
        recent_days=20,
        limit=50,
        dataset_preset="topix100",
        timeout_seconds=10,
    )

    assert cycle["backtest"]["executionTime"] is None
    assert cycle["datasetBuild"]["summary"] == {}
    assert failures[0].workload == "dataset_build"
    assert "manifest.v2.json not found" in failures[0].detail["error"]


def test_main_returns_1_when_market_db_is_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_smoke_module()
    args = Namespace(
        runs=1,
        data_root=tmp_path / "data",
        runtime_root=tmp_path / "runtime",
        output=tmp_path / "baseline.json",
        strategy="production/foo",
        screening_strategies="foo",
        markets="prime",
        recent_days=20,
        limit=50,
        dataset_preset="topix100",
        poll_timeout_seconds=30,
    )
    monkeypatch.setattr(module, "parse_args", lambda: args)
    monkeypatch.setattr(module, "configure_logging", lambda: None)
    monkeypatch.setattr(module.os, "chdir", lambda _: None)

    assert module.main() == 1


def test_main_writes_output_and_returns_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_smoke_module()
    market_db = tmp_path / "data" / "market-timeseries" / "market.duckdb"
    market_db.parent.mkdir(parents=True, exist_ok=True)
    market_db.write_text("duckdb", encoding="utf-8")

    args = Namespace(
        runs=2,
        data_root=tmp_path / "data",
        runtime_root=tmp_path / "runtime",
        output=tmp_path / "baseline.json",
        strategy="production/foo",
        screening_strategies="foo",
        markets="prime",
        recent_days=20,
        limit=50,
        dataset_preset="topix100",
        poll_timeout_seconds=30,
    )

    reload_calls: list[str] = []
    settings_module = ModuleType("src.shared.config.settings")
    settings_module.reload_settings = lambda: reload_calls.append("reload")
    app_module = ModuleType("src.entrypoints.http.app")
    app_module.create_app = lambda: object()
    monkeypatch.setitem(sys.modules, "src.shared.config.settings", settings_module)
    monkeypatch.setitem(sys.modules, "src.entrypoints.http.app", app_module)

    health_status = 200

    class _ManagedClient:
        def __init__(self, app: object) -> None:
            self.app = app

        def __enter__(self) -> "_ManagedClient":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            return False

        def get(self, path: str) -> _FakeResponse:
            if path == "/api/health":
                return _FakeResponse({"ok": True}, status_code=health_status, text="ok")
            raise AssertionError(f"Unexpected GET path: {path}")

    cycles = iter(
        [
            (
                {
                    "run": 1,
                    "screening": {"status": "completed", "elapsedSeconds": 1.0},
                    "backtest": {"status": "completed", "elapsedSeconds": 2.0},
                    "datasetBuild": {
                        "status": "completed",
                        "elapsedSeconds": 3.0,
                        "summary": {"stockDataRowsPerMinute": 600.0},
                    },
                },
                [],
            ),
            (
                {
                    "run": 2,
                    "screening": {"status": "completed", "elapsedSeconds": 1.5},
                    "backtest": {"status": "completed", "elapsedSeconds": 2.5},
                    "datasetBuild": {
                        "status": "completed",
                        "elapsedSeconds": 3.5,
                        "summary": {"stockDataRowsPerMinute": 700.0},
                    },
                },
                [],
            ),
        ]
    )
    monkeypatch.setattr(module, "parse_args", lambda: args)
    monkeypatch.setattr(module, "configure_logging", lambda: None)
    monkeypatch.setattr(module, "prepare_runtime_dirs", lambda _: None)
    monkeypatch.setattr(module, "mirror_source_datasets", lambda data_root, runtime_root: None)
    monkeypatch.setattr(module, "set_runtime_env", lambda data_root, runtime_root: None)
    monkeypatch.setattr(module, "query_counts", lambda path: {"stocks": 2})
    monkeypatch.setattr(module, "run_smoke_cycle", lambda **kwargs: next(cycles))
    monkeypatch.setattr(module, "TestClient", _ManagedClient)
    monkeypatch.setattr(module.os, "chdir", lambda _: None)

    assert module.main() == 0
    output = json.loads(args.output.read_text(encoding="utf-8"))
    assert reload_calls == ["reload"]
    assert output["summary"]["datasetBuild"]["medianStockDataRowsPerMinute"] == 650.0
    assert output["failures"] == []

    health_status = 503
    cycles = iter(
        [
            (
                {
                    "run": 1,
                    "screening": {"status": "failed", "elapsedSeconds": 1.0},
                    "backtest": {"status": "completed", "elapsedSeconds": 2.0},
                    "datasetBuild": {"status": "failed", "elapsedSeconds": 3.0, "summary": {}},
                },
                [],
            ),
            (
                {
                    "run": 2,
                    "screening": {"status": "completed", "elapsedSeconds": 1.0},
                    "backtest": {"status": "completed", "elapsedSeconds": 2.0},
                    "datasetBuild": {"status": "completed", "elapsedSeconds": 3.0, "summary": {}},
                },
                [],
            ),
        ]
    )
    monkeypatch.setattr(module, "run_smoke_cycle", lambda **kwargs: next(cycles))

    assert module.main() == 1
