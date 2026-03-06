#!/usr/bin/env python3
"""Collect production-scale smoke baseline for bt-035."""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from fastapi.testclient import TestClient
from loguru import logger


@dataclass
class RunFailure:
    run: int
    workload: str
    detail: dict[str, Any]


def p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return ordered[index]


def round4(value: float) -> float:
    return round(value, 4)


def redact_local_path(path: Path) -> str:
    resolved = path.resolve()
    home = Path.home().resolve()
    if resolved == home or home in resolved.parents:
        return f"$HOME/{resolved.relative_to(home).as_posix()}"

    tmp = Path("/tmp")
    private_tmp = Path("/private/tmp")
    if resolved == private_tmp or private_tmp in resolved.parents:
        return f"$TMPDIR/{resolved.relative_to(private_tmp).as_posix()}"
    if resolved == tmp or tmp in resolved.parents:
        return f"$TMPDIR/{resolved.relative_to(tmp).as_posix()}"

    return resolved.as_posix()


def redact_local_path_string(value: str) -> str:
    if not value.startswith("/"):
        return value
    return redact_local_path(Path(value))


def redact_local_paths_in_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: redact_local_paths_in_payload(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [redact_local_paths_in_payload(value) for value in payload]
    if isinstance(payload, str):
        return redact_local_path_string(payload)
    return payload


def configure_logging() -> None:
    # Reduce verbose signal debug logs during baseline collection.
    logger.remove()
    logger.add(sys.stderr, level="WARNING")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect production-scale smoke baseline")
    parser.add_argument("--runs", type=int, default=3, help="Number of repeated runs (default: 3)")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path.home() / ".local" / "share" / "trading25",
        help="Source data root path (default: ~/.local/share/trading25)",
    )
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path("/tmp/trading25-phase6-runtime"),
        help="Writable runtime path for portfolio/backtest artifacts",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/phase6-production-smoke-baseline.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="production/range_break_v15",
        help="Backtest strategy (default: production/range_break_v15)",
    )
    parser.add_argument(
        "--screening-strategies",
        type=str,
        default="range_break_v15",
        help="Screening strategies filter (default: range_break_v15)",
    )
    parser.add_argument("--markets", type=str, default="prime", help="Screening markets (default: prime)")
    parser.add_argument("--recent-days", type=int, default=20, help="Screening recentDays (default: 20)")
    parser.add_argument("--limit", type=int, default=200, help="Screening result limit (default: 200)")
    parser.add_argument(
        "--throughput-rows",
        type=int,
        default=50000,
        help="Synthetic throughput rows for build_stock_data_row benchmark (default: 50000)",
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=int,
        default=900,
        help="Per-job polling timeout seconds (default: 900)",
    )
    return parser.parse_args()


def prepare_runtime_dirs(runtime_root: Path) -> None:
    for rel in (
        "market-timeseries",
        "backtest/results",
        "backtest/attribution",
        "optimization",
        "cache",
    ):
        (runtime_root / rel).mkdir(parents=True, exist_ok=True)


def set_runtime_env(data_root: Path, runtime_root: Path) -> None:
    os.environ["MARKET_DB_PATH"] = str(data_root / "market-timeseries" / "market.duckdb")
    os.environ["DATASET_BASE_PATH"] = str(data_root / "datasets")
    os.environ["PORTFOLIO_DB_PATH"] = str(runtime_root / "portfolio.db")
    os.environ["MARKET_TIMESERIES_DIR"] = str(runtime_root / "market-timeseries")
    os.environ["MARKET_TIMESERIES_BACKEND"] = "duckdb-parquet"
    os.environ["TRADING25_STRATEGIES_DIR"] = str(data_root / "strategies")
    os.environ["TRADING25_BACKTEST_DIR"] = str(runtime_root / "backtest")
    os.environ.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")
    os.environ.setdefault("LOG_LEVEL", "WARNING")


def query_counts(db_path: Path) -> dict[str, int]:
    duckdb = importlib.import_module("duckdb")

    tables = ("stocks", "stock_data", "topix_data", "indices_data", "statements")
    counts: dict[str, int] = {}
    conn = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        for table in tables:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = int(row[0] if row is not None else 0)
    finally:
        conn.close()
    return counts


def poll_job_status(
    client: TestClient,
    path: str,
    terminal_statuses: set[str],
    sleep_seconds: float,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    while True:
        response = client.get(path)
        response.raise_for_status()
        payload = response.json()
        status = str(payload.get("status"))
        if status in terminal_statuses:
            return payload
        if (time.perf_counter() - started) > timeout_seconds:
            raise TimeoutError(f"Polling timeout for {path}: last_status={status}")
        time.sleep(sleep_seconds)


def run_smoke_cycle(
    *,
    client: TestClient,
    run_index: int,
    strategy: str,
    screening_strategies: str,
    markets: str,
    recent_days: int,
    limit: int,
    timeout_seconds: int,
) -> tuple[dict[str, Any], list[RunFailure]]:
    failures: list[RunFailure] = []
    cycle: dict[str, Any] = {"run": run_index}

    screening_payload = {
        "markets": markets,
        "strategies": screening_strategies,
        "recentDays": recent_days,
        "sortBy": "matchedDate",
        "order": "desc",
        "limit": limit,
    }
    screening_started = time.perf_counter()
    screening_create = client.post("/api/analytics/screening/jobs", json=screening_payload)
    screening_create.raise_for_status()
    screening_job_id = screening_create.json()["job_id"]

    screening_status_payload = poll_job_status(
        client,
        f"/api/analytics/screening/jobs/{screening_job_id}",
        terminal_statuses={"completed", "failed", "cancelled"},
        sleep_seconds=1.0,
        timeout_seconds=timeout_seconds,
    )
    screening_status_payload = redact_local_paths_in_payload(screening_status_payload)
    screening_elapsed = time.perf_counter() - screening_started
    screening_status = str(screening_status_payload.get("status"))

    screening_summary: dict[str, Any] = {}
    if screening_status == "completed":
        screening_result = client.get(f"/api/analytics/screening/result/{screening_job_id}")
        screening_result.raise_for_status()
        screening_summary = redact_local_paths_in_payload(screening_result.json().get("summary", {}))
    else:
        failures.append(
            RunFailure(
                run=run_index,
                workload="screening",
                detail={
                    "jobId": screening_job_id,
                    "status": screening_status,
                    "payload": screening_status_payload,
                },
            )
        )

    cycle["screening"] = {
        "jobId": screening_job_id,
        "status": screening_status,
        "elapsedSeconds": round4(screening_elapsed),
        "summary": screening_summary,
        "statusPayload": screening_status_payload,
    }

    backtest_payload = {"strategy_name": strategy}
    backtest_started = time.perf_counter()
    backtest_create = client.post("/api/backtest/run", json=backtest_payload)
    backtest_create.raise_for_status()
    backtest_job_id = backtest_create.json()["job_id"]

    backtest_status_payload = poll_job_status(
        client,
        f"/api/backtest/jobs/{backtest_job_id}",
        terminal_statuses={"completed", "failed", "cancelled"},
        sleep_seconds=2.0,
        timeout_seconds=timeout_seconds,
    )
    backtest_status_payload = redact_local_paths_in_payload(backtest_status_payload)
    backtest_elapsed = time.perf_counter() - backtest_started
    backtest_status = str(backtest_status_payload.get("status"))

    backtest_summary: dict[str, Any] = {}
    backtest_execution_time: float | None = None
    if backtest_status == "completed":
        backtest_result = client.get(f"/api/backtest/result/{backtest_job_id}")
        backtest_result.raise_for_status()
        payload = redact_local_paths_in_payload(backtest_result.json())
        backtest_summary = payload.get("summary", {})
        raw_exec = payload.get("execution_time")
        if isinstance(raw_exec, int | float):
            backtest_execution_time = float(raw_exec)
    else:
        failures.append(
            RunFailure(
                run=run_index,
                workload="backtest",
                detail={
                    "jobId": backtest_job_id,
                    "status": backtest_status,
                    "payload": backtest_status_payload,
                },
            )
        )

    cycle["backtest"] = {
        "jobId": backtest_job_id,
        "status": backtest_status,
        "elapsedSeconds": round4(backtest_elapsed),
        "summary": backtest_summary,
        "executionTime": backtest_execution_time,
        "statusPayload": backtest_status_payload,
    }

    return cycle, failures


def measure_throughput(rows: int) -> dict[str, float]:
    from src.application.services.stock_data_row_builder import build_stock_data_row

    quote = {
        "Code": "13010",
        "Date": "2026-01-05",
        "Open": 1000.0,
        "High": 1010.0,
        "Low": 990.0,
        "Close": 1005.0,
        "Volume": 1000000,
    }

    started = time.perf_counter()
    for _ in range(rows):
        build_stock_data_row(
            quote,
            normalized_code="1301",
            created_at="2026-01-05T09:00:00+00:00",
        )
    elapsed = time.perf_counter() - started
    rows_per_min = rows / (elapsed / 60.0)
    return {"elapsedSeconds": round4(elapsed), "rowsPerMinute": round4(rows_per_min)}


def main() -> int:
    args = parse_args()
    configure_logging()

    repo_root = Path(__file__).resolve().parents[1]
    bt_root = repo_root / "apps" / "bt"
    output_path = args.output if args.output.is_absolute() else (repo_root / args.output)
    os.chdir(bt_root)

    data_root = args.data_root.expanduser().resolve()
    runtime_root = args.runtime_root.resolve()
    prepare_runtime_dirs(runtime_root)
    set_runtime_env(data_root, runtime_root)

    market_db = data_root / "market-timeseries" / "market.duckdb"
    if not market_db.exists():
        print(f"market.duckdb not found: {market_db}", file=sys.stderr)
        return 1

    data_snapshot = {
        "marketDbPath": redact_local_path(market_db),
        "marketDbSizeBytes": market_db.stat().st_size,
        "tableCounts": query_counts(market_db),
    }

    cycles: list[dict[str, Any]] = []
    failures: list[RunFailure] = []

    from src.shared.config.settings import reload_settings
    from src.entrypoints.http.app import create_app

    reload_settings()
    app = create_app()
    with TestClient(app) as client:
        health = client.get("/api/health")
        if health.status_code != 200:
            failures.append(
                RunFailure(
                    run=0,
                    workload="health",
                    detail={"statusCode": health.status_code, "body": health.text},
                )
            )
        for run_index in range(1, args.runs + 1):
            cycle, run_failures = run_smoke_cycle(
                client=client,
                run_index=run_index,
                strategy=args.strategy,
                screening_strategies=args.screening_strategies,
                markets=args.markets,
                recent_days=args.recent_days,
                limit=args.limit,
                timeout_seconds=args.poll_timeout_seconds,
            )
            cycle["healthStatus"] = health.status_code
            cycles.append(cycle)
            failures.extend(run_failures)

    throughput_samples: list[float] = []
    throughput_runs: list[dict[str, float]] = []
    for _ in range(args.runs):
        sample = measure_throughput(args.throughput_rows)
        throughput_runs.append(sample)
        throughput_samples.append(sample["rowsPerMinute"])

    screening_samples = [
        float(c["screening"]["elapsedSeconds"])
        for c in cycles
        if c.get("screening", {}).get("status") == "completed"
    ]
    backtest_samples = [
        float(c["backtest"]["elapsedSeconds"])
        for c in cycles
        if c.get("backtest", {}).get("status") == "completed"
    ]

    output = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "runs": args.runs,
        "conditions": {
            "screening": {
                "markets": args.markets,
                "strategies": args.screening_strategies,
                "recentDays": args.recent_days,
                "sortBy": "matchedDate",
                "order": "desc",
                "limit": args.limit,
            },
            "backtest": {
                "strategy": args.strategy,
            },
            "datasetBuildThroughput": {
                "rowsPerRun": args.throughput_rows,
            },
            "dataSnapshot": data_snapshot,
            "runtimeRoot": redact_local_path(runtime_root),
        },
        "samples": cycles,
        "summary": {
            "screening": {
                "samplesSeconds": [round4(v) for v in screening_samples],
                "medianSeconds": round4(statistics.median(screening_samples)) if screening_samples else 0.0,
                "p95Seconds": round4(p95(screening_samples)) if screening_samples else 0.0,
            },
            "backtest": {
                "samplesSeconds": [round4(v) for v in backtest_samples],
                "medianSeconds": round4(statistics.median(backtest_samples)) if backtest_samples else 0.0,
                "p95Seconds": round4(p95(backtest_samples)) if backtest_samples else 0.0,
            },
            "datasetBuildThroughputRowsPerMinute": {
                "samples": [round4(v) for v in throughput_samples],
                "median": round4(statistics.median(throughput_samples)) if throughput_samples else 0.0,
                "p95": round4(p95(throughput_samples)) if throughput_samples else 0.0,
            },
        },
        "failures": [asdict(f) for f in failures],
        "notes": [
            "Screening/backtest are measured via in-process FastAPI job endpoints using market.duckdb + production strategy config.",
            "Backtest artifacts are written under runtimeRoot to avoid mutating the source data directory.",
            "datasetBuildThroughputRowsPerMinute is synthetic build_stock_data_row throughput.",
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote production smoke baseline to {output_path}")

    if failures:
        print("Some smoke runs failed. See failures in output JSON.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
