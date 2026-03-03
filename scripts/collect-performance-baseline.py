#!/usr/bin/env python3
"""Collect Phase 6 performance baseline for screening/backtest/build paths."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


@dataclass
class CommandResult:
    elapsed_seconds: float
    return_code: int
    stdout_tail: list[str]
    stderr_tail: list[str]


def _tail(text: str, lines: int = 20) -> list[str]:
    return text.splitlines()[-lines:]


def run_command(command: Sequence[str], cwd: Path) -> CommandResult:
    env = os.environ.copy()
    # CI/local sandbox 環境で権限エラーを避けるため uv cache を writable な場所に固定する。
    env.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = time.perf_counter() - started
    return CommandResult(
        elapsed_seconds=elapsed,
        return_code=completed.returncode,
        stdout_tail=_tail(completed.stdout),
        stderr_tail=_tail(completed.stderr),
    )


def p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return ordered[index]


def round4(value: float) -> float:
    return round(value, 4)


def build_throughput_command(rows: int) -> list[str]:
    snippet = f"""
import time
from src.application.services.stock_data_row_builder import build_stock_data_row

rows = {rows}
quote = {{
    "Code": "13010",
    "Date": "2026-01-05",
    "Open": 1000.0,
    "High": 1010.0,
    "Low": 990.0,
    "Close": 1005.0,
    "Volume": 1000000,
}}

started = time.perf_counter()
for _ in range(rows):
    build_stock_data_row(
        quote,
        normalized_code="1301",
        created_at="2026-01-05T09:00:00+00:00",
    )
elapsed = time.perf_counter() - started
rows_per_min = rows / (elapsed / 60.0)
print(f"{{rows_per_min:.6f}}")
"""
    return [
        "uv",
        "run",
        "--project",
        "apps/bt",
        "python",
        "-c",
        snippet,
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Phase 6 performance baseline")
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of repeated measurements per workload (default: 3)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50000,
        help="Rows for synthetic stock row throughput benchmark (default: 50000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/phase6-performance-baseline.json"),
        help="Output JSON path (default: docs/phase6-performance-baseline.json)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    bt_project = Path("apps/bt")

    workloads: dict[str, list[str]] = {
        "screening": [
            "uv",
            "run",
            "--project",
            str(bt_project),
            "pytest",
            str(bt_project / "tests/unit/server/services/test_screening_service.py"),
            "--maxfail=1",
            "-q",
        ],
        "backtest": [
            "uv",
            "run",
            "--project",
            str(bt_project),
            "pytest",
            str(bt_project / "tests/unit/server/routes/test_backtest.py"),
            "--maxfail=1",
            "-q",
        ],
        "dataset_build": [
            "uv",
            "run",
            "--project",
            str(bt_project),
            "pytest",
            str(
                bt_project
                / "tests/unit/server/test_dataset_builder_service_branches.py::test_build_dataset_writes_manifest_v1"
            ),
            "--maxfail=1",
            "-q",
        ],
    }

    measurements: dict[str, dict[str, object]] = {}
    failures: list[dict[str, object]] = []

    for name, command in workloads.items():
        samples: list[float] = []
        warmup = run_command(command, repo_root)
        if warmup.return_code != 0:
            failures.append(
                {
                    "workload": name,
                    "phase": "warmup",
                    "command": command,
                    "result": asdict(warmup),
                }
            )
            continue

        for _ in range(args.runs):
            result = run_command(command, repo_root)
            if result.return_code != 0:
                failures.append(
                    {
                        "workload": name,
                        "phase": "measure",
                        "command": command,
                        "result": asdict(result),
                    }
                )
                break
            samples.append(result.elapsed_seconds)

        if len(samples) == args.runs:
            measurements[name] = {
                "command": command,
                "samplesSeconds": [round4(v) for v in samples],
                "medianSeconds": round4(statistics.median(samples)),
                "p95Seconds": round4(p95(samples)),
            }

    throughput_cmd = build_throughput_command(args.rows)
    throughput_samples: list[float] = []
    throughput_warmup = run_command(throughput_cmd, repo_root)
    if throughput_warmup.return_code != 0:
        failures.append(
            {
                "workload": "dataset_throughput_rows_per_min",
                "phase": "warmup",
                "command": throughput_cmd,
                "result": asdict(throughput_warmup),
            }
        )

    for _ in range(args.runs):
        if failures:
            break
        result = run_command(throughput_cmd, repo_root)
        if result.return_code != 0:
            failures.append(
                {
                    "workload": "dataset_throughput_rows_per_min",
                    "phase": "measure",
                    "command": throughput_cmd,
                    "result": asdict(result),
                }
            )
            break
        try:
            throughput_samples.append(float(result.stdout_tail[-1]))
        except (IndexError, ValueError):
            failures.append(
                {
                    "workload": "dataset_throughput_rows_per_min",
                    "command": throughput_cmd,
                    "result": asdict(result),
                    "error": "unable to parse rows/min from command output",
                }
            )
            break

    output = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "runs": args.runs,
        "workloads": measurements,
        "datasetBuildThroughputRowsPerMinute": {
            "rowsPerRun": args.rows,
            "samples": [round4(v) for v in throughput_samples],
            "median": round4(statistics.median(throughput_samples)) if throughput_samples else 0.0,
            "p95": round4(p95(throughput_samples)) if throughput_samples else 0.0,
        },
        "failures": failures,
        "notes": [
            "Each workload executes one warmup run before measurement samples.",
            "screening/backtest/build are measured on deterministic unit-test workloads.",
            "dataset throughput is synthetic stock_data row transformation throughput.",
        ],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    print(f"Wrote baseline to {args.output}")

    if failures:
        print("Some baseline commands failed. See failures in output JSON.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
