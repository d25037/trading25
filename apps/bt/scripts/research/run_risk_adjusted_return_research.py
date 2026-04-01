#!/usr/bin/env python3
"""Runner-first entrypoint for the synthetic risk-adjusted-return playground."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _ensure_bt_root_on_path() -> Path:
    bt_root = Path(__file__).resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.insert(0, bt_root_str)
    return bt_root


_BT_ROOT = _ensure_bt_root_on_path()

from src.domains.analytics.risk_adjusted_return_research import (  # noqa: E402
    run_risk_adjusted_return_research,
    write_risk_adjusted_return_research_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the synthetic risk-adjusted-return playground and persist a "
            "reproducible artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--lookback-period", type=int, default=60)
    parser.add_argument("--ratio-type", choices=("sharpe", "sortino"), default="sortino")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-days", type=int, default=504)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--notes", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_risk_adjusted_return_research(
        lookback_period=args.lookback_period,
        ratio_type=args.ratio_type,
        seed=args.seed,
        n_days=args.n_days,
    )
    bundle = write_risk_adjusted_return_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    print(
        json.dumps(
            {
                "experimentId": bundle.experiment_id,
                "runId": bundle.run_id,
                "bundlePath": str(bundle.bundle_dir),
                "manifestPath": str(bundle.manifest_path),
                "resultsDbPath": str(bundle.results_db_path),
                "summaryPath": str(bundle.summary_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
