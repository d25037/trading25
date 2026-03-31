#!/usr/bin/env python3
"""Runner-first entrypoint for PRIME ex TOPIX500 SMA-ratio rank research."""

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

from src.domains.analytics.topix100_sma_ratio_rank_future_close import (  # noqa: E402
    run_prime_ex_topix500_sma_ratio_rank_future_close_research,
    write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PRIME ex TOPIX500 SMA-ratio rank research and persist a reproducible "
            "artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=get_settings().market_db_path)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--lookback-years", type=int, default=10)
    parser.add_argument("--min-constituents-per-day", type=int, default=400)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--notes", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        min_constituents_per_day=args.min_constituents_per_day,
    )
    bundle = write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle(
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
