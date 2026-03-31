#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX close / stock overnight distribution."""

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

from src.domains.analytics.topix_close_stock_overnight_distribution import (  # noqa: E402
    get_topix_close_return_stats,
    run_topix_close_stock_overnight_distribution,
    write_topix_close_stock_overnight_distribution_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_csv_list(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX close / stock overnight distribution research and persist "
            "a reproducible artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Event start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Event end date (YYYY-MM-DD).")
    parser.add_argument(
        "--sigma-threshold-1",
        type=float,
        default=1.0,
        help="TOPIX close lower sigma threshold.",
    )
    parser.add_argument(
        "--sigma-threshold-2",
        type=float,
        default=2.0,
        help="TOPIX close upper sigma threshold.",
    )
    parser.add_argument(
        "--selected-groups",
        default=None,
        help="Optional comma-separated stock group keys.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=2000,
        help="Deterministic sample size per group/bucket.",
    )
    parser.add_argument(
        "--clip-lower",
        type=float,
        default=1.0,
        help="Lower clipping percentile for bundle-ready samples.",
    )
    parser.add_argument(
        "--clip-upper",
        type=float,
        default=99.0,
        help="Upper clipping percentile for bundle-ready samples.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Optional override for the research bundle root directory.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional explicit run id. Defaults to a timestamp + git short SHA.",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional free-form note stored in manifest.json.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    close_return_stats = get_topix_close_return_stats(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        sigma_threshold_1=args.sigma_threshold_1,
        sigma_threshold_2=args.sigma_threshold_2,
    )
    if close_return_stats is None:
        raise ValueError("No analyzable TOPIX close rows in selected range.")
    result = run_topix_close_stock_overnight_distribution(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        close_threshold_1=close_return_stats.threshold_1,
        close_threshold_2=close_return_stats.threshold_2,
        close_return_stats=close_return_stats,
        selected_groups=_parse_csv_list(args.selected_groups),
        sample_size=args.sample_size,
        clip_percentiles=(args.clip_lower, args.clip_upper),
    )
    bundle = write_topix_close_stock_overnight_distribution_research_bundle(
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
