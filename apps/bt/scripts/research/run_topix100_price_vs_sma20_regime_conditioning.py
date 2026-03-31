"""Run TOPIX100 price-vs-SMA20 regime conditioning and persist a research bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from src.domains.analytics.topix100_price_vs_sma20_regime_conditioning import (
    run_topix100_price_vs_sma20_regime_conditioning_research,
    write_topix100_price_vs_sma20_regime_conditioning_research_bundle,
)
from src.shared.config.settings import get_settings


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 price-vs-SMA20 regime conditioning research and persist "
            "a reproducible artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--lookback-years", type=int, default=10)
    parser.add_argument("--min-constituents-per-day", type=int, default=80)
    parser.add_argument("--sigma-threshold-1", type=float, default=1.0)
    parser.add_argument("--sigma-threshold-2", type=float, default=2.0)
    parser.add_argument("--output-root")
    parser.add_argument("--run-id")
    parser.add_argument("--notes")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_price_vs_sma20_regime_conditioning_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        min_constituents_per_day=args.min_constituents_per_day,
        sigma_threshold_1=args.sigma_threshold_1,
        sigma_threshold_2=args.sigma_threshold_2,
    )
    bundle = write_topix100_price_vs_sma20_regime_conditioning_research_bundle(
        result,
        output_root=Path(args.output_root) if args.output_root else None,
        run_id=args.run_id,
        notes=args.notes,
    )
    payload = {
        "experimentId": bundle.experiment_id,
        "runId": bundle.run_id,
        "bundlePath": str(bundle.bundle_dir),
        "manifestPath": str(bundle.manifest_path),
        "resultsDbPath": str(bundle.results_db_path),
        "summaryPath": str(bundle.summary_path),
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
