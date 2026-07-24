#!/usr/bin/env python3
"""Runner for the exploratory stateful-rotation Low-Value appendix."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _ensure_bt_root_on_path() -> Path:
    bt_root = Path(__file__).resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.insert(0, bt_root_str)
    return bt_root


_BT_ROOT = _ensure_bt_root_on_path()

from scripts.research.common import (  # noqa: E402
    add_bundle_output_arguments,
    emit_bundle_payload,
    ensure_bt_workdir,
)
from src.domains.analytics.ranking_sma5_score_ring_stateful_rotation_evidence import (  # noqa: E402
    RankingSma5ScoreRingStatefulRotationResult,
)
from src.domains.analytics.ranking_sma5_stateful_rotation_low_value_appendix import (  # noqa: E402
    run_ranking_sma5_stateful_rotation_low_value_appendix,
)
from src.domains.analytics.research_bundle import (  # noqa: E402
    ResearchBundleInfo,
    write_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-stateful-rotation-low-value-appendix"
)
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_END_DATE = "2026-07-21"
DEFAULT_RUN_ID = (
    "20260724_prime_v5_sma5_stateful_rotation_low_value_appendix_v1"
)
_TABLE_NAMES = (
    "stateful_rotation_summary_df",
    "stateful_rotation_annual_df",
    "stateful_rotation_exit_reason_df",
    "stateful_rotation_decision_df",
    "stateful_rotation_event_df",
    "coverage_diagnostics_df",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the stateful-rotation Low-Value appendix."
    )
    parser.add_argument("--db-path", default=get_settings().market_db_path)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    add_bundle_output_arguments(parser)
    parser.set_defaults(run_id=DEFAULT_RUN_ID)
    return parser.parse_args(argv)


def write_ranking_sma5_stateful_rotation_low_value_appendix_bundle(
    result: RankingSma5ScoreRingStatefulRotationResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_tables = {name: getattr(result, name) for name in _TABLE_NAMES}
    return write_research_bundle(
        experiment_id=EXPERIMENT_ID,
        module=(
            "scripts.research."
            "run_ranking_sma5_stateful_rotation_low_value_appendix"
        ),
        function="run_ranking_sma5_stateful_rotation_low_value_appendix",
        params={"cost_levels_bps": [0, 10, 20], "holding_cap": 60},
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "execution_policy": "same_close_one_hop_stateful_rotation",
            "execution_is_optimistic": True,
            "research_status": "exploratory_appendix",
            "long_hybrid_min": 0.7,
            "value_max_by_ring": {
                "low_value_core": 0.2,
                "low_value_near1": 0.3,
                "low_value_near2": 0.4,
            },
            "holding_cap": 60,
            "cost_levels_bps": [0, 10, 20],
            "market_schema_version": result.market_schema_version,
            "stock_price_adjustment_mode": result.stock_price_adjustment_mode,
        },
        result_tables=result_tables,
        summary_markdown=_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def _summary_markdown(
    result: RankingSma5ScoreRingStatefulRotationResult,
) -> str:
    decisions = result.stateful_rotation_decision_df
    if {"decision", "source_trigger_id"}.issubset(decisions):
        candidates = decisions.loc[
            decisions["decision"].eq("stateful_rotation_candidate"),
            "source_trigger_id",
        ].tolist()
    else:
        candidates = []
    candidate_text = ", ".join(candidates) if candidates else "none"
    return (
        "# SMA5 Stateful Rotation Low-Value Appendix\n\n"
        "Exploratory same-Close one-hop stateful rotation appendix.\n\n"
        f"- Stateful rotation candidates: {candidate_text}\n"
        f"- Paired source events: {len(result.stateful_rotation_event_df)}\n"
    )


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_ranking_sma5_stateful_rotation_low_value_appendix(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    bundle = write_ranking_sma5_stateful_rotation_low_value_appendix_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
