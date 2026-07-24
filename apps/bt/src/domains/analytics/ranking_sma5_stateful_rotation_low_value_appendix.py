"""Low-Value appendix adapter for the existing stateful rotation research."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd

from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (
    run_ranking_sma5_score_ring_hard_filter_research,
)
from src.domains.analytics.ranking_sma5_score_ring_stateful_rotation_evidence import (
    RankingSma5ScoreRingStatefulRotationResult,
    build_stateful_rotation_evidence,
)


APPENDIX_RING_MAP = {
    "core_high_high": "low_value_core",
    "near_high_high_1": "low_value_near1",
    "near_high_high_2": "low_value_near2",
}
_RESULT_TABLE_NAMES = (
    "stateful_rotation_summary_df",
    "stateful_rotation_annual_df",
    "stateful_rotation_exit_reason_df",
    "stateful_rotation_decision_df",
    "stateful_rotation_event_df",
    "coverage_diagnostics_df",
)


def transform_low_value_appendix_scores(
    feature_df: pd.DataFrame,
) -> pd.DataFrame:
    """Encode the appendix masks for reuse by the existing three-ring engine."""

    frame = feature_df.copy()
    value = pd.to_numeric(
        frame["value_composite_equal_score"],
        errors="coerce",
    )
    leadership = pd.to_numeric(
        frame["long_hybrid_leadership_score"],
        errors="coerce",
    )
    frame["value_composite_equal_score"] = 1.0 - value
    frame["long_hybrid_leadership_score"] = (
        leadership.ge(0.7).fillna(False).astype(float)
    )
    return frame


def build_low_value_appendix_evidence(
    feature_df: pd.DataFrame,
) -> RankingSma5ScoreRingStatefulRotationResult:
    """Run the existing stateful engine with the appendix ring encoding."""

    result = build_stateful_rotation_evidence(
        transform_low_value_appendix_scores(feature_df)
    )
    remapped_tables = {}
    for table_name in _RESULT_TABLE_NAMES:
        table = getattr(result, table_name).copy()
        if "ring_id" in table:
            table["ring_id"] = table["ring_id"].replace(APPENDIX_RING_MAP)
        remapped_tables[table_name] = table
    return replace(result, **remapped_tables)


def run_ranking_sma5_stateful_rotation_low_value_appendix(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> RankingSma5ScoreRingStatefulRotationResult:
    """Build the Market v5 feature panel and run the Low-Value appendix."""

    research = run_ranking_sma5_score_ring_hard_filter_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
    )
    result = build_low_value_appendix_evidence(research.feature_df)
    result.db_path = research.db_path
    result.analysis_start_date = research.analysis_start_date
    result.analysis_end_date = research.analysis_end_date
    result.market_schema_version = research.pit_lineage.market_schema_version
    result.stock_price_adjustment_mode = (
        research.pit_lineage.stock_price_adjustment_mode
    )
    return result
