from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.topix100_sma50_raw_vs_atr_q10_bounce import (
    PERIOD_SEGMENT_ORDER,
    SIGNAL_VARIANT_ORDER,
    TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
    load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle,
    run_topix100_sma50_raw_vs_atr_q10_bounce_research,
    write_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle,
)
from tests.unit.analytics_market_research_db import build_topix100_research_market_db


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(
        tmp_path / "market-sma50-raw-vs-atr.duckdb",
        extra_topix100_constituents=10,
    )


@pytest.fixture
def divergence_db_path(tmp_path: Path) -> str:
    return build_topix100_research_market_db(
        tmp_path / "market-sma50-raw-vs-atr-divergence.duckdb",
        spec_overrides={
            "1111": (500.0, -0.0015, 5000.0, 0.0010),
            "2222": (500.0, -0.0015, 5000.0, 0.0010),
        },
        range_scale_by_code={
            "1111": 1.0,
            "2222": 4.0,
        },
    )


def test_builds_q10_bounce_tables_for_both_signal_variants(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma50_raw_vs_atr_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.signal_variant_order == SIGNAL_VARIANT_ORDER
    assert result.volume_feature == "volume_sma_5_20"
    assert {"open", "high", "low", "sma50", "atr14", "raw_gap", "atr_gap_14"}.issubset(
        result.event_panel_df.columns
    )
    assert set(
        result.q10_middle_volume_split_summary_df["signal_variant"].unique().tolist()
    ) == set(SIGNAL_VARIANT_ORDER)
    assert set(result.q10_low_hypothesis_df["hypothesis_label"]) == {
        "Q10 Low vs Q10 High",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }


def test_atr_gap_diverges_when_raw_gap_is_equal(
    divergence_db_path: str,
) -> None:
    result = run_topix100_sma50_raw_vs_atr_q10_bounce_research(
        divergence_db_path,
        min_constituents_per_day=10,
    )

    pair_df = result.event_panel_df[
        result.event_panel_df["code"].isin(["1111", "2222"])
    ].copy()
    target_date = str(pair_df["date"].max())
    pair_df = pair_df[pair_df["date"] == target_date].set_index("code")

    assert pair_df.loc["1111", "raw_gap"] == pytest.approx(
        pair_df.loc["2222", "raw_gap"]
    )
    assert pair_df.loc["1111", "atr_gap_14"] != pytest.approx(
        pair_df.loc["2222", "atr_gap_14"]
    )
    assert pair_df.loc["1111", "atr14"] < pair_df.loc["2222", "atr14"]


def test_sample_chart_candidates_cover_each_bucket_and_period_segment(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma50_raw_vs_atr_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    counts = (
        result.sample_chart_candidates_df.groupby(["signal_variant", "combined_bucket"])
        .size()
        .to_dict()
    )
    assert counts
    assert set(counts.values()) == {3}
    assert set(
        result.sample_chart_candidates_df["period_segment"].unique().tolist()
    ) == set(PERIOD_SEGMENT_ORDER)


def test_research_bundle_roundtrip_for_sma50_raw_vs_atr_result(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_sma50_raw_vs_atr_q10_bounce_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    bundle = write_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_210000_testabcd",
    )
    reloaded = load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID
    )
    pd.testing.assert_frame_equal(
        reloaded.q10_low_scorecard_df,
        result.q10_low_scorecard_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.sample_chart_candidates_df,
        result.sample_chart_candidates_df,
        check_dtype=False,
    )
