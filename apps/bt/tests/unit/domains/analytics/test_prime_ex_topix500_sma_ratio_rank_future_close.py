from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests.unit.analytics_market_research_db import (
    build_prime_ex_topix500_research_market_db,
)
from src.domains.analytics.prime_ex_topix500_sma_ratio_rank_future_close import (
    PRIME_EX_TOPIX500_SMA_RATIO_RESEARCH_EXPERIMENT_ID,
    get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
    get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
    get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path,
    load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
    run_prime_ex_topix500_sma_ratio_rank_future_close_research,
    write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return build_prime_ex_topix500_research_market_db(tmp_path / "market.duckdb")


def test_prime_ex_topix500_available_range_and_universe_counts(
    analytics_db_path: str,
) -> None:
    available_start, available_end = (
        get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range(
            analytics_db_path
        )
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=12,
    )

    assert result.universe_key == "prime_ex_topix500"
    assert result.universe_label == "PRIME ex TOPIX500"
    assert result.universe_constituent_count == 12
    assert result.topix100_constituent_count == 12
    assert result.analysis_start_date == "2023-07-28"
    assert result.analysis_end_date == "2023-11-03"
    assert result.valid_date_count == 71
    assert result.stock_day_count == 852
    assert result.ranked_event_count == 5112


def test_prime_ex_topix500_excludes_topix500_and_standard_codes(
    analytics_db_path: str,
) -> None:
    result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=12,
    )

    assert sorted(result.event_panel_df["code"].unique().tolist()) == [
        "1001",
        "1002",
        "1003",
        "1004",
        "1005",
        "1006",
        "1007",
        "1008",
        "1009",
        "1010",
        "1011",
        "1012",
    ]


def test_prime_ex_topix500_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=12,
    )

    bundle = write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_181500_testabcd",
    )
    reloaded = load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == PRIME_EX_TOPIX500_SMA_RATIO_RESEARCH_EXPERIMENT_ID
    assert (
        get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.universe_key == result.universe_key
    pd.testing.assert_frame_equal(
        reloaded.q10_low_hypothesis_df,
        result.q10_low_hypothesis_df,
        check_dtype=False,
    )
