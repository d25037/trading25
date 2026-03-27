from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    RANKING_FEATURE_ORDER,
    get_topix100_sma_ratio_rank_future_close_available_date_range,
    run_topix100_sma_ratio_rank_future_close_research,
)


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjustment_factor DOUBLE,
            created_at TEXT
        )
        """
    )

    stocks = [
        ("1111", "Alpha", "ALPHA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("11110", "Alpha Duplicate", "ALPHA DUP", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("2222", "Beta", "BETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("3333", "Gamma", "GAMMA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("4444", "Delta", "DELTA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("5555", "Epsilon", "EPSILON", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("6666", "Zeta", "ZETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("7777", "Eta", "ETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("8888", "Theta", "THETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("9999", "Iota", "IOTA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("1234", "Kappa", "KAPPA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("4321", "Outside", "OUTSIDE", "0111", "プライム", "1", "A", "1", "A", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    dates = pd.bdate_range("2023-01-02", periods=220)
    specs = {
        "1111": (1000.0, 0.0045, 10000.0, 0.0030),
        "2222": (900.0, 0.0035, 9000.0, 0.0025),
        "3333": (800.0, 0.0025, 8000.0, 0.0020),
        "4444": (700.0, 0.0015, 7000.0, 0.0015),
        "5555": (600.0, 0.0008, 6000.0, 0.0010),
        "6666": (500.0, -0.0002, 5000.0, 0.0002),
        "7777": (400.0, -0.0008, 4000.0, -0.0004),
        "8888": (300.0, -0.0015, 3000.0, -0.0010),
        "9999": (200.0, -0.0022, 2000.0, -0.0015),
        "1234": (100.0, -0.0030, 1000.0, -0.0020),
        "4321": (50.0, 0.0002, 1500.0, 0.0003),
    }

    stock_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for code, (base_close, close_growth, base_volume, volume_growth) in specs.items():
        for index, date in enumerate(dates):
            close = base_close * ((1.0 + close_growth) ** index)
            volume = int(round(base_volume * ((1.0 + volume_growth) ** index)))
            open_price = close * 0.995
            stock_rows.append(
                (
                    code,
                    date.strftime("%Y-%m-%d"),
                    open_price,
                    close * 1.01,
                    close * 0.99,
                    close,
                    volume,
                    1.0,
                    None,
                )
            )

    duplicate_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for index, date in enumerate(dates):
        close = 200.0 * ((1.0 + 0.0002) ** index)
        volume = int(round(1000.0 * ((1.0 + 0.0001) ** index)))
        duplicate_rows.append(
            (
                "11110",
                date.strftime("%Y-%m-%d"),
                close * 0.995,
                close * 1.01,
                close * 0.99,
                close,
                volume,
                1.0,
                None,
            )
        )

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows + duplicate_rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_available_date_range_and_default_start_are_returned(
    analytics_db_path: str,
) -> None:
    available_start, available_end = (
        get_topix100_sma_ratio_rank_future_close_available_date_range(analytics_db_path)
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=4,
    )

    assert result.available_start_date == "2023-01-02"
    assert result.available_end_date == "2023-11-03"
    assert result.default_start_date == "2023-01-02"
    assert result.analysis_start_date == "2023-07-28"
    assert result.analysis_end_date == "2023-11-03"
    assert result.topix100_constituent_count == 10
    assert result.valid_date_count == 71
    assert result.stock_day_count == 710
    assert result.ranked_event_count == 4260


def test_ranked_panel_uses_all_sma_ratio_features_and_4digit_preference(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert sorted(result.event_panel_df["code"].unique().tolist()) == [
        "1111",
        "1234",
        "2222",
        "3333",
        "4444",
        "5555",
        "6666",
        "7777",
        "8888",
        "9999",
    ]
    assert tuple(result.ranked_panel_df["ranking_feature"].unique()) == RANKING_FEATURE_ORDER

    feature_summary = result.ranking_feature_summary_df[
        result.ranking_feature_summary_df["ranking_feature"] == "price_sma_20_80"
    ].set_index("feature_quartile")
    assert list(feature_summary.index) == [f"Q{i}" for i in range(1, 11)]
    assert feature_summary["mean_ranking_value"].is_monotonic_decreasing

    volume_summary = result.ranking_feature_summary_df[
        result.ranking_feature_summary_df["ranking_feature"] == "volume_sma_20_80"
    ].set_index("feature_quartile")
    assert volume_summary["mean_ranking_value"].is_monotonic_decreasing


def test_feature_selection_and_composite_scores_are_returned(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.discovery_end_date == "2021-12-31"
    assert result.validation_start_date == "2022-01-01"
    assert set(result.selected_feature_df["feature_family"]) == {"price", "volume"}
    assert set(result.selected_feature_df["horizon_key"]) == {
        "t_plus_1",
        "t_plus_5",
        "t_plus_10",
    }
    assert len(result.selected_feature_df) == 6

    assert set(result.composite_candidate_df["selected_horizon_key"]) == {
        "t_plus_1",
        "t_plus_5",
        "t_plus_10",
    }
    assert set(result.composite_candidate_df["score_method"]) == {
        "rank_mean",
        "rank_product",
    }
    assert len(result.composite_candidate_df) == 6
    assert len(result.selected_composite_df) == 3

    selected_features = set(result.selected_composite_df["ranking_feature"])
    assert selected_features
    assert selected_features.issubset(
        set(result.selected_composite_ranking_summary_df["ranking_feature"])
    )
    assert selected_features.issubset(
        set(result.selected_composite_future_summary_df["ranking_feature"])
    )
    assert selected_features.issubset(
        set(result.selected_composite_global_significance_df["ranking_feature"])
    )
    assert selected_features.issubset(
        set(result.selected_composite_pairwise_significance_df["ranking_feature"])
    )
    assert not result.extreme_vs_middle_summary_df.empty
    assert not result.extreme_vs_middle_significance_df.empty


def test_extreme_vs_middle_tables_are_returned(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    row = result.extreme_vs_middle_significance_df[
        (result.extreme_vs_middle_significance_df["ranking_feature"] == "price_sma_20_80")
        & (result.extreme_vs_middle_significance_df["horizon_key"] == "t_plus_10")
        & (result.extreme_vs_middle_significance_df["metric_key"] == "future_return")
    ].iloc[0]

    assert row["n_dates"] > 0
    assert row["extreme_group_label"] == "Q1 + Q10"
    assert row["middle_group_label"] == "Q4 + Q5 + Q6"
    assert pd.notna(row["extreme_minus_middle_mean"])


def test_nested_volume_split_tables_are_returned(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    summary = result.nested_volume_split_summary_df[
        result.nested_volume_split_summary_df["horizon_key"] == "t_plus_10"
    ]
    assert set(summary["nested_combined_bucket"]) == {
        "extreme_volume_high",
        "extreme_volume_low",
        "middle_volume_high",
        "middle_volume_low",
    }

    global_row = result.nested_volume_split_global_significance_df[
        (result.nested_volume_split_global_significance_df["horizon_key"] == "t_plus_10")
        & (
            result.nested_volume_split_global_significance_df["metric_key"]
            == "future_return"
        )
    ].iloc[0]
    assert global_row["n_dates"] > 0

    interaction_row = result.nested_volume_split_interaction_df[
        (result.nested_volume_split_interaction_df["horizon_key"] == "t_plus_10")
        & (result.nested_volume_split_interaction_df["metric_key"] == "future_return")
    ].iloc[0]
    assert pd.notna(interaction_row["interaction_difference"])


def test_q1_q10_volume_split_tables_are_returned(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    summary = result.q1_q10_volume_split_summary_df[
        result.q1_q10_volume_split_summary_df["horizon_key"] == "t_plus_10"
    ]
    assert not result.q1_q10_volume_split_panel_df.empty
    assert {"q1_volume_high", "q10_volume_high"}.issubset(
        set(summary["q1_q10_combined_bucket"])
    )

    global_row = result.q1_q10_volume_split_global_significance_df[
        (result.q1_q10_volume_split_global_significance_df["horizon_key"] == "t_plus_10")
        & (
            result.q1_q10_volume_split_global_significance_df["metric_key"]
            == "future_return"
        )
    ].iloc[0]
    assert global_row["n_dates"] >= 0

    interaction_row = result.q1_q10_volume_split_interaction_df[
        (result.q1_q10_volume_split_interaction_df["horizon_key"] == "t_plus_10")
        & (result.q1_q10_volume_split_interaction_df["metric_key"] == "future_return")
    ].iloc[0]
    assert interaction_row["n_dates"] >= 0


def test_q10_low_hypothesis_tables_are_returned(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert not result.q10_middle_volume_split_panel_df.empty
    summary = result.q10_middle_volume_split_summary_df[
        result.q10_middle_volume_split_summary_df["horizon_key"] == "t_plus_10"
    ]
    assert {"q10_volume_high", "middle_volume_high"}.issubset(
        set(summary["q10_middle_combined_bucket"])
    )

    hypothesis = result.q10_low_hypothesis_df[
        (result.q10_low_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.q10_low_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(hypothesis["hypothesis_label"]) == {
        "Q10 Low vs Q10 High",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }


def test_significance_tables_detect_ratio_rank_difference(
    analytics_db_path: str,
) -> None:
    result = run_topix100_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    global_row = result.global_significance_df[
        (result.global_significance_df["ranking_feature"] == "price_sma_20_80")
        & (result.global_significance_df["horizon_key"] == "t_plus_10")
        & (result.global_significance_df["metric_key"] == "future_return")
    ].iloc[0]

    assert global_row["n_dates"] == 61
    assert global_row["q1_mean"] > global_row["q4_mean"]
    assert global_row["friedman_p_value"] < 0.05
    assert global_row["kruskal_p_value"] < 0.05

    pairwise_row = result.pairwise_significance_df[
        (result.pairwise_significance_df["ranking_feature"] == "price_sma_20_80")
        & (result.pairwise_significance_df["horizon_key"] == "t_plus_10")
        & (result.pairwise_significance_df["metric_key"] == "future_return")
        & (result.pairwise_significance_df["left_quartile"] == "Q1")
        & (result.pairwise_significance_df["right_quartile"] == "Q10")
    ].iloc[0]

    assert pairwise_row["mean_difference"] > 0
    assert pairwise_row["paired_t_p_value_holm"] < 0.05
    assert pairwise_row["wilcoxon_p_value_holm"] < 0.05
