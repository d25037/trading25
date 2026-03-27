from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_price_vs_sma20_rank_future_close import (
    PRIMARY_PRICE_FEATURE,
    get_topix100_price_vs_sma20_rank_future_close_available_date_range,
    run_topix100_price_vs_sma20_rank_future_close_research,
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
            stock_rows.append(
                (
                    code,
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


def test_available_date_range_and_primary_feature_are_returned(
    analytics_db_path: str,
) -> None:
    available_start, available_end = (
        get_topix100_price_vs_sma20_rank_future_close_available_date_range(
            analytics_db_path
        )
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = run_topix100_price_vs_sma20_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    assert result.available_start_date == "2023-01-02"
    assert result.available_end_date == "2023-11-03"
    assert result.default_start_date == "2023-01-02"
    assert result.analysis_end_date == "2023-11-03"
    assert result.topix100_constituent_count == 10
    assert result.valid_date_count > 0
    assert result.stock_day_count > 0
    assert result.ranked_panel_df["ranking_feature"].unique().tolist() == [
        PRIMARY_PRICE_FEATURE
    ]


def test_deciles_and_price_volume_split_tables_are_built(
    analytics_db_path: str,
) -> None:
    result = run_topix100_price_vs_sma20_rank_future_close_research(
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

    feature_summary = result.ranking_feature_summary_df[
        result.ranking_feature_summary_df["ranking_feature"] == PRIMARY_PRICE_FEATURE
    ].set_index("feature_quartile")
    assert list(feature_summary.index) == [f"Q{i}" for i in range(1, 11)]
    assert feature_summary["mean_ranking_value"].is_monotonic_decreasing

    assert set(result.price_bucket_summary_df["price_bucket"]) == {
        "q1",
        "middle",
        "q10",
    }
    assert {
        "q1_volume_high",
        "middle_volume_low",
        "q10_volume_high",
    }.issubset(set(result.price_volume_split_summary_df["combined_bucket"]))


def test_hypothesis_tables_contain_expected_labels(analytics_db_path: str) -> None:
    result = run_topix100_price_vs_sma20_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=10,
    )

    group_hypothesis = result.group_hypothesis_df[
        (result.group_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.group_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(group_hypothesis["hypothesis_label"]) == {
        "Q1 vs Middle",
        "Q10 vs Middle",
        "Q1 vs Q10",
    }

    split_hypothesis = result.split_hypothesis_df[
        (result.split_hypothesis_df["horizon_key"] == "t_plus_10")
        & (result.split_hypothesis_df["metric_key"] == "future_return")
    ]
    assert set(split_hypothesis["hypothesis_label"]) == {
        "Q1 High vs Q1 Low",
        "Q10 Low vs Q10 High",
        "Q1 High vs Middle High",
        "Q1 Low vs Middle Low",
        "Q10 Low vs Middle Low",
        "Q10 Low vs Middle High",
    }
