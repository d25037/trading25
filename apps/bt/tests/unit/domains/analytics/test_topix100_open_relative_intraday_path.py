from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_open_relative_intraday_path import (
    TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_RESEARCH_EXPERIMENT_ID,
    get_topix100_open_relative_intraday_path_bundle_path_for_run_id,
    get_topix100_open_relative_intraday_path_latest_bundle_path,
    load_topix100_open_relative_intraday_path_research_bundle,
    query_topix100_resampled_intraday_bars,
    run_topix100_open_relative_intraday_path_research,
    write_topix100_open_relative_intraday_path_research_bundle,
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
        CREATE TABLE stock_data_minute_raw (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            turnover_value DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date, time)
        )
        """
    )
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "72030",
                "Topix100 A",
                "TOPIX100 A",
                "0111",
                "プライム",
                "1",
                "A",
                "1",
                "A",
                "TOPIX Large70",
                "2000-01-01",
                None,
                None,
            ),
            (
                "67580",
                "Topix100 B",
                "TOPIX100 B",
                "0111",
                "プライム",
                "1",
                "A",
                "1",
                "A",
                "TOPIX Core30",
                "2000-01-01",
                None,
                None,
            ),
            (
                "11110",
                "Non Topix100",
                "NON TOPIX100",
                "0111",
                "プライム",
                "1",
                "A",
                "1",
                "A",
                "TOPIX Mid400",
                "2000-01-01",
                None,
                None,
            ),
        ],
    )

    rows = [
        ("72030", "2024-01-05", "09:00", 100.0, 100.0, 100.0, 100.0, 1000, 100000.0, None),
        ("72030", "2024-01-05", "09:01", 100.0, 100.0, 99.0, 99.0, 1000, 99000.0, None),
        ("72030", "2024-01-05", "09:04", 99.0, 99.0, 98.0, 98.0, 1000, 98000.0, None),
        ("72030", "2024-01-05", "09:05", 98.0, 98.0, 97.0, 97.0, 1000, 97000.0, None),
        ("72030", "2024-01-05", "09:30", 97.0, 97.0, 95.0, 96.0, 1000, 96000.0, None),
        ("72030", "2024-01-05", "15:30", 96.0, 100.0, 96.0, 99.0, 1000, 99000.0, None),
        ("67580", "2024-01-05", "09:00", 200.0, 200.0, 200.0, 200.0, 1000, 200000.0, None),
        ("67580", "2024-01-05", "09:01", 200.0, 200.0, 198.0, 199.0, 1000, 199000.0, None),
        ("67580", "2024-01-05", "09:04", 199.0, 199.0, 197.0, 198.0, 1000, 198000.0, None),
        ("67580", "2024-01-05", "09:05", 198.0, 198.0, 196.0, 197.0, 1000, 197000.0, None),
        ("67580", "2024-01-05", "09:30", 197.0, 197.0, 194.0, 195.0, 1000, 195000.0, None),
        ("67580", "2024-01-05", "15:30", 195.0, 203.0, 195.0, 202.0, 1000, 202000.0, None),
        ("72030", "2024-01-08", "09:00", 100.0, 100.0, 100.0, 100.0, 1000, 100000.0, None),
        ("72030", "2024-01-08", "09:01", 100.0, 100.0, 99.0, 99.0, 1000, 99000.0, None),
        ("72030", "2024-01-08", "09:04", 99.0, 99.0, 98.0, 98.0, 1000, 98000.0, None),
        ("72030", "2024-01-08", "09:05", 98.0, 98.0, 97.0, 97.0, 1000, 97000.0, None),
        ("72030", "2024-01-08", "09:30", 97.0, 97.0, 94.0, 95.0, 1000, 95000.0, None),
        ("72030", "2024-01-08", "15:30", 95.0, 99.0, 95.0, 98.0, 1000, 98000.0, None),
        ("67580", "2024-01-08", "09:00", 200.0, 200.0, 200.0, 200.0, 1000, 200000.0, None),
        ("67580", "2024-01-08", "09:01", 200.0, 200.0, 199.0, 199.0, 1000, 199000.0, None),
        ("67580", "2024-01-08", "09:04", 199.0, 199.0, 198.0, 198.0, 1000, 198000.0, None),
        ("67580", "2024-01-08", "09:05", 198.0, 198.0, 197.0, 197.0, 1000, 197000.0, None),
        ("67580", "2024-01-08", "09:30", 197.0, 197.0, 195.0, 196.0, 1000, 196000.0, None),
        ("67580", "2024-01-08", "15:30", 196.0, 202.0, 196.0, 201.0, 1000, 201000.0, None),
        ("11110", "2024-01-05", "09:00", 300.0, 301.0, 299.0, 300.0, 1000, 300000.0, None),
    ]
    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_query_resampled_intraday_bars_aggregates_requested_interval(
    analytics_db_path: str,
) -> None:
    bars_df = query_topix100_resampled_intraday_bars(
        analytics_db_path,
        interval_minutes=5,
    )

    assert len(bars_df) == 16
    assert tuple(sorted(bars_df["code"].unique())) == ("6758", "7203")

    first_bar = bars_df[
        (bars_df["date"] == "2024-01-05")
        & (bars_df["code"] == "7203")
        & (bars_df["bucket_time"] == "09:00")
    ].iloc[0]
    assert first_bar["open"] == pytest.approx(100.0)
    assert first_bar["high"] == pytest.approx(100.0)
    assert first_bar["low"] == pytest.approx(98.0)
    assert first_bar["close"] == pytest.approx(98.0)
    assert first_bar["source_bar_count"] == 3
    assert first_bar["close_return_from_open"] == pytest.approx(-0.02)


def test_run_research_summarizes_intraday_path_across_intervals(
    analytics_db_path: str,
) -> None:
    result = run_topix100_open_relative_intraday_path_research(
        analytics_db_path,
        interval_minutes_list=[5, 15, 30],
    )

    assert result.analysis_start_date == "2024-01-05"
    assert result.analysis_end_date == "2024-01-08"
    assert result.topix100_constituent_count == 2
    assert result.total_session_count == 4
    assert tuple(result.interval_summary_df["interval_minutes"]) == (5, 15, 30)

    row_5m = result.interval_summary_df[
        result.interval_summary_df["interval_minutes"] == 5
    ].iloc[0]
    assert row_5m["lowest_mean_close_bucket_time"] == "09:30"
    assert row_5m["highest_session_min_low_bucket_time"] == "09:30"
    assert row_5m["highest_session_min_low_share"] == pytest.approx(1.0)

    path_5m = result.path_summary_df[
        result.path_summary_df["interval_minutes"] == 5
    ]
    assert tuple(path_5m["bucket_time"]) == ("09:00", "09:05", "09:30", "15:30")


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_open_relative_intraday_path_research(
        analytics_db_path,
        interval_minutes_list=[5, 15, 30],
    )

    bundle = write_topix100_open_relative_intraday_path_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260415_130000_testabcd",
    )
    reloaded = load_topix100_open_relative_intraday_path_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_OPEN_RELATIVE_INTRADAY_PATH_RESEARCH_EXPERIMENT_ID
    )
    assert (
        get_topix100_open_relative_intraday_path_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_open_relative_intraday_path_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.interval_minutes_list == result.interval_minutes_list
    assert reloaded.total_session_count == result.total_session_count
    pd.testing.assert_frame_equal(
        reloaded.interval_summary_df,
        result.interval_summary_df,
        check_dtype=False,
    )
