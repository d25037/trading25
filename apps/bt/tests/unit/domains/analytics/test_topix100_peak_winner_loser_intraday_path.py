from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_peak_winner_loser_intraday_path import (
    TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_EXPERIMENT_ID,
    TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME,
    get_topix100_peak_winner_loser_intraday_path_bundle_path_for_run_id,
    get_topix100_peak_winner_loser_intraday_path_latest_bundle_path,
    load_topix100_peak_winner_loser_intraday_path_research_bundle,
    run_topix100_peak_winner_loser_intraday_path_research,
    write_topix100_peak_winner_loser_intraday_path_research_bundle,
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
            ("72030", "Topix A", "TOPIX A", "0111", "Prime", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
            ("67580", "Topix B", "TOPIX B", "0111", "Prime", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
            ("68570", "Topix C", "TOPIX C", "0111", "Prime", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
            ("80350", "Topix D", "TOPIX D", "0111", "Prime", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
            ("11110", "Non Topix", "NON", "0111", "Prime", "1", "A", "1", "A", "TOPIX Mid400", "2000-01-01", None, None),
        ],
    )

    dates = ("2024-01-05", "2024-01-08", "2024-01-09")
    profiles = {
        "72030": {
            "day1": (100.0, [100.0, 99.0, 98.5, 99.5, 100.0]),
            "day2": (101.0, [101.0, 100.0, 99.5, 100.5, 101.2]),
            "day3": (102.0, [102.0, 101.0, 100.7, 101.5, 102.1]),
        },
        "67580": {
            "day1": (200.0, [200.0, 198.5, 197.0, 198.8, 199.6]),
            "day2": (199.0, [199.0, 197.8, 196.5, 198.4, 199.2]),
            "day3": (198.5, [198.5, 197.0, 195.5, 197.6, 198.6]),
        },
        "68570": {
            "day1": (300.0, [300.0, 306.0, 309.0, 304.0, 302.0]),
            "day2": (302.0, [302.0, 308.0, 311.0, 306.0, 304.0]),
            "day3": (304.0, [304.0, 310.0, 313.5, 308.0, 306.0]),
        },
        "80350": {
            "day1": (400.0, [400.0, 407.0, 410.0, 405.0, 403.0]),
            "day2": (403.0, [403.0, 409.0, 412.5, 407.0, 405.0]),
            "day3": (405.0, [405.0, 411.0, 415.0, 409.0, 406.5]),
        },
    }
    day_keys = ("day1", "day2", "day3")
    times = ("09:00", "10:30", "10:45", "13:30", "15:30")
    rows: list[tuple[object, ...]] = []
    for date_value, day_key in zip(dates, day_keys, strict=True):
        for code, profile in profiles.items():
            day_open, closes = profile[day_key]
            previous_close = None
            for time_value, close_price in zip(times, closes, strict=True):
                open_price = day_open if previous_close is None else previous_close
                high_price = max(open_price, close_price) * 1.001
                low_price = min(open_price, close_price) * 0.999
                rows.append(
                    (
                        code,
                        date_value,
                        time_value,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        1000,
                        close_price * 1000,
                        None,
                    )
                )
                previous_close = close_price

    rows.append(("11110", "2024-01-05", "09:00", 100.0, 101.0, 99.0, 100.0, 1000, 100000.0, None))
    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_run_research_selects_1045_anchor_and_summarizes_paths(
    analytics_db_path: str,
) -> None:
    result = run_topix100_peak_winner_loser_intraday_path_research(
        analytics_db_path,
        interval_minutes=5,
        anchor_candidate_times=("10:45",),
        tail_fraction=0.10,
    )

    assert result.selected_anchor_time == "10:45"
    assert result.tail_fraction == pytest.approx(0.10)
    assert result.total_session_count == 12
    assert result.excluded_sessions_without_prev_close == 4

    open_comparison = result.comparison_summary_df[
        result.comparison_summary_df["split_basis"] == "open_to_peak"
    ].iloc[0]
    assert int(open_comparison["winners_count"]) == 3
    assert int(open_comparison["losers_count"]) == 3
    assert open_comparison["anchor_to_close_mean_spread"] < 0
    assert open_comparison["anchor_to_midday_mean_spread"] < 0

    prev_close_groups = result.group_summary_df[
        result.group_summary_df["split_basis"] == "prev_close_to_peak"
    ]
    assert int(prev_close_groups["sample_count"].max()) == 8

    selected_row = result.anchor_selection_df.loc[
        result.anchor_selection_df["is_selected_anchor"]
    ].iloc[0]
    assert selected_row["candidate_time"] == "10:45"


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_peak_winner_loser_intraday_path_research(
        analytics_db_path,
        interval_minutes=5,
        anchor_candidate_times=("10:45",),
        tail_fraction=0.10,
    )

    bundle = write_topix100_peak_winner_loser_intraday_path_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260415_150500_testabcd",
    )
    reloaded = load_topix100_peak_winner_loser_intraday_path_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_EXPERIMENT_ID
    )
    assert (
        get_topix100_peak_winner_loser_intraday_path_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_peak_winner_loser_intraday_path_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        bundle.bundle_dir
        / TOPIX100_PEAK_WINNER_LOSER_INTRADAY_PATH_OVERVIEW_PLOT_FILENAME
    ).exists()
    assert reloaded.selected_anchor_time == result.selected_anchor_time
    assert reloaded.tail_fraction == pytest.approx(result.tail_fraction)
    pd.testing.assert_frame_equal(
        reloaded.comparison_summary_df,
        result.comparison_summary_df,
        check_dtype=False,
    )
