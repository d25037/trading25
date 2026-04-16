from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_1330_entry_next_1045_exit import (
    TOPIX100_1330_ENTRY_NEXT_1045_EXIT_EXPERIMENT_ID,
    TOPIX100_1330_ENTRY_NEXT_1045_EXIT_OVERVIEW_PLOT_FILENAME,
    get_topix100_1330_entry_next_1045_exit_bundle_path_for_run_id,
    get_topix100_1330_entry_next_1045_exit_latest_bundle_path,
    load_topix100_1330_entry_next_1045_exit_research_bundle,
    run_topix100_1330_entry_next_1045_exit_research,
    write_topix100_1330_entry_next_1045_exit_research_bundle,
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
    times = ("09:00", "10:45", "13:30", "15:30")
    closes = {
        "72030": {
            "2024-01-05": [100.0, 101.0, 100.0, 100.0],
            "2024-01-08": [100.0, 102.0, 110.0, 112.0],
            "2024-01-09": [111.0, 108.0, 109.0, 110.0],
        },
        "67580": {
            "2024-01-05": [100.0, 99.0, 100.0, 100.0],
            "2024-01-08": [100.0, 98.0, 90.0, 89.0],
            "2024-01-09": [90.0, 95.0, 96.0, 97.0],
        },
        "68570": {
            "2024-01-05": [100.0, 100.0, 100.0, 100.0],
            "2024-01-08": [100.0, 100.0, 101.0, 101.0],
            "2024-01-09": [101.0, 102.0, 103.0, 103.0],
        },
        "80350": {
            "2024-01-05": [100.0, 100.0, 100.0, 100.0],
            "2024-01-08": [100.0, 100.0, 99.0, 99.0],
            "2024-01-09": [99.0, 98.0, 99.0, 99.0],
        },
    }
    rows: list[tuple[object, ...]] = []
    for code, date_map in closes.items():
        for date_value in dates:
            previous_close = None
            for time_value, close_price in zip(times, date_map[date_value], strict=True):
                open_price = close_price if previous_close is None else previous_close
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

    rows.append(("11110", "2024-01-08", "13:30", 100.0, 101.0, 99.0, 100.0, 1000, 100000.0, None))
    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_run_research_builds_expected_groups_and_returns(
    analytics_db_path: str,
) -> None:
    result = run_topix100_1330_entry_next_1045_exit_research(
        analytics_db_path,
        interval_minutes=5,
        entry_time="13:30",
        exit_time="10:45",
        tail_fraction=0.10,
    )

    assert result.entry_time == "13:30"
    assert result.exit_time == "10:45"
    assert result.total_entry_session_count == 12
    assert result.eligible_session_count == 4
    assert result.excluded_sessions_without_prev_close == 4
    assert result.excluded_sessions_without_next_session == 4

    comparison_row = result.comparison_summary_df.iloc[0]
    assert int(comparison_row["winners_count"]) == 1
    assert int(comparison_row["losers_count"]) == 1
    assert comparison_row["entry_to_next_exit_mean_spread"] < 0
    assert comparison_row["close_to_next_open_mean_spread"] < 0

    group_summary = result.group_summary_df.set_index("group_label")
    assert group_summary.loc["all", "sample_count"] == 4
    assert group_summary.loc["winners", "mean_prev_close_to_entry_return"] > 0
    assert group_summary.loc["losers", "mean_prev_close_to_entry_return"] < 0
    assert group_summary.loc["winners", "mean_entry_to_next_exit_return"] < 0
    assert group_summary.loc["losers", "mean_entry_to_next_exit_return"] > 0

    timeline_labels = set(result.group_path_summary_df["timeline_label"])
    assert "D 13:30" in timeline_labels
    assert "D+1 10:45" in timeline_labels


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_1330_entry_next_1045_exit_research(
        analytics_db_path,
        interval_minutes=5,
        entry_time="13:30",
        exit_time="10:45",
        tail_fraction=0.10,
    )

    bundle = write_topix100_1330_entry_next_1045_exit_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260416_010500_testabcd",
    )
    reloaded = load_topix100_1330_entry_next_1045_exit_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX100_1330_ENTRY_NEXT_1045_EXIT_EXPERIMENT_ID
    assert (
        get_topix100_1330_entry_next_1045_exit_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_1330_entry_next_1045_exit_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        bundle.bundle_dir
        / TOPIX100_1330_ENTRY_NEXT_1045_EXIT_OVERVIEW_PLOT_FILENAME
    ).exists()
    assert reloaded.entry_time == result.entry_time
    assert reloaded.exit_time == result.exit_time
    assert reloaded.tail_fraction == pytest.approx(result.tail_fraction)
    pd.testing.assert_frame_equal(
        reloaded.comparison_summary_df,
        result.comparison_summary_df,
        check_dtype=False,
    )
