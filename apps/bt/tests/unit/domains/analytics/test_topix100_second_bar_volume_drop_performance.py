from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_second_bar_volume_drop_performance import (
    SECOND_BAR_VOLUME_DROP_OVERVIEW_PLOT_FILENAME,
    SECOND_BAR_VOLUME_DROP_PERFORMANCE_EXPERIMENT_ID,
    get_topix100_second_bar_volume_drop_performance_bundle_path_for_run_id,
    get_topix100_second_bar_volume_drop_performance_latest_bundle_path,
    load_topix100_second_bar_volume_drop_performance_research_bundle,
    run_topix100_second_bar_volume_drop_performance_research,
    write_topix100_second_bar_volume_drop_performance_research_bundle,
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
                "99840",
                "Topix100 C",
                "TOPIX100 C",
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
        ],
    )

    session_specs = [
        ("72030", "2024-01-05", 50, 99.0, 100.8, 99.2, 98.0),
        ("72030", "2024-01-08", 100, 99.4, 100.6, 99.4, 99.0),
        ("67580", "2024-01-05", 200, 99.6, 100.4, 99.6, 99.5),
        ("67580", "2024-01-08", 400, 100.1, 100.3, 100.7, 100.2),
        ("99840", "2024-01-05", 600, 100.4, 100.8, 101.0, 100.8),
        ("99840", "2024-01-08", 800, 100.6, 101.0, 101.4, 101.2),
    ]
    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]] = []
    for (
        code,
        date,
        second_volume,
        second_close,
        performance_start_close,
        performance_end_close,
        day_close,
    ) in session_specs:
        rows.extend(
            [
                (
                    code,
                    date,
                    "09:00",
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    1000,
                    100000.0,
                    None,
                ),
                (
                    code,
                    date,
                    "09:05",
                    second_close,
                    second_close,
                    second_close,
                    second_close,
                    second_volume,
                    float(second_volume) * second_close,
                    None,
                ),
                (
                    code,
                    date,
                    "10:30",
                    performance_start_close,
                    performance_start_close,
                    performance_start_close,
                    performance_start_close,
                    1000,
                    1000.0 * performance_start_close,
                    None,
                ),
                (
                    code,
                    date,
                    "13:30",
                    performance_end_close,
                    performance_end_close,
                    performance_end_close,
                    performance_end_close,
                    1000,
                    1000.0 * performance_end_close,
                    None,
                ),
                (
                    code,
                    date,
                    "15:30",
                    day_close,
                    day_close,
                    day_close,
                    day_close,
                    1000,
                    1000.0 * day_close,
                    None,
                ),
            ]
        )
    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_run_research_splits_sharp_volume_drop_sessions(
    analytics_db_path: str,
) -> None:
    result = run_topix100_second_bar_volume_drop_performance_research(
        analytics_db_path,
        interval_minutes_list=[5],
        drop_percentile=0.5,
    )

    assert result.analysis_start_date == "2024-01-05"
    assert result.analysis_end_date == "2024-01-08"
    assert result.topix100_constituent_count == 3
    assert result.total_session_count == 6
    assert len(result.session_level_df) == 6

    interval_row = result.interval_summary_df.iloc[0]
    assert interval_row["threshold_ratio"] == pytest.approx(0.3)
    assert interval_row["sharp_drop_count"] == 3
    assert interval_row["non_sharp_drop_count"] == 3
    assert interval_row["performance_start_time"] == "10:30"
    assert interval_row["performance_end_time"] == "13:30"
    assert interval_row["open_to_performance_start_mean_spread"] < 0
    assert interval_row["performance_window_mean_spread"] < 0
    assert interval_row["performance_end_to_close_mean_spread"] < 0
    assert interval_row["open_to_close_mean_spread"] < 0
    assert interval_row["second_to_close_mean_spread"] < 0

    group_df = result.group_comparison_df.set_index("group_key")
    assert group_df.loc["sharp_drop", "open_to_performance_start_mean"] < group_df.loc[
        "non_sharp_drop", "open_to_performance_start_mean"
    ]
    assert group_df.loc["sharp_drop", "performance_window_mean"] < 0
    assert group_df.loc["non_sharp_drop", "performance_window_mean"] > 0
    assert group_df.loc["sharp_drop", "performance_end_to_close_mean"] < group_df.loc[
        "non_sharp_drop", "performance_end_to_close_mean"
    ]
    assert group_df.loc["sharp_drop", "open_to_close_mean"] < 0
    assert group_df.loc["non_sharp_drop", "open_to_close_mean"] > 0


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_second_bar_volume_drop_performance_research(
        analytics_db_path,
        interval_minutes_list=[5],
        drop_percentile=0.5,
    )

    bundle = write_topix100_second_bar_volume_drop_performance_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260415_140000_testabcd",
    )
    reloaded = load_topix100_second_bar_volume_drop_performance_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == SECOND_BAR_VOLUME_DROP_PERFORMANCE_EXPERIMENT_ID
    assert (
        get_topix100_second_bar_volume_drop_performance_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_second_bar_volume_drop_performance_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        bundle.bundle_dir / SECOND_BAR_VOLUME_DROP_OVERVIEW_PLOT_FILENAME
    ).exists()
    assert reloaded.interval_minutes_list == result.interval_minutes_list
    assert reloaded.drop_percentile == pytest.approx(result.drop_percentile)
    assert reloaded.performance_start_time == result.performance_start_time
    assert reloaded.performance_end_time == result.performance_end_time
    pd.testing.assert_frame_equal(
        reloaded.interval_summary_df,
        result.interval_summary_df,
        check_dtype=False,
    )
