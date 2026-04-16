from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_1330_entry_next_1045_exit_conditioning import (
    TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
    TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME,
    get_topix100_1330_entry_next_1045_exit_conditioning_bundle_path_for_run_id,
    get_topix100_1330_entry_next_1045_exit_conditioning_latest_bundle_path,
    load_topix100_1330_entry_next_1045_exit_conditioning_research_bundle,
    run_topix100_1330_entry_next_1045_exit_conditioning_research,
    write_topix100_1330_entry_next_1045_exit_conditioning_research_bundle,
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
            ("72030", "Alpha", "ALPHA", "0111", "Prime", "13", "Manufacturing", "25", "Electrical Equipment", "TOPIX Core30", "2000-01-01", None, None),
            ("67580", "Beta", "BETA", "0111", "Prime", "13", "Manufacturing", "25", "Electrical Equipment", "TOPIX Core30", "2000-01-01", None, None),
            ("68570", "Gamma", "GAMMA", "0111", "Prime", "10", "Information & Communication", "50", "Services", "TOPIX Large70", "2000-01-01", None, None),
            ("80350", "Delta", "DELTA", "0111", "Prime", "10", "Information & Communication", "50", "Services", "TOPIX Large70", "2000-01-01", None, None),
            ("11110", "Non Topix", "NON", "0111", "Prime", "10", "Information & Communication", "50", "Services", "TOPIX Mid400", "2000-01-01", None, None),
        ],
    )

    dates = (
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
        "2024-01-10",
        "2024-01-11",
    )
    times = ("09:00", "10:45", "13:30", "15:30")
    closes = {
        "72030": {
            "2024-01-05": [100.0, 101.0, 100.0, 100.0],
            "2024-01-08": [100.0, 102.0, 110.0, 111.0],
            "2024-01-09": [111.0, 108.0, 107.0, 108.0],
            "2024-01-10": [108.0, 112.0, 113.0, 114.0],
            "2024-01-11": [114.0, 111.0, 112.0, 113.0],
        },
        "67580": {
            "2024-01-05": [100.0, 99.0, 100.0, 100.0],
            "2024-01-08": [100.0, 98.0, 90.0, 89.0],
            "2024-01-09": [89.0, 94.0, 96.0, 97.0],
            "2024-01-10": [97.0, 93.0, 91.0, 90.0],
            "2024-01-11": [90.0, 92.0, 93.0, 94.0],
        },
        "68570": {
            "2024-01-05": [100.0, 100.0, 100.0, 100.0],
            "2024-01-08": [100.0, 100.0, 101.0, 102.0],
            "2024-01-09": [102.0, 103.0, 104.0, 105.0],
            "2024-01-10": [105.0, 106.0, 107.0, 108.0],
            "2024-01-11": [108.0, 107.0, 106.0, 105.0],
        },
        "80350": {
            "2024-01-05": [100.0, 100.0, 100.0, 100.0],
            "2024-01-08": [100.0, 100.0, 99.0, 98.0],
            "2024-01-09": [98.0, 97.0, 96.0, 95.0],
            "2024-01-10": [95.0, 96.0, 97.0, 98.0],
            "2024-01-11": [98.0, 99.0, 100.0, 101.0],
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


def test_run_research_builds_regime_sector_and_prev_day_cross_outputs(
    analytics_db_path: str,
) -> None:
    result = run_topix100_1330_entry_next_1045_exit_conditioning_research(
        analytics_db_path,
        interval_minutes=5,
        entry_time="13:30",
        exit_time="10:45",
        tail_fraction=0.10,
        prev_day_peak_time="10:45",
    )

    assert result.eligible_session_count == 12
    assert result.regime_day_count == 3
    assert set(result.regime_market_df["market_regime_bucket_key"]) == {
        "weak",
        "neutral",
        "strong",
    }
    assert "Electrical Equipment" in set(result.sector_group_summary_df["segment_label"])
    assert "Services" in set(result.sector_group_summary_df["segment_label"])
    assert "Prev-day 10:45 top 10%" in set(
        result.prev_day_peak_transition_df["prev_day_peak_group_label"]
    )
    assert "Prev-day 10:45 unclassified" in set(
        result.prev_day_peak_transition_df["prev_day_peak_group_label"]
    )
    assert result.regime_comparison_df.shape[0] == 3
    assert "Electrical Equipment" in set(result.sector_comparison_df["segment_label"])
    assert not result.prev_day_peak_group_summary_df.empty


def test_run_research_uses_dynamic_tail_fraction_labels(
    analytics_db_path: str,
) -> None:
    result = run_topix100_1330_entry_next_1045_exit_conditioning_research(
        analytics_db_path,
        interval_minutes=5,
        entry_time="13:30",
        exit_time="10:45",
        tail_fraction=0.05,
        prev_day_peak_time="10:45",
    )

    assert "Current 13:30 top 5%" in set(
        result.prev_day_peak_transition_df["current_entry_bucket_label"]
    )
    assert "Current 13:30 bottom 5%" in set(
        result.prev_day_peak_transition_df["current_entry_bucket_label"]
    )
    assert "Prev-day 10:45 top 5%" in set(
        result.prev_day_peak_transition_df["prev_day_peak_group_label"]
    )
    assert "Prev-day 10:45 bottom 5%" in set(
        result.prev_day_peak_transition_df["prev_day_peak_group_label"]
    )


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_1330_entry_next_1045_exit_conditioning_research(
        analytics_db_path,
        interval_minutes=5,
        entry_time="13:30",
        exit_time="10:45",
        tail_fraction=0.10,
        prev_day_peak_time="10:45",
    )

    bundle = write_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260416_101500_testabcd",
    )
    reloaded = load_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID
    )
    assert (
        get_topix100_1330_entry_next_1045_exit_conditioning_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_1330_entry_next_1045_exit_conditioning_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        bundle.bundle_dir
        / TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME
    ).exists()
    assert reloaded.prev_day_peak_time == result.prev_day_peak_time
    assert reloaded.regime_day_count == result.regime_day_count
    pd.testing.assert_frame_equal(
        reloaded.regime_comparison_df,
        result.regime_comparison_df,
        check_dtype=False,
    )
