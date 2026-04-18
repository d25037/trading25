from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_1445_entry_daily_sma_filter_comparison import (
    TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_COMPARISON_EXPERIMENT_ID,
    get_topix100_1445_entry_daily_sma_filter_comparison_bundle_path_for_run_id,
    get_topix100_1445_entry_daily_sma_filter_comparison_latest_bundle_path,
    load_topix100_1445_entry_daily_sma_filter_comparison_research_bundle,
    run_topix100_1445_entry_daily_sma_filter_comparison_research,
    write_topix100_1445_entry_daily_sma_filter_comparison_research_bundle,
)


def _create_tables(conn: duckdb.DuckDBPyConnection) -> None:
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


def _insert_stocks(conn: duckdb.DuckDBPyConnection) -> None:
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                "Alpha",
                "Alpha",
                "0111",
                "Prime",
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
                "2222",
                "Beta",
                "Beta",
                "0111",
                "Prime",
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
                "3333",
                "Gamma",
                "Gamma",
                "0111",
                "Prime",
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
                "4444",
                "Delta",
                "Delta",
                "0111",
                "Prime",
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


def _append_constant_rows(
    *,
    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]],
    code: str,
    date: str,
    times: list[str],
    price: float,
    total_volume: int,
) -> None:
    per_minute_volume = total_volume // len(times)
    for time in times:
        rows.append(
            (
                code,
                date,
                time,
                price,
                price,
                price,
                price,
                per_minute_volume,
                float(per_minute_volume) * price,
                None,
            )
        )


def _append_price_row(
    *,
    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]],
    code: str,
    date: str,
    time: str,
    price: float,
    volume: int = 100,
) -> None:
    rows.append(
        (
            code,
            date,
            time,
            price,
            price,
            price,
            price,
            volume,
            float(volume) * price,
            None,
        )
    )


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    _create_tables(conn)
    _insert_stocks(conn)

    opening_times = ["09:00", "09:01", "09:02", "09:03", "09:04"]
    closing_times = ["15:26", "15:27", "15:28", "15:29", "15:30"]
    dates = ["2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10"]
    profiles = {
        "1111": {
            "opening_volumes": [1600, 800, 400, 200, 100],
            "closing_volume": 100,
            "opens": [100.0, 99.0, 100.0, 102.0, 101.0],
            "at_1030": [100.0, 99.5, 100.0, 103.0, 101.5],
            "entry": [100.0, 95.0, 100.0, 102.0, 101.0],
        },
        "2222": {
            "opening_volumes": [800, 800, 800, 800, 800],
            "closing_volume": 200,
            "opens": [100.0, 100.0, 100.0, 103.0, 102.0],
            "at_1030": [100.0, 100.5, 100.0, 104.0, 102.5],
            "entry": [100.0, 96.0, 100.0, 103.0, 102.0],
        },
        "3333": {
            "opening_volumes": [200, 400, 800, 1600, 3200],
            "closing_volume": 400,
            "opens": [100.0, 101.0, 100.0, 104.0, 103.0],
            "at_1030": [100.0, 101.5, 100.0, 105.0, 103.5],
            "entry": [100.0, 97.0, 100.0, 104.0, 103.0],
        },
        "4444": {
            "opening_volumes": [100, 400, 1600, 6400, 25600],
            "closing_volume": 800,
            "opens": [100.0, 102.0, 100.0, 105.0, 104.0],
            "at_1030": [100.0, 102.5, 100.0, 106.0, 104.5],
            "entry": [100.0, 98.0, 100.0, 105.0, 104.0],
        },
    }

    minute_rows: list[tuple[str, str, str, float, float, float, float, int, float, None]] = []
    daily_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for code, profile in profiles.items():
        for index, date in enumerate(dates):
            _append_constant_rows(
                rows=minute_rows,
                code=code,
                date=date,
                times=opening_times,
                price=profile["opens"][index],
                total_volume=profile["opening_volumes"][index],
            )
            _append_price_row(
                rows=minute_rows,
                code=code,
                date=date,
                time="10:30",
                price=profile["at_1030"][index],
            )
            _append_price_row(
                rows=minute_rows,
                code=code,
                date=date,
                time="14:45",
                price=profile["entry"][index],
            )
            _append_constant_rows(
                rows=minute_rows,
                code=code,
                date=date,
                times=closing_times,
                price=100.0,
                total_volume=profile["closing_volume"],
            )
            daily_rows.append(
                (
                    code,
                    date,
                    profile["opens"][index],
                    max(profile["entry"][index], 100.0),
                    min(profile["entry"][index], profile["opens"][index]),
                    100.0,
                    profile["opening_volumes"][index] + profile["closing_volume"],
                    1.0,
                    None,
                )
            )

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        daily_rows,
    )
    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        minute_rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market-sma-filter.duckdb")


def test_run_research_builds_sma_filter_tables(
    analytics_db_path: str,
) -> None:
    result = run_topix100_1445_entry_daily_sma_filter_comparison_research(
        analytics_db_path,
        interval_minutes=5,
        signal_family="previous_open_vs_open",
        exit_label="next_open",
        daily_sma_windows=[2, 3],
        bucket_count=4,
        period_months=6,
        tail_fraction=0.10,
    )

    assert result.analysis_start_date == "2024-01-04"
    assert result.analysis_end_date == "2024-01-10"
    assert result.daily_sma_windows == (2, 3)
    assert result.interval_minutes == 5
    assert result.signal_family == "previous_open_vs_open"
    assert result.exit_label == "next_open"
    assert result.selected_trade_count > 0
    assert result.sma_trade_count > 0
    assert set(result.sma_trade_level_df["sma_window"]) == {2, 3}
    assert set(result.sma_filter_summary_df["sma_filter_state"]) == {
        "all",
        "above",
        "at_or_below",
    }
    assert set(result.sma_filter_summary_df["market_regime_bucket_key"]) <= {
        "weak",
        "neutral",
        "strong",
    }
    assert set(result.sma_filter_summary_df["subgroup_key"]) <= {
        "all",
        "winners",
        "middle",
        "losers",
    }
    assert "all" in set(result.sma_filter_summary_df["subgroup_key"])
    assert not result.sma_filter_comparison_df.empty
    assert not result.period_sma_filter_summary_df.empty


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_1445_entry_daily_sma_filter_comparison_research(
        analytics_db_path,
        interval_minutes=5,
        signal_family="previous_open_vs_open",
        exit_label="next_open",
        daily_sma_windows=[2, 3],
        bucket_count=4,
        period_months=6,
    )

    bundle = write_topix100_1445_entry_daily_sma_filter_comparison_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260419_120000_testabcd",
    )
    reloaded = load_topix100_1445_entry_daily_sma_filter_comparison_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_COMPARISON_EXPERIMENT_ID
    )
    assert (
        get_topix100_1445_entry_daily_sma_filter_comparison_bundle_path_for_run_id(
            "20260419_120000_testabcd",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_1445_entry_daily_sma_filter_comparison_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        reloaded.selected_trade_level_df,
        result.selected_trade_level_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.sma_trade_level_df,
        result.sma_trade_level_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.sma_filter_summary_df,
        result.sma_filter_summary_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.sma_filter_comparison_df,
        result.sma_filter_comparison_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.period_sma_filter_summary_df,
        result.period_sma_filter_summary_df,
        check_dtype=False,
    )
