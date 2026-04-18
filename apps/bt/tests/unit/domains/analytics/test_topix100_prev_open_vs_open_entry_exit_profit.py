from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_prev_open_vs_open_entry_exit_profit import (
    TOPIX100_PREV_OPEN_VS_OPEN_ENTRY_EXIT_PROFIT_EXPERIMENT_ID,
    get_topix100_prev_open_vs_open_entry_exit_profit_bundle_path_for_run_id,
    get_topix100_prev_open_vs_open_entry_exit_profit_latest_bundle_path,
    load_topix100_prev_open_vs_open_entry_exit_profit_research_bundle,
    run_topix100_prev_open_vs_open_entry_exit_profit_research,
    write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle,
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
                "Q1",
                "Q1",
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
                "Q2",
                "Q2",
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
                "Q3",
                "Q3",
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
                "Q4",
                "Q4",
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


def _append_opening_bucket_rows(
    *,
    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]],
    code: str,
    date: str,
    total_opening_volume: int,
    opening_price: float,
) -> None:
    per_minute_volume = total_opening_volume // 5
    for minute in ("09:00", "09:01", "09:02", "09:03", "09:04"):
        rows.append(
            (
                code,
                date,
                minute,
                opening_price,
                opening_price,
                opening_price,
                opening_price,
                per_minute_volume,
                float(per_minute_volume) * opening_price,
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

    ratios = {
        "1111": {"prev_open": 100, "curr_open": 50, "1030": 99.0, "1445": 98.0, "close": 97.0, "next_open": 96.0},
        "2222": {"prev_open": 200, "curr_open": 200, "1030": 100.0, "1445": 100.0, "close": 100.0, "next_open": 100.0},
        "3333": {"prev_open": 300, "curr_open": 450, "1030": 101.0, "1445": 102.0, "close": 103.0, "next_open": 104.0},
        "4444": {"prev_open": 400, "curr_open": 800, "1030": 102.0, "1445": 104.0, "close": 106.0, "next_open": 108.0},
    }

    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]] = []
    for code, profile in ratios.items():
        _append_opening_bucket_rows(
            rows=rows,
            code=code,
            date="2024-01-05",
            total_opening_volume=profile["prev_open"],
            opening_price=100.0,
        )
        _append_price_row(rows=rows, code=code, date="2024-01-05", time="09:05", price=100.0)
        _append_price_row(rows=rows, code=code, date="2024-01-05", time="10:30", price=100.0)
        _append_price_row(rows=rows, code=code, date="2024-01-05", time="14:45", price=100.0)
        _append_price_row(rows=rows, code=code, date="2024-01-05", time="15:30", price=100.0)

        _append_opening_bucket_rows(
            rows=rows,
            code=code,
            date="2024-01-08",
            total_opening_volume=profile["curr_open"],
            opening_price=100.0,
        )
        _append_price_row(rows=rows, code=code, date="2024-01-08", time="09:05", price=100.0)
        _append_price_row(rows=rows, code=code, date="2024-01-08", time="10:30", price=profile["1030"])
        _append_price_row(rows=rows, code=code, date="2024-01-08", time="14:45", price=profile["1445"])
        _append_price_row(rows=rows, code=code, date="2024-01-08", time="15:30", price=profile["close"])

        _append_price_row(
            rows=rows,
            code=code,
            date="2024-01-09",
            time="09:00",
            price=profile["next_open"],
            volume=100,
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


def test_run_research_builds_q_buckets_and_entry_exit_spreads(
    analytics_db_path: str,
) -> None:
    result = run_topix100_prev_open_vs_open_entry_exit_profit_research(
        analytics_db_path,
        interval_minutes_list=[5],
        bucket_count=4,
        period_months=6,
    )

    assert result.analysis_start_date == "2024-01-05"
    assert result.analysis_end_date == "2024-01-09"
    assert result.topix100_constituent_count == 4
    assert result.total_session_count == 4
    assert result.session_level_df["entry_time"].unique().tolist() == ["09:05"]
    assert set(result.session_level_df["ratio_bucket_label"]) == {"Q1", "Q2", "Q3", "Q4"}
    assert set(result.trade_level_df["exit_label"]) == {"10:30", "14:45", "close", "next_open"}

    interval_summary = result.interval_summary_df.set_index("exit_label")
    assert interval_summary.loc["10:30", "high_ratio_bucket_label"] == "Q4"
    assert interval_summary.loc["10:30", "low_ratio_bucket_label"] == "Q1"
    assert interval_summary.loc["10:30", "net_long_return_mean_spread_high_minus_low"] == pytest.approx(
        0.03
    )
    assert interval_summary.loc["next_open", "net_long_return_mean_spread_high_minus_low"] == pytest.approx(
        0.12
    )
    assert interval_summary.loc["10:30", "net_short_return_mean_spread_high_minus_low"] == pytest.approx(
        -0.03
    )

    next_open_buckets = result.interval_bucket_summary_df.loc[
        result.interval_bucket_summary_df["exit_label"] == "next_open"
    ].set_index("ratio_bucket_label")
    assert next_open_buckets.loc["Q1", "net_long_return_mean"] == pytest.approx(-0.04)
    assert next_open_buckets.loc["Q4", "net_long_return_mean"] == pytest.approx(0.08)


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_prev_open_vs_open_entry_exit_profit_research(
        analytics_db_path,
        interval_minutes_list=[5],
        bucket_count=4,
        period_months=6,
    )

    bundle = write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260418_150000_testabcd",
    )
    reloaded = load_topix100_prev_open_vs_open_entry_exit_profit_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX100_PREV_OPEN_VS_OPEN_ENTRY_EXIT_PROFIT_EXPERIMENT_ID
    assert bundle.run_id == "20260418_150000_testabcd"
    pd.testing.assert_frame_equal(reloaded.periods_df, result.periods_df)
    pd.testing.assert_frame_equal(reloaded.session_level_df, result.session_level_df)
    pd.testing.assert_frame_equal(reloaded.trade_level_df, result.trade_level_df)
    pd.testing.assert_frame_equal(
        reloaded.interval_summary_df,
        result.interval_summary_df,
    )
    assert (
        get_topix100_prev_open_vs_open_entry_exit_profit_bundle_path_for_run_id(
            "20260418_150000_testabcd",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_prev_open_vs_open_entry_exit_profit_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
