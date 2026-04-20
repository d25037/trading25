from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.jpx_daily_price_limits import (
    resolve_standard_daily_limit_width,
)
from src.domains.analytics.stop_limit_daily_classification import (
    UNMAPPED_LATEST_MARKET_NAME,
    run_stop_limit_daily_classification_research,
)



def _create_market_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT,
            market_name TEXT,
            sector_17_code TEXT,
            sector_17_name TEXT,
            sector_33_code TEXT,
            sector_33_name TEXT,
            scale_category TEXT,
            listed_date TEXT,
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


def _build_test_market_db(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        _create_market_tables(conn)
        conn.executemany(
            """
            INSERT INTO stocks (
                code,
                company_name,
                company_name_english,
                market_code,
                market_name,
                sector_17_code,
                sector_17_name,
                sector_33_code,
                sector_33_name,
                scale_category,
                listed_date,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "1111",
                    "Prime A",
                    "Prime A",
                    "0111",
                    "プライム",
                    "1",
                    "A",
                    "1",
                    "A",
                    "TOPIX Small 1",
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "2222",
                    "Standard A",
                    "Standard A",
                    "0112",
                    "スタンダード",
                    "1",
                    "A",
                    "1",
                    "A",
                    "-",
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "3333",
                    "Growth A",
                    "Growth A",
                    "0113",
                    "グロース",
                    "1",
                    "A",
                    "1",
                    "A",
                    "-",
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "33330",
                    "Growth A Duplicate",
                    "Growth A Duplicate",
                    "0113",
                    "グロース",
                    "1",
                    "A",
                    "1",
                    "A",
                    "-",
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "4444",
                    "Prime B",
                    "Prime B",
                    "0111",
                    "プライム",
                    "1",
                    "A",
                    "1",
                    "A",
                    "-",
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "5555",
                    "Standard Reversal",
                    "Standard Reversal",
                    "0112",
                    "スタンダード",
                    "1",
                    "A",
                    "1",
                    "A",
                    "-",
                    "2000-01-01",
                    None,
                    None,
                ),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_data (
                code,
                date,
                open,
                high,
                low,
                close,
                volume,
                adjustment_factor,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("1111", "2026-01-01", 99, 99, 99, 99, 100, 1, None),
                ("1111", "2026-01-02", 120, 129, 120, 129, 100, 1, None),
                ("1111", "2026-01-03", 100, 100, 79, 79, 200, 1, None),
                ("1111", "2026-01-04", 90, 90, 90, 90, 100, 1, None),
                ("2222", "2026-01-01", 420, 420, 420, 420, 100, 1, None),
                ("2222", "2026-01-02", 500, 500, 500, 500, 0, 1, None),
                ("2222", "2026-01-03", 510, 520, 510, 520, 100, 1, None),
                ("33330", "2026-01-01", 99, 99, 99, 99, 100, 1, None),
                ("3333", "2026-01-02", 69, 129, 69, 129, 100, 1, None),
                ("3333", "2026-01-03", 100, 100, 100, 100, 100, 1, None),
                ("9999", "2026-01-01", 99, 99, 99, 99, 100, 1, None),
                ("9999", "2026-01-02", 69, 69, 69, 69, 0, 1, None),
                ("9999", "2026-01-03", 60, 60, 60, 60, 100, 1, None),
                ("4444", "2026-01-01", 99, 99, 99, 99, 100, 1, None),
                ("4444", "2026-01-02", 100, 140, 100, 120, 100, 1, None),
                ("5555", "2026-01-01", 420, 420, 420, 420, 100, 1, None),
                ("5555", "2026-01-02", 430, 500, 430, 490, 100, 1, None),
                ("5555", "2026-01-03", 470, 470, 460, 460, 100, 1, None),
                ("5555", "2026-01-04", 455, 455, 455, 455, 100, 1, None),
                ("5555", "2026-01-05", 440, 440, 440, 440, 100, 1, None),
                ("5555", "2026-01-06", 430, 430, 430, 430, 100, 1, None),
                ("5555", "2026-01-07", 420, 420, 420, 420, 100, 1, None),
            ],
        )
    finally:
        conn.close()


def test_resolve_standard_daily_limit_width_boundary_cases() -> None:
    assert resolve_standard_daily_limit_width(None) is None
    assert resolve_standard_daily_limit_width(0) is None
    assert resolve_standard_daily_limit_width(99) == 30
    assert resolve_standard_daily_limit_width(100) == 50
    assert resolve_standard_daily_limit_width(499) == 80
    assert resolve_standard_daily_limit_width(500) == 100
    assert resolve_standard_daily_limit_width(50_000_000) == 10_000_000


def test_run_stop_limit_daily_classification_research(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _build_test_market_db(db_path)

    result = run_stop_limit_daily_classification_research(str(db_path))

    assert result.total_event_count == 6
    assert result.total_directional_event_count == 5
    assert result.total_outside_standard_band_count == 1
    assert result.unmapped_latest_market_event_count == 1

    event_df = result.event_df
    assert set(event_df["limit_side"]) == {"stop_high", "stop_low", "both"}

    prime_stop_high = event_df[
        (event_df["code"] == "1111") & (event_df["date"] == "2026-01-02")
    ].iloc[0]
    assert prime_stop_high["market_name"] == "プライム"
    assert bool(prime_stop_high["intraday_range"]) is True
    assert prime_stop_high["limit_side"] == "stop_high"
    assert prime_stop_high["close_limit_state"] == "stop_high"
    assert prime_stop_high["next_date"] == "2026-01-03"
    assert float(prime_stop_high["next_close_return"]) == 79 / 129 - 1
    assert float(prime_stop_high["next_close_directional_return"]) == 79 / 129 - 1

    standard_single_price = event_df[
        (event_df["code"] == "2222") & (event_df["date"] == "2026-01-02")
    ].iloc[0]
    assert bool(standard_single_price["single_price_day"]) is True
    assert standard_single_price["intraday_state"] == "single_price"
    assert float(standard_single_price["next_close_directional_return"]) == 520 / 500 - 1

    growth_both = event_df[
        (event_df["code"] == "3333") & (event_df["date"] == "2026-01-02")
    ].iloc[0]
    assert growth_both["market_name"] == "グロース"
    assert growth_both["limit_side"] == "both"
    assert pd.isna(growth_both["next_close_directional_return"])

    unmapped = event_df[
        (event_df["code"] == "9999") & (event_df["date"] == "2026-01-02")
    ].iloc[0]
    assert unmapped["market_name"] == UNMAPPED_LATEST_MARKET_NAME
    assert bool(unmapped["latest_market_mapped"]) is False

    outside_df = result.outside_standard_band_df
    assert len(outside_df) == 1
    anomaly = outside_df.iloc[0]
    assert anomaly["code"] == "4444"
    assert anomaly["outside_standard_side"] == "above_standard_upper"

    reversal_candidate = event_df[
        (event_df["code"] == "5555") & (event_df["date"] == "2026-01-02")
    ].iloc[0]
    assert reversal_candidate["close_limit_state"] == "off_limit_close"
    assert reversal_candidate["intraday_state"] == "intraday_range"
    assert float(reversal_candidate["next_open_to_next_close_return"]) == 460 / 470 - 1

    summary_df = result.summary_df
    summary_row = summary_df[
        (summary_df["market_name"] == "スタンダード")
        & (summary_df["limit_side"] == "stop_high")
        & (summary_df["intraday_state"] == "single_price")
    ].iloc[0]
    assert int(summary_row["event_count"]) == 1
    assert int(summary_row["unique_code_count"]) == 1

    continuation_summary_df = result.continuation_summary_df
    standard_next_close = continuation_summary_df[
        (continuation_summary_df["market_name"] == "スタンダード")
        & (continuation_summary_df["limit_side"] == "stop_high")
        & (continuation_summary_df["intraday_state"] == "single_price")
        & (continuation_summary_df["close_limit_state"] == "stop_high")
        & (continuation_summary_df["horizon_key"] == "next_close")
    ].iloc[0]
    assert int(standard_next_close["directional_sample_count"]) == 1
    assert int(standard_next_close["continuation_count"]) == 1
    assert int(standard_next_close["reversal_count"]) == 0

    prime_next_close = continuation_summary_df[
        (continuation_summary_df["market_name"] == "プライム")
        & (continuation_summary_df["limit_side"] == "stop_high")
        & (continuation_summary_df["intraday_state"] == "intraday_range")
        & (continuation_summary_df["close_limit_state"] == "stop_high")
        & (continuation_summary_df["horizon_key"] == "next_close")
    ].iloc[0]
    assert int(prime_next_close["directional_sample_count"]) == 1
    assert int(prime_next_close["continuation_count"]) == 0
    assert int(prime_next_close["reversal_count"]) == 1

    growth_both_next_close = continuation_summary_df[
        (continuation_summary_df["market_name"] == "グロース")
        & (continuation_summary_df["limit_side"] == "both")
        & (continuation_summary_df["horizon_key"] == "next_close")
    ].iloc[0]
    assert int(growth_both_next_close["event_count"]) == 1
    assert int(growth_both_next_close["directional_sample_count"]) == 0

    candidate_trade_summary_df = result.candidate_trade_summary_df
    continuation_trade = candidate_trade_summary_df[
        (candidate_trade_summary_df["candidate_strategy_family"] == "continuation_single_price")
        & (candidate_trade_summary_df["market_name"] == "スタンダード")
        & (candidate_trade_summary_df["limit_side"] == "stop_high")
        & (candidate_trade_summary_df["trade_horizon_key"] == "next_open_to_next_close")
    ].iloc[0]
    assert continuation_trade["trade_direction_label"] == "long"
    assert int(continuation_trade["sample_count"]) == 1
    assert int(continuation_trade["profit_count"]) == 1

    reversal_trade = candidate_trade_summary_df[
        (candidate_trade_summary_df["candidate_strategy_family"] == "reversal_intraday_off_limit_close")
        & (candidate_trade_summary_df["market_name"] == "スタンダード")
        & (candidate_trade_summary_df["limit_side"] == "stop_high")
        & (candidate_trade_summary_df["trade_horizon_key"] == "next_open_to_next_close")
    ].iloc[0]
    assert reversal_trade["trade_direction_label"] == "short"
    assert int(reversal_trade["sample_count"]) == 1
    assert int(reversal_trade["profit_count"]) == 1
    assert float(reversal_trade["mean_trade_return"]) == -(460 / 470 - 1)
