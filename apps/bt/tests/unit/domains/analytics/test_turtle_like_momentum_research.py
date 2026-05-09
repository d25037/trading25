from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.turtle_like_momentum_research import (
    TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID,
    load_turtle_like_momentum_research_bundle,
    run_turtle_like_momentum_research,
    write_turtle_like_momentum_research_bundle,
)


def _write_fixture_db(db_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=80)
    stock_rows: list[tuple[object, ...]] = []
    master_rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str]] = []
    topix_rows: list[tuple[object, ...]] = []
    for index, date in enumerate(dates):
        date_str = date.strftime("%Y-%m-%d")
        topix_close = 1000.0 + index
        topix_rows.append((date_str, topix_close, topix_close, topix_close, topix_close, 1_000_000))
        for code, company_name, trend in (
            ("1000", "Trend Winner", 1.4),
            ("2000", "Flat Stock", 0.05),
            ("3000", "Late Breaker", 0.4),
        ):
            close = 80.0 + (10.0 if code == "2000" else 0.0) + index * trend
            if code == "3000" and index > 45:
                close += (index - 45) * 1.2
            open_price = close * 0.995
            high = close * 1.01
            low = close * 0.99
            stock_rows.append((code, date_str, open_price, high, low, close, 100_000, 1.0, None))
            master_rows.append(
                (
                    date_str,
                    code,
                    company_name,
                    company_name,
                    "0112",
                    "Standard",
                    "17",
                    "Sector17",
                    "33",
                    "Sector33",
                    "TOPIX Small 1",
                    "2020-01-01",
                    date_str,
                )
            )
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_data (
                code VARCHAR,
                date VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adjustment_factor DOUBLE,
                created_at VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE topix_data (
                date VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_master_daily (
                date VARCHAR,
                code VARCHAR,
                company_name VARCHAR,
                company_name_english VARCHAR,
                market_code VARCHAR,
                market_name VARCHAR,
                sector_17_code VARCHAR,
                sector_17_name VARCHAR,
                sector_33_code VARCHAR,
                sector_33_name VARCHAR,
                scale_category VARCHAR,
                listed_date VARCHAR,
                created_at VARCHAR
            )
            """
        )
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
        conn.executemany(
            "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            master_rows,
        )
    finally:
        conn.close()


def test_turtle_like_momentum_builds_trade_and_portfolio_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _write_fixture_db(db_path)

    result = run_turtle_like_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-04-30",
        channel_specs=((20, 10),),
        entry_modes=("close_confirmed", "high_touch_next_open"),
        sizing_methods=("equal_weight", "inverse_atr"),
        min_avg_trading_value_mil_jpy=0.0,
    )

    assert set(result.universe_summary_df["universe_key"]) == {"standard"}
    assert not result.trade_ledger_df.empty
    assert set(result.trade_ledger_df["entry_mode"]).issuperset(
        {"close_confirmed", "high_touch_next_open"}
    )
    assert {"equal_weight", "inverse_atr"}.issubset(set(result.portfolio_summary_df["sizing_method"]))
    assert result.portfolio_summary_df["trade_count"].gt(0).all()
    assert result.portfolio_summary_df["cagr_pct"].notna().all()


def test_turtle_like_momentum_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _write_fixture_db(db_path)
    result = run_turtle_like_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-04-30",
        channel_specs=((20, 10),),
        entry_modes=("close_confirmed",),
        sizing_methods=("equal_weight",),
        min_avg_trading_value_mil_jpy=0.0,
    )

    bundle = write_turtle_like_momentum_research_bundle(
        result,
        output_root=tmp_path,
        run_id="turtle-like",
    )
    loaded = load_turtle_like_momentum_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID
    pd.testing.assert_frame_equal(
        loaded.portfolio_summary_df.reset_index(drop=True),
        result.portfolio_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
