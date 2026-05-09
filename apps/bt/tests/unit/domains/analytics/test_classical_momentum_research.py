from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.classical_momentum_research import (
    CLASSICAL_MOMENTUM_RESEARCH_EXPERIMENT_ID,
    load_classical_momentum_research_bundle,
    run_classical_momentum_research,
    write_classical_momentum_research_bundle,
)


def _write_fixture_db(db_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=55).strftime("%Y-%m-%d").tolist()
    stock_rows: list[tuple[str, str, float, float, float, float, int, float, str]] = []
    master_rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str]] = []
    topix_rows: list[tuple[str, float, float, float, float, str]] = []
    for index, date in enumerate(dates):
        topix_close = 1000.0 + index
        topix_rows.append((date, topix_close, topix_close + 1.0, topix_close - 1.0, topix_close, date))
        for code, company_name, offset, trend in (
            ("1000", "Momentum Winner", 0.0, 1.8),
            ("2000", "Momentum Loser", 20.0, 0.2),
            ("3000", "Momentum Middle", 40.0, 0.8),
        ):
            close = 100.0 + offset + index * trend
            open_price = close - 0.5
            high = close + 1.0
            low = close - 1.0
            volume = 10_000 + index * 100
            stock_rows.append((code, date, open_price, high, low, close, volume, 1.0, date))
            master_rows.append(
                (
                    date,
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
                    date,
                )
            )
    conn = duckdb.connect(str(db_path))
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
            created_at VARCHAR
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
    conn.close()


def test_classical_momentum_selects_top_cross_section_and_builds_portfolio(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market.duckdb"
    _write_fixture_db(db_path)

    result = run_classical_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-03-15",
        lookback_specs=((20, 5),),
        hold_sessions=(5,),
        rebalance_interval_sessions=5,
        selection_fractions=(1 / 3,),
        min_avg_trading_value_mil_jpy=0.0,
    )

    assert set(result.universe_summary_df["universe_key"]) == {"standard"}
    assert not result.selected_event_df.empty
    assert set(result.selected_event_df["code"]) == {"1000"}
    assert not result.portfolio_summary_df.empty
    standard_summary = result.portfolio_summary_df.loc[
        result.portfolio_summary_df["universe_key"].eq("standard")
        & result.portfolio_summary_df["lookback_sessions"].eq(20)
        & result.portfolio_summary_df["skip_sessions"].eq(5)
        & result.portfolio_summary_df["hold_sessions"].eq(5)
    ]
    assert not standard_summary.empty
    assert standard_summary["event_count"].iloc[0] >= 1
    assert standard_summary["cagr_pct"].notna().all()


def test_classical_momentum_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    output_root = tmp_path / "research"
    _write_fixture_db(db_path)
    result = run_classical_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-03-15",
        lookback_specs=((20, 5),),
        hold_sessions=(5,),
        rebalance_interval_sessions=5,
        selection_fractions=(1 / 3,),
        min_avg_trading_value_mil_jpy=0.0,
    )

    bundle = write_classical_momentum_research_bundle(
        result,
        output_root=output_root,
        run_id="20260101_000000_test",
    )
    loaded = load_classical_momentum_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == CLASSICAL_MOMENTUM_RESEARCH_EXPERIMENT_ID
    assert loaded.lookback_specs == ((20, 5),)
    assert loaded.portfolio_summary_df.shape == result.portfolio_summary_df.shape
