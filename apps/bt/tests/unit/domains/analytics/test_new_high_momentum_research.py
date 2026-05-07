from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.new_high_momentum_research import (
    NEW_HIGH_MOMENTUM_EXPERIMENT_ID,
    load_new_high_momentum_research_bundle,
    run_new_high_momentum_research,
    write_new_high_momentum_research_bundle,
)


def _write_fixture_db(db_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=18).strftime("%Y-%m-%d").tolist()
    stock_rows: list[tuple[str, str, float, float, float, float, int, float, str]] = []
    master_rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str]] = []
    topix_rows: list[tuple[str, float, float, float, float, str]] = []
    for index, date in enumerate(dates):
        topix_rows.append((date, 1000.0 + index, 1002.0 + index, 998.0 + index, 1001.0 + index, date))
        for code, market_code, scale_category, company_name, offset in (
            ("1000", "0111", "TOPIX Mid400", "Topix500 Test", 0.0),
            ("2000", "0112", "TOPIX Small 1", "Standard Test", 10.0),
        ):
            open_price = 100.0 + index + offset
            close_price = open_price + 1.0
            high = close_price + 0.5
            low = open_price - 0.5
            volume = 1000 + index * 10
            if index == 9:
                open_price = 109.0 + offset
                close_price = 118.0 + offset
                high = 120.0 + offset
                low = 108.0 + offset
                volume = 4000
            if index == 10:
                open_price = 119.0 + offset
                close_price = 123.0 + offset
                high = 125.0 + offset
                low = 118.0 + offset
                volume = 5000
            if index > 10:
                open_price = 124.0 + offset + (index - 11)
                close_price = open_price + 1.0
                high = close_price + 0.5
                low = open_price - 0.5
                volume = 1500
            stock_rows.append(
                (code, date, open_price, high, low, close_price, volume, 1.0, date)
            )
            master_rows.append(
                (
                    date,
                    code,
                    company_name,
                    company_name,
                    market_code,
                    "Market",
                    "17",
                    "Sector17",
                    "33",
                    "Sector33",
                    scale_category,
                    "2020-01-01",
                    date,
                )
            )
    statement_rows = [
        (
            "1000",
            dates[8],
            10.0,
            1000.0,
            500.0,
            "FY",
            "FinancialStatements",
            12.0,
            100.0,
            2000.0,
            200.0,
            180.0,
            120.0,
            2000.0,
            1_000_000.0,
            11.0,
        ),
        (
            "1000",
            dates[12],
            -10.0,
            -100.0,
            100.0,
            "FY",
            "FinancialStatements",
            -8.0,
            80.0,
            1000.0,
            -100.0,
            -120.0,
            -80.0,
            1000.0,
            1_000_000.0,
            -9.0,
        ),
        (
            "2000",
            dates[8],
            5.0,
            100.0,
            200.0,
            "FY",
            "FinancialStatements",
            4.0,
            50.0,
            1000.0,
            80.0,
            70.0,
            40.0,
            1000.0,
            1_000_000.0,
            4.0,
        ),
    ]
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
    conn.execute(
        """
        CREATE TABLE statements (
            code VARCHAR,
            disclosed_date VARCHAR,
            earnings_per_share DOUBLE,
            profit DOUBLE,
            equity DOUBLE,
            type_of_current_period VARCHAR,
            type_of_document VARCHAR,
            next_year_forecast_earnings_per_share DOUBLE,
            bps DOUBLE,
            sales DOUBLE,
            operating_profit DOUBLE,
            ordinary_profit DOUBLE,
            operating_cash_flow DOUBLE,
            total_assets DOUBLE,
            shares_outstanding DOUBLE,
            forecast_eps DOUBLE
        )
        """
    )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        master_rows,
    )
    conn.executemany("INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", statement_rows)
    conn.close()


def test_run_new_high_momentum_research_builds_event_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _write_fixture_db(db_path)

    result = run_new_high_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-01-25",
        high_windows=(5,),
        horizons=(1, 5),
        sample_event_size=2,
    )

    assert set(result.universe_summary_df["universe_key"]) == {"topix500", "standard"}
    baseline = result.new_high_summary_df.loc[
        result.new_high_summary_df["new_high_window"].eq(5)
        & result.new_high_summary_df["condition_key"].eq("all")
        & result.new_high_summary_df["horizon_days"].eq(5)
    ]
    assert not baseline.empty
    assert baseline["event_count"].min() >= 1
    quality_rows = result.new_high_summary_df.loc[
        result.new_high_summary_df["condition_key"].eq("quality_score_ge_3")
    ]
    assert not quality_rows.empty
    assert not result.sampled_events_df.empty


def test_new_high_momentum_uses_statement_as_of_signal_date(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _write_fixture_db(db_path)

    result = run_new_high_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-01-25",
        high_windows=(5,),
        horizons=(5,),
    )

    sampled = result.sampled_events_df.loc[
        result.sampled_events_df["code"].eq("1000")
        & result.sampled_events_df["date"].eq("2024-01-15")
    ]
    if not sampled.empty:
        assert sampled["forecast_eps"].iloc[0] == 12.0


def test_new_high_momentum_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    output_root = tmp_path / "research"
    _write_fixture_db(db_path)
    result = run_new_high_momentum_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-01-25",
        high_windows=(5,),
        horizons=(5,),
    )

    bundle = write_new_high_momentum_research_bundle(
        result,
        output_root=output_root,
        run_id="20260101_000000_test",
    )
    loaded = load_new_high_momentum_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == NEW_HIGH_MOMENTUM_EXPERIMENT_ID
    assert loaded.high_windows == (5,)
    assert loaded.new_high_summary_df.shape == result.new_high_summary_df.shape
