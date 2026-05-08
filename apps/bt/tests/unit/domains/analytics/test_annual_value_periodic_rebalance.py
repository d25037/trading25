from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.annual_value_periodic_rebalance import (
    ANNUAL_VALUE_PERIODIC_REBALANCE_EXPERIMENT_ID,
    get_annual_value_periodic_rebalance_bundle_path_for_run_id,
    get_annual_value_periodic_rebalance_latest_bundle_path,
    load_annual_value_periodic_rebalance_bundle,
    run_annual_value_periodic_rebalance,
    write_annual_value_periodic_rebalance_bundle,
)


_CODES = ("1111", "2222", "3333")


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    try:
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
            CREATE TABLE stock_master_daily (
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                company_name TEXT NOT NULL,
                company_name_english TEXT,
                market_code TEXT NOT NULL,
                market_name TEXT NOT NULL,
                sector_17_code TEXT NOT NULL,
                sector_17_name TEXT NOT NULL,
                sector_33_code TEXT NOT NULL,
                sector_33_name TEXT NOT NULL,
                scale_category TEXT,
                listed_date TEXT,
                created_at TEXT,
                PRIMARY KEY (date, code)
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
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE statements (
                code TEXT NOT NULL,
                disclosed_date TEXT NOT NULL,
                earnings_per_share DOUBLE,
                profit DOUBLE,
                equity DOUBLE,
                type_of_current_period TEXT,
                type_of_document TEXT,
                next_year_forecast_earnings_per_share DOUBLE,
                bps DOUBLE,
                sales DOUBLE,
                operating_profit DOUBLE,
                ordinary_profit DOUBLE,
                operating_cash_flow DOUBLE,
                dividend_fy DOUBLE,
                forecast_dividend_fy DOUBLE,
                next_year_forecast_dividend_fy DOUBLE,
                payout_ratio DOUBLE,
                forecast_payout_ratio DOUBLE,
                next_year_forecast_payout_ratio DOUBLE,
                forecast_eps DOUBLE,
                investing_cash_flow DOUBLE,
                financing_cash_flow DOUBLE,
                cash_and_equivalents DOUBLE,
                total_assets DOUBLE,
                shares_outstanding DOUBLE,
                treasury_shares DOUBLE,
                PRIMARY KEY (code, disclosed_date)
            )
            """
        )
        stock_rows = [
            (
                code,
                f"Standard {code}",
                None,
                "0112",
                "Standard",
                "1",
                "A",
                "1",
                "Machinery",
                "-",
                "2000-01-01",
                None,
                None,
            )
            for code in _CODES
        ]
        conn.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
        master_rows = []
        for date_value in ("2024-01-04", "2024-07-01"):
            for row in stock_rows:
                master_rows.append((date_value, *row[:-1]))
        conn.executemany(
            "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            master_rows,
        )
        base_prices = {"1111": 100.0, "2222": 100.0, "3333": 100.0}
        closes_by_date = {
            "2023-12-29": {"1111": 100.0, "2222": 100.0, "3333": 100.0},
            "2024-01-04": {"1111": 104.0, "2222": 99.0, "3333": 101.0},
            "2024-06-28": {"1111": 130.0, "2222": 92.0, "3333": 110.0},
            "2024-07-01": {"1111": 128.0, "2222": 96.0, "3333": 111.0},
            "2024-12-30": {"1111": 140.0, "2222": 105.0, "3333": 120.0},
        }
        price_rows = []
        for date_value, close_map in closes_by_date.items():
            for code, close in close_map.items():
                open_value = base_prices[code] if date_value in {"2024-01-04", "2024-07-01"} else close
                price_rows.append(
                    (
                        code,
                        date_value,
                        open_value,
                        max(open_value, close) * 1.01,
                        min(open_value, close) * 0.99,
                        close,
                        100_000,
                        1.0,
                        None,
                    )
                )
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", price_rows)
        statement_rows = []
        for index, code in enumerate(_CODES):
            statement_rows.append(
                (
                    code,
                    "2023-05-10",
                    20.0 + index,
                    100.0,
                    1000.0,
                    "FY",
                    "FYFinancialStatements_Consolidated_JP",
                    25.0 - index,
                    250.0 + index * 50.0,
                    500.0,
                    50.0,
                    None,
                    80.0,
                    10.0,
                    None,
                    14.0,
                    0.1,
                    None,
                    0.2,
                    25.0 - index,
                    -20.0,
                    None,
                    None,
                    2000.0,
                    100.0,
                    0.0,
                )
            )
            statement_rows.append(
                (
                    code,
                    "2024-05-10",
                    22.0 + index,
                    110.0,
                    1100.0,
                    "FY",
                    "FYFinancialStatements_Consolidated_JP",
                    27.0 - index,
                    260.0 + index * 50.0,
                    520.0,
                    55.0,
                    None,
                    85.0,
                    11.0,
                    None,
                    15.0,
                    0.1,
                    None,
                    0.2,
                    27.0 - index,
                    -20.0,
                    None,
                    None,
                    2100.0,
                    100.0,
                    0.0,
                )
            )
        conn.executemany(
            "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            statement_rows,
        )
    finally:
        conn.close()
    return str(db_path)


def test_run_annual_value_periodic_rebalance_builds_top_count_portfolio(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_annual_value_periodic_rebalance(
        db_path,
        markets=("standard",),
        rebalance_months=(6, 12),
        selection_counts=(2,),
        start_year=2024,
        end_year=2024,
        min_train_observations=5,
    )

    assert result.rebalance_months == (6, 12)
    assert result.selection_counts == (2,)
    assert result.rebalance_calendar_df["year"].tolist() == [
        "2024-M01-6m",
        "2024-M07-6m",
        "2024-M01-12m",
    ]
    assert not result.selected_event_df.empty
    assert set(result.selected_event_df["selection_count"].astype(int)) == {2}
    assert not result.portfolio_daily_df.empty
    assert not result.portfolio_summary_df.empty

    focus = result.portfolio_summary_df[
        (result.portfolio_summary_df["market_scope"].astype(str) == "standard")
        & (result.portfolio_summary_df["score_method"].astype(str) == "equal_weight")
        & (result.portfolio_summary_df["liquidity_scenario"].astype(str) == "none")
        & (result.portfolio_summary_df["rebalance_months"].astype(int) == 6)
        & (result.portfolio_summary_df["selection_count"].astype(int) == 2)
    ]
    assert len(focus) == 1
    assert int(focus.iloc[0]["realized_event_count"]) == 4
    assert float(focus.iloc[0]["total_return_pct"]) > 0.0
    assert {6, 12}.issubset(set(result.portfolio_summary_df["rebalance_months"].astype(int)))


def test_write_and_load_annual_value_periodic_rebalance_bundle(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")
    result = run_annual_value_periodic_rebalance(
        db_path,
        markets=("standard",),
        rebalance_months=(6,),
        selection_counts=(2,),
        start_year=2024,
        end_year=2024,
        min_train_observations=5,
    )

    bundle = write_annual_value_periodic_rebalance_bundle(
        result,
        output_root=tmp_path,
        run_id="periodic-test",
    )
    loaded = load_annual_value_periodic_rebalance_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_VALUE_PERIODIC_REBALANCE_EXPERIMENT_ID
    assert (
        get_annual_value_periodic_rebalance_bundle_path_for_run_id(
            "periodic-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_value_periodic_rebalance_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    assert loaded.portfolio_summary_df.shape == result.portfolio_summary_df.shape
