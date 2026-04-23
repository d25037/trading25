from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
)
from src.domains.analytics.annual_value_composite_selection import (
    ANNUAL_VALUE_COMPOSITE_SELECTION_EXPERIMENT_ID,
    get_annual_value_composite_selection_bundle_path_for_run_id,
    get_annual_value_composite_selection_latest_bundle_path,
    load_annual_value_composite_selection_bundle,
    run_annual_value_composite_selection,
    write_annual_value_composite_selection_bundle,
)
from src.domains.analytics.research_bundle import write_research_bundle


_MARKET_CODES = {"prime": "0111", "standard": "0112", "growth": "0113"}
_MARKET_NAMES = {"prime": "Prime", "standard": "Standard", "growth": "Growth"}


def _sample_event_ledger() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for year_index, year in enumerate(("2021", "2022", "2023", "2024")):
        for market_index, market in enumerate(("prime", "standard", "growth")):
            for rank in range(12):
                code = f"{market_index + 1}{year_index}{rank:02d}"
                value_strength = 11 - rank
                return_pct = (
                    -6.0
                    + value_strength * 2.1
                    + (2.0 if market == "standard" else 0.0)
                    + year_index * 0.4
                )
                entry_open = 100.0
                entry_close = entry_open * (1.0 + return_pct / 300.0)
                mid_close = entry_open * (1.0 + return_pct / 200.0)
                exit_close = entry_open * (1.0 + return_pct / 100.0)
                records.append(
                    {
                        "event_id": f"{code}:{year}",
                        "year": year,
                        "code": code,
                        "company_name": f"Stock {code}",
                        "market": market,
                        "market_code": _MARKET_CODES[market],
                        "sector_33_name": "Machinery" if rank % 2 == 0 else "Services",
                        "status": "realized",
                        "entry_date": f"{year}-01-04",
                        "exit_date": f"{year}-12-30",
                        "entry_open": entry_open,
                        "entry_close": entry_close,
                        "mid_close": mid_close,
                        "exit_close": exit_close,
                        "holding_trading_days": 3,
                        "event_return_pct": return_pct,
                        "pbr": 0.4 + rank * 0.12,
                        "forward_per": 4.0 + rank * 0.8,
                        "per": 5.0 + rank * 0.9,
                        "market_cap_bil_jpy": 3.0 + rank * 5.0,
                        "avg_trading_value_60d_mil_jpy": 5.0 + rank * 4.0,
                        "forecast_dividend_yield_pct": 0.2 + value_strength * 0.15,
                        "dividend_yield_pct": 0.1 + value_strength * 0.12,
                        "cfo_yield_pct": 1.0 + value_strength * 0.3,
                        "forward_eps_to_actual_eps": 0.8 + rank * 0.04,
                    }
                )
    return pd.DataFrame(records)


def _build_market_db(db_path: Path, event_ledger_df: pd.DataFrame) -> str:
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
        stock_rows = [
            (
                str(row["code"]),
                str(row["company_name"]),
                None,
                str(row["market_code"]),
                _MARKET_NAMES[str(row["market"])],
                "1",
                "A",
                "1",
                str(row["sector_33_name"]),
                "-",
                "2000-01-01",
                None,
                None,
            )
            for row in event_ledger_df.to_dict(orient="records")
        ]
        price_rows: list[tuple[object, ...]] = []
        for row in event_ledger_df.to_dict(orient="records"):
            code = str(row["code"])
            year = str(row["year"])
            entry_open = float(row["entry_open"])
            entry_close = float(row["entry_close"])
            mid_close = float(row["mid_close"])
            exit_close = float(row["exit_close"])
            price_rows.extend(
                [
                    (
                        code,
                        f"{year}-01-04",
                        entry_open,
                        max(entry_open, entry_close) * 1.01,
                        min(entry_open, entry_close) * 0.99,
                        entry_close,
                        100_000,
                        1.0,
                        None,
                    ),
                    (
                        code,
                        f"{year}-07-01",
                        entry_close,
                        max(entry_close, mid_close) * 1.01,
                        min(entry_close, mid_close) * 0.99,
                        mid_close,
                        100_000,
                        1.0,
                        None,
                    ),
                    (
                        code,
                        f"{year}-12-30",
                        mid_close,
                        max(mid_close, exit_close) * 1.01,
                        min(mid_close, exit_close) * 0.99,
                        exit_close,
                        100_000,
                        1.0,
                        None,
                    ),
                ]
            )
        conn.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", price_rows)
    finally:
        conn.close()
    return str(db_path)


def _write_input_bundle(tmp_path: Path, db_path: str, event_ledger_df: pd.DataFrame) -> Path:
    bundle = write_research_bundle(
        experiment_id=ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        module="tests.fixture",
        function="build",
        params={},
        db_path=db_path,
        analysis_start_date="2021-01-04",
        analysis_end_date="2024-12-30",
        result_metadata={"db_path": db_path},
        result_tables={"event_ledger_df": event_ledger_df},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-panel",
    )
    return bundle.bundle_dir


def test_run_annual_value_composite_selection_builds_portfolio_tables(tmp_path: Path) -> None:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)

    result = run_annual_value_composite_selection(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.10, 0.20),
        min_train_observations=8,
    )

    assert result.input_run_id == "input-panel"
    assert {"equal_weight_score", "bucket_sum_score"}.issubset(result.scored_panel_df.columns)
    assert not result.walkforward_weight_df.empty
    assert not result.selected_event_df.empty
    assert not result.selection_summary_df.empty
    assert not result.portfolio_daily_df.empty
    assert not result.portfolio_summary_df.empty

    focus = result.portfolio_summary_df[
        (result.portfolio_summary_df["market_scope"].astype(str) == "standard")
        & (result.portfolio_summary_df["score_method"].astype(str) == "equal_weight")
        & (result.portfolio_summary_df["liquidity_scenario"].astype(str) == "adv10m")
        & (result.portfolio_summary_df["selection_fraction"].astype(float) == 0.10)
    ]
    assert len(focus) == 1
    assert float(focus.iloc[0]["total_return_pct"]) > 0.0
    assert int(focus.iloc[0]["realized_event_count"]) == 4


def test_write_and_load_annual_value_composite_selection_bundle(tmp_path: Path) -> None:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)
    result = run_annual_value_composite_selection(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.10,),
        min_train_observations=8,
    )

    bundle = write_annual_value_composite_selection_bundle(
        result,
        output_root=tmp_path,
        run_id="value-composite-test",
    )
    loaded = load_annual_value_composite_selection_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_VALUE_COMPOSITE_SELECTION_EXPERIMENT_ID
    assert (
        get_annual_value_composite_selection_bundle_path_for_run_id(
            "value-composite-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_value_composite_selection_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.portfolio_summary_df.reset_index(drop=True),
        result.portfolio_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
