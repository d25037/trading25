from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
)
from src.domains.analytics.annual_value_composite_selection import (
    run_annual_value_composite_selection,
    write_annual_value_composite_selection_bundle,
)
from src.domains.analytics.annual_value_composite_technical_filter import (
    ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID,
    get_annual_value_composite_technical_filter_bundle_path_for_run_id,
    get_annual_value_composite_technical_filter_latest_bundle_path,
    load_annual_value_composite_technical_filter_bundle,
    run_annual_value_composite_technical_filter,
    write_annual_value_composite_technical_filter_bundle,
)
from src.domains.analytics.research_bundle import write_research_bundle


_MARKET_CODES = {"prime": "0111", "standard": "0112", "growth": "0113"}
_MARKET_NAMES = {"prime": "Prime", "standard": "Standard", "growth": "Growth"}


def _sample_event_ledger() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for year_index, year in enumerate(("2021", "2022")):
        for market_index, market in enumerate(("prime", "standard", "growth")):
            for rank in range(5):
                code = f"{market_index + 1}{year_index}{rank:02d}"
                value_strength = 4 - rank
                return_pct = -3.0 + value_strength * 2.2 + (1.0 if market == "standard" else 0.0)
                records.append(
                    {
                        "event_id": f"{code}:{year}",
                        "year": year,
                        "code": code,
                        "company_name": f"Stock {code}",
                        "market": market,
                        "market_code": _MARKET_CODES[market],
                        "sector_33_name": "Machinery",
                        "status": "realized",
                        "entry_date": f"{year}-01-04",
                        "exit_date": f"{year}-12-30",
                        "entry_open": 100.0,
                        "entry_close": 100.0 + return_pct / 2.0,
                        "mid_close": 100.0 + return_pct,
                        "exit_close": 100.0 * (1.0 + return_pct / 100.0),
                        "holding_trading_days": 240,
                        "event_return_pct": return_pct,
                        "pbr": 0.4 + rank * 0.15,
                        "forward_per": 5.0 + rank,
                        "per": 6.0 + rank,
                        "market_cap_bil_jpy": 2.0 + rank * 6.0,
                        "avg_trading_value_60d_mil_jpy": 20.0 + rank * 5.0,
                        "forecast_dividend_yield_pct": 0.5 + value_strength * 0.1,
                        "dividend_yield_pct": 0.3 + value_strength * 0.1,
                        "cfo_yield_pct": 1.0 + value_strength * 0.2,
                        "forward_eps_to_actual_eps": 0.8 + rank * 0.05,
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
        conn.execute(
            """
            CREATE TABLE topix_data (
                date TEXT PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
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
        dates = pd.bdate_range("2020-01-01", "2022-12-30")
        price_rows: list[tuple[object, ...]] = []
        for row in event_ledger_df.to_dict(orient="records"):
            code = str(row["code"])
            rank = int(code[-1])
            trend = 0.08 if rank <= 1 else -0.04
            base = 80.0 + rank * 3.0
            for i, date in enumerate(dates):
                close = base + trend * i
                price_rows.append(
                    (
                        code,
                        date.strftime("%Y-%m-%d"),
                        close,
                        close * 1.01,
                        close * 0.99,
                        close,
                        100_000,
                        1.0,
                        None,
                    )
                )
        topix_rows = [
            (
                date.strftime("%Y-%m-%d"),
                1000.0 + i * 0.5,
                1000.0 + i * 0.5,
                1000.0 + i * 0.5,
                1000.0 + i * 0.5,
                1_000_000,
            )
            for i, date in enumerate(dates)
        ]
        conn.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", price_rows)
        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
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
        analysis_end_date="2022-12-30",
        result_metadata={"db_path": db_path},
        result_tables={"event_ledger_df": event_ledger_df},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-panel",
    )
    return bundle.bundle_dir


def _write_value_bundle(tmp_path: Path) -> Path:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)
    value_result = run_annual_value_composite_selection(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.80,),
        min_train_observations=5,
    )
    value_bundle = write_annual_value_composite_selection_bundle(
        value_result,
        output_root=tmp_path,
        run_id="value-selection",
    )
    return value_bundle.bundle_dir


def test_run_annual_value_composite_technical_filter_builds_overlay_tables(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)

    result = run_annual_value_composite_technical_filter(
        value_bundle,
        output_root=tmp_path,
        sma_window=20,
        slope_window=5,
        near_sma_threshold=0.95,
    )

    assert result.input_run_id == "value-selection"
    assert result.selected_event_count > 0
    assert result.technical_feature_count > 0
    assert "stock_price_to_sma250" in result.enriched_selected_event_df.columns
    assert set(result.technical_filter_summary_df["technical_filter"]).issuperset(
        {
            "baseline",
            "stock_above_sma250",
            "stock_and_topix_above_sma250",
            "stock_below_sma250",
        }
    )

    baseline = result.technical_filter_summary_df[
        result.technical_filter_summary_df["technical_filter"].astype(str) == "baseline"
    ]
    stock_pass = result.technical_filter_summary_df[
        result.technical_filter_summary_df["technical_filter"].astype(str) == "stock_above_sma250"
    ]
    stock_below = result.technical_filter_summary_df[
        result.technical_filter_summary_df["technical_filter"].astype(str) == "stock_below_sma250"
    ]
    assert float(baseline["kept_event_pct"].max()) == 100.0
    assert float(stock_pass["kept_event_pct"].min()) < 100.0
    assert not stock_below.empty
    assert not result.portfolio_summary_df.empty


def test_write_and_load_annual_value_composite_technical_filter_bundle(
    tmp_path: Path,
) -> None:
    value_bundle = _write_value_bundle(tmp_path)
    result = run_annual_value_composite_technical_filter(
        value_bundle,
        output_root=tmp_path,
        sma_window=20,
        slope_window=5,
    )

    bundle = write_annual_value_composite_technical_filter_bundle(
        result,
        output_root=tmp_path,
        run_id="technical-filter",
    )
    loaded = load_annual_value_composite_technical_filter_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID
    assert (
        get_annual_value_composite_technical_filter_bundle_path_for_run_id(
            "technical-filter",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_value_composite_technical_filter_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.technical_filter_summary_df.reset_index(drop=True),
        result.technical_filter_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
