from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
)
from src.domains.analytics.annual_forward_per_regime_decomposition import (
    ANNUAL_FORWARD_PER_REGIME_DECOMPOSITION_EXPERIMENT_ID,
    get_annual_forward_per_regime_decomposition_bundle_path_for_run_id,
    get_annual_forward_per_regime_decomposition_latest_bundle_path,
    load_annual_forward_per_regime_decomposition_bundle,
    run_annual_forward_per_regime_decomposition,
    write_annual_forward_per_regime_decomposition_bundle,
)
from src.domains.analytics.research_bundle import write_research_bundle


_MARKET_CODES = {"prime": "0111", "standard": "0112", "growth": "0113"}
_MARKET_NAMES = {"prime": "Prime", "standard": "Standard", "growth": "Growth"}


def _sample_event_ledger() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    forward_per_patterns = {
        "prime": [-2.0, 2.0, 3.0, 4.0, 5.0, 7.0, 9.0],
        "standard": [-3.0, -1.0, 1.5, 2.0, 4.0, 7.0, 8.5],
        "growth": [-4.0, -2.0, -0.5, 3.0, 6.0, 9.0, 11.0],
    }
    for year_index, year in enumerate(("2021", "2022", "2023", "2024")):
        for market_index, market in enumerate(("prime", "standard", "growth")):
            for rank, forward_per in enumerate(forward_per_patterns[market]):
                code = f"{market_index + 1}{year_index}{rank:02d}"
                if forward_per <= 0:
                    return_pct = 16.0 - rank * 0.6 + (2.0 if market == "standard" else 0.0)
                elif forward_per <= 2.0:
                    return_pct = 12.0 - rank * 0.4 + (2.0 if market == "standard" else 0.0)
                else:
                    return_pct = 4.0 - rank * 0.5 + (1.0 if market == "standard" else 0.0)
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
                        "pbr": 0.4 + rank * 0.10,
                        "forward_per": forward_per,
                        "per": max(1.0, forward_per + 1.0),
                        "market_cap_bil_jpy": 2.0 + rank * 3.0,
                        "avg_trading_value_60d_mil_jpy": 6.0 + rank * 3.0,
                        "forecast_dividend_yield_pct": 0.1 + (5 - rank) * 0.10,
                        "dividend_yield_pct": 0.1 + (5 - rank) * 0.08,
                        "cfo_yield_pct": 1.0 + (5 - rank) * 0.2,
                        "forward_eps_to_actual_eps": 0.9 + rank * 0.05,
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


def test_run_annual_forward_per_regime_decomposition_builds_tables(tmp_path: Path) -> None:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)

    result = run_annual_forward_per_regime_decomposition(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.10,),
        min_train_observations=6,
    )

    assert result.input_run_id == "input-panel"
    assert "positive_low_forward_per_score" in result.prepared_panel_df.columns
    assert "forward_per_regime" in result.prepared_panel_df.columns
    assert not result.regime_coverage_df.empty
    assert not result.regime_return_summary_df.empty
    assert not result.conditional_regime_summary_df.empty
    assert not result.panel_regression_df.empty
    assert not result.selected_event_df.empty
    assert not result.selection_mix_df.empty
    assert not result.portfolio_daily_df.empty
    assert not result.portfolio_summary_df.empty
    assert not result.portfolio_regime_contribution_df.empty

    standard_regimes = set(
        result.prepared_panel_df[
            result.prepared_panel_df["market"].astype(str) == "standard"
        ]["forward_per_regime"].astype(str)
    )
    assert {"non_positive", "positive_low", "positive_other"}.issubset(standard_regimes)


def test_write_and_load_annual_forward_per_regime_decomposition_bundle(tmp_path: Path) -> None:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)
    result = run_annual_forward_per_regime_decomposition(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.10,),
        min_train_observations=6,
    )

    bundle = write_annual_forward_per_regime_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="forward-per-regime-test",
    )
    loaded = load_annual_forward_per_regime_decomposition_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_FORWARD_PER_REGIME_DECOMPOSITION_EXPERIMENT_ID
    assert (
        get_annual_forward_per_regime_decomposition_bundle_path_for_run_id(
            "forward-per-regime-test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_annual_forward_per_regime_decomposition_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        loaded.portfolio_summary_df.reset_index(drop=True),
        result.portfolio_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
