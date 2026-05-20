from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.short_term_shock_forward_response import (
    ShortTermShockForwardResponseResult,
    build_summary_markdown,
    run_short_term_shock_forward_response_research,
    write_short_term_shock_forward_response_bundle,
)


def test_short_term_shock_forward_response_emits_tables(tmp_path: Path) -> None:
    db_path = _build_short_term_shock_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.market_shock_calendar_df.empty
    assert not result.general_short_term_response_df.empty
    assert not result.pullback_in_uptrend_response_df.empty
    assert not result.market_shock_window_response_df.empty
    assert not result.stock_market_interaction_df.empty
    assert not result.liquidity_valuation_interaction_df.empty
    assert not result.case_study_response_df.empty
    assert {"pullback_in_uptrend", "persistent_runup"}.issubset(
        set(result.general_short_term_response_df["price_action_state"].astype(str))
    )
    assert {"topix_excess", "raw"}.issubset(
        set(result.general_short_term_response_df["return_metric"].astype(str))
    )
    assert {
        "shock_threshold_pct",
        "shock_offset_bucket",
        "median_topix_return_1d_pct",
    }.issubset(result.market_shock_window_response_df.columns)
    assert {"case_anchor_date", "case_offset_bucket"}.issubset(
        result.case_study_response_df.columns
    )


def test_short_term_shock_forward_response_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_short_term_shock_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Market Shock Window Response" in summary
    assert "Pullback In Uptrend Response" in summary
    assert "Case Study Response" in summary

    bundle = write_short_term_shock_forward_response_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"min_observations": 0}, "min_observations must be positive"),
        (
            {"severe_loss_threshold_pct": 0.0},
            "severe_loss_threshold_pct must be negative",
        ),
        (
            {"market_shock_thresholds": (1.0,)},
            "market_shock_thresholds must contain finite negative values",
        ),
    ],
)
def test_short_term_shock_forward_response_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_short_term_shock_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_short_term_shock_forward_response_research(db_path, **kwargs)


def _run_test_research(db_path: Path) -> ShortTermShockForwardResponseResult:
    return run_short_term_shock_forward_response_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-06-30",
        horizons=(5, 20),
        pullback_thresholds_20d=(0.0, 5.0),
        uptrend_thresholds_60d=(0.0, 5.0),
        market_shock_thresholds=(-3.0,),
        case_study_dates=("2024-04-15",),
        case_study_window_sessions=3,
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_short_term_shock_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", "2024-07-31").strftime("%Y-%m-%d").tolist()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
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
        CREATE TABLE topix_data (
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            date TEXT,
            code TEXT,
            company_name TEXT,
            market_code TEXT,
            market_name TEXT,
            scale_category TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE statements (
            code TEXT,
            disclosed_date TEXT,
            type_of_document TEXT,
            type_of_current_period TEXT,
            operating_profit DOUBLE,
            forecast_operating_profit DOUBLE,
            next_year_forecast_operating_profit DOUBLE,
            shares_outstanding DOUBLE,
            treasury_shares DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_valuation (
            code TEXT,
            date TEXT,
            price_basis_date TEXT,
            per DOUBLE,
            forward_per DOUBLE,
            pbr DOUBLE,
            market_cap DOUBLE,
            basis_version TEXT
        )
        """
    )

    codes = [
        ("1111", "Alpha", "0111", 100.0, 0.18),
        ("2222", "Beta", "0111", 120.0, -0.04),
        ("3333", "Gamma", "0111", 90.0, 0.25),
        ("4444", "Delta", "0111", 150.0, -0.12),
        ("5555", "Epsilon", "0111", 80.0, 0.14),
        ("6666", "Zeta", "0111", 110.0, 0.02),
    ]
    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    topix_rows: list[tuple[str, float, float, float, float]] = []
    valuation_rows: list[tuple[str, str, str, float, float, float, float, str]] = []
    shock_index = dates.index("2024-04-15")
    for index, date in enumerate(dates):
        topix_close = 1000.0 + index * 1.2
        if index == shock_index:
            topix_close *= 0.94
        if index > shock_index:
            topix_close += 35.0
        topix_rows.append(
            (date, topix_close * 0.995, topix_close * 1.01, topix_close * 0.99, topix_close)
        )
        for code, name, market_code, base, slope in codes:
            close = base + index * slope
            if code in {"2222", "4444"} and index >= shock_index - 10:
                close *= 0.92
            if code == "3333" and shock_index - 10 <= index <= shock_index + 15:
                close *= 0.94
            if code in {"1111", "5555"} and index >= shock_index + 1:
                close *= 1.08
            stock_rows.append(
                (code, date, close * 0.995, close * 1.01, close * 0.99, close, 10_000 + index)
            )
            master_rows.append((date, code, name, market_code, "Market", None))
            valuation_rows.append(
                (
                    code,
                    date,
                    date,
                    10.0 + index % 5,
                    8.0 + index % 7,
                    0.5 + (index % 6) * 0.2,
                    close * 1_000_000.0,
                    "unit",
                )
            )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                code,
                "2023-10-01",
                "FinancialStatement",
                "FY",
                100_000_000.0,
                120_000_000.0,
                None,
                1_000_000.0,
                0.0,
            )
            for code, *_ in codes
        ],
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?)", valuation_rows
    )
    conn.close()
    return db_path
