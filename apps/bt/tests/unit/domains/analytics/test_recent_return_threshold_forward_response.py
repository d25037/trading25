from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.recent_return_threshold_forward_response import (
    RecentReturnThresholdForwardResponseResult,
    build_summary_markdown,
    run_recent_return_threshold_forward_response_research,
    write_recent_return_threshold_forward_response_bundle,
)


def test_recent_return_threshold_forward_response_emits_tables(tmp_path: Path) -> None:
    db_path = _build_recent_return_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.threshold_response_df.empty
    assert not result.joint_threshold_response_df.empty
    assert not result.percentile_response_df.empty
    assert not result.valuation_response_df.empty
    assert not result.nonoverlap_response_df.empty
    assert not result.annual_threshold_response_df.empty
    assert not result.liquidity_interaction_df.empty
    assert {
        "pre_window",
        "threshold_pct",
        "entry_mode",
        "horizon",
        "median_forward_excess_return_pct",
        "severe_loss_rate_pct",
    }.issubset(result.threshold_response_df.columns)
    assert {
        "market",
        "recent_return_20d_pct",
        "forward_close_excess_return_5d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {"valuation_feature", "valuation_bucket", "median_forward_p_op"}.issubset(
        result.valuation_response_df.columns
    )
    assert {"forward_per", "forward_p_op", "p_op"}.issubset(
        result.observation_sample_df.columns
    )


def test_recent_return_threshold_forward_response_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_recent_return_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Threshold Response" in summary
    assert "Valuation Response" in summary
    assert "Non-Overlap Response" in summary

    bundle = write_recent_return_threshold_forward_response_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"pre_windows": (0,)}, "pre_windows must be positive"),
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"min_observations": 0}, "min_observations must be positive"),
        (
            {"severe_loss_threshold_pct": 0.0},
            "severe_loss_threshold_pct must be negative",
        ),
    ],
)
def test_recent_return_threshold_forward_response_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, object],
    message: str,
) -> None:
    db_path = _build_recent_return_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_recent_return_threshold_forward_response_research(db_path, **kwargs)


def test_recent_return_threshold_forward_response_requires_existing_db(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        run_recent_return_threshold_forward_response_research(
            tmp_path / "missing.duckdb"
        )


def _run_test_research(db_path: Path) -> RecentReturnThresholdForwardResponseResult:
    return run_recent_return_threshold_forward_response_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        pre_windows=(20, 60),
        horizons=(5, 20),
        thresholds_20d=(0.0, 5.0, 10.0),
        thresholds_60d=(0.0, 10.0, 20.0),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_recent_return_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", "2024-06-28").strftime("%Y-%m-%d").tolist()
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
            market_cap DOUBLE,
            basis_version TEXT
        )
        """
    )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    codes = [
        ("1111", "Alpha", "0111", 100.0, 0.35),
        ("2222", "Beta", "0111", 180.0, -0.15),
        ("3333", "Gamma", "0112", 90.0, 0.05),
    ]
    for index, date in enumerate(dates):
        for code, name, market_code, base, slope in codes:
            close = base + index * slope
            open_price = close * 0.995
            stock_rows.append(
                (code, date, open_price, close * 1.01, close * 0.99, close, 10_000)
            )
            master_rows.append((date, code, name, market_code, "Market", None))
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    conn.executemany(
        "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
        [
            (
                date,
                1000.0 + index * 0.2,
                1002.0 + index * 0.2,
                998.0 + index * 0.2,
                1000.0 + index * 0.2,
            )
            for index, date in enumerate(dates)
        ],
    )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                "2023-10-01",
                "FinancialStatement",
                "FY",
                100_000_000.0,
                120_000_000.0,
                None,
                1_000_000.0,
                0.0,
            ),
            (
                "2222",
                "2023-10-01",
                "FinancialStatement",
                "FY",
                80_000_000.0,
                90_000_000.0,
                None,
                1_200_000.0,
                100_000.0,
            ),
            (
                "3333",
                "2023-10-01",
                "FinancialStatement",
                "FY",
                60_000_000.0,
                70_000_000.0,
                None,
                900_000.0,
                0.0,
            ),
        ],
    )
    valuation_rows: list[tuple[str, str, str, float, float, float, str]] = []
    for date in dates:
        valuation_rows.extend(
            [
                ("1111", date, date, 12.0, 10.0, 110_000_000.0, "unit"),
                ("2222", date, date, 18.0, 16.0, 220_000_000.0, "unit"),
                ("3333", date, date, 14.0, 12.0, 90_000_000.0, "unit"),
            ]
        )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?)", valuation_rows
    )
    conn.close()
    return db_path
