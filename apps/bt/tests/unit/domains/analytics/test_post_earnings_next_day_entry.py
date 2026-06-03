from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.domains.analytics.post_earnings_next_day_entry import (
    PostEarningsNextDayEntryResult,
    build_summary_markdown,
    run_post_earnings_next_day_entry_research,
    write_post_earnings_next_day_entry_bundle,
)


def test_post_earnings_entry_timing_and_returns_are_after_disclosure(
    tmp_path: Path,
) -> None:
    db_path = _build_post_earnings_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    event = result.event_feature_df[
        (result.event_feature_df["code"] == "1111")
        & (result.event_feature_df["disclosed_date"] == "2024-01-10")
    ].iloc[0]

    assert event["pre_event_date"] == "2024-01-09"
    assert event["entry_date"] == "2024-01-11"
    assert event["entry_executable"] is True
    assert event["execution_label"] == "executable_open"
    assert event["entry_price"] == pytest.approx(event["entry_open"])
    assert event["forward_return_1d_pct"] == pytest.approx((115.0 / 110.0 - 1.0) * 100.0)


def test_post_earnings_entry_separates_stop_limit_no_fill_events(
    tmp_path: Path,
) -> None:
    db_path = _build_post_earnings_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    stop_high_event = result.event_feature_df[
        (result.event_feature_df["code"] == "2222")
        & (result.event_feature_df["disclosed_date"] == "2024-01-10")
    ].iloc[0]
    stop_low_event = result.event_feature_df[
        (result.event_feature_df["code"] == "3333")
        & (result.event_feature_df["disclosed_date"] == "2024-01-10")
    ].iloc[0]

    assert stop_high_event["execution_label"] == "limit_up_no_fill"
    assert stop_high_event["entry_executable"] is False
    assert stop_low_event["execution_label"] == "limit_down_no_fill"
    assert stop_low_event["entry_executable"] is False

    assert "execution_label" in result.execution_diagnostics_df.columns
    assert "limit_up_no_fill_rate_pct" in result.attempted_entry_outcome_df.columns
    assert "median_forward_excess_return_pct" in result.post_entry_expectancy_df.columns
    assert set(result.post_entry_expectancy_df["execution_scope"]) == {"executable"}
    assert {"limit_up_no_fill", "limit_down_no_fill"}.issubset(
        set(result.limit_no_fill_df["execution_label"])
    )


def test_post_earnings_entry_writes_bundle_and_summary(tmp_path: Path) -> None:
    db_path = _build_post_earnings_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Execution Diagnostics" in summary
    assert "Post-Entry Expectancy" in summary

    bundle = write_post_earnings_next_day_entry_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
        notes="coverage",
    )

    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.read_text(encoding="utf-8").startswith(
        "# Post-Earnings Next-Day Entry"
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"pre_windows": (0,)}, "pre_windows must be positive"),
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"liquidity_window": 0}, "liquidity_window must be positive"),
        ({"severe_loss_threshold_pct": 0.0}, "severe_loss_threshold_pct must be negative"),
    ],
)
def test_post_earnings_entry_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_post_earnings_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_post_earnings_next_day_entry_research(db_path, **kwargs)


def test_post_earnings_entry_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_post_earnings_next_day_entry_research(tmp_path / "missing.duckdb")


def _run_test_research(db_path: Path) -> PostEarningsNextDayEntryResult:
    return run_post_earnings_next_day_entry_research(
        db_path,
        start_date="2024-01-10",
        end_date="2024-01-12",
        pre_windows=(3, 5),
        horizons=(1, 3),
        liquidity_window=3,
        severe_loss_threshold_pct=-10.0,
    )


def _build_post_earnings_db(db_path: Path) -> Path:
    dates = [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
        "2024-01-10",
        "2024-01-11",
        "2024-01-12",
        "2024-01-15",
    ]
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
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
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
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
        CREATE TABLE statements (
            code TEXT,
            disclosed_date TEXT,
            type_of_document TEXT,
            type_of_current_period TEXT,
            forecast_eps DOUBLE,
            next_year_forecast_earnings_per_share DOUBLE,
            profit DOUBLE,
            earnings_per_share DOUBLE,
            shares_outstanding DOUBLE,
            treasury_shares DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?)",
        [
            ("1111", "Alpha", "0111", "Prime", "TOPIX Core30"),
            ("2222", "Beta", "0111", "Prime", None),
            ("3333", "Gamma", "0111", "Prime", None),
            ("4444", "Delta", "0111", "Prime", None),
        ],
    )
    _insert_stock_master_daily(
        conn,
        dates,
        [
            ("1111", "Alpha", "0111", "Prime", "TOPIX Core30"),
            ("2222", "Beta", "0111", "Prime", None),
            ("3333", "Gamma", "0111", "Prime", None),
            ("4444", "Delta", "0111", "Prime", None),
        ],
    )

    price_paths = {
        "1111": [100.0, 101.0, 102.0, 103.0, 104.0, 106.0, 107.0, 115.0, 116.0, 117.0],
        "2222": [100.0, 101.0, 102.0, 103.0, 104.0, 100.0, 100.0, 150.0, 151.0, 152.0],
        "3333": [100.0, 99.0, 98.0, 97.0, 96.0, 100.0, 100.0, 50.0, 51.0, 52.0],
        "4444": [80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 86.0, 90.0, 91.0, 92.0],
    }
    stock_rows: list[tuple[str, str, float, float, float, float, float]] = []
    for code, closes in price_paths.items():
        for date, close in zip(dates, closes, strict=True):
            if code == "2222" and date == "2024-01-11":
                stock_rows.append((code, date, 150.0, 150.0, 150.0, 150.0, 10_000.0))
            elif code == "3333" and date == "2024-01-11":
                stock_rows.append((code, date, 50.0, 50.0, 50.0, 50.0, 10_000.0))
            elif code == "1111" and date == "2024-01-11":
                stock_rows.append((code, date, 110.0, 116.0, 109.0, close, 400.0))
            else:
                stock_rows.append((code, date, close - 1.0, close + 1.0, close - 2.0, close, 300.0))
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)

    topix_rows = []
    for idx, date in enumerate(dates):
        close = 1000.0 + (10.0 * idx)
        topix_rows.append((date, close - 1.0, close + 1.0, close - 2.0, close))
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)

    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2024-01-05", "FinancialStatement", "3Q", 80.0, None, 100.0, 70.0, 1_000_000.0, 0.0),
            ("1111", "2024-01-10", "FinancialStatement", "FY", 100.0, 120.0, 130.0, 95.0, 1_000_000.0, 0.0),
            ("2222", "2024-01-05", "FinancialStatement", "3Q", 80.0, None, 100.0, 70.0, 1_000_000.0, 0.0),
            ("2222", "2024-01-10", "FinancialStatement", "FY", 100.0, 120.0, 130.0, 95.0, 1_000_000.0, 0.0),
            ("3333", "2024-01-05", "FinancialStatement", "3Q", 100.0, None, 120.0, 80.0, 1_000_000.0, 0.0),
            ("3333", "2024-01-10", "FinancialStatement", "FY", 70.0, 60.0, 90.0, 60.0, 1_000_000.0, 0.0),
            ("4444", "2024-01-05", "FinancialStatement", "2Q", 40.0, None, 60.0, 30.0, 900_000.0, 0.0),
            ("4444", "2024-01-12", "FinancialStatement", "3Q", 60.0, None, 90.0, 45.0, 900_000.0, 0.0),
        ],
    )
    conn.close()
    return db_path


def _insert_stock_master_daily(
    conn: duckdb.DuckDBPyConnection,
    dates: list[str],
    stock_rows: list[tuple[str, str, str, str, str | None]],
) -> None:
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)",
        [(date, *row) for date in dates for row in stock_rows],
    )
