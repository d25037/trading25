from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.pre_earnings_eps120_proxy import (
    PreEarningsEps120ProxyResult,
    run_pre_earnings_eps120_proxy_research,
    write_pre_earnings_eps120_proxy_bundle,
)


def test_pre_earnings_eps120_proxy_uses_only_pre_disclosure_valuation(
    tmp_path: Path,
) -> None:
    db_path = _build_proxy_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    event = result.event_feature_df[result.event_feature_df["code"] == "1111"].iloc[0]
    assert event["pre_event_date"] == "2024-01-09"
    assert bool(event["eps120_positive_target"]) is True
    assert event["actual_eps"] == pytest.approx(120.0)
    assert event["next_forecast_eps"] == pytest.approx(150.0)

    # Valuation must be based on the previous FY row, not the current disclosure.
    assert event["valuation_actual_eps"] == pytest.approx(100.0)
    assert event["valuation_forward_eps"] == pytest.approx(110.0)
    assert event["per"] == pytest.approx(105.0 / 100.0)
    assert event["forward_per"] == pytest.approx(105.0 / 110.0)
    assert event["pbr"] == pytest.approx(105.0 / 500.0)
    assert event["market_cap_bil_jpy"] == pytest.approx(105.0 * 1_000_000.0 / 1e9)


def test_pre_earnings_eps120_proxy_emits_summary_tables(tmp_path: Path) -> None:
    db_path = _build_proxy_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert not result.feature_bucket_df.empty
    assert not result.threshold_grid_df.empty
    assert not result.combo_grid_df.empty
    assert {"target_rate_pct", "lift_vs_base"}.issubset(result.feature_bucket_df.columns)
    assert {"condition", "target_rate_pct", "lift_vs_base"}.issubset(
        result.threshold_grid_df.columns
    )

    bundle = write_pre_earnings_eps120_proxy_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_pre_earnings_eps120_proxy_rejects_invalid_params(tmp_path: Path) -> None:
    db_path = _build_proxy_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match="min_events must be positive"):
        run_pre_earnings_eps120_proxy_research(db_path, min_events=0)


def test_pre_earnings_eps120_proxy_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_pre_earnings_eps120_proxy_research(tmp_path / "missing.duckdb")


def _run_test_research(db_path: Path) -> PreEarningsEps120ProxyResult:
    return run_pre_earnings_eps120_proxy_research(
        db_path,
        start_date="2024-01-10",
        end_date="2024-01-10",
        min_events=1,
    )


def _build_proxy_db(db_path: Path) -> Path:
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
            bps DOUBLE,
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
        ],
    )
    stock_rows = []
    for code, close in [("1111", 105.0), ("2222", 200.0)]:
        stock_rows.extend(
            [
                (code, "2024-01-05", close - 2.0, close - 1.0, close - 3.0, close - 2.0, 100.0),
                (code, "2024-01-09", close - 1.0, close + 1.0, close - 2.0, close, 100.0),
                (code, "2024-01-10", close, close + 2.0, close - 1.0, close + 1.0, 100.0),
                (code, "2024-01-11", close + 1.0, close + 3.0, close, close + 2.0, 100.0),
            ]
        )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
        [
            ("2024-01-05", 1000.0, 1001.0, 999.0, 1000.0),
            ("2024-01-09", 1010.0, 1011.0, 1009.0, 1010.0),
            ("2024-01-10", 1020.0, 1021.0, 1019.0, 1020.0),
            ("2024-01-11", 1030.0, 1031.0, 1029.0, 1030.0),
        ],
    )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("1111", "2023-09-10", "FinancialStatement", "FY", 110.0, None, 1_000.0, 100.0, 500.0, 1_000_000.0, 0.0),
            ("1111", "2024-01-10", "FinancialStatement", "FY", 130.0, 150.0, 1_200.0, 120.0, 560.0, 1_100_000.0, 0.0),
            ("2222", "2023-09-10", "FinancialStatement", "FY", 80.0, None, 900.0, 90.0, 400.0, 2_000_000.0, 0.0),
            ("2222", "2024-01-10", "FinancialStatement", "FY", 90.0, 95.0, 950.0, 92.0, 420.0, 2_000_000.0, 0.0),
        ],
    )
    conn.close()
    return db_path
