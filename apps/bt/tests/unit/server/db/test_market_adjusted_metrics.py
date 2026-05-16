from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture()
def market_db(tmp_path: Path) -> MarketDb:
    db = MarketDb(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _columns(market_db: MarketDb, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in market_db._execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def test_adjusted_metric_tables_are_created(market_db: MarketDb) -> None:
    schema = market_db.validate_schema()

    assert schema["valid"] is True
    assert "statement_metrics_adjusted" in schema["required_tables"]
    assert "daily_valuation" in schema["required_tables"]

    assert {
        "code",
        "disclosed_date",
        "period_end",
        "period_type",
        "price_basis_date",
        "raw_eps",
        "adjusted_eps",
        "raw_bps",
        "adjusted_bps",
        "raw_forecast_eps",
        "adjusted_forecast_eps",
        "raw_dividend_fy",
        "adjusted_dividend_fy",
        "raw_shares_outstanding",
        "adjusted_shares_outstanding",
        "raw_treasury_shares",
        "adjusted_treasury_shares",
        "adjustment_factor_cumulative",
        "basis_version",
        "created_at",
    } <= _columns(market_db, "statement_metrics_adjusted")

    assert {
        "code",
        "date",
        "price_basis_date",
        "close",
        "eps",
        "bps",
        "forward_eps",
        "per",
        "forward_per",
        "pbr",
        "market_cap",
        "free_float_market_cap",
        "statement_disclosed_date",
        "forward_eps_disclosed_date",
        "forward_eps_source",
        "basis_version",
        "created_at",
    } <= _columns(market_db, "daily_valuation")


def test_upsert_and_read_adjusted_statement_metrics(market_db: MarketDb) -> None:
    rows: list[dict[str, Any]] = [
        {
            "code": "7203",
            "disclosed_date": "2024-05-10",
            "period_end": "2024-03-31",
            "period_type": "FY",
            "price_basis_date": "2024-12-30",
            "raw_eps": 100.0,
            "adjusted_eps": 50.0,
            "raw_bps": 1000.0,
            "adjusted_bps": 500.0,
            "raw_forecast_eps": 120.0,
            "adjusted_forecast_eps": 60.0,
            "raw_dividend_fy": 30.0,
            "adjusted_dividend_fy": 15.0,
            "raw_shares_outstanding": 10_000_000.0,
            "adjusted_shares_outstanding": 20_000_000.0,
            "raw_treasury_shares": 1_000_000.0,
            "adjusted_treasury_shares": 2_000_000.0,
            "adjustment_factor_cumulative": 0.5,
            "basis_version": "adjusted-v1:2024-12-30",
            "created_at": "2026-05-16T00:00:00",
        },
        {
            "code": "7203",
            "disclosed_date": "2025-05-10",
            "period_end": "2025-03-31",
            "period_type": "FY",
            "price_basis_date": "2025-12-30",
            "raw_eps": 200.0,
            "adjusted_eps": 200.0,
            "raw_bps": 2000.0,
            "adjusted_bps": 2000.0,
            "raw_forecast_eps": 220.0,
            "adjusted_forecast_eps": 220.0,
            "raw_dividend_fy": 60.0,
            "adjusted_dividend_fy": 60.0,
            "raw_shares_outstanding": 10_000_000.0,
            "adjusted_shares_outstanding": 10_000_000.0,
            "raw_treasury_shares": 1_000_000.0,
            "adjusted_treasury_shares": 1_000_000.0,
            "adjustment_factor_cumulative": 1.0,
            "basis_version": "adjusted-v1:2025-12-30",
            "created_at": "2026-05-16T00:00:00",
        },
    ]

    assert market_db.upsert_statement_metrics_adjusted(rows) == 2

    as_of_rows = market_db.get_adjusted_statement_metrics(
        "7203",
        as_of_date="2024-12-31",
    )

    assert len(as_of_rows) == 1
    assert as_of_rows[0]["code"] == "7203"
    assert as_of_rows[0]["adjusted_eps"] == pytest.approx(50.0)
    assert as_of_rows[0]["basis_version"] == "adjusted-v1:2024-12-30"


def test_upsert_and_read_daily_valuation(market_db: MarketDb) -> None:
    rows = [
        {
            "code": "7203",
            "date": "2024-12-30",
            "price_basis_date": "2024-12-30",
            "close": 500.0,
            "eps": 50.0,
            "bps": 500.0,
            "forward_eps": 60.0,
            "per": 10.0,
            "forward_per": 8.3333333333,
            "pbr": 1.0,
            "market_cap": 10_000_000_000.0,
            "free_float_market_cap": 9_000_000_000.0,
            "statement_disclosed_date": "2024-05-10",
            "forward_eps_disclosed_date": "2024-08-01",
            "forward_eps_source": "revised",
            "basis_version": "adjusted-v1:2024-12-30",
            "created_at": "2026-05-16T00:00:00",
        }
    ]

    assert market_db.upsert_daily_valuation(rows) == 1

    by_code = market_db.get_daily_valuation(
        "7203",
        start="2024-01-01",
        end="2024-12-31",
    )
    batched = market_db.get_daily_valuation_for_codes(["7203", "9984"], "2024-12-30")

    assert by_code == batched
    assert by_code[0]["forward_eps_source"] == "revised"
    assert by_code[0]["forward_per"] == pytest.approx(8.3333333333)


def test_adjusted_metrics_snapshot_reports_freshness(market_db: MarketDb) -> None:
    market_db.upsert_statement_metrics_adjusted([
        {
            "code": "7203",
            "disclosed_date": "2024-05-10",
            "period_end": "2024-03-31",
            "period_type": "FY",
            "price_basis_date": "2024-12-30",
            "adjusted_eps": 50.0,
            "basis_version": "adjusted-v1:2024-12-30",
        }
    ])
    market_db.upsert_daily_valuation([
        {
            "code": "7203",
            "date": "2024-12-30",
            "price_basis_date": "2024-12-30",
            "close": 500.0,
            "basis_version": "adjusted-v1:2024-12-30",
        }
    ])

    snapshot = market_db.get_adjusted_metrics_snapshot()

    assert snapshot == {
        "statementRows": 1,
        "dailyValuationRows": 1,
        "priceBasisDate": "2024-12-30",
        "basisVersion": "adjusted-v1:2024-12-30",
    }
