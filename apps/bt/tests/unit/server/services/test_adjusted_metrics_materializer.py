from __future__ import annotations

from pathlib import Path

import pytest

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture()
def market_db(tmp_path: Path) -> MarketDb:
    db = MarketDb(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def test_rebuild_all_materializes_adjusted_metrics_from_raw_sources(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "forecast_eps": 120.0,
            "dividend_fy": 30.0,
            "shares_outstanding": 10_000_000.0,
        }
    ])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2024-08-01",
            "open": 1000.0,
            "high": 1000.0,
            "low": 1000.0,
            "close": 1000.0,
            "volume": 100,
            "adjustment_factor": 0.5,
            "created_at": "2026-05-16T00:00:00",
        },
        {
            "code": "7203",
            "date": "2024-12-30",
            "open": 500.0,
            "high": 500.0,
            "low": 500.0,
            "close": 500.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        },
    ])
    market_db._execute("DELETE FROM stock_data WHERE code = '7203' AND date = '2024-08-01'")

    result = AdjustedMetricsMaterializer(market_db).rebuild_all()

    assert result.statement_rows == 1
    assert result.daily_valuation_rows == 1
    assert result.price_basis_date == "2024-12-30"
    assert result.basis_version == "adjusted-v1:2024-12-30"

    statements = market_db.get_adjusted_statement_metrics("7203")
    valuation = market_db.get_daily_valuation("7203")

    assert statements[0]["adjusted_eps"] == pytest.approx(50.0)
    assert statements[0]["adjusted_bps"] == pytest.approx(500.0)
    assert statements[0]["adjusted_forecast_eps"] == pytest.approx(60.0)
    assert valuation[0]["close"] == pytest.approx(500.0)
    assert valuation[0]["per"] == pytest.approx(10.0)
    assert valuation[0]["forward_per"] == pytest.approx(500.0 / 60.0)
    assert valuation[0]["statement_disclosed_date"] == "2024-05-10"


def test_rebuild_excludes_future_disclosures_from_daily_valuation(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "forecast_eps": 120.0,
            "shares_outstanding": 10_000_000.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2025-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 300.0,
            "bps": 3000.0,
            "forecast_eps": 330.0,
            "shares_outstanding": 10_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2024-12-30",
            "open": 500.0,
            "high": 500.0,
            "low": 500.0,
            "close": 500.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    valuation = market_db.get_daily_valuation("7203")

    assert valuation[0]["eps"] == pytest.approx(100.0)
    assert valuation[0]["statement_disclosed_date"] == "2024-05-10"


def test_rebuild_is_idempotent_for_same_basis_version(market_db: MarketDb) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "forecast_eps": 120.0,
            "shares_outstanding": 10_000_000.0,
        }
    ])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2024-12-30",
            "open": 500.0,
            "high": 500.0,
            "low": 500.0,
            "close": 500.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])
    materializer = AdjustedMetricsMaterializer(market_db)

    first = materializer.rebuild_all()
    second = materializer.rebuild_all()

    assert first == second
    assert market_db.get_adjusted_metrics_snapshot()["statementRows"] == 1
    assert market_db.get_adjusted_metrics_snapshot()["dailyValuationRows"] == 1
