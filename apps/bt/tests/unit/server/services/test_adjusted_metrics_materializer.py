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
            "sales": 2_500_000_000.0,
            "forecast_sales": 4_000_000_000.0,
            "operating_profit": 1_000_000_000.0,
            "forecast_operating_profit": 2_000_000_000.0,
            "dividend_fy": 30.0,
            "shares_outstanding": 10_000_000.0,
            "treasury_shares": 1_000_000.0,
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
    assert result.daily_valuation_rows == 2
    assert result.active_price_basis_date == "2024-08-01"
    assert result.active_basis_version == "event-pit-v1:7203:2024-08-01"

    statements = market_db.get_adjusted_statement_metrics("7203")
    valuation = market_db.get_daily_valuation("7203")

    assert statements[0]["adjusted_eps"] == pytest.approx(50.0)
    assert statements[0]["adjusted_bps"] == pytest.approx(500.0)
    assert statements[0]["adjusted_forecast_eps"] == pytest.approx(60.0)
    assert statements[0]["adjusted_treasury_shares"] == pytest.approx(2_000_000.0)
    assert valuation[-1]["close"] == pytest.approx(500.0)
    assert valuation[-1]["per"] == pytest.approx(10.0)
    assert valuation[-1]["forward_per"] == pytest.approx(500.0 / 60.0)
    assert valuation[-1]["sales"] == pytest.approx(2_500_000_000.0)
    assert valuation[-1]["forward_sales"] == pytest.approx(4_000_000_000.0)
    assert valuation[-1]["psr"] == pytest.approx(4.0)
    assert valuation[-1]["forward_psr"] == pytest.approx(2.5)
    assert valuation[-1]["p_op"] == pytest.approx(10.0)
    assert valuation[-1]["forward_p_op"] == pytest.approx(5.0)
    assert valuation[-1]["market_cap"] == pytest.approx(10_000_000_000.0)
    assert valuation[-1]["free_float_market_cap"] == pytest.approx(9_000_000_000.0)
    assert valuation[-1]["statement_disclosed_date"] == "2024-05-10"
    assert valuation[-1]["forward_sales_disclosed_date"] == "2024-05-10"
    assert valuation[-1]["forward_sales_source"] == "fy"


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
            "sales": 2_000_000_000.0,
            "forecast_sales": 2_400_000_000.0,
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


def test_rebuild_daily_valuation_uses_fy_bps_when_latest_revision_has_no_bps(
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
            "sales": 2_000_000_000.0,
            "forecast_sales": 2_400_000_000.0,
            "shares_outstanding": 10_000_000.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2024-08-01",
            "type_of_current_period": "1Q",
            "earnings_per_share": 20.0,
            "bps": None,
            "forecast_eps": 160.0,
            "forecast_sales": 3_200_000_000.0,
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
    assert valuation[0]["bps"] == pytest.approx(1000.0)
    assert valuation[0]["per"] == pytest.approx(5.0)
    assert valuation[0]["pbr"] == pytest.approx(0.5)
    assert valuation[0]["forward_eps"] == pytest.approx(160.0)
    assert valuation[0]["forward_per"] == pytest.approx(500.0 / 160.0)
    assert valuation[0]["sales"] == pytest.approx(2_000_000_000.0)
    assert valuation[0]["forward_sales"] == pytest.approx(3_200_000_000.0)
    assert valuation[0]["psr"] == pytest.approx(2.5)
    assert valuation[0]["forward_psr"] == pytest.approx(1.5625)
    assert valuation[0]["statement_disclosed_date"] == "2024-05-10"
    assert valuation[0]["forward_eps_disclosed_date"] == "2024-08-01"
    assert valuation[0]["forward_eps_source"] == "revised"
    assert valuation[0]["forward_sales_disclosed_date"] == "2024-08-01"
    assert valuation[0]["forward_sales_source"] == "revised"


def test_rebuild_daily_valuation_does_not_carry_forward_eps_before_latest_fy(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "3853",
            "disclosed_date": "2022-02-14",
            "type_of_current_period": "3Q",
            "earnings_per_share": 40.0,
            "forecast_eps": 120.0,
            "forecast_sales": 1_200_000_000.0,
            "shares_outstanding": 10_000_000.0,
        },
        {
            "code": "3853",
            "disclosed_date": "2024-01-10",
            "type_of_current_period": "FY",
            "type_of_document": "EarnForecastRevision",
            "forecast_eps": 240.0,
            "forecast_sales": 2_400_000_000.0,
            "shares_outstanding": 10_000_000.0,
        },
        {
            "code": "3853",
            "disclosed_date": "2025-05-14",
            "type_of_current_period": "FY",
            "earnings_per_share": 35.0,
            "bps": 500.0,
            "sales": 900_000_000.0,
            "shares_outstanding": 10_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": "3853",
            "date": "2026-05-15",
            "open": 1500.0,
            "high": 1500.0,
            "low": 1500.0,
            "close": 1500.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    valuation = market_db.get_daily_valuation("3853")

    assert valuation[0]["eps"] == pytest.approx(35.0)
    assert valuation[0]["per"] == pytest.approx(1500.0 / 35.0)
    assert valuation[0]["forward_eps"] is None
    assert valuation[0]["forward_per"] is None
    assert valuation[0]["sales"] == pytest.approx(900_000_000.0)
    assert valuation[0]["psr"] == pytest.approx((1500.0 * 10_000_000.0) / 900_000_000.0)
    assert valuation[0]["forward_sales"] is None
    assert valuation[0]["forward_psr"] is None
    assert valuation[0]["forward_eps_disclosed_date"] is None
    assert valuation[0]["forward_eps_source"] is None
    assert valuation[0]["forward_sales_disclosed_date"] is None
    assert valuation[0]["forward_sales_source"] is None


def test_rebuild_recomputes_daily_valuation_when_statement_sales_changes(
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
    materializer.rebuild_all()

    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "sales": 2_500_000_000.0,
            "next_year_forecast_sales": 4_000_000_000.0,
        }
    ])

    materializer.rebuild_all()
    valuation = market_db.get_daily_valuation("7203")

    assert valuation[0]["sales"] == pytest.approx(2_500_000_000.0)
    assert valuation[0]["forward_sales"] == pytest.approx(4_000_000_000.0)
    assert valuation[0]["psr"] == pytest.approx(2.0)
    assert valuation[0]["forward_psr"] == pytest.approx(1.25)


def test_rebuild_reused_basis_does_not_treat_quarterly_sales_null_as_stale(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-08-01",
            "type_of_current_period": "1Q",
            "earnings_per_share": 20.0,
            "sales": 600_000_000.0,
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
    valuation = market_db.get_daily_valuation("7203")
    assert valuation[0]["sales"] is None

    calls: list[object] = []
    original = market_db.publish_adjusted_basis_materialization

    def _capture_publish(plan: object) -> object:
        calls.append(plan)
        return original(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(
        market_db,
        "publish_adjusted_basis_materialization",
        _capture_publish,
    )

    second = materializer.rebuild_all()

    assert second.active_basis_version == first.active_basis_version
    assert calls == []


def test_rebuild_daily_valuation_ignores_quarterly_next_year_forecast_fallback(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-15",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "next_year_forecast_earnings_per_share": 150.0,
            "operating_profit": 200_000_000.0,
            "next_year_forecast_operating_profit": 300_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2024-08-15",
            "type_of_current_period": "1Q",
            "earnings_per_share": 20.0,
            "forecast_eps": None,
            "next_year_forecast_earnings_per_share": 400.0,
            "forecast_operating_profit": None,
            "next_year_forecast_operating_profit": 900_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2024-09-01",
            "open": 600.0,
            "high": 600.0,
            "low": 600.0,
            "close": 600.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    valuation = market_db.get_daily_valuation("7203")

    assert valuation[0]["forward_eps"] == pytest.approx(150.0)
    assert valuation[0]["forward_per"] == pytest.approx(600.0 / 150.0)
    assert valuation[0]["forward_p_op"] == pytest.approx(
        (600.0 * 1_000_000.0) / 300_000_000.0
    )
    assert valuation[0]["forward_eps_disclosed_date"] == "2024-05-15"
    assert valuation[0]["forward_eps_source"] == "fy"


def test_rebuild_daily_valuation_does_not_carry_actual_operating_profit_past_latest_fy(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-15",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "operating_profit": 200_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2025-05-15",
            "type_of_current_period": "FY",
            "earnings_per_share": 120.0,
            "bps": 1200.0,
            "operating_profit": None,
            "shares_outstanding": 1_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2025-09-01",
            "open": 600.0,
            "high": 600.0,
            "low": 600.0,
            "close": 600.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    valuation = market_db.get_daily_valuation("7203")

    assert valuation[0]["eps"] == pytest.approx(120.0)
    assert valuation[0]["p_op"] is None


def test_rebuild_daily_valuation_uses_fy_forecast_revision_after_valid_anchor(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-15",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "next_year_forecast_earnings_per_share": 150.0,
            "operating_profit": 200_000_000.0,
            "next_year_forecast_operating_profit": 300_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2025-05-15",
            "type_of_current_period": "FY",
            "type_of_document": "EarnForecastRevision",
            "earnings_per_share": None,
            "bps": None,
            "forecast_eps": 500.0,
            "next_year_forecast_earnings_per_share": 500.0,
            "forecast_operating_profit": 900_000_000.0,
            "next_year_forecast_operating_profit": 900_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2025-09-01",
            "open": 600.0,
            "high": 600.0,
            "low": 600.0,
            "close": 600.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    valuation = market_db.get_daily_valuation("7203")

    assert valuation[0]["forward_eps"] == pytest.approx(500.0)
    assert valuation[0]["forward_per"] == pytest.approx(600.0 / 500.0)
    assert valuation[0]["forward_p_op"] == pytest.approx(
        (600.0 * 1_000_000.0) / 900_000_000.0
    )
    assert valuation[0]["forward_eps_disclosed_date"] == "2025-05-15"
    assert valuation[0]["forward_eps_source"] == "revised"


def test_rebuild_preserves_zero_fy_forecasts_instead_of_nonzero_fallbacks(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([{
        "code": "7203",
        "disclosed_date": "2024-05-15",
        "type_of_current_period": "FY",
        "earnings_per_share": 100.0,
        "bps": 1000.0,
        "forecast_eps": 999.0,
        "next_year_forecast_earnings_per_share": 0.0,
        "sales": 1_000_000_000.0,
        "forecast_sales": 9_000_000_000.0,
        "next_year_forecast_sales": 0.0,
        "operating_profit": 100_000_000.0,
        "forecast_operating_profit": 900_000_000.0,
        "next_year_forecast_operating_profit": 0.0,
        "shares_outstanding": 1_000_000.0,
    }])
    market_db.upsert_stock_data([{
        "code": "7203",
        "date": "2024-06-03",
        "open": 500.0,
        "high": 500.0,
        "low": 500.0,
        "close": 500.0,
        "volume": 100,
        "adjustment_factor": 1.0,
        "created_at": "2026-07-14T00:00:00",
    }])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    statements = market_db.get_adjusted_statement_metrics_for_basis(
        "7203", basis_id="event-pit-v1:7203:2024-06-03"
    )
    valuation = market_db.get_daily_valuation_for_basis(
        "7203", basis_id="event-pit-v1:7203:2024-06-03"
    )
    assert statements[0]["raw_forecast_eps"] == pytest.approx(0.0)
    assert statements[0]["adjusted_forecast_eps"] == pytest.approx(0.0)
    assert valuation[0]["forward_eps"] == pytest.approx(0.0)
    assert valuation[0]["forward_sales"] == pytest.approx(0.0)
    assert valuation[0]["forward_p_op"] is None


def test_rebuild_preserves_zero_revision_forecasts_instead_of_nonzero_fallbacks(
    market_db: MarketDb,
) -> None:
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-05-15",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1000.0,
            "next_year_forecast_earnings_per_share": 150.0,
            "sales": 1_000_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2024-08-01",
            "type_of_current_period": "FY",
            "type_of_document": "EarnForecastRevision",
            "forecast_eps": 0.0,
            "next_year_forecast_earnings_per_share": 777.0,
            "forecast_sales": 0.0,
            "next_year_forecast_sales": 7_000_000_000.0,
            "forecast_operating_profit": 0.0,
            "next_year_forecast_operating_profit": 700_000_000.0,
            "shares_outstanding": 1_000_000.0,
        },
    ])
    market_db.upsert_stock_data([{
        "code": "7203",
        "date": "2024-08-02",
        "open": 500.0,
        "high": 500.0,
        "low": 500.0,
        "close": 500.0,
        "volume": 100,
        "adjustment_factor": 1.0,
        "created_at": "2026-07-14T00:00:00",
    }])

    AdjustedMetricsMaterializer(market_db).rebuild_all()

    statements = market_db.get_adjusted_statement_metrics_for_basis(
        "7203", basis_id="event-pit-v1:7203:2024-08-02"
    )
    revision = next(row for row in statements if row["disclosed_date"] == "2024-08-01")
    valuation = market_db.get_daily_valuation_for_basis(
        "7203", basis_id="event-pit-v1:7203:2024-08-02"
    )
    assert revision["raw_forecast_eps"] == pytest.approx(0.0)
    assert revision["adjusted_forecast_eps"] == pytest.approx(0.0)
    assert valuation[0]["forward_eps"] == pytest.approx(0.0)
    assert valuation[0]["forward_sales"] == pytest.approx(0.0)
    assert valuation[0]["forward_p_op"] is None


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


def test_rebuild_reuses_existing_basis_when_only_price_date_advances(
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
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2025-01-06",
            "open": 600.0,
            "high": 600.0,
            "low": 600.0,
            "close": 600.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
    ])
    second = materializer.rebuild_all()

    assert second.active_basis_version == first.active_basis_version
    assert second.active_price_basis_date == first.active_price_basis_date
    assert second.daily_valuation_latest_date == "2025-01-06"
    snapshot = market_db.get_adjusted_metrics_snapshot()
    assert snapshot["statementRows"] == 1
    assert snapshot["dailyValuationRows"] == 2
    assert snapshot["dailyValuationLatestDate"] == "2025-01-06"
    assert snapshot["basisVersion"] == first.active_basis_version
    valuation = market_db.get_daily_valuation("7203")
    assert [row["basis_version"] for row in valuation] == [first.active_basis_version] * 2
    assert [row["close"] for row in valuation] == [pytest.approx(500.0), pytest.approx(600.0)]


def test_rebuild_reused_basis_updates_valuation_from_changed_disclosure(
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
        },
        {
            "code": "7203",
            "date": "2025-01-06",
            "open": 600.0,
            "high": 600.0,
            "low": 600.0,
            "close": 600.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        },
    ])
    materializer = AdjustedMetricsMaterializer(market_db)

    first = materializer.rebuild_all()
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2025-01-02",
            "type_of_current_period": "FY",
            "earnings_per_share": 200.0,
            "bps": 2000.0,
            "forecast_eps": 220.0,
            "shares_outstanding": 10_000_000.0,
        }
    ])
    second = materializer.rebuild_all()

    assert second.active_basis_version == first.active_basis_version
    valuation = market_db.get_daily_valuation("7203")
    assert [row["date"] for row in valuation] == ["2024-12-30", "2025-01-06"]
    assert valuation[0]["eps"] == pytest.approx(100.0)
    assert valuation[0]["statement_disclosed_date"] == "2024-05-10"
    assert valuation[1]["eps"] == pytest.approx(200.0)
    assert valuation[1]["statement_disclosed_date"] == "2025-01-02"


def test_rebuild_reused_basis_appends_new_price_date_when_disclosure_changes(
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
            "code": "6758",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 50.0,
            "bps": 500.0,
            "forecast_eps": 60.0,
            "shares_outstanding": 10_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": code,
            "date": "2024-12-30",
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
        for code, close in (("7203", 500.0), ("6758", 300.0))
    ])
    materializer = AdjustedMetricsMaterializer(market_db)

    first = materializer.rebuild_all()
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2025-01-02",
            "type_of_current_period": "FY",
            "earnings_per_share": 200.0,
            "bps": 2000.0,
            "forecast_eps": 220.0,
            "shares_outstanding": 10_000_000.0,
        }
    ])
    market_db.upsert_stock_data([
        {
            "code": code,
            "date": "2025-01-06",
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
        for code, close in (("7203", 600.0), ("6758", 360.0))
    ])
    second = materializer.rebuild_all()

    assert second.active_basis_version == first.active_basis_version
    assert second.daily_valuation_rows == 4
    valuations = market_db._fetchall_dicts(
        """
        SELECT code, date, eps, statement_disclosed_date
        FROM daily_valuation
        ORDER BY code, date
        """
    )
    assert [(row["code"], row["date"]) for row in valuations] == [
        ("6758", "2024-12-30"),
        ("6758", "2025-01-06"),
        ("7203", "2024-12-30"),
        ("7203", "2025-01-06"),
    ]
    assert valuations[1]["eps"] == pytest.approx(50.0)
    assert valuations[1]["statement_disclosed_date"] == "2024-05-10"
    assert valuations[3]["eps"] == pytest.approx(200.0)
    assert valuations[3]["statement_disclosed_date"] == "2025-01-02"


def test_rebuild_reused_basis_repairs_sparse_latest_valuation_date(
    market_db: MarketDb,
) -> None:
    code_eps = {
        "1301": 30.0,
        "6758": 50.0,
        "7203": 100.0,
        "9984": 80.0,
    }
    market_db.upsert_statements([
        {
            "code": code,
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": eps,
            "bps": eps * 10.0,
            "forecast_eps": eps * 1.2,
            "shares_outstanding": 10_000_000.0,
        }
        for code, eps in code_eps.items()
    ])
    market_db.upsert_stock_data([
        {
            "code": code,
            "date": date,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
        for date, multiplier in (("2024-12-30", 10.0), ("2025-01-06", 12.0))
        for code, eps in code_eps.items()
        for close in (eps * multiplier,)
    ])
    materializer = AdjustedMetricsMaterializer(market_db)

    first = materializer.rebuild_all()
    market_db._execute(
        """
        DELETE FROM daily_valuation
        WHERE date = '2025-01-06'
          AND code <> '7203'
        """
    )
    sparse_snapshot = market_db.get_adjusted_metrics_snapshot()
    assert sparse_snapshot["dailyValuationLatestCodeCount"] == 1
    assert sparse_snapshot["dailyValuationPreviousCodeCount"] == 4

    second = materializer.rebuild_all()

    assert second.active_basis_version == first.active_basis_version
    assert second.daily_valuation_rows == 8
    repaired_snapshot = market_db.get_adjusted_metrics_snapshot()
    assert repaired_snapshot["dailyValuationLatestDate"] == "2025-01-06"
    assert repaired_snapshot["dailyValuationLatestCodeCount"] == 4
    valuations = market_db._fetchall_dicts(
        """
        SELECT code, date
        FROM daily_valuation
        WHERE date = '2025-01-06'
        ORDER BY code
        """
    )
    assert [row["code"] for row in valuations] == ["1301", "6758", "7203", "9984"]


def test_rebuild_reused_basis_refreshes_only_codes_with_changed_disclosures(
    market_db: MarketDb,
    monkeypatch: pytest.MonkeyPatch,
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
            "code": "6758",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 50.0,
            "bps": 500.0,
            "forecast_eps": 60.0,
            "shares_outstanding": 10_000_000.0,
        },
    ])
    market_db.upsert_stock_data([
        {
            "code": code,
            "date": "2025-01-06",
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        }
        for code, close in (("7203", 600.0), ("6758", 300.0))
    ])
    materializer = AdjustedMetricsMaterializer(market_db)
    first = materializer.rebuild_all()
    market_db.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2025-01-02",
            "type_of_current_period": "FY",
            "earnings_per_share": 200.0,
            "bps": 2000.0,
            "forecast_eps": 220.0,
            "shares_outstanding": 10_000_000.0,
        }
    ])
    calls: list[object] = []
    original = market_db.publish_adjusted_basis_materialization

    def _capture_publish(plan: object) -> object:
        calls.append(plan)
        return original(plan)  # type: ignore[arg-type]

    monkeypatch.setattr(
        market_db,
        "publish_adjusted_basis_materialization",
        _capture_publish,
    )

    second = materializer.rebuild_all()

    assert second.active_basis_version == first.active_basis_version
    assert calls
    assert calls[0].replace_basis_ids == {
        "7203": ("event-pit-v1:7203:2025-01-06",)
    }


def test_rebuild_retains_closed_basis_and_appends_active_basis(
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
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": "2025-01-06",
            "open": 600.0,
            "high": 600.0,
            "low": 600.0,
            "close": 600.0,
            "volume": 100,
            "adjustment_factor": 0.5,
            "created_at": "2026-05-16T00:00:00",
        }
    ])
    second = materializer.rebuild_all()

    assert first.active_basis_version == "event-pit-v1:7203:2024-12-30"
    assert second.active_basis_version == "event-pit-v1:7203:2025-01-06"
    assert second.basis_count == 2
    assert second.ready_basis_count == 2
    assert {
        row["basis_version"]
        for row in market_db._fetchall_dicts("SELECT basis_version FROM daily_valuation")
    } == {
        "event-pit-v1:7203:2024-12-30",
        "event-pit-v1:7203:2025-01-06",
    }
    assert {
        row["basis_version"]
        for row in market_db._fetchall_dicts(
            "SELECT basis_version FROM statement_metrics_adjusted"
        )
    } == {
        "event-pit-v1:7203:2024-12-30",
        "event-pit-v1:7203:2025-01-06",
    }


def test_exact_basis_queries_do_not_select_latest_basis(market_db: MarketDb) -> None:
    market_db.upsert_statements([{
        "code": "7203",
        "disclosed_date": "2024-05-10",
        "type_of_current_period": "FY",
        "earnings_per_share": 100.0,
        "bps": 1000.0,
        "forecast_eps": 120.0,
        "shares_outstanding": 10_000_000.0,
    }])
    market_db.upsert_stock_data([
        {
            "code": "7203",
            "date": date,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 100,
            "adjustment_factor": adjustment_factor,
            "created_at": "2026-07-14T00:00:00",
        }
        for date, close, adjustment_factor in (
            ("2024-12-30", 500.0, 1.0),
            ("2025-01-06", 600.0, 0.5),
        )
    ])
    AdjustedMetricsMaterializer(market_db).rebuild_all()

    closed_basis = "event-pit-v1:7203:2024-12-30"
    active_basis = "event-pit-v1:7203:2025-01-06"
    closed_statements = market_db.get_adjusted_statement_metrics_for_basis(
        "7203", basis_id=closed_basis
    )
    active_statements = market_db.get_adjusted_statement_metrics_for_basis(
        "7203", basis_id=active_basis
    )
    closed_valuation = market_db.get_daily_valuation_for_basis(
        "7203", basis_id=closed_basis
    )

    assert {row["basis_version"] for row in closed_statements} == {closed_basis}
    assert {row["basis_version"] for row in active_statements} == {active_basis}
    assert closed_statements[0]["adjusted_eps"] == pytest.approx(100.0)
    assert active_statements[0]["adjusted_eps"] == pytest.approx(50.0)
    assert [row["date"] for row in closed_valuation] == ["2024-12-30"]
    assert {row["basis_version"] for row in closed_valuation} == {closed_basis}


def test_daily_valuation_queries_return_latest_basis_only(market_db: MarketDb) -> None:
    market_db.upsert_daily_valuation([
        {
            "code": "7203",
            "date": "2024-12-30",
            "price_basis_date": "2024-12-30",
            "close": 500.0,
            "eps": 100.0,
            "per": 5.0,
            "basis_version": "adjusted-v1:2024-12-30",
        },
        {
            "code": "7203",
            "date": "2024-12-30",
            "price_basis_date": "2025-01-06",
            "close": 600.0,
            "eps": 100.0,
            "per": 6.0,
            "basis_version": "adjusted-v1:2025-01-06",
        },
    ])

    valuation = market_db.get_daily_valuation("7203")
    same_day = market_db.get_daily_valuation_for_codes(["7203"], "2024-12-30")

    assert [row["basis_version"] for row in valuation] == ["adjusted-v1:2025-01-06"]
    assert valuation[0]["close"] == pytest.approx(600.0)
    assert [row["basis_version"] for row in same_day] == ["adjusted-v1:2025-01-06"]
    assert same_day[0]["close"] == pytest.approx(600.0)


def test_rebuild_codes_materializes_only_requested_codes(market_db: MarketDb) -> None:
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
            "code": "6758",
            "disclosed_date": "2024-05-10",
            "type_of_current_period": "FY",
            "earnings_per_share": 200.0,
            "bps": 2000.0,
            "forecast_eps": 240.0,
            "shares_outstanding": 20_000_000.0,
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
        },
        {
            "code": "6758",
            "date": "2024-12-30",
            "open": 1000.0,
            "high": 1000.0,
            "low": 1000.0,
            "close": 1000.0,
            "volume": 100,
            "adjustment_factor": 1.0,
            "created_at": "2026-05-16T00:00:00",
        },
    ])

    result = AdjustedMetricsMaterializer(market_db).rebuild_codes(["7203"])

    assert result.statement_rows == 1
    assert result.daily_valuation_rows == 1
    assert market_db.get_adjusted_statement_metrics("7203")
    assert market_db.get_daily_valuation("7203")
    assert market_db.get_adjusted_statement_metrics("6758") == []
    assert market_db.get_daily_valuation("6758") == []
