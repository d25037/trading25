from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.infrastructure.db.market.market_db import MarketDb
from tests.unit.server.db.market_writer_test_support import (
    open_market_db,
    publish_statements,
    publish_stock_data,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> Iterator[MarketDb]:
    db = open_market_db(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _columns(market_db: MarketDb, relation: str) -> set[str]:
    return {
        str(row[1])
        for row in market_db._execute(f"PRAGMA table_info('{relation}')").fetchall()
    }


def _statement(*, eps: float = 100.0) -> dict[str, Any]:
    return {
        "code": "72030",
        "statement_id": "disclosure-1",
        "disclosure_number": "disclosure-1",
        "disclosed_date": "2024-05-10",
        "disclosed_at": "2024-05-10T15:30:00+09:00",
        "period_start": "2023-04-01",
        "period_end": "2024-03-31",
        "type_of_current_period": "FY",
        "type_of_document": "FYFinancialStatements",
        "earnings_per_share": eps,
        "diluted_earnings_per_share": eps - 2,
        "bps": eps * 10,
        "forecast_eps": eps * 1.2,
        "dividend_fy": 30.0,
        "forecast_dividend_fy": 40.0,
        "shares_outstanding": 10_000_000.0,
        "treasury_shares": 1_000_000.0,
    }


def _seed_current_sources(db: MarketDb) -> None:
    publish_stock_data(
        db,
        [
            {
                "code": "7203",
                "date": "2024-12-30",
                "open": 500.0,
                "high": 500.0,
                "low": 500.0,
                "close": 500.0,
                "volume": 100,
                "adjustment_factor": 1.0,
            }
        ],
    )
    publish_statements(db, [_statement()])


def test_v5_adjusted_metric_relations_have_current_basis_identity(
    market_db: MarketDb,
) -> None:
    schema = market_db.validate_schema()

    assert schema["valid"] is True
    metric_columns = _columns(market_db, "statement_metrics_adjusted")
    assert {
        "code",
        "statement_id",
        "disclosed_at",
        "fundamentals_adjustment_basis_date",
        "raw_diluted_eps",
        "adjusted_diluted_eps",
        "source_fingerprint",
    } <= metric_columns
    assert "basis_version" not in metric_columns
    assert "price_basis_date" not in metric_columns
    assert "basis_version" not in _columns(market_db, "daily_valuation")


def test_current_basis_snapshot_reports_provider_window_and_pending(
    market_db: MarketDb,
) -> None:
    _seed_current_sources(market_db)

    pending = market_db.get_adjusted_metrics_snapshot()
    AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])
    ready = market_db.get_adjusted_metrics_snapshot()

    assert pending["currentBasisStatementCount"] == 0
    assert pending["currentBasisStateCount"] == 0
    assert pending["invalidCurrentBasisStateCount"] == 1
    assert pending["readyProviderWindowCount"] == 0
    assert pending["pendingCurrentBasisCodeCount"] == 1
    assert pending["fundamentalsAdjustmentBasisDate"] == "2024-12-30"
    assert ready["currentBasisStatementCount"] == 1
    assert ready["currentBasisStateCount"] == 1
    assert ready["invalidCurrentBasisStateCount"] == 0
    assert ready["dailyValuationRows"] == 1
    assert ready["providerWindowCount"] == 1
    assert ready["readyProviderWindowCount"] == 1
    assert ready["pendingCurrentBasisCodeCount"] == 0
    assert "basisVersion" not in ready
    assert "retainedBasisCount" not in ready


def test_current_basis_snapshot_marks_stale_provenance_state_unready(
    market_db: MarketDb,
) -> None:
    _seed_current_sources(market_db)
    AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])
    market_db._execute(
        "UPDATE statement_metrics_adjusted SET source_fingerprint = 'stale' "
        "WHERE code = '7203'"
    )

    snapshot = market_db.get_adjusted_metrics_snapshot()

    assert snapshot["currentBasisStateCount"] == 1
    assert snapshot["invalidCurrentBasisStateCount"] == 1
    assert snapshot["readyProviderWindowCount"] == 0
    assert snapshot["pendingCurrentBasisCodeCount"] == 1


def test_current_basis_source_diagnostics_detect_missing_stale_and_wrong_basis(
    market_db: MarketDb,
) -> None:
    _seed_current_sources(market_db)

    missing = market_db.get_adjusted_metrics_source_diagnostics()
    AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])
    market_db._execute(
        "UPDATE statement_metrics_adjusted SET raw_eps = 999, "
        "fundamentals_adjustment_basis_date = '2024-12-29' "
        "WHERE code = '7203'"
    )
    stale = market_db.get_adjusted_metrics_source_diagnostics()

    assert missing["sourceStatementKeyCount"] == 1
    assert missing["expectedAdjustedStatementRows"] == 1
    assert missing["missingAdjustedStatementRows"] == 1
    assert stale["missingAdjustedStatementRows"] == 0
    assert stale["staleAdjustedStatementRows"] == 1
    assert stale["wrongBasisAdjustedStatementRows"] == 1


def test_current_basis_readers_return_identity_rows_and_view_valuations(
    market_db: MarketDb,
) -> None:
    _seed_current_sources(market_db)
    AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])

    metrics = market_db.get_adjusted_statement_metrics("72030")
    valuations = market_db.get_daily_valuation("72030")
    by_codes = market_db.get_daily_valuation_for_codes(
        ["72030", "6758"], "2024-12-30"
    )

    assert [row["statement_id"] for row in metrics] == ["disclosure-1"]
    assert [(row["code"], row["date"]) for row in valuations] == [
        ("7203", "2024-12-30")
    ]
    assert by_codes == valuations


def test_rebuild_daily_technical_metrics_remains_independent(
    market_db: MarketDb,
) -> None:
    for day, close in enumerate(
        (10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0),
        start=1,
    ):
        market_db._execute(
            """
            INSERT INTO stock_data (code, date, open, high, low, close, volume)
            VALUES ('7203', ?, ?, ?, ?, ?, 1000)
            """,
            [f"2024-01-0{day}", close, close, close, close],
        )

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.final_count == 1
    assert market_db._fetchone(
        "SELECT sma5, close_above_sma5_flag FROM daily_technical_metrics"
    ) == (16.0, True)
