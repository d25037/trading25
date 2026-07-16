from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import pytest

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.valuation_queries import get_adjusted_metrics_snapshot
from tests.unit.server.db.market_writer_test_support import (
    seed_adjusted_statement_metrics,
    seed_daily_valuation,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> Iterator[MarketDb]:
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
    assert "daily_technical_metrics" in schema["required_tables"]

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
        "sales",
        "forward_sales",
        "psr",
        "forward_psr",
        "p_op",
        "forward_p_op",
        "pbr",
        "market_cap",
        "free_float_market_cap",
        "statement_disclosed_date",
        "forward_eps_disclosed_date",
        "forward_eps_source",
        "forward_sales_disclosed_date",
        "forward_sales_source",
        "basis_version",
        "created_at",
    } <= _columns(market_db, "daily_valuation")

    assert {
        "code",
        "date",
        "close",
        "sma5",
        "sma5_sessions",
        "close_above_sma5_flag",
        "sma5_above_count_5d",
        "sma5_above_count_sessions",
        "sma5_above_count_group",
        "sma5_below_streak",
        "created_at",
    } <= _columns(market_db, "daily_technical_metrics")


def test_rebuild_daily_technical_metrics_materializes_sma5_above_count(market_db: MarketDb) -> None:
    rows = [
        ("72030", "2024-01-01", 10.0),
        ("72030", "2024-01-02", 11.0),
        ("72030", "2024-01-03", 12.0),
        ("72030", "2024-01-04", 13.0),
        ("72030", "2024-01-05", 14.0),
        ("72030", "2024-01-06", 13.0),
        ("72030", "2024-01-07", 15.0),
        ("72030", "2024-01-08", 16.0),
        ("72030", "2024-01-09", 17.0),
    ]
    for code, trade_date, close in rows:
        market_db._execute(
            """
            INSERT INTO stock_data (code, date, open, high, low, close, volume, adjustment_factor, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [code, trade_date, close, close, close, close, 1000, 1.0, None],
        )

    assert (
        market_db.rebuild_daily_technical_metrics_from_stock_data().final_count == 1
    )

    stored = market_db._fetchone(
        """
        SELECT code, date, sma5_above_count_5d, sma5_above_count_sessions, sma5_above_count_group, sma5_below_streak
        FROM daily_technical_metrics
        WHERE code = ? AND date = ?
        """,
        ["7203", "2024-01-09"],
    )

    assert stored == ("7203", "2024-01-09", 5, 5, "strong", 0)


def test_rebuild_daily_technical_metrics_materializes_sma5_below_streak(market_db: MarketDb) -> None:
    rows = [
        ("7203", "2024-01-01", 10.0),
        ("7203", "2024-01-02", 10.0),
        ("7203", "2024-01-03", 10.0),
        ("7203", "2024-01-04", 10.0),
        ("7203", "2024-01-05", 10.0),
        ("7203", "2024-01-06", 9.0),
        ("7203", "2024-01-07", 8.0),
        ("7203", "2024-01-08", 7.0),
        ("7203", "2024-01-09", 6.0),
    ]
    for code, trade_date, close in rows:
        market_db._execute(
            """
            INSERT INTO stock_data (code, date, open, high, low, close, volume, adjustment_factor, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [code, trade_date, close, close, close, close, 1000, 1.0, None],
        )

    assert (
        market_db.rebuild_daily_technical_metrics_from_stock_data().final_count == 1
    )

    stored = market_db._fetchone(
        """
        SELECT code, date, sma5_above_count_5d, sma5_below_streak
        FROM daily_technical_metrics
        WHERE code = ? AND date = ?
        """,
        ["7203", "2024-01-09"],
    )

    assert stored == ("7203", "2024-01-09", 0, 4)


def test_adjusted_metrics_snapshot_reports_freshness(market_db: MarketDb) -> None:
    market_db._execute(
        """
        INSERT INTO stock_adjustment_bases (
            code, basis_id, valid_from, valid_to_exclusive,
            adjustment_through_date, source_fingerprint,
            materialized_through_date, status
        ) VALUES
            ('7203', 'adjusted-v1:2024-12-30', '2024-01-01', NULL,
             '2024-01-01', 'ready-fp', '2024-12-30', 'ready'),
            ('6758', 'event-pit-v1:6758:2024-01-01', '2024-01-01', NULL,
             '2024-01-01', 'invalid-fp', '2024-06-30', 'invalid')
        """
    )
    seed_adjusted_statement_metrics(market_db,[
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
    seed_daily_valuation(market_db,[
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
        "dailyTechnicalMetricRows": 0,
        "dailyValuationLatestDate": "2024-12-30",
        "dailyValuationLatestCodeCount": 1,
        "dailyValuationPreviousCodeCount": 0,
        "priceBasisDate": "2024-12-30",
        "basisVersion": "adjusted-v1:2024-12-30",
        "basisVersionCount": 1,
        "retainedBasisCount": 2,
        "readyBasisCount": 1,
        "invalidBasisCount": 1,
        "activeCoverageFrontier": "2024-12-30",
        "underCoveredActiveBasisCount": 0,
        "overlappingBasisCount": 0,
        "orphanAdjustedStatementRows": 0,
        "orphanDailyValuationRows": 0,
    }


def test_adjusted_metrics_snapshot_counts_missing_active_basis_as_under_covered(
    market_db: MarketDb,
) -> None:
    market_db._execute(
        """
        INSERT INTO stock_data_raw (
            code, date, open, high, low, close, volume, adjustment_factor
        ) VALUES ('7203', '2024-12-30', 100, 100, 100, 100, 1000, 1.0)
        """
    )

    snapshot = market_db.get_adjusted_metrics_snapshot()

    assert snapshot["underCoveredActiveBasisCount"] == 1


def test_adjusted_metrics_snapshot_detects_equal_start_basis_overlap(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "equal-start-overlap.duckdb"))
    conn.execute("""
        CREATE TABLE stock_adjustment_bases (
            code TEXT, basis_id TEXT, valid_from TEXT, valid_to_exclusive TEXT,
            adjustment_through_date TEXT, source_fingerprint TEXT,
            materialized_through_date TEXT, status TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO stock_adjustment_bases VALUES ('7203', ?, '2024-01-01', NULL, '2024-01-01', 'fp', '2024-12-31', 'ready')",
        [("basis-a",), ("basis-b",)],
    )

    def table_exists(name: str) -> bool:
        return conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()[0] > 0

    snapshot = get_adjusted_metrics_snapshot(
        table_exists,
        lambda name: (
            int(conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0])
            if table_exists(name)
            else 0
        ),
        lambda sql, params: conn.execute(sql, params or []).fetchone(),
    )
    conn.close()

    assert snapshot["overlappingBasisCount"] == 1


def test_adjusted_metrics_source_diagnostics_detects_stale_canonical_statement(
    market_db: MarketDb,
) -> None:
    market_db._execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            type_of_document, next_year_forecast_earnings_per_share, forecast_eps,
            bps, dividend_fy, shares_outstanding, treasury_shares
        ) VALUES
            ('72030', '2024-05-10', 999, 'FY', 'FYFinancialStatements', 999, 998,
             9999, 99, 999999, 9999),
            ('7203', '2024-05-10', 100, 'FY', 'FYFinancialStatements', 120, 119,
             1000, 30, 1000000, 10000)
        """
    )
    market_db._execute(
        """
        INSERT INTO stock_adjustment_bases (
            code, basis_id, valid_from, valid_to_exclusive,
            adjustment_through_date, source_fingerprint,
            materialized_through_date, status
        ) VALUES (
            '7203', 'event-pit-v1:7203:2024-01-01', '2024-01-01', NULL,
            '2024-12-30', 'fp', '2024-12-30', 'ready'
        )
        """
    )
    market_db._execute(
        """
        INSERT INTO statement_metrics_adjusted (
            code, disclosed_date, period_end, period_type, price_basis_date,
            raw_eps, raw_bps, raw_forecast_eps, raw_dividend_fy,
            raw_shares_outstanding, raw_treasury_shares, basis_version
        ) VALUES (
            '7203', '2024-05-10', '2024-05-10', 'FY', '2024-12-30',
            90, 1000, 120, 30, 1000000, 10000,
            'event-pit-v1:7203:2024-01-01'
        )
        """
    )

    diagnostics = market_db.get_adjusted_metrics_source_diagnostics()

    assert diagnostics["sourceStatementKeyCount"] == 1
    assert diagnostics["expectedAdjustedStatementRows"] == 1
    assert diagnostics["staleAdjustedStatementRows"] == 1
    assert diagnostics["missingAdjustedStatementRows"] == 0


def test_adjusted_metrics_source_diagnostics_classifies_missing_extra_and_wrong_basis_rows(
    market_db: MarketDb,
) -> None:
    market_db._execute(
        """
        INSERT INTO statements (code, disclosed_date, earnings_per_share, type_of_current_period)
        VALUES
            ('6758', '2024-05-10', 100, 'FY'),
            ('9432', '2024-05-10', 100, 'FY'),
            ('8306', '2024-05-10', 100, 'FY')
        """
    )
    market_db._execute(
        """
        INSERT INTO stock_adjustment_bases (
            code, basis_id, valid_from, valid_to_exclusive,
            adjustment_through_date, source_fingerprint,
            materialized_through_date, status
        ) VALUES
            ('6758', 'ready-6758', '2024-01-01', NULL, '2024-12-30', 'fp', '2024-12-30', 'ready'),
            ('6758', 'ready-6758-v2', '2024-02-01', NULL, '2024-12-30', 'fp2', '2024-12-30', 'ready'),
            ('9432', 'invalid-9432', '2024-01-01', NULL, '2024-12-30', 'fp', '2024-12-30', 'invalid'),
            ('8306', 'expired-8306', '2024-01-01', '2024-05-01', '2024-04-30', 'fp', '2024-04-30', 'ready')
        """
    )
    market_db._execute(
        """
        INSERT INTO statement_metrics_adjusted (
            code, disclosed_date, period_end, period_type, price_basis_date,
            raw_eps, basis_version
        ) VALUES
            ('6758', '2024-05-10', '2024-05-10', 'FY', '2024-12-30', 100, 'ready-6758'),
            ('9999', '2024-05-10', '2024-05-10', 'FY', '2024-12-30', 100, 'source-less'),
            ('9432', '2024-05-10', '2024-05-10', 'FY', '2024-12-30', 100, 'invalid-9432'),
            ('8306', '2024-05-10', '2024-05-10', 'FY', '2024-04-30', 100, 'expired-8306')
        """
    )

    diagnostics = market_db.get_adjusted_metrics_source_diagnostics()

    assert diagnostics["sourceStatementKeyCount"] == 3
    assert diagnostics["expectedAdjustedStatementRows"] == 2
    assert diagnostics["missingAdjustedStatementRows"] == 1
    assert diagnostics["extraAdjustedStatementRows"] == 1
    assert diagnostics["wrongBasisAdjustedStatementRows"] == 2


def test_adjusted_metrics_source_diagnostics_uses_complete_raw_valuation_observations(
    market_db: MarketDb,
) -> None:
    market_db._execute(
        """
        INSERT INTO stock_data_raw (
            code, date, open, high, low, close, volume, adjustment_factor
        ) VALUES
            ('8306', '2024-06-03', 100, 110, 90, 105, 1000, 1),
            ('83070', '2024-06-03', NULL, 110, 90, 105, 1000, 1),
            ('9432', '2024-06-03', 100, 110, 90, 105, 1000, 1)
        """
    )
    market_db._execute(
        """
        INSERT INTO stock_adjustment_bases (
            code, basis_id, valid_from, valid_to_exclusive,
            adjustment_through_date, source_fingerprint,
            materialized_through_date, status
        ) VALUES
            ('8306', 'ready-8306', '2024-01-01', NULL, '2024-12-30', 'fp', '2024-12-30', 'ready'),
            ('8307', 'ready-8307', '2024-01-01', NULL, '2024-12-30', 'fp', '2024-12-30', 'ready'),
            ('9432', 'invalid-9432', '2024-01-01', NULL, '2024-12-30', 'fp', '2024-12-30', 'invalid')
        """
    )
    market_db._execute(
        """
        INSERT INTO stock_adjustment_basis_segments (
            code, basis_id, source_date_from, source_date_to_exclusive,
            cumulative_factor
        ) VALUES
            ('8306', 'ready-8306', '2024-01-01', NULL, 1),
            ('8307', 'ready-8307', '2024-01-01', NULL, 1),
            ('9432', 'invalid-9432', '2024-01-01', NULL, 1)
        """
    )
    market_db._execute(
        """
        INSERT INTO daily_valuation (code, date, close, basis_version)
        VALUES
            ('9999', '2024-06-03', 105, 'source-less'),
            ('9432', '2024-06-03', 105, 'invalid-9432')
        """
    )

    diagnostics = market_db.get_adjusted_metrics_source_diagnostics()

    assert diagnostics["missingDailyValuationRows"] == 1
    assert diagnostics["extraDailyValuationRows"] == 1
    assert diagnostics["wrongBasisDailyValuationRows"] == 1
