"""
Tests for MarketDb (DuckDB implementation).
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market import market_schema
from src.infrastructure.db.market.market_schema import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import DuckDbParquetTimeSeriesStore
from tests.unit.server.db.market_writer_test_support import open_market_db
from tests.unit.server.db.market_writer_test_support import open_time_series_store
from tests.unit.server.db.market_writer_test_support import (
    publish_indices_data,
    publish_margin_data,
    publish_options_225_data,
    publish_statements,
    publish_stock_data,
    publish_topix_data,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> Generator[MarketDb]:
    db_path = str(tmp_path / "market.duckdb")
    db = open_market_db(db_path)
    yield db
    db.close()


def _query_one(db_path: str, sql: str) -> tuple | None:
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchone()
    finally:
        conn.close()


def _table_columns(db: MarketDb, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in db._fetchall(f"PRAGMA table_info('{table_name}')")
        if row and len(row) > 1
    }


def _open_versioned_market_db(
    tmp_path: Path,
    *,
    version: int,
    mode: str,
    read_only: bool = False,
) -> MarketDb:
    db_path = str(tmp_path / f"market-v{version}.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE market_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL,
                notes TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO market_schema_version VALUES (?, '2026-07-14T00:00:00', 'existing')",
            [version],
        )
        conn.execute(
            """
            CREATE TABLE sync_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO sync_metadata VALUES ('stock_price_adjustment_mode', ?, 'unchanged')",
            [mode],
        )
    finally:
        conn.close()
    return open_market_db(db_path, read_only=read_only)


class TestMarketDbBasics:
    def test_empty_stats_and_schema(self, market_db: MarketDb) -> None:
        stats = market_db.get_stats()
        assert stats["market_schema_version"] == 1
        assert stats["stocks"] == 0
        assert stats["stocks_latest"] == 0
        assert stats["stock_master_daily"] == 0
        assert stats["stock_master_intervals"] == 0
        assert stats["stock_data_raw"] == 0
        assert stats["stock_data"] == 0
        assert stats["topix_data"] == 0
        assert stats["indices_data"] == 0
        assert stats["options_225_data"] == 0
        assert stats["margin_data"] == 0
        assert stats["sync_metadata"] == 1
        assert stats["index_membership_daily"] == 0
        assert stats["stock_adjustment_events"] == 0
        assert stats["stock_provider_windows"] == 0
        assert market_db.get_market_schema_version() == 5
        assert market_db.is_market_schema_current() is True
        assert market_db.get_stock_price_adjustment_mode() == "provider_adjusted_v1"
        assert market_db._table_exists("stock_adjustment_events")
        assert market_db._table_exists("stock_provider_windows")
        assert not market_db._table_exists("stock_adjustment_bases")
        assert not market_db._table_exists("stock_adjustment_basis_segments")

        schema = market_db.validate_schema()
        assert schema["valid"] is True
        assert "market_schema_version" in schema["required_tables"]
        assert "stocks" in schema["required_tables"]
        assert "stocks_latest" in schema["required_tables"]
        assert "stock_master_daily" in schema["required_tables"]
        assert "stock_master_intervals" in schema["required_tables"]
        assert "stock_data_raw" in schema["required_tables"]
        assert "stock_data" in schema["required_tables"]
        assert "topix_data" in schema["required_tables"]
        assert "options_225_data" in schema["required_tables"]
        assert "margin_data" in schema["required_tables"]
        assert "sync_metadata" in schema["required_tables"]

    def test_v5_price_and_adjustment_ledger_columns_are_exact(
        self, market_db: MarketDb
    ) -> None:
        assert _table_columns(market_db, "stock_data_raw") == {
            "code",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover_value",
            "adjustment_factor",
            "adjusted_open",
            "adjusted_high",
            "adjusted_low",
            "adjusted_close",
            "adjusted_volume",
            "created_at",
        }
        assert _table_columns(market_db, "stock_adjustment_events") == {
            "code",
            "date",
            "adjustment_factor",
            "source_fingerprint",
            "created_at",
        }
        assert _table_columns(market_db, "stock_provider_windows") == {
            "code",
            "coverage_start",
            "coverage_end",
            "provider_plan",
            "provider_as_of",
            "source_fingerprint",
            "updated_at",
        }
        provider_window_info = market_db._fetchall(
            "PRAGMA table_info('stock_provider_windows')"
        )
        assert [row[1] for row in provider_window_info if row[5]] == ["code"]
        assert all(bool(row[3]) for row in provider_window_info)
        assert _table_columns(market_db, "current_basis_fundamentals_state") == {
            "code",
            "fundamentals_adjustment_basis_date",
            "source_fingerprint",
            "statement_count",
            "materialized_at",
        }
        state_info = market_db._fetchall(
            "PRAGMA table_info('current_basis_fundamentals_state')"
        )
        assert [row[1] for row in state_info if row[5]] == ["code"]
        assert all(bool(row[3]) for row in state_info)
        pk_rows = market_db._fetchall("PRAGMA table_info('stock_adjustment_events')")
        assert [row[1] for row in sorted(pk_rows, key=lambda row: row[5]) if row[5]] == [
            "code",
            "date",
        ]
        raw_info = {
            str(row[1]): bool(row[3])
            for row in market_db._fetchall("PRAGMA table_info('stock_data_raw')")
        }
        assert all(
            raw_info[column]
            for column in ("code", "date", "open", "high", "low", "close", "volume")
        )
        raw_types = {
            str(row[1]): str(row[2])
            for row in market_db._fetchall("PRAGMA table_info('stock_data_raw')")
        }
        consumer_types = {
            str(row[1]): str(row[2])
            for row in market_db._fetchall("PRAGMA table_info('stock_data')")
        }
        minute_types = {
            str(row[1]): str(row[2])
            for row in market_db._fetchall("PRAGMA table_info('stock_data_minute_raw')")
        }
        assert raw_types["adjusted_volume"] == "DOUBLE"
        assert consumer_types["volume"] == "DOUBLE"
        assert raw_types["volume"] == "BIGINT"
        assert minute_types["volume"] == "BIGINT"

        for invalid_factor in (0.0, -1.0, 1.0):
            with pytest.raises(duckdb.ConstraintException):
                market_db._execute(
                    """
                    INSERT INTO stock_adjustment_events
                        (code, date, adjustment_factor, source_fingerprint)
                    VALUES ('7203', '2026-01-01', ?, 'fingerprint')
                    """,
                    [invalid_factor],
                )

    def test_v5_current_basis_statement_schema_and_valuation_view(
        self, market_db: MarketDb
    ) -> None:
        assert _table_columns(market_db, "statement_metrics_adjusted") == {
            "code",
            "statement_id",
            "disclosed_date",
            "disclosed_at",
            "period_end",
            "period_type",
            "fundamentals_adjustment_basis_date",
            "raw_eps",
            "adjusted_eps",
            "raw_diluted_eps",
            "adjusted_diluted_eps",
            "raw_bps",
            "adjusted_bps",
            "raw_forecast_eps",
            "adjusted_forecast_eps",
            "raw_dividend_fy",
            "adjusted_dividend_fy",
            "raw_forecast_dividend_fy",
            "adjusted_forecast_dividend_fy",
            "raw_shares_outstanding",
            "adjusted_shares_outstanding",
            "raw_treasury_shares",
            "adjusted_treasury_shares",
            "adjustment_factor_cumulative",
            "source_fingerprint",
            "created_at",
        }
        pk_rows = market_db._fetchall("PRAGMA table_info('statement_metrics_adjusted')")
        assert [row[1] for row in sorted(pk_rows, key=lambda row: row[5]) if row[5]] == [
            "code",
            "statement_id",
        ]
        relation = market_db._fetchone(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_name = 'daily_valuation'
            """
        )
        assert relation == ("VIEW",)
        assert "basis_version" not in _table_columns(market_db, "daily_valuation")
        view_sql = market_db._fetchone(
            "SELECT sql FROM duckdb_views() WHERE view_name = 'daily_valuation'"
        )
        assert view_sql is not None
        assert "disclosed_at" in str(view_sql[0]).split("ASOF", maxsplit=1)[1]

        market_db._execute(
            """
            INSERT INTO stock_data
                (code, date, open, high, low, close, volume)
            VALUES ('7203', '2026-02-10', 100, 110, 90, 105, 1000)
            """
        )
        market_db._execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, statement_id, disclosed_date, disclosed_at, period_end,
                period_type, fundamentals_adjustment_basis_date, adjusted_eps,
                adjustment_factor_cumulative, source_fingerprint
            ) VALUES
                ('7203', 'a-late', '2026-02-10', '2026-02-10T15:30:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', 15, 1, 'late-fingerprint'),
                ('7203', 'z-early', '2026-02-10', '2026-02-10T09:00:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', 10, 1, 'early-fingerprint'),
                ('7203', 'future', '2026-02-11', '2026-02-11T09:00:00+09:00',
                 '2026-03-31', 'FY', '2026-02-11', 20, 1, 'future-fingerprint')
            """
        )
        assert market_db._fetchone(
            "SELECT statement_id, eps FROM daily_valuation WHERE code = '7203'"
        ) == ("a-late", 15.0)

    def test_v5_current_basis_recompute_pending_schema(self, market_db: MarketDb) -> None:
        assert _table_columns(market_db, "current_basis_recompute_pending") == {
            "code",
            "reason",
            "source_fingerprint",
            "updated_at",
        }
        pk_rows = market_db._fetchall(
            "PRAGMA table_info('current_basis_recompute_pending')"
        )
        assert [row[1] for row in pk_rows if row[5]] == ["code"]

    def test_daily_valuation_resolves_each_metric_family_asof(
        self, market_db: MarketDb
    ) -> None:
        market_db._execute(
            """
            INSERT INTO stock_data
                (code, date, open, high, low, close, volume)
            VALUES ('67580', '2026-02-10', 100, 110, 90, 105, 1000)
            """
        )
        market_db._execute(
            """
            INSERT INTO statements (
                code, statement_id, disclosed_date, disclosed_at,
                period_start, period_end, type_of_current_period,
                type_of_document
            ) VALUES
                ('67580', 'actual', '2026-02-09', '2026-02-09T09:00:00+09:00',
                 '2025-04-01', '2026-03-31', 'FY', 'FinancialStatements'),
                ('67580', 'revision', '2026-02-10', '2026-02-10T15:00:00+09:00',
                 '2025-04-01', '2026-03-31', 'FY', 'EarnForecastRevision'),
                ('6758', 'revision', '2026-02-10', '2026-02-10T15:00:00+09:00',
                 '2025-04-01', '2026-03-31', 'FY', 'FinancialStatements')
            """
        )
        market_db._execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, statement_id, disclosed_date, disclosed_at, period_end,
                period_type, fundamentals_adjustment_basis_date,
                adjusted_eps, adjusted_bps, adjusted_forecast_eps,
                adjusted_shares_outstanding, adjusted_treasury_shares,
                adjustment_factor_cumulative, source_fingerprint
            ) VALUES
                ('6758', 'actual', '2026-02-09', '2026-02-09T09:00:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', 10, 100, NULL,
                 1000, 100, 1, 'state-fingerprint'),
                ('6758', 'revision', '2026-02-10', '2026-02-10T15:00:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', NULL, NULL, 20,
                 NULL, NULL, 1, 'state-fingerprint')
            """
        )

        row = market_db._fetchone(
            """
            SELECT eps, bps, forward_eps, per, forward_per, pbr,
                   market_cap, free_float_market_cap, statement_id,
                   forward_eps_disclosed_date, forward_eps_source
            FROM daily_valuation
            WHERE code = '67580' AND date = '2026-02-10'
            """
        )

        assert row is not None
        assert row[:8] == pytest.approx(
            (10.0, 100.0, 20.0, 10.5, 5.25, 1.05, 105000.0, 94500.0),
            rel=1e-9,
        )
        assert row[8:] == ("actual", "2026-02-10", "fy")

    def test_reopen_refreshes_the_schema_v5_daily_valuation_view(
        self, tmp_path: Path
    ) -> None:
        db_path = str(tmp_path / "refresh-view.duckdb")
        initial = open_market_db(db_path)
        initial._execute(
            "CREATE OR REPLACE VIEW daily_valuation AS SELECT 'stale' AS marker"
        )
        initial.close()

        reopened = open_market_db(db_path)
        try:
            columns = _table_columns(reopened, "daily_valuation")
        finally:
            reopened.close()

        assert "marker" not in columns
        assert {"eps", "bps", "forward_eps", "source_fingerprint"} <= columns

    def test_v5_current_basis_state_rejects_negative_statement_count(
        self, market_db: MarketDb
    ) -> None:
        with pytest.raises(duckdb.ConstraintException):
            market_db._execute(
                """
                INSERT INTO current_basis_fundamentals_state VALUES (
                    '7203', '2026-01-01', 'fingerprint', -1,
                    '2026-01-01T00:00:00Z'
                )
                """
            )

    def test_statement_updatable_columns_include_disclosed_date_consistently(self) -> None:
        assert "disclosed_date" in market_schema.STATEMENTS_UPDATABLE_COLUMNS
        assert (
            DuckDbParquetTimeSeriesStore._STATEMENT_UPDATABLE_COLUMNS  # noqa: SLF001
            == market_schema.STATEMENTS_UPDATABLE_COLUMNS
        )

    def test_v5_provider_metadata_keys_are_canonical(self) -> None:
        assert {
            key: METADATA_KEYS[key]
            for key in (
                "PROVIDER_PLAN",
                "PROVIDER_AS_OF",
                "PROVIDER_COVERAGE_START",
                "PROVIDER_COVERAGE_END",
                "PROVIDER_SOURCE_FINGERPRINT",
                "FUNDAMENTALS_ADJUSTMENT_BASIS_DATE",
            )
        } == {
            "PROVIDER_PLAN": "provider_plan",
            "PROVIDER_AS_OF": "provider_as_of",
            "PROVIDER_COVERAGE_START": "provider_coverage_start",
            "PROVIDER_COVERAGE_END": "provider_coverage_end",
            "PROVIDER_SOURCE_FINGERPRINT": "provider_source_fingerprint",
            "FUNDAMENTALS_ADJUSTMENT_BASIS_DATE": "fundamentals_adjustment_basis_date",
        }
        assert not hasattr(market_schema, "LOCAL_STOCK_PRICE_ADJUSTMENT_MODE")

    def test_pre_v3_existing_market_db_is_marked_incompatible(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "legacy-market.duckdb")
        conn = duckdb.connect(db_path)
        try:
            conn.execute("CREATE TABLE stocks (code TEXT PRIMARY KEY)")
        finally:
            conn.close()

        db = open_market_db(db_path)
        try:
            assert db.get_market_schema_version() == 0
            assert db.is_market_schema_current() is False
        finally:
            db.close()

    def test_existing_v3_is_not_partially_upgraded(self, tmp_path: Path) -> None:
        db = _open_versioned_market_db(
            tmp_path,
            version=3,
            mode="local_projection_v1",
            read_only=True,
        )
        try:
            assert db.get_market_schema_version() == 3
            assert db.get_stock_price_adjustment_mode() == "local_projection_v1"
            assert not db._table_exists("stock_adjustment_bases")
            assert not db._table_exists("stock_adjustment_basis_segments")
            assert db._existing_table_names() == {
                "market_schema_version",
                "sync_metadata",
            }
        finally:
            db.close()

    def test_existing_v4_is_rejected_without_partial_upgrade(self, tmp_path: Path) -> None:
        db = _open_versioned_market_db(
            tmp_path,
            version=4,
            mode="local_projection_v2_event_time",
            read_only=True,
        )
        try:
            assert db.get_market_schema_version() == 4
            assert db.is_market_schema_current() is False
            assert db.get_stock_price_adjustment_mode() == "local_projection_v2_event_time"
            assert not db._table_exists("stock_adjustment_events")
            assert db._existing_table_names() == {
                "market_schema_version",
                "sync_metadata",
            }
        finally:
            db.close()

    def test_writable_market_db_rejects_v4_before_exposing_handle(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(RuntimeError, match="schema version 4.*required version 5"):
            _open_versioned_market_db(
                tmp_path,
                version=4,
                mode="local_projection_v2_event_time",
            )

        db = MarketDb(str(tmp_path / "market-v4.duckdb"), read_only=True)
        try:
            assert db._existing_table_names() == {
                "market_schema_version",
                "sync_metadata",
            }
        finally:
            db.close()

    def test_daily_valuation_share_count_semantics(self, market_db: MarketDb) -> None:
        market_db._execute(
            """
            INSERT INTO stock_data
                (code, date, open, high, low, close, volume)
            VALUES
                ('1001', '2026-02-10', 100, 110, 90, 105, 1000),
                ('1002', '2026-02-10', 100, 110, 90, 105, 1000),
                ('1003', '2026-02-10', 100, 110, 90, 105, 1000)
            """
        )
        market_db._execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, statement_id, disclosed_date, disclosed_at, period_end,
                period_type, fundamentals_adjustment_basis_date,
                adjusted_shares_outstanding, adjusted_treasury_shares,
                adjustment_factor_cumulative, source_fingerprint
            ) VALUES
                ('1001', 'normal', '2026-02-09', '2026-02-09T15:30:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', 100, 20, 1, 'normal'),
                ('1002', 'over-treasury', '2026-02-09', '2026-02-09T15:30:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', 100, 120, 1, 'over'),
                ('1003', 'missing', '2026-02-09', '2026-02-09T15:30:00+09:00',
                 '2026-03-31', 'FY', '2026-02-10', NULL, 20, 1, 'missing')
            """
        )

        assert market_db._fetchall(
            """
            SELECT code, market_cap, free_float_market_cap
            FROM daily_valuation
            ORDER BY code
            """
        ) == [
            ("1001", 10_500.0, 8_400.0),
            ("1002", 10_500.0, 0.0),
            ("1003", None, None),
        ]

    def test_time_series_store_uses_v5_raw_and_statement_identity_columns(
        self, tmp_path: Path
    ) -> None:
        store = open_time_series_store(
            duckdb_path=str(tmp_path / "time-series.duckdb"),
            parquet_dir=str(tmp_path / "parquet"),
        )
        try:
            raw_columns = {
                str(row[1])
                for row in store._conn.execute(  # noqa: SLF001
                    "PRAGMA table_info('stock_data_raw')"
                ).fetchall()
            }
            statement_columns = {
                str(row[1])
                for row in store._conn.execute(  # noqa: SLF001
                    "PRAGMA table_info('statements')"
                ).fetchall()
            }
            assert {
                "turnover_value",
                "adjusted_open",
                "adjusted_high",
                "adjusted_low",
                "adjusted_close",
                "adjusted_volume",
            }.issubset(raw_columns)
            assert {
                "statement_id",
                "disclosure_number",
                "disclosed_at",
                "period_start",
                "period_end",
            }.issubset(statement_columns)
        finally:
            store.close()

    def test_stock_master_coverage_reports_v3_tables(self, market_db: MarketDb) -> None:
        market_db._execute(
            """
            INSERT INTO stock_master_daily (
                date, code, company_name, market_code, market_name,
                sector_17_code, sector_17_name, sector_33_code, sector_33_name,
                listed_date
            ) VALUES
                ('2024-01-04', '7203', 'トヨタ', '0111', 'プライム', '6', '自動車', '3700', '輸送用機器', '1949-05-16'),
                ('2024-01-05', '7203', 'トヨタ', '0111', 'プライム', '6', '自動車', '3700', '輸送用機器', '1949-05-16')
            """
        )

        coverage = market_db.get_stock_master_coverage()

        assert coverage["dailyCount"] == 2
        assert coverage["dateMin"] == "2024-01-04"
        assert coverage["dateMax"] == "2024-01-05"
        assert coverage["dateCount"] == 2
        assert coverage["codeCount"] == 1

    def test_stock_master_missing_dates_and_latest_are_derived_from_daily_master(self, market_db: MarketDb) -> None:
        publish_topix_data(market_db,[
            {"date": "2024-01-04", "open": 1, "high": 2, "low": 1, "close": 2, "created_at": "now"},
            {"date": "2024-01-05", "open": 2, "high": 3, "low": 2, "close": 3, "created_at": "now"},
        ])
        market_db.publish_stock_master_daily_rows(
            [
                {
                    "date": "2024-01-05",
                    "code": "7203",
                    "company_name": "トヨタ",
                    "company_name_english": "TOYOTA",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "scale_category": "TOPIX Core30",
                    "listed_date": "1949-05-16",
                    "created_at": "now",
                }
            ],
        )

        assert market_db.get_missing_stock_master_dates() == ["2024-01-04"]
        assert market_db.get_missing_stock_master_dates_count() == 1
        coverage = market_db.get_stock_master_coverage()
        assert coverage["latestCount"] == 1
        assert coverage["intervalCount"] == 1
        assert coverage["missingTopixDatesCount"] == 1
        assert coverage["missingTopixDates"] == ["2024-01-04"]

    def test_stock_master_daily_rows_relation_upsert(self, market_db: MarketDb) -> None:
        assert market_db.publish_stock_master_daily_rows([
            {
                "date": "2024-01-04",
                "code": "7203",
                "company_name": "トヨタ",
                "company_name_english": "TOYOTA",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "1949-05-16",
                "created_at": "first",
            },
            {
                "date": "2024-01-05",
                "code": "6758",
                "company_name": "ソニー",
                "company_name_english": "SONY",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "1",
                "sector_17_name": "電機",
                "sector_33_code": "3650",
                "sector_33_name": "電気機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "1958-12-01",
                "created_at": "first",
            },
            {
                "date": "2024-01-05",
                "code": "6758",
                "company_name": "ソニーグループ",
                "company_name_english": "SONY GROUP",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "1",
                "sector_17_name": "電機",
                "sector_33_code": "3650",
                "sector_33_name": "電気機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "1958-12-01",
                "created_at": "deduped",
            },
        ]).daily.stats.mutated_rows == 2

        assert market_db.publish_stock_master_daily_rows([
            {
                "date": "2024-01-04",
                "code": "7203",
                "company_name": "トヨタ自動車",
                "company_name_english": "TOYOTA MOTOR",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "1949-05-16",
                "created_at": "second",
            }
        ]).daily.stats.mutated_rows == 1

        row = market_db._fetchone(
            """
            SELECT COUNT(*),
                   MAX(CASE WHEN date = '2024-01-04' AND code = '7203' THEN company_name END),
                   MAX(CASE WHEN date = '2024-01-04' AND code = '7203' THEN created_at END),
                   MAX(CASE WHEN date = '2024-01-05' AND code = '6758' THEN company_name END)
            FROM stock_master_daily
            """
        )

        assert row == (2, "トヨタ自動車", "first", "ソニーグループ")
        assert market_db.get_index_membership_codes("2024-01-04", "TOPIX500") == {"7203"}
        assert market_db.get_index_membership_codes("2024-01-05", "TOPIX500") == {"6758"}

    def test_stock_master_daily_upsert_materializes_topix500_membership(
        self, market_db: MarketDb
    ) -> None:
        assert market_db.publish_stock_master_daily_rows(
            [
                {
                    "date": "2024-01-04",
                    "code": "7203",
                    "company_name": "トヨタ",
                    "company_name_english": "TOYOTA",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "scale_category": "TOPIX Core30",
                    "listed_date": "1949-05-16",
                    "created_at": "now",
                },
                {
                    "date": "2024-01-04",
                    "code": "1301",
                    "company_name": "極洋",
                    "company_name_english": "KYOKUYO",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "1",
                    "sector_17_name": "食品",
                    "sector_33_code": "0050",
                    "sector_33_name": "水産・農林業",
                    "scale_category": "TOPIX Small 1",
                    "listed_date": "1949-05-16",
                    "created_at": "now",
                },
            ],
        ).daily.stats.mutated_rows == 2

        assert market_db.get_index_membership_codes("2024-01-04", "TOPIX500") == {"7203"}

    def test_existing_stock_master_does_not_mutate_membership_on_open(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "existing-market.duckdb"
        db = open_market_db(str(db_path))
        try:
            db.publish_stock_master_daily_rows([
                {
                    "date": "2024-01-04",
                    "code": "7203",
                    "company_name": "トヨタ",
                    "company_name_english": "TOYOTA",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "scale_category": "TOPIX Core30",
                    "listed_date": "1949-05-16",
                    "created_at": "now",
                }
            ])
            db._execute("DELETE FROM index_membership_daily")
        finally:
            db.close()

        reopened = open_market_db(str(db_path))
        try:
            assert reopened.get_index_membership_codes("2024-01-04", "TOPIX500") == set()
        finally:
            reopened.close()

    def test_stock_master_daily_rows_relation_upsert_skips_empty_and_invalid_rows(
        self, market_db: MarketDb
    ) -> None:
        assert market_db.publish_stock_master_daily_rows([]).mutated_rows == 0
        assert market_db.publish_stock_master_daily_rows([
            {"date": "", "code": "7203", "company_name": "missing date"},
            {"date": "2024-01-04", "code": "", "company_name": "missing code"},
        ]).mutated_rows == 0

    def test_stock_master_intervals_ignore_snapshot_date_listed_date_pollution(
        self, market_db: MarketDb
    ) -> None:
        market_db.publish_stock_master_daily_rows([
            {
                "date": "2024-01-04",
                "code": "7203",
                "company_name": "トヨタ自動車",
                "company_name_english": "TOYOTA MOTOR",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "2024-01-04",
                "created_at": "first",
            },
            {
                "date": "2024-01-05",
                "code": "7203",
                "company_name": "トヨタ自動車",
                "company_name_english": "TOYOTA MOTOR",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "2024-01-05",
                "created_at": "second",
            },
            {
                "date": "2024-01-05",
                "code": "6758",
                "company_name": "ソニーグループ",
                "company_name_english": "SONY GROUP",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "1",
                "sector_17_name": "電機",
                "sector_33_code": "3650",
                "sector_33_name": "電気機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "1958-12-01",
                "created_at": "third",
            },
        ])
        market_db._execute(
            """
            INSERT INTO stock_master_intervals (
                code, valid_from, valid_to, fingerprint, company_name, company_name_english,
                market_code, market_name, sector_17_code, sector_17_name, sector_33_code,
                sector_33_name, scale_category, listed_date, created_at
            ) VALUES (
                '9999', '2000-01-01', '2000-01-01', 'stale', 'stale', NULL,
                '0111', 'プライム', '1', 'stale', '1', 'stale', NULL, '', 'stale'
            )
            """
        )

        rows = market_db._fetchall(
            """
            SELECT code, valid_from, valid_to, listed_date
            FROM stock_master_intervals
            ORDER BY code
            """
        )

        assert rows == [
            ("6758", "2024-01-05", "2024-01-05", "1958-12-01"),
            ("7203", "2024-01-04", "2024-01-05", ""),
            ("9999", "2000-01-01", "2000-01-01", ""),
        ]
        assert not market_db._table_exists("__stock_master_intervals_rebuild")

    def test_stock_master_daily_pit_query_filters(self, market_db: MarketDb) -> None:
        publish_topix_data(market_db,[
            {"date": "2024-01-04", "open": 1, "high": 2, "low": 1, "close": 2, "created_at": "now"},
            {"date": "2024-01-05", "open": 2, "high": 3, "low": 2, "close": 3, "created_at": "now"},
            {"date": "2024-01-09", "open": 3, "high": 4, "low": 3, "close": 4, "created_at": "now"},
        ])
        market_db.publish_stock_master_daily_rows([
            {
                "date": "2024-01-04",
                "code": "7203",
                "company_name": "トヨタ",
                "company_name_english": "TOYOTA",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "scale_category": "TOPIX Core30",
                "listed_date": "2024-01-04",
                "created_at": "now",
            },
            {
                "date": "2024-01-04",
                "code": "6758",
                "company_name": "ソニー",
                "company_name_english": "SONY",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "1",
                "sector_17_name": "電機",
                "sector_33_code": "3650",
                "sector_33_name": "電気機器",
                "scale_category": "TOPIX Large70",
                "listed_date": "2024-01-04",
                "created_at": "now",
            },
            {
                "date": "2024-01-04",
                "code": "9999",
                "company_name": "グロース",
                "company_name_english": "GROWTH",
                "market_code": "0113",
                "market_name": "グロース",
                "sector_17_code": "10",
                "sector_17_name": "情報通信",
                "sector_33_code": "5250",
                "sector_33_name": "情報通信業",
                "scale_category": "",
                "listed_date": "2024-01-04",
                "created_at": "now",
            },
        ])

        assert market_db.get_topix_dates(start_date="2024-01-05", end_date="2024-01-09") == [
            "2024-01-05",
            "2024-01-09",
        ]
        rows = market_db.get_stock_master_rows_for_date(
            "2024-01-04",
            market_codes=["0111"],
            scale_categories=["TOPIX Core30"],
        )
        assert [row["code"] for row in rows] == ["7203"]
        assert market_db.get_stock_master_codes_for_date(
            "2024-01-04",
            market_codes=["0111"],
            exclude_scale_categories=["TOPIX Core30"],
        ) == ["6758"]
        assert market_db.get_topix_dates() == [
            "2024-01-04",
            "2024-01-05",
            "2024-01-09",
        ]
        assert market_db.get_topix_dates(start_date="2024-01-05") == [
            "2024-01-05",
            "2024-01-09",
        ]
        assert market_db.get_topix_dates(end_date="2024-01-05") == [
            "2024-01-04",
            "2024-01-05",
        ]
        assert [row["code"] for row in market_db.get_stock_master_rows_for_date("2024-01-04")] == [
            "6758",
            "7203",
            "9999",
        ]
        assert market_db.get_stock_master_codes_for_date(
            "2024-01-04",
            scale_categories=["TOPIX Core30", "TOPIX Large70"],
        ) == ["6758", "7203"]

    def test_market_db_missing_optional_tables_return_empty_values(self, tmp_path: Path) -> None:
        db = open_market_db(str(tmp_path / "missing-tables.duckdb"))
        try:
            db._execute("DROP TABLE topix_data")
            db._execute("DROP TABLE stock_master_daily")
            db._execute("DROP TABLE index_membership_daily")
            db._execute("DROP TABLE stocks")

            assert db.get_topix_dates() == []
            assert db.get_latest_stock_master_date() is None
            assert db.get_missing_stock_master_dates() == []
            assert db.get_missing_stock_master_dates_count() == 0
            assert db.get_stock_master_rows_for_date("2024-01-04") == []
            assert db.get_stock_master_codes_for_date("2024-01-04") == []
            assert db.get_index_membership_codes("2024-01-04", "TOPIX500") == set()
            assert db.get_fundamentals_target_stock_rows() == []
        finally:
            db.close()

    def test_sync_metadata_roundtrip(self, market_db: MarketDb) -> None:
        assert market_db.get_sync_metadata("nonexistent") is None

        market_db.set_sync_metadata("last_sync", "2024-01-01")
        assert market_db.get_sync_metadata("last_sync") == "2024-01-01"

        market_db.set_sync_metadata("last_sync", "2024-01-02")
        assert market_db.get_sync_metadata("last_sync") == "2024-01-02"

    def test_is_initialized_falls_back_to_existing_data_when_metadata_is_missing(
        self, market_db: MarketDb
    ) -> None:
        assert market_db.get_sync_metadata("init_completed") is None
        assert market_db.is_initialized() is False

        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        publish_stock_data(market_db,
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2490.0,
                    "close": 2505.0,
                    "volume": 1000000,
                }
            ]
        )

        assert market_db.is_initialized() is True
        assert market_db.get_stock_price_adjustment_mode() == "provider_adjusted_v1"

    def test_is_initialized_prioritizes_explicit_metadata_flag(
        self, market_db: MarketDb
    ) -> None:
        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        publish_stock_data(market_db,
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2490.0,
                    "close": 2505.0,
                    "volume": 1000000,
                }
            ]
        )
        market_db.set_sync_metadata("init_completed", "false")

        assert market_db.is_initialized() is False


class TestMarketDbUpserts:
    def test_upsert_stocks_and_counts(self, market_db: MarketDb) -> None:
        count = market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        assert count.stats.mutated_rows == 1
        assert market_db.get_stats()["stocks"] == 1

        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ自動車",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        assert market_db.get_stats()["stocks"] == 1
        row = _query_one(
            market_db.db_path,
            "SELECT company_name FROM stocks WHERE code='7203'",
        )
        assert row is not None
        assert row[0] == "トヨタ自動車"

    def test_upsert_timeseries_rows(self, market_db: MarketDb) -> None:
        assert publish_stock_data(market_db,
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2490.0,
                    "close": 2505.0,
                    "volume": 1000000,
                }
            ]
        ).mutated_rows == 1
        assert publish_topix_data(market_db,
            [{"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0}]
        ).mutated_rows == 1
        assert publish_indices_data(market_db,
            [{"code": "0000", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0}]
        ).mutated_rows == 1
        assert publish_margin_data(market_db,
            [{"code": "7203", "date": "2024-01-15", "long_margin_volume": 1000.0, "short_margin_volume": 200.0}]
        ).mutated_rows == 1
        assert publish_options_225_data(market_db,
            [
                {
                    "code": "131040018",
                    "date": "2024-01-15",
                    "contract_month": "2024-04",
                    "strike_price": 32000.0,
                    "put_call_division": "1",
                    "underlying_price": 36000.0,
                }
            ]
        ).mutated_rows == 1

    def test_options_225_range_and_underlying_issue_counts(self, market_db: MarketDb) -> None:
        publish_options_225_data(market_db,
            [
                {
                    "code": "131040018",
                    "date": "2024-01-15",
                    "underlying_price": 36000.0,
                },
                {
                    "code": "141040018",
                    "date": "2024-01-15",
                    "underlying_price": 36000.0,
                },
                {
                    "code": "131040019",
                    "date": "2024-01-16",
                    "underlying_price": None,
                },
                {
                    "code": "141040019",
                    "date": "2024-01-16",
                    "underlying_price": None,
                },
                {
                    "code": "131040020",
                    "date": "2024-01-17",
                    "underlying_price": 36100.0,
                },
                {
                    "code": "141040020",
                    "date": "2024-01-17",
                    "underlying_price": 36200.0,
                },
            ]
        )

        assert market_db.get_latest_options_225_date() == "2024-01-17"
        assert market_db.get_options_225_data_range() == {
            "count": 6,
            "dateCount": 3,
            "dateRange": {"min": "2024-01-15", "max": "2024-01-17"},
        }
        assert market_db.get_options_225_underlying_price_issue_count(issue_type="missing") == 1
        assert market_db.get_options_225_underlying_price_issue_dates(issue_type="missing") == ["2024-01-16"]
        assert market_db.get_options_225_underlying_price_issue_count(issue_type="conflicting") == 1
        assert market_db.get_options_225_underlying_price_issue_dates(issue_type="conflicting") == ["2024-01-17"]

    def test_upsert_statements_merges_non_null_fields_on_conflict(self, market_db: MarketDb) -> None:
        publish_statements(market_db,
            [
                {
                    "code": "1899",
                    "statement_id": "disc-1899-20260213",
                    "disclosed_date": "2026-02-13",
                    "disclosed_at": "2026-02-13T15:30:00+09:00",
                    "period_start": "2025-04-01",
                    "period_end": "2026-03-31",
                    "type_of_current_period": "FY",
                    "type_of_document": "EarnForecastRevision",
                    "forecast_eps": 580.0,
                }
            ]
        )
        publish_statements(market_db,
            [
                {
                    "code": "1899",
                    "statement_id": "disc-1899-20260213",
                    "disclosed_date": "2026-02-13",
                    "disclosed_at": "2026-02-13T15:30:00+09:00",
                    "period_start": "2025-04-01",
                    "period_end": "2026-03-31",
                    "type_of_current_period": "FY",
                    "type_of_document": "DividendForecastRevision",
                    "dividend_fy": 200.0,
                    "forecast_eps": None,
                }
            ]
        )

        row = _query_one(
            market_db.db_path,
            """
            SELECT forecast_eps, dividend_fy, type_of_document
            FROM statements
            WHERE code='1899' AND disclosed_date='2026-02-13'
            """,
        )
        assert row is not None
        assert row[0] == 580.0
        assert row[1] == 200.0
        assert row[2] == "DividendForecastRevision"

        publish_statements(market_db,
            [
                {
                    "code": "1899",
                    "statement_id": "disc-1899-20260213",
                    "disclosed_date": "2026-02-13",
                    "disclosed_at": "2026-02-13T15:30:00+09:00",
                    "period_start": "2025-04-01",
                    "period_end": "2026-03-31",
                    "type_of_current_period": "FY",
                    "type_of_document": "EarnForecastRevision",
                    "forecast_eps": 604.0,
                }
            ]
        )
        updated = _query_one(
            market_db.db_path,
            """
            SELECT forecast_eps, dividend_fy, type_of_document
            FROM statements
            WHERE code='1899' AND disclosed_date='2026-02-13'
            """,
        )
        assert updated is not None
        assert updated[0] == 604.0
        assert updated[1] == 200.0
        assert updated[2] == "EarnForecastRevision"

    def test_upsert_index_master_preserves_child_rows(self, market_db: MarketDb) -> None:
        market_db.upsert_index_master(
            [{"code": "0000", "name": "TOPIX", "category": "topix"}]
        )
        publish_indices_data(market_db,
            [{"code": "0000", "date": "2026-02-10", "close": 100.0}]
        )
        market_db.upsert_index_master(
            [{"code": "0000", "name": "TOPIX Updated", "category": "topix"}]
        )

        name_row = _query_one(
            market_db.db_path,
            "SELECT name FROM index_master WHERE code='0000'",
        )
        count_row = _query_one(
            market_db.db_path,
            "SELECT COUNT(*) FROM indices_data WHERE code='0000'",
        )
        assert name_row is not None and name_row[0] == "TOPIX Updated"
        assert count_row is not None and count_row[0] == 1

    def test_upsert_index_master_preserves_earliest_data_start_date(self, market_db: MarketDb) -> None:
        market_db.upsert_index_master(
            [{"code": "N225_UNDERPX", "name": "日経平均", "category": "synthetic", "data_start_date": "2024-01-16"}]
        )
        market_db.upsert_index_master(
            [{"code": "N225_UNDERPX", "name": "日経平均", "category": "synthetic", "data_start_date": "2024-01-20"}]
        )

        data_start_row = _query_one(
            market_db.db_path,
            "SELECT data_start_date FROM index_master WHERE code='N225_UNDERPX'",
        )

        assert data_start_row is not None
        assert data_start_row[0] == "2024-01-16"


class TestMarketDbDerivedStats:
    def test_latest_dates_and_ranges(self, market_db: MarketDb) -> None:
        assert market_db.get_latest_trading_date() is None
        assert market_db.get_latest_stock_data_date() is None
        assert market_db.get_latest_indices_data_dates() == {}
        assert market_db.get_latest_margin_date() is None
        assert market_db.get_margin_codes() == set()
        assert market_db.get_topix_date_range() is None
        assert market_db.get_stock_data_date_range() is None

        publish_topix_data(market_db,
            [
                {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
                {"date": "2024-01-16", "open": 2505.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
            ]
        )
        publish_stock_data(market_db,
            [
                {"code": "7203", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0, "volume": 1000000},
                {"code": "7203", "date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0, "volume": 1200000},
            ]
        )
        market_db.upsert_index_master(
            [{"code": "0000", "name": "TOPIX", "category": "topix"}]
        )
        publish_indices_data(market_db,
            [
                {"code": "0000", "date": "2024-01-15", "close": 2505.0},
                {"code": "0000", "date": "2024-01-16", "close": 2515.0},
                {"code": "0001", "date": "2024-01-14", "close": 1110.0},
            ]
        )
        publish_margin_data(market_db,
            [
                {"code": "7203", "date": "2024-01-10", "long_margin_volume": 900.0, "short_margin_volume": 150.0},
                {"code": "7203", "date": "2024-01-17", "long_margin_volume": 1000.0, "short_margin_volume": 200.0},
                {"code": "6758", "date": "2024-01-12", "long_margin_volume": 800.0, "short_margin_volume": 120.0},
            ]
        )

        assert market_db.get_latest_trading_date() == "2024-01-16"
        assert market_db.get_latest_stock_data_date() == "2024-01-16"
        assert market_db.get_latest_indices_data_dates() == {
            "0000": "2024-01-16",
            "0001": "2024-01-14",
        }
        assert market_db.get_latest_margin_date() == "2024-01-17"
        assert market_db.get_margin_codes() == {"6758", "7203"}

        topix_range = market_db.get_topix_date_range()
        assert topix_range == {"count": 2, "min": "2024-01-15", "max": "2024-01-16"}

        stock_range = market_db.get_stock_data_date_range()
        assert stock_range is not None
        assert stock_range["count"] == 2
        assert stock_range["dateCount"] == 2
        assert stock_range["averageStocksPerDay"] == 1.0

        indices_range = market_db.get_indices_data_range()
        assert indices_range is not None
        assert indices_range["masterCount"] == 1
        assert indices_range["dataCount"] == 3
        assert indices_range["dateRange"] == {"min": "2024-01-14", "max": "2024-01-16"}

    def test_stock_refresh_and_missing_date_helpers(self, market_db: MarketDb) -> None:
        publish_topix_data(market_db,
            [
                {"date": "2024-01-14", "open": 2490.0, "high": 2500.0, "low": 2480.0, "close": 2495.0},
                {"date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
            ]
        )
        publish_stock_data(market_db,
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 5000.0,
                    "high": 5020.0,
                    "low": 4980.0,
                    "close": 5010.0,
                    "volume": 1000000,
                    "adjustment_factor": 1.0,
                },
                {
                    "code": "7203",
                    "date": "2024-01-16",
                    "open": 2510.0,
                    "high": 2520.0,
                    "low": 2500.0,
                    "close": 2515.0,
                    "volume": 1200000,
                    "adjustment_factor": 0.5,
                },
                {
                    "code": "6501",
                    "date": "2024-01-15",
                    "open": 4000.0,
                    "high": 4020.0,
                    "low": 3980.0,
                    "close": 4010.0,
                    "volume": 450000,
                    "adjustment_factor": 1.0,
                },
                {
                    "code": "6501",
                    "date": "2024-01-16",
                    "open": 8000.0,
                    "high": 8100.0,
                    "low": 7900.0,
                    "close": 8050.0,
                    "volume": 500000,
                    "adjustment_factor": 1.5,
                },
            ]
        )

        assert market_db.get_missing_stock_data_dates() == ["2024-01-14"]
        assert market_db.get_missing_stock_data_dates_count() == 1
        events = market_db.get_adjustment_events()
        assert len(events) == 2
        assert {event["eventType"] for event in events} == {"stock_split", "reverse_split"}
        assert market_db.get_stocks_needing_refresh() == []
        assert market_db.get_stocks_needing_refresh_count() == 0

        publish_stock_data(market_db,
            [
                {
                    "code": "7203",
                    "date": "2024-01-17",
                    "open": 1250.0,
                    "high": 1260.0,
                    "low": 1240.0,
                    "close": 1255.0,
                    "volume": 2400000,
                    "adjustment_factor": 0.25,
                    "created_at": "2024-01-20T00:00:00+00:00",
                },
            ]
        )

        assert market_db.get_stocks_needing_refresh() == []
        assert market_db.get_stocks_needing_refresh_count() == 0
        assert market_db.get_stock_data_unique_date_count() == 3
        assert market_db.get_db_file_size() > 0


class TestMarketDbFundamentals:
    def test_fundamentals_target_coverage_helpers(self, market_db: MarketDb) -> None:
        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                },
                {
                    "code": "6758",
                    "company_name": "ソニー",
                    "market_code": "prime",
                    "market_name": "プライム",
                    "sector_17_code": "8",
                    "sector_17_name": "電気機器",
                    "sector_33_code": "3600",
                    "sector_33_name": "電気機器",
                    "listed_date": "1958-12-01",
                },
                {
                    "code": "9999",
                    "company_name": "Standard",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "sector_17_code": "8",
                    "sector_17_name": "電気機器",
                    "sector_33_code": "3600",
                    "sector_33_name": "電気機器",
                    "listed_date": "1958-12-01",
                },
                {
                    "code": "4477",
                    "company_name": "Growth",
                    "market_code": "growth",
                    "market_name": "グロース",
                    "sector_17_code": "8",
                    "sector_17_name": "電気機器",
                    "sector_33_code": "3600",
                    "sector_33_name": "電気機器",
                    "listed_date": "2019-04-24",
                },
            ]
        )
        publish_statements(market_db,
            [
                {
                    "code": "7203",
                    "statement_id": "disc-7203-20240510",
                    "disclosed_date": "2024-05-10",
                    "disclosed_at": "2024-05-10T15:30:00+09:00",
                    "period_start": "2023-04-01",
                    "period_end": "2024-03-31",
                    "earnings_per_share": 100.0,
                    "profit": 1000.0,
                    "equity": 2000.0,
                    "type_of_current_period": "FY",
                },
                {
                    "code": "9999",
                    "statement_id": "disc-9999-20240509",
                    "disclosed_date": "2024-05-09",
                    "disclosed_at": "2024-05-09T15:30:00+09:00",
                    "period_start": "2023-04-01",
                    "period_end": "2024-03-31",
                    "earnings_per_share": 10.0,
                    "profit": 100.0,
                    "equity": 200.0,
                    "type_of_current_period": "FY",
                },
            ]
        )

        assert market_db.get_latest_statement_disclosed_date() == "2024-05-10"
        assert market_db.get_statement_codes() == {"7203", "9999"}
        assert market_db.get_prime_codes() == {"6758", "7203"}
        assert market_db.get_fundamentals_target_codes() == {"4477", "6758", "7203", "9999"}

        coverage = market_db.get_prime_statement_coverage()
        assert coverage["primeCount"] == 2
        assert coverage["coveredCount"] == 1
        assert coverage["missingCount"] == 1
        assert coverage["missingCodes"] == ["6758"]


class TestMarketDbReadOnly:
    def test_read_only_prevents_write(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "market_ro.duckdb")
        rw = open_market_db(db_path)
        rw.close()

        ro = open_market_db(db_path, read_only=True)
        with pytest.raises(PermissionError):
            ro.upsert_stocks(
                [
                    {
                        "code": "7203",
                        "company_name": "トヨタ",
                        "market_code": "0111",
                        "market_name": "プライム",
                        "sector_17_code": "6",
                        "sector_17_name": "自動車",
                        "sector_33_code": "3700",
                        "sector_33_name": "輸送用機器",
                        "listed_date": "1949-05-16",
                    }
                ]
            )
        ro.close()

    def test_read_only_detects_legacy_stock_snapshot_without_raw_table(
        self, tmp_path: Path
    ) -> None:
        db_path = str(tmp_path / "market_ro_legacy.duckdb")
        rw = open_market_db(db_path)
        publish_stock_data(rw,
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 5000.0,
                    "high": 5020.0,
                    "low": 4980.0,
                    "close": 5010.0,
                    "volume": 1000000,
                    "adjustment_factor": 1.0,
                },
                {
                    "code": "7203",
                    "date": "2024-01-16",
                    "open": 2510.0,
                    "high": 2520.0,
                    "low": 2500.0,
                    "close": 2515.0,
                    "volume": 1200000,
                    "adjustment_factor": 0.5,
                },
            ]
        )
        rw.close()

        conn = duckdb.connect(db_path)
        try:
            conn.execute("DROP TABLE stock_data_raw")
            conn.execute("DELETE FROM sync_metadata WHERE key = 'stock_price_adjustment_mode'")
        finally:
            conn.close()

        ro = open_market_db(db_path, read_only=True)
        assert ro.is_legacy_stock_price_snapshot() is True
        ro.close()

    def test_get_db_file_size_handles_os_error(
        self, market_db: MarketDb, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _raise_os_error(_path: str) -> int:
            raise OSError("stat failed")

        monkeypatch.setattr(
            "src.infrastructure.db.market.market_db.os.path.getsize",
            _raise_os_error,
        )
        assert market_db.get_db_file_size() == 0


class TestMarketDbEdgeCases:
    def test_upsert_methods_return_zero_for_empty_rows(self, market_db: MarketDb) -> None:
        assert market_db.upsert_stocks([]).mutated_rows == 0
        assert publish_stock_data(market_db,[]).mutated_rows == 0
        assert publish_topix_data(market_db,[]).mutated_rows == 0
        assert publish_indices_data(market_db,[]).mutated_rows == 0
        assert publish_margin_data(market_db,[]).mutated_rows == 0
        assert publish_statements(market_db,[]).mutated_rows == 0
        assert market_db.upsert_index_master([]).mutated_rows == 0

    def test_methods_return_safe_defaults_when_tables_are_missing(self, market_db: MarketDb) -> None:
        for table in (
            "sync_metadata",
            "topix_data",
            "stock_data",
            "indices_data",
            "margin_data",
            "index_master",
            "statements",
            "stocks",
        ):
            market_db._execute(f"DROP TABLE IF EXISTS {table}")

        # Missing-table guards
        assert market_db.get_sync_metadata("any") is None
        assert market_db.get_latest_trading_date() is None
        assert market_db.get_latest_stock_data_date() is None
        assert market_db.get_latest_indices_data_dates() == {}
        assert market_db.get_latest_margin_date() is None
        assert market_db.get_margin_codes() == set()
        assert market_db.get_index_master_codes() == set()
        assert market_db.get_latest_statement_disclosed_date() is None
        assert market_db.get_statement_codes() == set()
        assert market_db.get_prime_codes() == set()
        assert market_db.get_fundamentals_target_codes() == set()
        assert market_db.get_stock_count_by_market() == {}
        assert market_db.get_topix_date_range() is None
        assert market_db.get_stock_data_date_range() is None
        assert market_db.get_missing_stock_data_dates(limit=0) == []
        assert market_db.get_missing_stock_data_dates(limit=10) == []
        assert market_db.get_missing_stock_data_dates_count() == 0
        assert market_db.get_adjustment_events(limit=0) == []
        assert market_db.get_stocks_needing_refresh(limit=0) == []
        assert market_db.get_stocks_needing_refresh_count() == 0

    def test_adjustment_without_prior_history_is_not_flagged(
        self, market_db: MarketDb
    ) -> None:
        publish_stock_data(market_db,
            [
                {
                    "code": "7203",
                    "date": "2024-01-16",
                    "open": 2510.0,
                    "high": 2520.0,
                    "low": 2500.0,
                    "close": 2515.0,
                    "volume": 1200000,
                    "adjustment_factor": 0.5,
                },
            ]
        )

        assert market_db.get_adjustment_events()
        assert market_db.get_stocks_needing_refresh() == []
        assert market_db.get_stocks_needing_refresh_count() == 0
        assert market_db.get_stock_data_unique_date_count() == 1

        # Coverage helper branches
        assert market_db.get_statement_non_null_counts([]) == {}
        assert market_db.get_statement_non_null_counts(["eps", "profit"]) == {
            "eps": 0,
            "profit": 0,
        }
        coverage = market_db.get_prime_statement_coverage(limit_missing=None)
        assert coverage["primeCount"] == 0
        assert coverage["coveredCount"] == 0
        assert coverage["missingCount"] == 0
        assert coverage["missingCodes"] == []

        indices_range = market_db.get_indices_data_range()
        assert indices_range == {
            "masterCount": 0,
            "dataCount": 0,
            "dateCount": 0,
            "dateRange": None,
            "byCategory": {},
        }
        assert market_db.get_stats()["index_master"] == 0

    def test_get_indices_data_range_handles_empty_table(self, market_db: MarketDb) -> None:
        market_db.upsert_index_master(
            [
                {"code": "0000", "name": "TOPIX", "category": "topix"},
                {"code": "0040", "name": "水産", "category": "sector33"},
            ]
        )
        result = market_db.get_indices_data_range()
        assert result is not None
        assert result["masterCount"] == 2
        assert result["dataCount"] == 0
        assert result["dateCount"] == 0
        assert result["dateRange"] is None
        assert result["byCategory"] == {"topix": 1, "sector33": 1}

    def test_ensure_schema_does_not_alter_unversioned_statements(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "legacy-market.duckdb")
        conn = duckdb.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE statements (
                    code TEXT,
                    disclosed_date TEXT,
                    earnings_per_share DOUBLE,
                    profit DOUBLE,
                    equity DOUBLE,
                    type_of_current_period TEXT,
                    type_of_document TEXT,
                    next_year_forecast_earnings_per_share DOUBLE,
                    bps DOUBLE,
                    sales DOUBLE,
                    operating_profit DOUBLE,
                    ordinary_profit DOUBLE,
                    operating_cash_flow DOUBLE,
                    dividend_fy DOUBLE,
                    forecast_eps DOUBLE,
                    investing_cash_flow DOUBLE,
                    financing_cash_flow DOUBLE,
                    cash_and_equivalents DOUBLE,
                    total_assets DOUBLE,
                    shares_outstanding DOUBLE,
                    treasury_shares DOUBLE,
                    PRIMARY KEY (code, disclosed_date)
                )
                """
            )
        finally:
            conn.close()

        db = open_market_db(db_path)
        try:
            columns = {
                str(row[1])
                for row in db._execute("PRAGMA table_info('statements')").fetchall()
                if row and len(row) > 1
            }
            assert db.get_market_schema_version() == 0
            assert "forecast_dividend_fy" not in columns
            assert "next_year_forecast_dividend_fy" not in columns
            assert "payout_ratio" not in columns
            assert "forecast_payout_ratio" not in columns
            assert "next_year_forecast_payout_ratio" not in columns
        finally:
            db.close()
