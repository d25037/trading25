"""
Ranking Service Unit Tests
"""

from datetime import date as calendar_date, timedelta

import duckdb
import pandas as pd

import pytest

import src.application.services.ranking_service as ranking_service_module
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    FundamentalRankingCalculator,
    ForecastValue as _ForecastValue,
    LatestFyRow as _LatestFyRow,
    StatementRow as _StatementRow,
    adjust_per_share_value as _adjust_per_share_value,
    calculate_eps_ratio as _calculate_eps_ratio,
    is_valid_share_count as _is_valid_share_count,
    normalize_period_label as _normalize_period_label,
    to_nullable_float as _to_nullable_float,
)
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_share_count_to_price_basis,
)
from src.application.services.ranking_service import (
    RankingService,
)
from src.application.services.ranking_query_helpers import (
    build_market_filter,
)
from src.application.services.ranking_daily_queries import (
    ranking_by_period_high,
    ranking_by_period_low,
    ranking_by_price_change,
    ranking_by_price_change_from_days,
    ranking_by_trading_value_average,
)
from src.application.services.ranking_value_composite_config import (
    VALUE_COMPOSITE_PROFILE_BY_ID,
    ensure_supported_value_composite_forward_eps_mode,
    normalize_value_composite_weights,
    resolve_value_composite_profile_and_score_method,
    value_composite_ranking_score_policy,
    value_composite_response_weights,
    value_composite_score_policy,
)
from src.application.services.ranking_liquidity import (
    PrimeLiquidityMetrics,
    _classify_stale_overvalued_or_no_earnings_flags,
    classify_prime_liquidity_regime,
    classify_risk_flags,
)
from src.application.services.ranking_technical_flags import (
    classify_technical_flags,
    load_ranking_technical_metrics,
)
from src.application.services.ranking_valuation import (
    with_prime_valuation_percentiles,
)
from src.application.services.ranking_response_items import (
    build_fundamental_ranking_item,
    build_ranking_item,
    build_value_composite_item,
    build_value_composite_score_response,
)
from src.entrypoints.http.schemas.ranking import RankingItem
from src.application.services.ranking_statement_selection import (
    latest_actual_fy_disclosed_date,
    latest_value_bps_statement,
)
from src.application.services.ranking_statement_rows import (
    statement_row_from_mapping,
    statement_rows_by_code,
)


def _create_stock_master_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DROP VIEW IF EXISTS stock_master_daily")
    conn.execute("DROP VIEW IF EXISTS stocks_latest")
    conn.execute("""
        CREATE VIEW stock_master_daily AS
        SELECT d.date, s.*
        FROM (SELECT DISTINCT date FROM stock_data) d
        CROSS JOIN stocks s
    """)
    conn.execute("CREATE VIEW stocks_latest AS SELECT * FROM stocks")


@pytest.fixture
def ranking_db(tmp_path):
    """ランキングテスト用DB"""
    db_path = str(tmp_path / "ranking.db")
    conn = duckdb.connect(db_path)

    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT NOT NULL, company_name_english TEXT,
            market_code TEXT NOT NULL, market_name TEXT NOT NULL,
            sector_17_code TEXT, sector_17_name TEXT,
            sector_33_code TEXT, sector_33_name TEXT NOT NULL,
            scale_category TEXT, listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            volume INTEGER NOT NULL, adjustment_factor REAL, created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE statements (
            code TEXT NOT NULL,
            disclosed_date TEXT NOT NULL,
            earnings_per_share REAL,
            profit REAL,
            equity REAL,
            type_of_current_period TEXT,
            type_of_document TEXT,
            next_year_forecast_earnings_per_share REAL,
            bps REAL,
            sales REAL,
            operating_profit REAL,
            forecast_operating_profit REAL,
            next_year_forecast_operating_profit REAL,
            ordinary_profit REAL,
            operating_cash_flow REAL,
            dividend_fy REAL,
            forecast_dividend_fy REAL,
            next_year_forecast_dividend_fy REAL,
            payout_ratio REAL,
            forecast_payout_ratio REAL,
            next_year_forecast_payout_ratio REAL,
            forecast_eps REAL,
            investing_cash_flow REAL,
            financing_cash_flow REAL,
            cash_and_equivalents REAL,
            total_assets REAL,
            shares_outstanding REAL,
            treasury_shares REAL,
            PRIMARY KEY (code, disclosed_date)
        )
    """)
    conn.execute("""
        CREATE TABLE index_master (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            name_english TEXT,
            category TEXT NOT NULL,
            data_start_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE indices_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            sector_name TEXT,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    # 3 銘柄
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "72030",
            "トヨタ",
            "TOYOTA",
            "prime",
            "P",
            "S17",
            "輸送",
            "S33",
            "輸送用機器",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "67580",
            "ソニー",
            "SONY",
            "prime",
            "P",
            "S17",
            "電気",
            "S33",
            "電気機器",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "83060",
            "Numeric Prime",
            "NPRIME",
            "0111",
            "P",
            "S17",
            "銀行",
            "S33",
            "銀行業",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "46890",
            "Alt Prime",
            "APRIME",
            "prime",
            "P",
            "S17",
            "情報",
            "S33",
            "情報通信",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "11110",
            "No Statement Prime",
            "NOSTMT",
            "prime",
            "P",
            "S17",
            "情報",
            "S33",
            "情報通信",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "22220",
            "Zero Actual Prime",
            "ZEROACT",
            "prime",
            "P",
            "S17",
            "サービス",
            "S33",
            "サービス業",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "33330",
            "Peak Actual Prime",
            "PEAK",
            "prime",
            "P",
            "S17",
            "情報",
            "S33",
            "情報通信",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "44440",
            "No Forecast Prime",
            "NOFC",
            "prime",
            "P",
            "S17",
            "情報",
            "S33",
            "情報通信",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "5555",
            "Mixed Format Prime",
            "MIXED",
            "prime",
            "P",
            "S17",
            "情報",
            "S33",
            "情報通信",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "99840",
            "テスト",
            "TEST",
            "standard",
            "S",
            "S17",
            "情報",
            "S33",
            "情報通信",
            None,
            "2000-01-01",
            None,
            None,
        ),
    )

    # 5日分のOHLCVデータ
    dates = ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"]
    for code, base_v in [
        ("72030", 2000000),
        ("67580", 1500000),
        ("83060", 1100000),
        ("46890", 900000),
        ("11110", 850000),
        ("22220", 800000),
        ("33330", 780000),
        ("44440", 760000),
        ("55550", 1700000),
        ("99840", 100000),
    ]:
        for i, d in enumerate(dates):
            price = (
                2500.0 + i * 10
                if code == "72030"
                else (13000.0 + i * 50 if code == "67580" else 500.0 + i * 5)
            )
            vol = base_v + i * 10000
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                (code, d, price, price + 20, price - 10, price, vol, 1.0, None),
            )

    conn.execute(
        "INSERT INTO index_master VALUES (?,?,?,?,?)",
        ("TOPIX", "TOPIX", None, "topix", "2024-01-12"),
    )
    conn.execute(
        "INSERT INTO index_master VALUES (?,?,?,?,?)",
        ("N225", "Nikkei 225", None, "market", "2024-01-12"),
    )

    index_rows = {
        "TOPIX": [1000.0, 1012.0, 1020.0, 1032.0, 1045.0, 1060.0],
        "N225": [33000.0, 33200.0, 33120.0, 33480.0, 33720.0, 33990.0],
    }
    index_dates = [
        "2024-01-12",
        "2024-01-15",
        "2024-01-16",
        "2024-01-17",
        "2024-01-18",
        "2024-01-19",
    ]
    for code, closes in index_rows.items():
        for current_date, close in zip(index_dates, closes, strict=True):
            conn.execute(
                "INSERT INTO indices_data VALUES (?,?,?,?,?,?,?,?)",
                (code, current_date, close, close, close, close, None, None),
            )

    # statements (FY + quarter revisions)
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("72030", "2024-01-10", 100.0, "FY", 120.0, 118.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("72030", "2024-01-18", "1Q", 140.0, 100.0),
    )

    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, shares_outstanding
        )
        VALUES (?,?,?,?,?,?)
        """,
        ("67580", "2024-01-12", 200.0, "FY", 220.0, 200.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("67580", "2024-08-12", "Q1", None, 250.0),
    )

    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("83060", "2024-01-15", -50.0, "FY", -40.0, -42.0, 50.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("83060", "2024-08-15", "Q2", -20.0, 50.0),
    )

    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("46890", "2024-01-16", 80.0, "FY", None, None, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, next_year_forecast_earnings_per_share, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("46890", "2024-01-18", "2Q", 95.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("22220", "2024-01-17", 0.0, "FY", 60.0, 60.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("33330", "2023-05-21", 300.0, "FY", 320.0, 320.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("33330", "2024-01-11", 200.0, "FY", 250.0, 250.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("33330", "2024-11-21", 210.0, "FY", 260.0, 260.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("44440", "2024-01-12", 140.0, "FY", None, None, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("5555", "2024-01-18", 50.0, "FY", 60.0, 60.0, 100.0),
    )

    _create_stock_master_views(conn)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(ranking_db):
    reader = MarketDbReader(ranking_db)
    yield RankingService(reader)
    reader.close()


def _create_adjusted_metric_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_valuation (
            code TEXT,
            date TEXT,
            price_basis_date TEXT,
            close DOUBLE,
            eps DOUBLE,
            bps DOUBLE,
            forward_eps DOUBLE,
            per DOUBLE,
            forward_per DOUBLE,
            p_op DOUBLE,
            forward_p_op DOUBLE,
            pbr DOUBLE,
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            statement_disclosed_date TEXT,
            forward_eps_disclosed_date TEXT,
            forward_eps_source TEXT,
            basis_version TEXT,
            created_at TEXT,
            PRIMARY KEY (code, date, basis_version)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS statement_metrics_adjusted (
            code TEXT,
            disclosed_date TEXT,
            period_end TEXT,
            period_type TEXT,
            price_basis_date TEXT,
            raw_eps DOUBLE,
            adjusted_eps DOUBLE,
            raw_bps DOUBLE,
            adjusted_bps DOUBLE,
            raw_forecast_eps DOUBLE,
            adjusted_forecast_eps DOUBLE,
            raw_dividend_fy DOUBLE,
            adjusted_dividend_fy DOUBLE,
            raw_shares_outstanding DOUBLE,
            adjusted_shares_outstanding DOUBLE,
            adjustment_factor_cumulative DOUBLE,
            basis_version TEXT,
            created_at TEXT,
            PRIMARY KEY (code, disclosed_date, period_end, period_type, basis_version)
        )
    """)


def _insert_daily_valuation(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    eps: float,
    bps: float,
    forward_eps: float,
    per: float,
    forward_per: float,
    pbr: float,
    market_cap: float,
    p_op: float = 2.8,
    forward_p_op: float = 2.0,
    source: str = "fy",
    forward_date: str = "2024-01-19",
) -> None:
    conn.execute(
        """
        INSERT INTO daily_valuation VALUES (
            ?, '2024-01-19', '2024-01-19', 520.0,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
            '2024-01-10', ?, ?, 'adjusted-v1:2024-01-19', NULL
        )
        """,
        (
            code,
            eps,
            bps,
            forward_eps,
            per,
            forward_per,
            p_op,
            forward_p_op,
            pbr,
            market_cap,
            forward_date,
            source,
        ),
    )


class TestGetRankings:
    def test_default(self, service):
        result = service.get_rankings()
        assert result.date == "2024-01-19"
        assert result.markets == ["prime"]
        assert result.lookbackDays == 1
        assert result.periodDays == 250
        assert len(result.indexPerformance) == 2

    def test_trading_value_ranking(self, service):
        result = service.get_rankings(markets="prime", limit=10)
        items = result.rankings.tradingValue
        assert len(items) >= 1
        # 売買代金降順
        if len(items) >= 2:
            assert items[0].tradingValue >= items[1].tradingValue

    def test_gainers_losers(self, service):
        result = service.get_rankings(markets="prime")
        gainers = result.rankings.gainers
        losers = result.rankings.losers
        assert isinstance(gainers, list)
        assert isinstance(losers, list)

    def test_with_date(self, service):
        result = service.get_rankings(date="2024-01-17")
        assert result.date == "2024-01-17"

    def test_include_valuation_uses_latest_adjusted_price_basis_for_old_date(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute("""
                CREATE TABLE stock_data_raw (
                    code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    adjustment_factor REAL
                )
            """)
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "99990",
                    "Split Adjusted",
                    "SPLIT",
                    "prime",
                    "P",
                    "S17",
                    "情報",
                    "S33",
                    "情報通信",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            for trade_date, price in [
                ("2024-01-17", 500.0),
                ("2024-01-19", 520.0),
            ]:
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        "99990",
                        trade_date,
                        price,
                        price,
                        price,
                        price,
                        10_000_000,
                        1.0,
                        None,
                    ),
                )
            conn.execute(
                "INSERT INTO stock_data_raw VALUES (?, ?, ?)",
                ("99990", "2024-01-18", 0.5),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    type_of_document, forecast_eps, bps, shares_outstanding,
                    treasury_shares
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "99990",
                    "2024-01-10",
                    100.0,
                    "FY",
                    "FYFinancialStatements",
                    125.0,
                    1000.0,
                    100.0,
                    0.0,
                ),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_rankings(
            date="2024-01-17",
            markets="prime",
            limit=200,
            include_valuation=True,
        )
        reader.close()

        item = next(row for row in result.rankings.tradingValue if row.code == "99990")
        assert item.per == pytest.approx(10.0)
        assert item.forwardPer == pytest.approx(8.0)
        assert item.pbr == pytest.approx(1.0)
        assert item.marketCap == pytest.approx(100_000.0)

    def test_include_valuation_prefers_daily_valuation_sot_for_forward_pop(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            _create_adjusted_metric_tables(conn)
            _insert_daily_valuation(
                conn,
                code="72030",
                eps=101.0,
                bps=2020.0,
                forward_eps=155.0,
                per=24.5,
                forward_per=15.9,
                p_op=18.4,
                forward_p_op=9.7,
                pbr=1.21,
                market_cap=2_460_000.0,
                source="revised",
                forward_date="2024-01-18",
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=20,
            include_valuation=True,
        )
        reader.close()

        item = next(row for row in result.rankings.tradingValue if row.code == "72030")
        assert item.per == pytest.approx(24.5)
        assert item.forwardPer == pytest.approx(15.9)
        assert item.pOp == pytest.approx(18.4)
        assert item.forwardPOp == pytest.approx(9.7)
        assert item.pbr == pytest.approx(1.21)
        assert item.marketCap == pytest.approx(2_460_000.0)
        assert item.forwardEpsDisclosedDate == "2024-01-18"
        assert item.forwardEpsSource == "revised"

    def test_include_valuation_adds_prime_relative_percentiles(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            _create_adjusted_metric_tables(conn)
            valuation_inputs = [
                ("72030", 10.0, 8.0, 0.5, 5.0),
                ("67580", 20.0, 20.0, 1.0, 15.0),
                ("83060", 30.0, 36.0, 1.5, 30.0),
                ("46890", 40.0, 56.0, 2.0, 60.0),
            ]
            for code, per, forward_per, pbr, forward_p_op in valuation_inputs:
                _insert_daily_valuation(
                    conn,
                    code=code,
                    eps=100.0,
                    bps=1000.0,
                    forward_eps=120.0,
                    per=per,
                    forward_per=forward_per,
                    pbr=pbr,
                    p_op=7.0,
                    forward_p_op=forward_p_op,
                    market_cap=1_000_000.0,
                )
            _insert_daily_valuation(
                conn,
                code="99840",
                eps=100.0,
                bps=1000.0,
                forward_eps=120.0,
                per=1.0,
                forward_per=1.0,
                pbr=0.1,
                p_op=1.0,
                forward_p_op=1.0,
                market_cap=1_000_000.0,
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        prime_result = svc.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=20,
            include_valuation=True,
        )
        standard_result = svc.get_rankings(
            date="2024-01-19",
            markets="standard",
            limit=20,
            include_valuation=True,
        )
        reader.close()

        cheapest = next(
            row for row in prime_result.rankings.tradingValue if row.code == "72030"
        )
        expensive = next(
            row for row in prime_result.rankings.tradingValue if row.code == "46890"
        )
        standard = next(
            row for row in standard_result.rankings.tradingValue if row.code == "99840"
        )
        assert cheapest.perPercentile == pytest.approx(0.0)
        assert cheapest.forwardPerPercentile == pytest.approx(0.0)
        assert cheapest.forwardPOpPercentile == pytest.approx(0.0)
        assert cheapest.pbrPercentile == pytest.approx(0.0)
        assert expensive.perPercentile == pytest.approx(1.0)
        assert expensive.forwardPerPercentile == pytest.approx(1.0)
        assert expensive.forwardPOpPercentile == pytest.approx(1.0)
        assert expensive.pbrPercentile == pytest.approx(1.0)
        assert standard.perPercentile is None
        assert standard.forwardPerPercentile is None
        assert standard.forwardPOpPercentile is None
        assert standard.pbrPercentile is None

    def test_include_valuation_filters_forward_eps_source_disclosure_before_limit(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            for code, name, price, volume, disclosed_date in [
                ("77770", "Stale Forward EPS", 10_000.0, 10_000_000, "2023-01-01"),
                ("77780", "Fresh Forward EPS", 9_000.0, 10_000_000, "2024-01-18"),
            ]:
                conn.execute(
                    "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        code,
                        name,
                        "FWD",
                        "prime",
                        "P",
                        "S17",
                        "情報",
                        "S33",
                        "情報通信",
                        None,
                        "2000-01-01",
                        None,
                        None,
                    ),
                )
                for trade_date in ["2024-01-18", "2024-01-19"]:
                    conn.execute(
                        "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            code,
                            trade_date,
                            price,
                            price,
                            price,
                            price,
                            volume,
                            1.0,
                            None,
                        ),
                    )
                conn.execute(
                    """
                    INSERT INTO statements (
                        code, disclosed_date, earnings_per_share, type_of_current_period,
                        type_of_document, forecast_eps, bps, shares_outstanding
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        code,
                        disclosed_date,
                        100.0,
                        "FY",
                        "FYFinancialStatements",
                        120.0,
                        1000.0,
                        1_000_000.0,
                    ),
                )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            forward_eps_disclosed_within_days=252,
        )
        reader.close()

        items = result.rankings.tradingValue
        assert [item.code for item in items] == ["77780"]
        assert items[0].rank == 1
        assert items[0].forwardEpsDisclosedDate == "2024-01-18"
        assert items[0].forwardEpsSource == "fy"

    def test_include_valuation_forward_eps_source_disclosure_filter_can_be_disabled(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "77770",
                    "Stale Forward EPS",
                    "FWD",
                    "prime",
                    "P",
                    "S17",
                    "情報",
                    "S33",
                    "情報通信",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                ("77770", "2024-01-19", 10_000.0, 10_000.0, 10_000.0, 10_000.0, 10_000_000, 1.0, None),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    type_of_document, forecast_eps, bps, shares_outstanding
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("77770", "2023-01-01", 100.0, "FY", "FYFinancialStatements", 120.0, 1000.0, 1_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            forward_eps_disclosed_within_days=0,
        )
        reader.close()

        assert result.rankings.tradingValue[0].code == "77770"
        assert result.rankings.tradingValue[0].forwardEpsDisclosedDate == "2023-01-01"

    def test_liquidity_state_filter_applies_before_limit(self, service, monkeypatch):
        def fake_enrich_prime_liquidity(
            reader,
            collections,
            *,
            target_date,
            price_basis_date,
        ):
            del reader, target_date, price_basis_date
            for collection in collections:
                for item in collection:
                    if item.code == "72030":
                        item.liquidityRegime = "crowded_rerating"
                    elif item.code == "67580":
                        item.liquidityRegime = "distribution_stress"

        monkeypatch.setattr(
            ranking_service_module,
            "_enrich_ranking_collections_with_prime_liquidity",
            fake_enrich_prime_liquidity,
        )

        result = service.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            liquidity_state="crowded_rerating",
        )

        assert [item.code for item in result.rankings.tradingValue] == ["72030"]
        assert result.rankings.tradingValue[0].rank == 1

    def test_liquidity_state_filter_can_match_overheat_risk_flag(
        self, service, monkeypatch
    ):
        def fake_enrich_prime_liquidity(
            reader,
            collections,
            *,
            target_date,
            price_basis_date,
        ):
            del reader, target_date, price_basis_date
            for collection in collections:
                for item in collection:
                    item.liquidityRegime = "neutral"
                    if item.code == "67580":
                        item.riskFlags = ["overheat"]

        monkeypatch.setattr(
            ranking_service_module,
            "_enrich_ranking_collections_with_prime_liquidity",
            fake_enrich_prime_liquidity,
        )

        result = service.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            liquidity_state="overheat",
        )

        assert [item.code for item in result.rankings.tradingValue] == ["67580"]
        assert result.rankings.tradingValue[0].riskFlags == ["overheat"]

    def test_liquidity_state_filter_can_match_stale_rally_fade_risk_flag(
        self, service, monkeypatch
    ):
        def fake_enrich_prime_liquidity(
            reader,
            collections,
            *,
            target_date,
            price_basis_date,
        ):
            del reader, target_date, price_basis_date
            for collection in collections:
                for item in collection:
                    item.liquidityRegime = "stale_liquidity"
                    if item.code == "67580":
                        item.riskFlags = ["stale_rally_fade"]

        monkeypatch.setattr(
            ranking_service_module,
            "_enrich_ranking_collections_with_prime_liquidity",
            fake_enrich_prime_liquidity,
        )

        result = service.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            liquidity_state="stale_rally_fade",
        )

        assert [item.code for item in result.rankings.tradingValue] == ["67580"]
        assert result.rankings.tradingValue[0].riskFlags == ["stale_rally_fade"]

    def test_regime_state_filter_can_match_neutral_rerating_good_before_limit(
        self, service, monkeypatch
    ):
        def fake_enrich_prime_liquidity(
            reader,
            collections,
            *,
            target_date,
            price_basis_date,
        ):
            del reader, target_date, price_basis_date
            for collection in collections:
                for item in collection:
                    item.liquidityRegime = "neutral_rerating"
                    item.liquidityResidualZ = 0.2
                    item.adv60ToFreeFloatPct = 1.0
                    if item.code == "72030":
                        item.pbrPercentile = 0.1
                        item.forwardPerPercentile = 0.5
                        item.perPercentile = 0.5
                        item.per = 20.0
                        item.forwardPer = 20.0
                    elif item.code == "67580":
                        item.pbrPercentile = 0.1
                        item.forwardPerPercentile = 0.1
                        item.perPercentile = 0.5
                        item.per = 20.0
                        item.forwardPer = 20.0
                    else:
                        item.pbrPercentile = 0.5
                        item.forwardPerPercentile = 0.5
                        item.perPercentile = 0.5
                        item.per = 20.0
                        item.forwardPer = 20.0

        monkeypatch.setattr(
            ranking_service_module,
            "_enrich_ranking_collections_with_prime_liquidity",
            fake_enrich_prime_liquidity,
        )

        result = service.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            regime_state="neutral_rerating_good",
        )

        assert [item.code for item in result.rankings.tradingValue] == ["67580"]
        assert result.rankings.tradingValue[0].liquidityRegime == "neutral_rerating"

    def test_regime_and_risk_state_filters_can_combine(self, service, monkeypatch):
        def fake_enrich_prime_liquidity(
            reader,
            collections,
            *,
            target_date,
            price_basis_date,
        ):
            del reader, target_date, price_basis_date
            for collection in collections:
                for item in collection:
                    item.liquidityRegime = "crowded_rerating"
                    item.liquidityResidualZ = 1.2
                    item.adv60ToFreeFloatPct = 8.0
                    item.pbrPercentile = 0.1
                    item.forwardPerPercentile = 0.1
                    if item.code == "67580":
                        item.riskFlags = ["overheat"]

        monkeypatch.setattr(
            ranking_service_module,
            "_enrich_ranking_collections_with_prime_liquidity",
            fake_enrich_prime_liquidity,
        )

        result = service.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            include_valuation=True,
            regime_state="crowded_rerating_good",
            risk_state="overheat",
        )

        assert [item.code for item in result.rankings.tradingValue] == ["67580"]
        assert result.rankings.tradingValue[0].riskFlags == ["overheat"]

    def test_technical_state_filter_can_match_atr20_acceleration(
        self, service, monkeypatch
    ):
        def fake_enrich_technical_flags(
            reader,
            collections,
            *,
            target_date,
            market_codes=None,
        ):
            del reader, target_date, market_codes
            for collection in collections:
                for item in collection:
                    if item.code == "67580":
                        item.technicalFlags = ["atr20_acceleration"]

        monkeypatch.setattr(
            ranking_service_module,
            "_enrich_ranking_collections_with_technical_flags",
            fake_enrich_technical_flags,
        )

        result = service.get_rankings(
            date="2024-01-19",
            markets="prime",
            limit=1,
            technical_state="atr20_acceleration",
        )

        assert [item.code for item in result.rankings.tradingValue] == ["67580"]
        assert result.rankings.tradingValue[0].technicalFlags == ["atr20_acceleration"]

    def test_market_filter(self, service):
        result = service.get_rankings(markets="standard")
        # standard は 99840 のみ
        items = result.rankings.tradingValue
        for item in items:
            assert item.marketCode == "standard"

    def test_market_filter_alias_prime_includes_numeric_codes(self, service):
        result = service.get_rankings(markets="prime", limit=20)
        market_codes = {item.marketCode for item in result.rankings.tradingValue}
        assert "prime" in market_codes
        assert "0111" in market_codes

    def test_market_filter_alias_0111_includes_legacy_codes(self, service):
        result = service.get_rankings(markets="0111", limit=20)
        market_codes = {item.marketCode for item in result.rankings.tradingValue}
        assert "prime" in market_codes
        assert "0111" in market_codes

    def test_trading_value_ranking_includes_daily_change(self, service):
        result = service.get_rankings(date="2024-01-19", markets="prime", limit=20)
        sony = next(item for item in result.rankings.tradingValue if item.code == "67580")

        assert sony.currentPrice == pytest.approx(13200.0)
        assert sony.previousPrice == pytest.approx(13150.0)
        assert sony.changeAmount == pytest.approx(50.0)
        assert sony.changePercentage == pytest.approx(50.0 / 13150.0 * 100.0)

    def test_trading_value_average_ranking_includes_lookback_change(self, service):
        result = service.get_rankings(date="2024-01-19", markets="prime", lookback_days=3, limit=20)
        toyota = next(item for item in result.rankings.tradingValue if item.code == "72030")

        assert toyota.currentPrice == pytest.approx(2540.0)
        assert toyota.basePrice == pytest.approx(2500.0)
        assert toyota.changeAmount == pytest.approx(40.0)
        assert toyota.changePercentage == pytest.approx(40.0 / 2500.0 * 100.0)
        assert toyota.lookbackDays == 3

    def test_rankings_support_mixed_stock_and_stock_data_code_formats(self, service):
        result = service.get_rankings(markets="prime", limit=50)
        codes = {item.code for item in result.rankings.tradingValue}
        assert "5555" in codes

    def test_price_change_prefers_4digit_row_when_mixed_codes_exist(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        conn.execute(
            "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
            ("5555", "2024-01-18", 100.0, 101.0, 99.0, 100.0, 100_000, 1.0, None),
        )
        conn.execute(
            "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
            ("5555", "2024-01-19", 110.0, 111.0, 109.0, 110.0, 120_000, 1.0, None),
        )
        conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_rankings(date="2024-01-19", markets="prime", limit=100)
        rows = [item for item in result.rankings.gainers if item.code == "5555"]
        reader.close()

        assert len(rows) == 1
        assert rows[0].previousPrice == pytest.approx(100.0)
        assert rows[0].currentPrice == pytest.approx(110.0)
        assert rows[0].changePercentage == pytest.approx(10.0)

    def test_lookback_days(self, service):
        result = service.get_rankings(lookback_days=3)
        assert result.lookbackDays == 3
        # N日平均ランキング
        items = result.rankings.tradingValue
        assert len(items) >= 1

    def test_limit(self, service):
        result = service.get_rankings(limit=1)
        assert len(result.rankings.tradingValue) <= 1
        assert len(result.rankings.gainers) <= 1

    def test_zero_limit_returns_all_matching_rows_for_sector_sorting(self, service):
        limited = service.get_rankings(markets="prime,standard", sector33_name="情報通信", limit=1)
        unlimited = service.get_rankings(markets="prime,standard", sector33_name="情報通信", limit=0)

        assert len(limited.rankings.tradingValue) == 1
        assert len(unlimited.rankings.tradingValue) > len(limited.rankings.tradingValue)
        assert {item.sector33Name for item in unlimited.rankings.tradingValue} == {"情報通信"}

    def test_include_valuation_adds_prime_liquidity_metrics_as_of_target_date(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        dates = [
            (calendar_date(2024, 1, 1) + timedelta(days=offset)).isoformat()
            for offset in range(70)
        ]
        target_date = dates[-1]
        future_disclosed_date = (
            calendar_date.fromisoformat(target_date) + timedelta(days=1)
        ).isoformat()
        conn.execute("""
            CREATE TABLE stock_data_raw (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                adjustment_factor REAL
            )
        """)
        conn.execute(
            "INSERT INTO stock_data_raw VALUES (?, ?, ?)",
            ("1000", "2024-02-20", 0.5),
        )
        for idx in range(105):
            code = f"{1000 + idx}"
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    code,
                    f"Prime Liquidity {idx}",
                    f"PL{idx}",
                    "prime",
                    "P",
                    "S17",
                    "情報",
                    "S33",
                    "情報通信",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            shares_outstanding = 1_000_000 + idx * 10_000
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    type_of_document, forecast_eps, bps, shares_outstanding,
                    treasury_shares
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code,
                    "2023-12-31",
                    10.0,
                    "FY",
                    "FYFinancialStatements",
                    12.0,
                    100.0,
                    shares_outstanding,
                    0.0,
                ),
            )
            if idx == 0:
                conn.execute(
                    """
                    INSERT INTO statements (
                        code, disclosed_date, earnings_per_share, type_of_current_period,
                        type_of_document, forecast_eps, bps, shares_outstanding,
                        treasury_shares
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        code,
                        future_disclosed_date,
                        10.0,
                        "FY",
                        "FYFinancialStatements",
                        12.0,
                        100.0,
                        100_000_000.0,
                        0.0,
                    ),
                )
            base_volume = 100_000 if idx == 0 else 1_000 + idx * 20
            for day_idx, trade_date in enumerate(dates):
                price = 100.0 + day_idx + idx * 0.1
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        code,
                        trade_date,
                        price,
                        price + 1.0,
                        price - 1.0,
                        price,
                        base_volume,
                        1.0,
                        None,
                    ),
                )
        conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_rankings(
            date=target_date,
            markets="prime",
            limit=200,
            include_valuation=True,
        )
        reader.close()

        item = next(row for row in result.rankings.tradingValue if row.code == "1000")
        assert item.liquidityResidualZ is not None
        assert item.liquidityRegime in {
            "neutral_rerating",
            "crowded_rerating",
            "distribution_stress",
            "stale_liquidity",
            "neutral",
        }
        assert item.adv60ToFreeFloatPct is not None
        assert item.adv60ToFreeFloatPct == pytest.approx(
            (13_950_000.0 / (169.0 * 2_000_000.0)) * 100.0,
            rel=1e-4,
        )
        assert item.adv60ToFreeFloatPct < 5.0
        assert item.riskFlags == []

    def test_classifies_short_term_overheat_risk_flag(self):
        assert classify_risk_flags(29.99) == ()
        assert classify_risk_flags(30.0) == ("overheat",)

    def test_classifies_atr20_acceleration_technical_flag(self):
        assert classify_technical_flags(
            recent_return_20d_pct=29.99,
            momentum_20d_percentile=None,
            momentum_60d_percentile=None,
            atr20_change_20d_pct=25.0,
            atr20_to_atr60=1.249,
        ) == ("atr20_acceleration",)
        assert (
            classify_technical_flags(
                recent_return_20d_pct=30.0,
                momentum_20d_percentile=None,
                momentum_60d_percentile=None,
                atr20_change_20d_pct=25.0,
                atr20_to_atr60=1.249,
            )
            == ()
        )
        assert (
            classify_technical_flags(
                recent_return_20d_pct=29.99,
                momentum_20d_percentile=None,
                momentum_60d_percentile=None,
                atr20_change_20d_pct=25.0,
                atr20_to_atr60=1.25,
            )
            == ()
        )

    def test_classifies_momentum_20_60_top20_technical_flag(self):
        assert classify_technical_flags(
            recent_return_20d_pct=10.0,
            momentum_20d_percentile=0.8,
            momentum_60d_percentile=0.8,
            atr20_change_20d_pct=None,
            atr20_to_atr60=None,
        ) == ("momentum_20_60_top20",)
        assert (
            classify_technical_flags(
                recent_return_20d_pct=10.0,
                momentum_20d_percentile=0.79,
                momentum_60d_percentile=0.8,
                atr20_change_20d_pct=None,
                atr20_to_atr60=None,
            )
            == ()
        )

    def test_loads_momentum_percentile_against_market_universe_not_candidates(
        self, tmp_path
    ):
        db_path = str(tmp_path / "technical-momentum.db")
        conn = duckdb.connect(db_path)
        conn.execute("""
            CREATE TABLE stocks (
                code TEXT PRIMARY KEY, company_name TEXT NOT NULL, company_name_english TEXT,
                market_code TEXT NOT NULL, market_name TEXT NOT NULL,
                sector_17_code TEXT, sector_17_name TEXT,
                sector_33_code TEXT, sector_33_name TEXT NOT NULL,
                scale_category TEXT, listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE stock_data (
                code TEXT NOT NULL, date TEXT NOT NULL,
                open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
                volume INTEGER NOT NULL, adjustment_factor REAL, created_at TEXT,
                PRIMARY KEY (code, date)
            )
        """)
        for code in ("10010", "10020", "10030", "10040", "10050"):
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    code,
                    code,
                    code,
                    "prime",
                    "P",
                    "S17",
                    "Sector17",
                    "S33",
                    "Sector33",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
        start = calendar_date(2024, 1, 1)
        daily_steps = {
            "10010": 5.0,
            "10020": 4.0,
            "10030": 3.0,
            "10040": 2.0,
            "10050": 1.0,
        }
        for day in range(90):
            current_date = (start + timedelta(days=day)).isoformat()
            for code, step in daily_steps.items():
                close = 100.0 + step * day
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (code, current_date, close, close + 1.0, close - 1.0, close, 1000, 1.0, None),
                )
        _create_stock_master_views(conn)
        conn.close()

        reader = MarketDbReader(db_path)
        metrics = load_ranking_technical_metrics(
            reader,
            target_date=(start + timedelta(days=89)).isoformat(),
            codes=("1001", "1002"),
            market_codes=["prime"],
        )
        reader.close()

        assert metrics["1001"].technical_flags == ("momentum_20_60_top20",)
        assert metrics["1002"].technical_flags == ()

    def test_classifies_stale_rally_fade_only_for_stale_overvalued_recent_positive(
        self,
    ):
        item = RankingItem(
            rank=1,
            code="1000",
            companyName="Test",
            marketCode="prime",
            sector33Name="Sector",
            currentPrice=100.0,
            volume=1000.0,
            perPercentile=0.85,
            forwardPerPercentile=0.5,
        )
        metrics = PrimeLiquidityMetrics(
            liquidity_residual_z=-1.2,
            liquidity_regime="stale_liquidity",
            adv60_to_free_float_pct=1.0,
            risk_flags=(),
            recent_return_20d_pct=1.0,
            recent_return_60d_pct=2.0,
        )

        assert _classify_stale_overvalued_or_no_earnings_flags(item, metrics) == (
            "stale_rally_fade",
        )

        assert (
            _classify_stale_overvalued_or_no_earnings_flags(
                item,
                PrimeLiquidityMetrics(
                    liquidity_residual_z=-1.2,
                    liquidity_regime="stale_liquidity",
                    adv60_to_free_float_pct=1.0,
                    risk_flags=(),
                    recent_return_20d_pct=-1.0,
                    recent_return_60d_pct=2.0,
                ),
            )
            == ()
        )
        assert (
            _classify_stale_overvalued_or_no_earnings_flags(
                item.model_copy(update={"perPercentile": 0.5}),
                metrics,
            )
            == ()
        )

    def test_classifies_neutral_and_crowded_rerating_states(self):
        assert (
            classify_prime_liquidity_regime(0.5, 1.0, 2.0)
            == "neutral_rerating"
        )
        assert (
            classify_prime_liquidity_regime(1.2, 1.0, 2.0)
            == "crowded_rerating"
        )
        assert (
            classify_prime_liquidity_regime(1.2, 0.0, 2.0)
            == "distribution_stress"
        )

    def test_includes_variable_lookback_index_performance(self, service):
        result = service.get_rankings(date="2024-01-19", lookback_days=3)

        topix = next(
            (item for item in result.indexPerformance if item.code == "TOPIX"), None
        )
        assert topix is not None
        assert topix.currentDate == "2024-01-19"
        assert topix.baseDate == "2024-01-16"
        assert topix.currentClose == pytest.approx(1060.0)
        assert topix.baseClose == pytest.approx(1020.0)
        assert topix.changeAmount == pytest.approx(40.0)
        assert topix.changePercentage == pytest.approx(40.0 / 1020.0 * 100.0)
        assert topix.lookbackDays == 3

    def test_includes_sector_strength_for_sector33_index_performance(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute("DROP VIEW stock_master_daily")
            conn.execute("""
                CREATE TABLE stock_master_daily (
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    market_code TEXT NOT NULL,
                    sector_17_name TEXT,
                    sector_33_name TEXT NOT NULL
                )
            """)
            conn.execute(
                "INSERT INTO index_master VALUES (?,?,?,?,?)",
                ("004F", "東証業種別 電気機器", None, "sector33", "2024-01-01"),
            )
            conn.execute(
                "INSERT INTO index_master VALUES (?,?,?,?,?)",
                ("0050", "東証業種別 輸送用機器", None, "sector33", "2024-01-01"),
            )
            start = calendar_date(2024, 2, 1)
            dates = [(start + timedelta(days=offset)).isoformat() for offset in range(61)]
            for index, current_date in enumerate(dates):
                topix_close = 100.0 + index
                strong_close = 100.0 + index * 2.0
                weak_close = 100.0 + index * 0.5
                conn.execute(
                    "INSERT OR REPLACE INTO topix_data VALUES (?,?,?,?,?,?)",
                    (
                        current_date,
                        topix_close,
                        topix_close,
                        topix_close,
                        topix_close,
                        None,
                    ),
                )
                for code, close in (("67580", strong_close), ("72030", weak_close)):
                    conn.execute(
                        "INSERT OR REPLACE INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            code,
                            current_date,
                            close,
                            close,
                            close,
                            close,
                            1_000_000,
                            1.0,
                            None,
                        ),
                    )
                conn.execute(
                    "INSERT INTO stock_master_daily VALUES (?,?,?,?,?,?)",
                    (current_date, "67580", "ソニー", "prime", "電気", "電気機器"),
                )
                conn.execute(
                    "INSERT INTO stock_master_daily VALUES (?,?,?,?,?,?)",
                    (current_date, "72030", "トヨタ", "prime", "輸送", "輸送用機器"),
                )
                for code, close in (
                    ("004F", 1000.0 + index * 3.0),
                    ("0050", 900.0 - index),
                ):
                    conn.execute(
                        "INSERT INTO indices_data VALUES (?,?,?,?,?,?,?,?)",
                        (code, current_date, close, close, close, close, None, None),
                    )
            conn.commit()
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        try:
            result = RankingService(reader).get_rankings(
                date=dates[-1],
                lookback_days=3,
                markets="prime",
                include_sector_strength=True,
            )
        finally:
            reader.close()

        electric = next(
            (item for item in result.indexPerformance if item.code == "004F"), None
        )
        transport = next(
            (item for item in result.indexPerformance if item.code == "0050"), None
        )
        assert electric is not None
        assert transport is not None
        assert result.sectorScoreFamily == "current"
        assert electric.sectorStrengthScore == pytest.approx(1.0)
        assert electric.sectorStrengthBucket == "sector_strong"
        assert electric.sector20dTopixExcessPct is not None
        assert electric.sector20dTopixExcessPct > 0
        assert electric.sectorBreadth20dPct == pytest.approx(100.0)
        assert electric.sectorStockCount == 1
        assert transport.sectorStrengthScore == pytest.approx(0.0)
        assert transport.sectorStrengthBucket == "sector_weak"
        assert transport.sector20dTopixExcessPct is not None
        assert transport.sector20dTopixExcessPct < 0
        assert transport.sectorBreadth20dPct == pytest.approx(0.0)

        electric_stock = next(
            (item for item in result.rankings.tradingValue if item.code == "67580"),
            None,
        )
        assert electric_stock is not None
        assert electric_stock.sectorStrengthScore == pytest.approx(1.0)
        assert electric_stock.sectorStrengthBucket == "sector_strong"

    def test_includes_long_hybrid_sector_leadership_for_sector33_index_performance(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute("DROP VIEW stock_master_daily")
            conn.execute("""
                CREATE TABLE stock_master_daily (
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    market_code TEXT NOT NULL,
                    sector_17_name TEXT,
                    sector_33_name TEXT NOT NULL
                )
            """)
            conn.execute(
                "INSERT INTO index_master VALUES (?,?,?,?,?)",
                ("004F", "東証業種別 電気機器", None, "sector33", "2022-01-01"),
            )
            conn.execute(
                "INSERT INTO index_master VALUES (?,?,?,?,?)",
                ("0050", "東証業種別 輸送用機器", None, "sector33", "2022-01-01"),
            )
            start = calendar_date(2022, 1, 1)
            dates = [(start + timedelta(days=offset)).isoformat() for offset in range(505)]
            for index, current_date in enumerate(dates):
                topix_close = 100.0 + index
                strong_close = 100.0 + index * 2.0
                weak_close = 100.0 + index * 0.4
                conn.execute(
                    "INSERT OR REPLACE INTO topix_data VALUES (?,?,?,?,?,?)",
                    (
                        current_date,
                        topix_close,
                        topix_close,
                        topix_close,
                        topix_close,
                        None,
                    ),
                )
                for code, close in (("67580", strong_close), ("72030", weak_close)):
                    conn.execute(
                        "INSERT OR REPLACE INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            code,
                            current_date,
                            close,
                            close,
                            close,
                            close,
                            1_000_000,
                            1.0,
                            None,
                        ),
                    )
                conn.execute(
                    "INSERT INTO stock_master_daily VALUES (?,?,?,?,?,?)",
                    (current_date, "67580", "ソニー", "prime", "電気", "電気機器"),
                )
                conn.execute(
                    "INSERT INTO stock_master_daily VALUES (?,?,?,?,?,?)",
                    (current_date, "72030", "トヨタ", "prime", "輸送", "輸送用機器"),
                )
                for code, close in (
                    ("004F", 1000.0 + index * 3.0),
                    ("0050", 1000.0 - index * 0.5),
                ):
                    conn.execute(
                        "INSERT INTO indices_data VALUES (?,?,?,?,?,?,?,?)",
                        (code, current_date, close, close, close, close, None, None),
                    )
            conn.commit()
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        try:
            result = RankingService(reader).get_rankings(
                date=dates[-1],
                lookback_days=3,
                markets="prime",
                include_sector_strength=True,
                sector_score_family="long_hybrid_leadership",
            )
        finally:
            reader.close()

        electric = next(
            (item for item in result.indexPerformance if item.code == "004F"), None
        )
        transport = next(
            (item for item in result.indexPerformance if item.code == "0050"), None
        )
        assert result.sectorScoreFamily == "long_hybrid_leadership"
        assert electric is not None
        assert transport is not None
        assert electric.sectorStrengthScore == pytest.approx(1.0)
        assert electric.sectorStrengthBucket == "sector_strong"
        assert electric.sector20dTopixExcessPct is None
        assert electric.sectorStockCount == 1
        assert transport.sectorStrengthScore == pytest.approx(0.0)
        assert transport.sectorStrengthBucket == "sector_weak"

    def test_no_data_raises(self, tmp_path):
        """データなしDBの場合"""
        db_path = str(tmp_path / "empty.db")
        conn = duckdb.connect(db_path)
        conn.execute("""CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT, company_name_english TEXT,
            market_code TEXT, market_name TEXT, sector_17_code TEXT, sector_17_name TEXT,
            sector_33_code TEXT, sector_33_name TEXT, scale_category TEXT, listed_date TEXT,
            created_at TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE stock_data (
            code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, adjustment_factor REAL, created_at TEXT, PRIMARY KEY (code, date))""")
        conn.execute("""CREATE TABLE index_master (
            code TEXT PRIMARY KEY, name TEXT, name_english TEXT, category TEXT, data_start_date TEXT)""")
        conn.execute("""CREATE TABLE indices_data (
            code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
            sector_name TEXT, created_at TEXT, PRIMARY KEY (code, date))""")
        _create_stock_master_views(conn)
        conn.commit()
        conn.close()

        reader = MarketDbReader(db_path)
        service = RankingService(reader)
        with pytest.raises(ValueError, match="No trading data"):
            service.get_rankings()
        reader.close()


class TestGetFundamentalRankings:
    def test_default_shape(self, service):
        result = service.get_fundamental_rankings()
        assert result.date == "2024-01-19"
        assert result.markets == ["prime"]
        assert result.metricKey == "eps_forecast_to_actual"
        assert "ratioHigh" in result.rankings.model_dump()
        assert "ratioLow" in result.rankings.model_dump()

    def test_revised_forecast_is_prioritized(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        toyota = next(
            (item for item in result.rankings.ratioHigh if item.code == "72030"), None
        )
        assert toyota is not None
        assert toyota.source == "revised"
        # forecast 140.0 / actual 100.0 = 1.4
        assert toyota.epsValue == 1.4
        assert toyota.periodType == "1Q"
        assert toyota.disclosedDate == "2024-01-18"

    def test_fundamental_rankings_prefer_adjusted_daily_valuation_sot(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            _create_adjusted_metric_tables(conn)
            _insert_daily_valuation(
                conn,
                code="72030",
                eps=100.0,
                bps=1000.0,
                forward_eps=120.0,
                per=5.2,
                forward_per=4.3333,
                pbr=0.52,
                market_cap=52_000_000_000.0,
                source="fy",
                forward_date="2024-01-10",
            )
            _insert_daily_valuation(
                conn,
                code="67580",
                eps=10.0,
                bps=800.0,
                forward_eps=90.0,
                per=52.0,
                forward_per=5.7778,
                pbr=0.65,
                market_cap=26_000_000_000.0,
                source="revised",
                forward_date="2024-01-18",
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_fundamental_rankings(markets="prime", limit=20)
        reader.close()

        assert result.rankings.ratioHigh[0].code == "67580"
        assert result.rankings.ratioHigh[0].epsValue == pytest.approx(9.0)
        assert result.rankings.ratioHigh[0].source == "revised"
        assert result.rankings.ratioHigh[0].disclosedDate == "2024-01-18"

    def test_fy_forecast_fallback_when_revision_missing(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        sony = next(
            (item for item in result.rankings.ratioHigh if item.code == "67580"), None
        )
        assert sony is not None
        assert sony.source == "fy"
        assert sony.periodType == "FY"
        assert sony.disclosedDate == "2024-01-12"
        # forecast: 220 * (200 / 250) = 176.0
        # actual: 200 * (200 / 250) = 160.0
        # ratio: 176.0 / 160.0 = 1.1
        assert sony.epsValue == 1.1

    def test_revised_quarter_uses_next_year_forecast_fallback(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        alt = next(
            (item for item in result.rankings.ratioHigh if item.code == "46890"), None
        )
        assert alt is not None
        assert alt.source == "revised"
        assert alt.periodType == "2Q"
        assert alt.disclosedDate == "2024-01-18"
        # forecast 95.0 / actual 80.0 = 1.1875
        assert alt.epsValue == 1.1875

    def test_ratio_high_low_ordering(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        assert (
            result.rankings.ratioHigh[0].epsValue
            >= result.rankings.ratioHigh[1].epsValue
        )
        assert (
            result.rankings.ratioLow[0].epsValue <= result.rankings.ratioLow[1].epsValue
        )

    def test_market_filter_alias_prime_includes_numeric_codes(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=20)
        market_codes = {item.marketCode for item in result.rankings.ratioHigh}
        assert "prime" in market_codes
        assert "0111" in market_codes

    def test_fundamental_rankings_support_mixed_stock_and_stock_data_code_formats(
        self, service
    ):
        result = service.get_fundamental_rankings(markets="prime", limit=100)
        codes = {item.code for item in result.rankings.ratioHigh}
        assert "5555" in codes

    def test_fundamental_rankings_deduplicate_mixed_codes(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        conn.execute(
            "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
            ("5555", "2024-01-19", 600.0, 620.0, 590.0, 610.0, 500_000, 1.0, None),
        )
        conn.execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period,
                next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            ("55550", "2024-01-18", 400.0, "FY", 200.0, 200.0, 100.0),
        )
        conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_fundamental_rankings(markets="prime", limit=100)
        rows = [item for item in result.rankings.ratioHigh if item.code == "5555"]
        reader.close()

        assert len(rows) == 1
        assert rows[0].epsValue == pytest.approx(1.2)

    def test_skips_stocks_without_statements_and_invalid_ratio(self, service):
        result = service.get_fundamental_rankings(markets="prime", limit=100)
        codes = {item.code for item in result.rankings.ratioHigh}
        assert "11110" not in codes
        assert "22220" not in codes

    def test_filters_forecast_above_recent_fy_actuals(self, service):
        unfiltered = service.get_fundamental_rankings(markets="prime", limit=100)
        unfiltered_codes = {item.code for item in unfiltered.rankings.ratioHigh}
        assert "33330" in unfiltered_codes

        filtered = service.get_fundamental_rankings(
            markets="prime",
            limit=100,
            forecast_above_recent_fy_actuals=True,
            forecast_lookback_fy_count=2,
        )
        filtered_codes = {item.code for item in filtered.rankings.ratioHigh}
        assert "33330" not in filtered_codes

    def test_forecast_filter_with_lookback_1_keeps_recent_fy_breakout(self, service):
        filtered = service.get_fundamental_rankings(
            markets="prime",
            limit=100,
            forecast_above_recent_fy_actuals=True,
            forecast_lookback_fy_count=1,
        )
        filtered_codes = {item.code for item in filtered.rankings.ratioHigh}
        assert "33330" in filtered_codes

    def test_forecast_filter_counts_unique_fy_years_not_fy_disclosures(self, service):
        filtered = service.get_fundamental_rankings(
            markets="prime",
            limit=100,
            forecast_above_recent_fy_actuals=True,
            forecast_lookback_fy_count=2,
        )
        filtered_codes = {item.code for item in filtered.rankings.ratioHigh}
        # 33330 has a later future FY disclosure too, but lookback=2 should still count 2024+2023 as of the target date.
        assert "33330" not in filtered_codes

    def test_fundamental_rankings_ignore_future_disclosures(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        conn.execute(
            """
            INSERT INTO statements (
                code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
            )
            VALUES (?,?,?,?,?)
            """,
            ("72030", "2024-01-22", "2Q", 999.0, 100.0),
        )
        conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_fundamental_rankings(markets="prime", limit=20)
        reader.close()

        toyota = next(
            (item for item in result.rankings.ratioHigh if item.code == "72030"), None
        )
        assert toyota is not None
        assert toyota.disclosedDate == "2024-01-18"
        assert toyota.epsValue == pytest.approx(1.4)

    def test_filter_handles_stock_without_forecast_snapshot(self, service):
        filtered = service.get_fundamental_rankings(
            markets="prime",
            limit=100,
            forecast_above_recent_fy_actuals=True,
            forecast_lookback_fy_count=1,
        )
        filtered_codes = {item.code for item in filtered.rankings.ratioHigh}
        assert "44440" not in filtered_codes

    def test_unsupported_metric_key_raises(self, service):
        with pytest.raises(ValueError, match="Unsupported metricKey"):
            service.get_fundamental_rankings(metric_key="roe_forecast_to_actual")

    def test_no_data_raises_for_fundamental(self, tmp_path):
        db_path = str(tmp_path / "empty_fundamental.db")
        conn = duckdb.connect(db_path)
        conn.execute("""CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT, company_name_english TEXT,
            market_code TEXT, market_name TEXT, sector_17_code TEXT, sector_17_name TEXT,
            sector_33_code TEXT, sector_33_name TEXT, scale_category TEXT, listed_date TEXT,
            created_at TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE stock_data (
            code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, adjustment_factor REAL, created_at TEXT, PRIMARY KEY (code, date))""")
        _create_stock_master_views(conn)
        conn.commit()
        conn.close()

        reader = MarketDbReader(db_path)
        svc = RankingService(reader)
        with pytest.raises(ValueError, match="No trading data"):
            svc.get_fundamental_rankings()
        reader.close()


class TestGetValueCompositeRanking:
    def test_default_standard_value_score_uses_research_formula_without_adv_floor(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "77770",
                    "Large Standard",
                    "LSTD",
                    "standard",
                    "S",
                    "S17",
                    "情報",
                    "S33",
                    "情報通信",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "88880",
                    "Mid Standard",
                    "MSTD",
                    "standard",
                    "S",
                    "S17",
                    "サービス",
                    "S33",
                    "サービス業",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            for code, price, volume in [
                ("77770", 1000.0, 10_000_000),
                ("88880", 800.0, 20_000_000),
            ]:
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (code, "2024-01-19", price, price, price, price, volume, 1.0, None),
                )
            for code, eps, forecast, bps, shares in [
                ("99840", 50.0, 104.0, 1000.0, 10_000_000.0),
                ("77770", 80.0, 50.0, 800.0, 50_000_000.0),
                ("88880", 70.0, 80.0, 1000.0, 20_000_000.0),
            ]:
                conn.execute(
                    """
                    INSERT INTO statements (
                        code, disclosed_date, earnings_per_share, type_of_current_period,
                        next_year_forecast_earnings_per_share, bps, shares_outstanding
                    )
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (code, "2024-01-10", eps, "FY", forecast, bps, shares),
                )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(limit=10)
        reader.close()

        assert result.date == "2024-01-19"
        assert result.markets == ["standard"]
        assert result.metricKey == "standard_value_composite"
        assert result.scoreMethod == "standard_pbr_tilt"
        assert "requires PBR > 0 and forward PER > 0" in result.scorePolicy
        assert result.weights == {
            "smallMarketCap": 0.35,
            "lowPbr": 0.4,
            "lowForwardPer": 0.25,
        }
        assert [item.code for item in result.items] == ["99840", "88880", "77770"]
        assert result.items[0].score > result.items[-1].score
        assert result.items[0].pbr == pytest.approx(0.52)
        assert result.items[0].forwardPer == pytest.approx(5.0)
        assert result.items[0].marketCapBilJpy == pytest.approx(5.2)

    def test_value_composite_ranking_prefers_adjusted_daily_valuation_sot(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            _create_adjusted_metric_tables(conn)
            for code, name, price, volume in [
                ("77770", "Large Standard", 1000.0, 10_000_000),
                ("88880", "Mid Standard", 800.0, 20_000_000),
            ]:
                conn.execute(
                    "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        code,
                        name,
                        name,
                        "standard",
                        "S",
                        "S17",
                        "情報",
                        "S33",
                        "情報通信",
                        None,
                        "2000-01-01",
                        None,
                        None,
                    ),
                )
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (code, "2024-01-19", price, price, price, price, volume, 1.0, None),
                )
            _insert_daily_valuation(
                conn,
                code="99840",
                eps=50.0,
                bps=2000.0,
                forward_eps=260.0,
                per=10.4,
                forward_per=2.0,
                pbr=0.26,
                market_cap=1_000_000_000.0,
                source="revised",
                forward_date="2024-01-18",
            )
            _insert_daily_valuation(
                conn,
                code="77770",
                eps=80.0,
                bps=800.0,
                forward_eps=100.0,
                per=6.5,
                forward_per=5.2,
                pbr=0.65,
                market_cap=52_000_000_000.0,
            )
            _insert_daily_valuation(
                conn,
                code="88880",
                eps=70.0,
                bps=1000.0,
                forward_eps=130.0,
                per=7.4286,
                forward_per=4.0,
                pbr=0.52,
                market_cap=10_400_000_000.0,
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(limit=10)
        reader.close()

        item = next(row for row in result.items if row.code == "99840")
        assert item.pbr == pytest.approx(0.26)
        assert item.forwardPer == pytest.approx(2.0)
        assert item.forwardEps == pytest.approx(260.0)
        assert item.forwardEpsSource == "revised"
        assert item.marketCapBilJpy == pytest.approx(1.0)

    def test_value_composite_ranking_can_use_latest_revised_forward_eps(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("99840", "2024-01-10", 50.0, "FY", 104.0, 1000.0, 10_000_000.0),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
                )
                VALUES (?,?,?,?,?)
                """,
                ("99840", "2024-01-18", "1Q", 130.0, 10_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        latest = svc.get_value_composite_ranking(limit=10)
        fy_only = svc.get_value_composite_ranking(limit=10, forward_eps_mode="fy")
        reader.close()

        latest_item = next((row for row in latest.items if row.code == "99840"), None)
        fy_item = next((row for row in fy_only.items if row.code == "99840"), None)
        assert latest.forwardEpsMode == "latest"
        assert fy_only.forwardEpsMode == "fy"
        assert latest_item is not None
        assert fy_item is not None
        assert latest_item.forwardEpsSource == "revised"
        assert latest_item.forwardEps == pytest.approx(130.0)
        assert latest_item.forwardPer == pytest.approx(4.0)
        assert fy_item.forwardEpsSource == "fy"
        assert fy_item.forwardEps == pytest.approx(104.0)
        assert fy_item.forwardPer == pytest.approx(5.0)

    def test_value_composite_ranking_ignores_future_disclosures(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("99840", "2024-01-10", 50.0, "FY", 104.0, 1000.0, 10_000_000.0),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("99840", "2024-12-31", 50.0, "FY", 999.0, 10.0, 10_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(limit=10)
        reader.close()

        item = next((row for row in result.items if row.code == "99840"), None)
        assert item is not None
        assert item.latestFyDisclosedDate == "2024-01-10"
        assert item.pbr == pytest.approx(0.52)
        assert item.forwardPer == pytest.approx(5.0)

    def test_value_composite_ranking_exposes_raw_technical_metrics(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            start = calendar_date(2023, 4, 30)
            for i in range(260):
                current_date = start + timedelta(days=i)
                if current_date >= calendar_date(2024, 1, 15):
                    break
                close = 430.0 + i * 0.35 + (12.0 if i % 11 == 0 else 0.0)
                conn.execute(
                    "INSERT OR REPLACE INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        "99840",
                        current_date.isoformat(),
                        close,
                        close + 2.0,
                        close - 2.0,
                        close,
                        120_000 + i,
                        1.0,
                        None,
                    ),
                )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("99840", "2024-01-10", 50.0, "FY", 104.0, 1000.0, 10_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(markets="standard", limit=10)
        reader.close()

        item = next((row for row in result.items if row.code == "99840"), None)
        assert item is not None
        metrics = item.technicalMetrics
        assert metrics is not None
        assert metrics.featureDate == "2024-01-19"
        assert metrics.reboundFrom252dLowPct is not None
        assert metrics.reboundFrom252dLowPct > 0
        assert metrics.return252dPct is not None
        assert metrics.return252dPct > 0
        assert metrics.volatility20dPct is not None
        assert metrics.volatility60dPct is not None
        assert metrics.downsideVolatility60dPct is not None

    def test_value_composite_ranking_score_methods_expose_expected_profiles(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            for row in [
                (
                    "66660",
                    "Tiny Expensive Standard",
                    "TINY",
                    "standard",
                    "S",
                    "S17",
                    "情報",
                    "S33",
                    "情報通信",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "77770",
                    "Large Cheap Standard",
                    "CHEAP",
                    "standard",
                    "S",
                    "S17",
                    "サービス",
                    "S33",
                    "サービス業",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            ]:
                conn.execute(
                    "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", row
                )
            for code, price, volume in [
                ("66660", 100.0, 10_000_000),
                ("77770", 1000.0, 20_000_000),
            ]:
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (code, "2024-01-19", price, price, price, price, volume, 1.0, None),
                )
            for code, forecast, bps, shares in [
                ("99840", 104.0, 1000.0, 10_000_000.0),
                ("66660", 2.0, 50.0, 1_000_000.0),
                ("77770", 500.0, 5000.0, 100_000_000.0),
            ]:
                conn.execute(
                    """
                    INSERT INTO statements (
                        code, disclosed_date, earnings_per_share, type_of_current_period,
                        next_year_forecast_earnings_per_share, bps, shares_outstanding
                    )
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (code, "2024-01-10", 50.0, "FY", forecast, bps, shares),
                )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        pbr_tilt = svc.get_value_composite_ranking(limit=10)
        prime_size_tilt = svc.get_value_composite_ranking(
            limit=10, score_method="prime_size_tilt"
        )
        equal = svc.get_value_composite_ranking(limit=10, score_method="equal_weight")
        reader.close()

        assert pbr_tilt.scoreMethod == "standard_pbr_tilt"
        assert prime_size_tilt.scoreMethod == "prime_size_tilt"
        assert equal.scoreMethod == "equal_weight"
        assert prime_size_tilt.weights == {
            "smallMarketCap": 0.465,
            "lowPbr": 0.05,
            "lowForwardPer": 0.485,
        }
        assert equal.weights == {
            "smallMarketCap": pytest.approx(1 / 3),
            "lowPbr": pytest.approx(1 / 3),
            "lowForwardPer": pytest.approx(1 / 3),
        }
        assert [item.code for item in prime_size_tilt.items[:3]] == [
            "77770",
            "99840",
            "66660",
        ]
        assert [item.code for item in equal.items[:3]] == ["77770", "99840", "66660"]

    def test_value_composite_ranking_standard_pbr_tilt_weights(self, ranking_db):
        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(
            limit=10, score_method="standard_pbr_tilt"
        )
        reader.close()

        assert result.scoreMethod == "standard_pbr_tilt"
        assert (
            "35% small market cap + 40% low PBR + 25% low forward PER"
            in result.scorePolicy
        )
        assert result.weights == {
            "smallMarketCap": 0.35,
            "lowPbr": 0.4,
            "lowForwardPer": 0.25,
        }

    def test_value_composite_ranking_prime_size_tilt_weights(self, ranking_db):
        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(
            limit=10, score_method="prime_size_tilt"
        )
        reader.close()

        assert result.scoreMethod == "prime_size_tilt"
        assert (
            "46.5% small market cap + 5% low PBR + 48.5% low forward PER"
            in result.scorePolicy
        )
        assert result.weights == {
            "smallMarketCap": 0.465,
            "lowPbr": 0.05,
            "lowForwardPer": 0.485,
        }

    def test_value_composite_ranking_standard_breakout_profile_adds_prior_session_boost(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("99840", "2024-01-10", 50.0, "FY", 104.0, 1000.0, 10_000_000.0),
            )
            start = calendar_date(2023, 9, 1)
            for i in range(140):
                current_date = start + timedelta(days=i)
                close = 400.0 + i
                high = close + 1.0
                if current_date.isoformat() == "2024-01-18":
                    high = 600.0
                    close = 590.0
                conn.execute(
                    "INSERT OR REPLACE INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        "99840",
                        current_date.isoformat(),
                        close,
                        high,
                        close - 1.0,
                        close,
                        200_000,
                        1.0,
                        None,
                    ),
                )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(
            markets="standard",
            limit=10,
            profile_id="standard_breakout_120d20",
        )
        reader.close()

        item = next((row for row in result.items if row.code == "99840"), None)
        assert result.profileId == "standard_breakout_120d20"
        assert result.profileLabel == "Standard value + 120d breakout boost"
        assert result.scoreMethod == "prime_size_tilt"
        assert result.rebalanceMonths == 3
        assert result.breakoutWindow == 120
        assert result.breakoutLookbackSessions == 20
        assert result.breakoutScoreBoost == pytest.approx(0.1)
        assert item is not None
        assert item.scoreBeforeBoost is not None
        assert item.breakoutBoost == pytest.approx(0.1)
        assert item.score == pytest.approx(item.scoreBeforeBoost + 0.1)
        assert item.avgTradingValue60dMilJpy is not None
        assert item.liquidityEligible is True
        assert item.technicalMetrics is not None
        assert item.technicalMetrics.breakoutFeatureDate == "2024-01-18"
        assert item.technicalMetrics.newHigh120d is True
        assert item.technicalMetrics.daysSinceNewHigh120d == 0

    def test_value_composite_ranking_standard_profile_hard_filters_below_adv60_floor(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            for code, company, volume in [
                ("77770", "Liquid Standard", 200_000),
                ("88880", "Thin Standard", 1_000),
            ]:
                conn.execute(
                    "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        code,
                        company,
                        company,
                        "standard",
                        "S",
                        "S17",
                        "情報",
                        "S33",
                        "情報通信",
                        None,
                        "2000-01-01",
                        None,
                        None,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO statements (
                        code, disclosed_date, earnings_per_share, type_of_current_period,
                        next_year_forecast_earnings_per_share, bps, shares_outstanding
                    )
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (code, "2024-01-10", 50.0, "FY", 100.0, 1000.0, 10_000_000.0),
                )
                start = calendar_date(2023, 11, 15)
                for i in range(66):
                    current_date = start + timedelta(days=i)
                    close = 100.0 + i
                    conn.execute(
                        "INSERT OR REPLACE INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            code,
                            current_date.isoformat(),
                            close,
                            close + 1.0,
                            close - 1.0,
                            close,
                            volume,
                            1.0,
                            None,
                        ),
                    )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(
            markets="standard",
            limit=10,
            profile_id="standard_breakout_120d20",
        )
        reader.close()

        codes = {item.code for item in result.items}
        assert result.applyLiquidityFilter is True
        assert "77770" in codes
        assert "88880" not in codes
        assert "hard ADV60 >= 10mn JPY liquidity floor" in result.scorePolicy

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        without_filter = svc.get_value_composite_ranking(
            markets="standard",
            limit=10,
            profile_id="standard_breakout_120d20",
            apply_liquidity_filter=False,
        )
        reader.close()

        codes_without_filter = {item.code for item in without_filter.items}
        assert without_filter.applyLiquidityFilter is False
        assert "88880" in codes_without_filter
        thin_item = next(item for item in without_filter.items if item.code == "88880")
        assert thin_item.liquidityEligible is False
        assert "diagnostic ADV60 >= 10mn JPY liquidity floor" in without_filter.scorePolicy

    def test_value_composite_ranking_prime_profile_uses_size75_forward_per25_weights(
        self, ranking_db
    ):
        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_ranking(
            markets="prime",
            limit=10,
            profile_id="prime_size75_forward_per25",
        )
        reader.close()

        assert result.profileId == "prime_size75_forward_per25"
        assert result.profileLabel == "Prime size75 / forward PER25"
        assert result.scoreMethod == "prime_size75_forward_per25"
        assert result.rebalanceMonths == 2
        assert result.breakoutWindow is None
        assert result.weights == {
            "smallMarketCap": 0.75,
            "lowPbr": 0.0,
            "lowForwardPer": 0.25,
        }

    def test_value_composite_score_returns_market_specific_rank_for_symbol(
        self, ranking_db
    ):
        conn = duckdb.connect(ranking_db)
        try:
            for code, forecast, bps, shares in [
                ("72030", 140.0, 1000.0, 100.0),
                ("67580", 220.0, 2000.0, 200.0),
            ]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO statements (
                        code, disclosed_date, earnings_per_share, type_of_current_period,
                        next_year_forecast_earnings_per_share, bps, shares_outstanding
                    )
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (code, "2024-01-10", 100.0, "FY", forecast, bps, shares),
                )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_score("7203")
        ranking = svc.get_value_composite_ranking(
            markets="prime",
            score_method="prime_size_tilt",
            limit=10,
        )
        reader.close()

        ranking_item = next(item for item in ranking.items if item.code == "72030")
        assert result.scoreAvailable is True
        assert result.unsupportedReason is None
        assert result.scoreMethod == "prime_size_tilt"
        assert result.market == "prime"
        assert result.universeCount == ranking.itemCount
        assert result.item is not None
        assert result.item.code == "72030"
        assert result.item.rank == ranking_item.rank
        assert result.item.score == pytest.approx(ranking_item.score)

    def test_value_composite_score_hides_unsupported_growth_symbol(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "39990",
                    "Growth Test",
                    "GROWTH",
                    "growth",
                    "G",
                    "S17",
                    "情報",
                    "S33",
                    "情報通信",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                ("39990", "2024-01-19", 100.0, 100.0, 100.0, 100.0, 100_000, 1.0, None),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("39990", "2024-01-10", 10.0, "FY", 12.0, 100.0, 1_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_score("3999")
        reader.close()

        assert result.scoreAvailable is False
        assert result.unsupportedReason == "unsupported_market"
        assert result.market == "growth"
        assert result.scoreMethod is None
        assert result.item is None

    def test_value_composite_score_reports_missing_forward_eps(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "285A",
                    "Kioxia Test",
                    "KIOXIA",
                    "prime",
                    "P",
                    "S17",
                    "電気",
                    "S33",
                    "電気機器",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                ("285A", "2024-01-19", 100.0, 100.0, 100.0, 100.0, 100_000, 1.0, None),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, forecast_eps, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                ("285A", "2024-01-10", 10.0, "FY", None, None, 100.0, 1_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_score("285A")
        reader.close()

        assert result.scoreAvailable is False
        assert result.unsupportedReason == "forward_eps_missing"
        assert result.item is None

    def test_value_composite_score_uses_latest_fy_row_with_bps(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            for row in [
                (
                    "68090",
                    "Latest BPS Fallback",
                    "BPSFB",
                    "prime",
                    "P",
                    "S17",
                    "電気",
                    "S33",
                    "電気機器",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "68100",
                    "Comparable Prime",
                    "COMP",
                    "prime",
                    "P",
                    "S17",
                    "電気",
                    "S33",
                    "電気機器",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            ]:
                conn.execute(
                    "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", row
                )
            for code, price in [("68090", 120.0), ("68100", 100.0)]:
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        code,
                        "2024-01-19",
                        price,
                        price,
                        price,
                        price,
                        100_000,
                        1.0,
                        None,
                    ),
                )
            for row in [
                (
                    "68090",
                    "2024-01-15",
                    "3Q",
                    "3QFinancialStatements_Consolidated_JP",
                    None,
                    None,
                    None,
                    1_000_000.0,
                ),
                (
                    "68090",
                    "2024-01-10",
                    "FY",
                    "EarnForecastRevision",
                    10.0,
                    None,
                    None,
                    None,
                ),
                (
                    "68090",
                    "2023-01-10",
                    "FY",
                    "FYFinancialStatements_Consolidated_JP",
                    9.0,
                    11.0,
                    100.0,
                    None,
                ),
                (
                    "68100",
                    "2024-01-10",
                    "FY",
                    "FYFinancialStatements_Consolidated_JP",
                    10.0,
                    12.0,
                    100.0,
                    1_000_000.0,
                ),
            ]:
                conn.execute(
                    """
                    INSERT INTO statements (
                        code, disclosed_date, type_of_current_period, type_of_document, earnings_per_share,
                        next_year_forecast_earnings_per_share, bps, shares_outstanding
                    )
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    row,
                )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_score("6809")
        reader.close()

        assert result.scoreAvailable is True
        assert result.unsupportedReason is None
        assert result.item is not None
        assert result.item.code == "68090"
        assert result.item.pbr == pytest.approx(1.2)
        assert result.item.latestFyDisclosedDate == "2023-01-10"

    def test_value_composite_score_falls_back_to_symbol_latest_price_date(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "39470",
                    "Thin Standard",
                    "THIN",
                    "standard",
                    "S",
                    "S17",
                    "素材",
                    "S33",
                    "パルプ・紙",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                ("39470", "2024-01-18", 2370.0, 2370.0, 2370.0, 2370.0, 600, 1.0, None),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    forecast_eps, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("39470", "2024-01-10", 320.0, "FY", 256.0, 4800.0, 10_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_score("3947")
        reader.close()

        assert result.date == "2024-01-18"
        assert result.scoreAvailable is True
        assert result.unsupportedReason is None
        assert result.market == "standard"
        assert result.item is not None
        assert result.item.code == "39470"
        assert result.item.currentPrice == pytest.approx(2370.0)

    def test_value_composite_score_reports_missing_bps(self, ranking_db):
        conn = duckdb.connect(ranking_db)
        try:
            conn.execute(
                "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "68200",
                    "No BPS Prime",
                    "NOBPS",
                    "prime",
                    "P",
                    "S17",
                    "電気",
                    "S33",
                    "電気機器",
                    None,
                    "2000-01-01",
                    None,
                    None,
                ),
            )
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                ("68200", "2024-01-19", 100.0, 100.0, 100.0, 100.0, 100_000, 1.0, None),
            )
            conn.execute(
                """
                INSERT INTO statements (
                    code, disclosed_date, earnings_per_share, type_of_current_period,
                    next_year_forecast_earnings_per_share, bps, shares_outstanding
                )
                VALUES (?,?,?,?,?,?,?)
                """,
                ("68200", "2024-01-10", 10.0, "FY", 12.0, None, 1_000_000.0),
            )
        finally:
            conn.close()

        reader = MarketDbReader(ranking_db)
        svc = RankingService(reader)
        result = svc.get_value_composite_score("6820")
        reader.close()

        assert result.scoreAvailable is False
        assert result.unsupportedReason == "bps_missing"
        assert result.item is None


class _BadFloat:
    def __float__(self) -> float:
        raise TypeError("cannot cast")


class TestRankingHelperBranches:
    def test_module_level_helpers_edge_cases(self):
        assert build_market_filter([]) == ("", [])
        assert _normalize_period_label(None) == ""
        assert _is_valid_share_count(None) is False
        assert _is_valid_share_count(0.0) is False
        assert _adjust_per_share_value(10.128, None, 100.0) == 10.13
        assert _to_nullable_float(_BadFloat()) is None

    def test_value_composite_config_exports_profiles(self):
        profile = VALUE_COMPOSITE_PROFILE_BY_ID["prime_size75_forward_per25"]
        assert profile.score_method == "prime_size75_forward_per25"
        assert profile.rebalance_months == 2
        resolved_profile, score_method = resolve_value_composite_profile_and_score_method(
            profile_id="standard_breakout_120d20",
            score_method="equal_weight",
        )
        assert resolved_profile is VALUE_COMPOSITE_PROFILE_BY_ID["standard_breakout_120d20"]
        assert score_method == "prime_size_tilt"

        with pytest.raises(ValueError, match="Unsupported scoreMethod"):
            resolve_value_composite_profile_and_score_method(
                profile_id=None,
                score_method="unknown",  # type: ignore[arg-type]
            )
        ensure_supported_value_composite_forward_eps_mode("latest")
        with pytest.raises(ValueError, match="Unsupported forwardEpsMode"):
            ensure_supported_value_composite_forward_eps_mode("unknown")  # type: ignore[arg-type]

    def test_value_composite_config_formats_score_response_parts(self):
        weights = normalize_value_composite_weights(
            {
                "small_market_cap_score": 2.0,
                "low_pbr_score": 1.0,
                "low_forward_per_score": 1.0,
            }
        )

        assert value_composite_response_weights(weights) == {
            "smallMarketCap": 0.5,
            "lowPbr": 0.25,
            "lowForwardPer": 0.25,
        }
        assert (
            "forward EPS basis: latest revised forecast EPS"
            in value_composite_score_policy("standard_pbr_tilt", "latest")
        )
        ranking_policy = value_composite_ranking_score_policy(
            "prime_size_tilt",
            "latest",
            profile=VALUE_COMPOSITE_PROFILE_BY_ID["standard_breakout_120d20"],
            apply_liquidity_filter=False,
        )
        assert ranking_policy.startswith("Standard value + 120d breakout boost: ")
        assert "breakout additive boost: 120d high within 20 sessions" in ranking_policy
        assert "diagnostic ADV60 >= 10mn JPY liquidity floor" in ranking_policy

    def test_prime_valuation_percentiles_only_rank_prime_positive_values(self):
        frame = pd.DataFrame(
            [
                {"code": "1000", "market_code": "prime", "per": 10.0},
                {"code": "1001", "market_code": "prime", "per": 20.0},
                {"code": "2000", "market_code": "standard", "per": 1.0},
                {"code": "1002", "market_code": "prime", "per": -5.0},
            ]
        )

        result = with_prime_valuation_percentiles(frame)

        assert result.loc[0, "per_percentile"] == 0.0
        assert result.loc[1, "per_percentile"] == 1.0
        assert result.loc[2, "per_percentile"] is None
        assert result.loc[3, "per_percentile"] is None

    def test_build_value_composite_item_normalizes_optional_fields(self):
        item = build_value_composite_item(
            {
                "code": "7203",
                "company_name": "Toyota",
                "market_code": "prime",
                "sector_33_name": "輸送用機器",
                "current_price": 1000.0,
                "volume": 12345.0,
                "value_composite_score": 0.8,
                "score_before_boost": 0.7,
                "breakout_boost": 0.1,
                "liquidity_eligible": 1,
                "avg_trading_value_60d_mil_jpy": 250.0,
                "low_pbr_score": 0.2,
                "small_market_cap_score": 0.3,
                "low_forward_per_score": 0.4,
                "pbr": 1.2,
                "forward_per": 10.0,
                "market_cap_bil_jpy": 500.0,
                "bps": float("nan"),
                "forward_eps": 100.0,
                "latest_fy_disclosed_date": "2024-01-10",
                "forward_eps_disclosed_date": "2024-01-11",
                "forward_eps_source": "unexpected",
                "technical_feature_date": "2024-01-12",
                "breakout_feature_date": "2024-01-13",
                "rebound_from_252d_low_pct": 12.5,
                "return_252d_pct": 20.0,
                "volatility_20d_pct": 3.0,
                "volatility_60d_pct": 5.0,
                "downside_volatility_60d_pct": 4.0,
                "avg_trading_value_60d_source_sessions": 60.0,
                "new_high_20d": 0,
                "days_since_new_high_20d": 2.0,
                "close_to_prior_high_20d_pct": -1.0,
                "new_high_120d": 1,
                "days_since_new_high_120d": 0.0,
                "close_to_prior_high_120d_pct": 0.0,
            },
            rank=3,
        )

        assert item.rank == 3
        assert item.bps is None
        assert item.forwardEpsSource is None
        assert item.liquidityEligible is True
        assert item.technicalMetrics.newHigh20d is False
        assert item.technicalMetrics.newHigh120d is True

    def test_build_ranking_item_keeps_none_extra_as_default(self):
        item = build_ranking_item(
            {
                "code": "7203",
                "company_name": "Toyota",
                "market_code": "prime",
                "sector_33_name": "輸送用機器",
                "current_price": 1000.0,
                "volume": 12345.0,
            },
            rank=2,
            tradingValue=12_345_000.0,
            previousPrice=None,
        )

        assert item.rank == 2
        assert item.tradingValue == 12_345_000.0
        assert item.previousPrice is None
        assert "previousPrice" not in item.model_fields_set

    def test_build_fundamental_ranking_item_maps_domain_item(self):
        item = build_fundamental_ranking_item(
            FundamentalItem(
                code="7203",
                company_name="Toyota",
                market_code="prime",
                sector_33_name="輸送用機器",
                current_price=1000.0,
                volume=12345.0,
                eps_value=1.25,
                disclosed_date="2024-01-18",
                period_type="1Q",
                source="revised",
            ),
            rank=4,
        )

        assert item.rank == 4
        assert item.code == "7203"
        assert item.epsValue == 1.25
        assert item.disclosedDate == "2024-01-18"
        assert item.source == "revised"

    def test_build_value_composite_score_response_maps_stock_identity(self):
        response = build_value_composite_score_response(
            date="2024-01-19",
            code="72030",
            target_stock={
                "code": "72030",
                "company_name": "Toyota",
                "market_code": "prime",
            },
            market="prime",
            score_method="prime_size_tilt",
            forward_eps_mode="latest",
            score_available=False,
            unsupported_reason="forward_eps_missing",
            last_updated="2024-01-19T00:00:00+00:00",
        )

        assert response.code == "72030"
        assert response.companyName == "Toyota"
        assert response.marketCode == "prime"
        assert response.scoreAvailable is False
        assert response.unsupportedReason == "forward_eps_missing"

    def test_statement_selection_respects_as_of_and_positive_adjusted_bps(self):
        rows = [
            {
                "disclosed_date": "2024-01-01",
                "type_of_current_period": "FY",
                "type_of_document": "FinancialStatement",
                "bps": 120.0,
                "shares_outstanding": 100.0,
            },
            {
                "disclosed_date": "2024-02-01",
                "type_of_current_period": "FY",
                "type_of_document": "FinancialStatement",
                "bps": -1.0,
                "shares_outstanding": 100.0,
            },
            {
                "disclosed_date": "2024-03-01",
                "type_of_current_period": "FY",
                "type_of_document": "FinancialStatement",
                "bps": 999.0,
                "shares_outstanding": 100.0,
            },
        ]

        selected = latest_value_bps_statement(rows, baseline_shares=100.0, as_of_date="2024-02-15")

        assert selected is rows[0]
        assert latest_actual_fy_disclosed_date(rows, as_of_date="2024-02-15") == "2024-02-01"
        assert _calculate_eps_ratio(float("nan"), 1.0) is None
        assert _calculate_eps_ratio(1.0, 0.0) is None
        assert _calculate_eps_ratio(1e308, 2e-12) is None

    def test_statement_row_adapter_normalizes_period_and_groups_by_code(self):
        rows = [
            {
                "code": "72030",
                "disclosed_date": "2024-01-18",
                "type_of_current_period": "1Q",
                "type_of_document": "EarnForecastRevision",
                "earnings_per_share": None,
                "forecast_eps": "140.5",
                "next_year_forecast_earnings_per_share": None,
                "shares_outstanding": "100",
            },
            {
                "code": "67580",
                "disclosed_date": "2024-01-12",
                "type_of_current_period": "FY",
                "earnings_per_share": "200",
                "forecast_eps": None,
                "next_year_forecast_earnings_per_share": "220",
                "shares_outstanding": None,
            },
        ]

        converted = statement_row_from_mapping(rows[0])
        grouped = statement_rows_by_code(rows)

        assert converted == _StatementRow(
            code="72030",
            disclosed_date="2024-01-18",
            period_type="1Q",
            earnings_per_share=None,
            forecast_eps=140.5,
            next_year_forecast_earnings_per_share=None,
            shares_outstanding=100.0,
            fy_cycle_key="2024",
            type_of_document="EarnForecastRevision",
        )
        assert list(grouped) == ["72030", "67580"]
        assert grouped["67580"][0].period_type == "FY"
        assert grouped["67580"][0].type_of_document is None

    def test_fundamental_calculator_none_paths(self):
        calculator = FundamentalRankingCalculator()
        baseline_fallback = calculator.resolve_baseline_shares(
            [
                _StatementRow("X", "2024-05-01", "FY", 100.0, None, 120.0, 200.0),
            ]
        )
        assert baseline_fallback == 200.0

        baseline_none = calculator.resolve_baseline_shares(
            [
                _StatementRow("X", "2024-05-01", "FY", 100.0, None, 120.0, None),
            ]
        )
        assert baseline_none is None

        assert (
            calculator.resolve_latest_actual_snapshot(
                [_StatementRow("X", "2024-05-01", "FY", None, None, None, 100.0)],
                baseline_shares=100.0,
            )
            is None
        )

        assert (
            calculator.resolve_latest_fy_row(
                [_StatementRow("X", "2024-08-01", "1Q", None, 120.0, None, 100.0)]
            )
            is None
        )

        assert (
            calculator.resolve_latest_fy_forecast_snapshot(None, baseline_shares=100.0)
            is None
        )
        assert (
            calculator.resolve_latest_fy_forecast_snapshot(
                _LatestFyRow(
                    disclosed_date="2024-05-01",
                    period_type="FY",
                    shares_outstanding=100.0,
                    forecast_value=None,
                ),
                baseline_shares=100.0,
            )
            is None
        )

        revised_none = calculator.resolve_latest_revised_forecast_snapshot(
            [
                _StatementRow("X", "2024-05-01", "FY", 100.0, 120.0, 120.0, 100.0),
                _StatementRow("X", "2024-04-01", "1Q", None, 130.0, None, 100.0),
            ],
            baseline_shares=100.0,
            fy_disclosed_date="2024-05-01",
        )
        assert revised_none is None

        assert (
            calculator.resolve_latest_forecast_snapshot(
                [_StatementRow("X", "2024-08-01", "1Q", None, 120.0, None, 100.0)],
                baseline_shares=100.0,
            )
            is None
        )
        assert (
            calculator.resolve_recent_actual_eps_max(
                [_StatementRow("X", "2024-05-01", "FY", None, None, None, 100.0)],
                baseline_shares=100.0,
                lookback_fy_count=1,
            )
            is None
        )
        assert (
            calculator.resolve_recent_actual_eps_max(
                [
                    _StatementRow("X", "2023-05-01", "FY", 90.0, None, None, 100.0),
                    _StatementRow("X", "2024-05-01", "FY", 110.0, None, None, 100.0),
                ],
                baseline_shares=100.0,
                lookback_fy_count=2,
            )
            == 110.0
        )
        assert calculator.resolve_latest_forecast_snapshot(
            [
                _StatementRow("X", "2024-01-10", "FY", 100.0, 120.0, 120.0, 100.0),
                _StatementRow("X", "2024-01-18", "1Q", None, 140.0, None, 100.0),
                _StatementRow("X", "2024-01-22", "2Q", None, 999.0, None, 100.0),
            ],
            baseline_shares=100.0,
            as_of_date="2024-01-19",
        ) == _ForecastValue(
            value=140.0,
            disclosed_date="2024-01-18",
            period_type="1Q",
            source="revised",
        )

        forecast = _ForecastValue(
            value=1.0,
            disclosed_date="2024-08-01",
            period_type="1Q",
            source="revised",
        )
        assert calculator.resolve_latest_ratio_snapshot(None, forecast) is None
        assert (
            calculator.resolve_latest_ratio_snapshot(
                _ForecastValue(
                    value=0.0,
                    disclosed_date="2024-05-01",
                    period_type="FY",
                    source="fy",
                ),
                forecast,
            )
            is None
        )

    def test_adjust_share_count_to_price_basis_uses_split_events(self):
        adjusted = adjust_share_count_to_price_basis(
            2_260_000.0,
            [
                ShareAdjustmentEvent(
                    date="2026-03-30",
                    adjustment_factor=0.2,
                )
            ],
            from_date="2026-02-09",
            through_date="2026-05-01",
        )

        assert adjusted == pytest.approx(11_300_000.0)


class TestRankingDateEdgeCases:
    def test_returns_empty_when_reference_dates_are_unavailable(self, ranking_db):
        reader = MarketDbReader(ranking_db)
        try:
            assert ranking_by_trading_value_average(reader, "2024-01-15", 3, 20, []) == []
            assert ranking_by_price_change(reader, "2024-01-15", 20, [], "DESC") == []
            assert (
                ranking_by_price_change_from_days(reader, "2024-01-15", 3, 20, [], "DESC")
                == []
            )
        finally:
            reader.close()

    def test_period_high_low_paths_with_available_window(self, ranking_db):
        reader = MarketDbReader(ranking_db)
        try:
            high = ranking_by_period_high(reader, "2024-01-19", 2, 20, [])
            low = ranking_by_period_low(reader, "2024-01-19", 2, 20, [])
            assert isinstance(high, list)
            assert isinstance(low, list)
        finally:
            reader.close()
