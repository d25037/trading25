"""
Complex Analytics Routes Unit Tests

Ranking, Factor Regression, Screening のルートテスト。
"""

import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app


def _generate_dates(n: int, start: str = "2023-01-02") -> list[str]:
    """テスト用の営業日リストを生成"""
    from datetime import date, timedelta

    d = date.fromisoformat(start)
    dates: list[str] = []
    while len(dates) < n:
        if d.weekday() < 5:  # 平日のみ
            dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


@pytest.fixture
def analytics_db_path(tmp_path):
    """factor-regression/screening 用に十分なデータを持つ market.db"""
    db_path = str(tmp_path / "market.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # テーブル作成
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
            date TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL,
            low REAL NOT NULL, close REAL NOT NULL, created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE index_master (
            code TEXT PRIMARY KEY, name TEXT NOT NULL, name_english TEXT,
            category TEXT NOT NULL, data_start_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE indices_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            sector_name TEXT, created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    # 銘柄
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("72030", "トヨタ自動車", "TOYOTA", "prime", "プライム", "S17", "輸送用機器", "S33", "輸送用機器", "TOPIX Large70", "1949-05-16", None, None),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("67580", "ソニーグループ", "SONY", "prime", "プライム", "S17", "電気機器", "S33", "電気機器", "TOPIX Large70", "1958-12-01", None, None),
    )

    # 300営業日分のデータ生成
    dates = _generate_dates(300)

    # 銘柄データ（ランダムウォーク的）
    import random
    random.seed(42)

    for code, base_price in [("72030", 2500.0), ("67580", 13000.0)]:
        price = base_price
        for d in dates:
            change = random.uniform(-0.02, 0.025)
            price = price * (1 + change)
            o = price * random.uniform(0.99, 1.01)
            h = price * random.uniform(1.0, 1.02)
            lo = price * random.uniform(0.98, 1.0)
            vol = random.randint(500000, 2000000)
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                (code, d, o, h, lo, price, vol, 1.0, None),
            )

    # TOPIX データ
    topix_price = 2500.0
    for d in dates:
        change = random.uniform(-0.01, 0.015)
        topix_price = topix_price * (1 + change)
        conn.execute(
            "INSERT INTO topix_data VALUES (?,?,?,?,?,?)",
            (d, topix_price * 0.99, topix_price * 1.01, topix_price * 0.98, topix_price, None),
        )

    # 指数マスター
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0000", "TOPIX", "TOPIX", "topix", "2008-05-07"))
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0040", "水産農林", "Fishery", "sector33", "2010-01-04"))
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0080", "食品", "Foods", "sector17", "2010-01-04"))

    # 指数データ
    for idx_code, idx_base in [("0000", 2500.0), ("0040", 800.0), ("0080", 1200.0)]:
        idx_price = idx_base
        for d in dates:
            change = random.uniform(-0.012, 0.016)
            idx_price = idx_price * (1 + change)
            conn.execute(
                "INSERT INTO indices_data VALUES (?,?,?,?,?,?,?,?)",
                (idx_code, d, idx_price * 0.99, idx_price * 1.01, idx_price * 0.98, idx_price, None, None),
            )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def analytics_client(analytics_db_path, monkeypatch):
    """analytics テスト用クライアント"""
    monkeypatch.setenv("MARKET_DB_PATH", analytics_db_path)
    monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
    monkeypatch.setenv("JQUANTS_PLAN", "free")
    from src.config.settings import reload_settings
    reload_settings()
    app = create_app()
    with TestClient(app) as client:
        yield client
    reload_settings()


# --- Ranking Tests ---


class TestRanking:
    def test_200_default(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking")
        assert resp.status_code == 200
        data = resp.json()
        assert "date" in data
        assert "markets" in data
        assert "rankings" in data
        assert "lastUpdated" in data
        rankings = data["rankings"]
        assert "tradingValue" in rankings
        assert "gainers" in rankings
        assert "losers" in rankings
        assert "periodHigh" in rankings
        assert "periodLow" in rankings

    def test_response_shape(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking")
        data = resp.json()
        assert data["lookbackDays"] == 1
        assert data["periodDays"] == 250
        assert data["markets"] == ["prime"]

        # tradingValue has items
        if data["rankings"]["tradingValue"]:
            item = data["rankings"]["tradingValue"][0]
            assert "rank" in item
            assert "code" in item
            assert "companyName" in item
            assert "currentPrice" in item

    def test_with_lookback_days(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking?lookbackDays=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["lookbackDays"] == 5

    def test_with_limit(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking?limit=1")
        assert resp.status_code == 200
        data = resp.json()
        for cat in ["tradingValue", "gainers", "losers"]:
            assert len(data["rankings"][cat]) <= 1

    def test_422_no_db(self):
        """DB なしの場合 422"""
        app = create_app()
        with TestClient(app) as client:
            # lifespan 後に market_reader を None に上書き
            app.state.market_reader = None
            resp = client.get("/api/analytics/ranking")
            assert resp.status_code == 422


# --- Factor Regression Tests ---


class TestFactorRegression:
    def test_200(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stockCode"] == "7203"
        assert data["companyName"] == "トヨタ自動車"
        assert "marketBeta" in data
        assert "marketRSquared" in data
        assert isinstance(data["sector17Matches"], list)
        assert isinstance(data["sector33Matches"], list)
        assert isinstance(data["topixStyleMatches"], list)
        assert "analysisDate" in data
        assert "dataPoints" in data
        assert "dateRange" in data

    def test_response_numeric_precision(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        data = resp.json()
        # 数値は3桁精度
        assert isinstance(data["marketBeta"], float)
        assert isinstance(data["marketRSquared"], float)
        assert 0 <= data["marketRSquared"] <= 1

    def test_date_range(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        data = resp.json()
        dr = data["dateRange"]
        assert "from" in dr
        assert "to" in dr
        assert dr["from"] < dr["to"]
        assert data["dataPoints"] <= 100

    def test_400_invalid_symbol(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/ABC")
        assert resp.status_code == 400

    def test_alphanumeric_symbol_is_accepted_by_validation(self, analytics_client):
        """英数字4桁コードはバリデーションで弾かない（存在しなければ404）。"""
        resp = analytics_client.get("/api/analytics/factor-regression/285A")
        assert resp.status_code == 404

    def test_404_not_found(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/0000")
        assert resp.status_code == 404

    def test_422_no_db(self):
        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = None
            resp = client.get("/api/analytics/factor-regression/7203")
            assert resp.status_code == 422


# --- Screening Tests ---


class TestScreening:
    def test_200_default(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening")
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert "summary" in data
        assert "markets" in data
        assert "recentDays" in data
        assert "backtestMetric" in data
        assert "sortBy" in data
        assert "order" in data
        assert "lastUpdated" in data

    def test_summary_shape(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening")
        data = resp.json()
        summary = data["summary"]
        assert "totalStocksScreened" in summary
        assert "matchCount" in summary
        assert "byStrategy" in summary
        assert "strategiesEvaluated" in summary
        assert "strategiesWithoutBacktestMetrics" in summary
        assert "warnings" in summary

    def test_with_params(self, analytics_client):
        resp = analytics_client.get(
            "/api/analytics/screening"
            "?strategies=range_break_v15"
            "&recentDays=5"
            "&backtestMetric=calmar_ratio"
            "&sortBy=matchedDate"
            "&order=asc"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["recentDays"] == 5
        assert data["backtestMetric"] == "calmar_ratio"
        assert data["sortBy"] == "matchedDate"
        assert data["order"] == "asc"

    def test_old_query_params_are_rejected(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening?rangeBreakFast=true")
        assert resp.status_code == 422

    def test_old_sort_value_is_rejected(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening?sortBy=breakPercentage")
        assert resp.status_code == 422

    def test_422_no_db(self):
        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = None
            resp = client.get("/api/analytics/screening")
            assert resp.status_code == 422

    def test_result_item_shape(self, analytics_client):
        """結果がある場合のレスポンス形状"""
        resp = analytics_client.get("/api/analytics/screening")
        data = resp.json()
        if data["results"]:
            item = data["results"][0]
            assert "stockCode" in item
            assert "companyName" in item
            assert "matchedDate" in item
            assert "bestStrategyName" in item
            assert "bestStrategyScore" in item
            assert "matchStrategyCount" in item
            assert "matchedStrategies" in item


class TestAnalyticsRouteErrorMapping:
    def test_ranking_maps_value_error_to_422(self, analytics_client, monkeypatch):
        from src.server.services.ranking_service import RankingService

        def _raise_value_error(self, **_kwargs):  # noqa: ANN001
            raise ValueError("invalid ranking params")

        monkeypatch.setattr(RankingService, "get_rankings", _raise_value_error)
        resp = analytics_client.get("/api/analytics/ranking")
        assert resp.status_code == 422
        assert "invalid ranking params" in str(resp.json())

    def test_ranking_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.server.services.ranking_service import RankingService

        def _raise_runtime_error(self, **_kwargs):  # noqa: ANN001
            raise RuntimeError("ranking boom")

        monkeypatch.setattr(RankingService, "get_rankings", _raise_runtime_error)
        resp = analytics_client.get("/api/analytics/ranking")
        assert resp.status_code == 500
        assert "Failed to get rankings" in str(resp.json())

    def test_factor_regression_maps_insufficient_to_422(self, analytics_client, monkeypatch):
        from src.server.services.factor_regression_service import FactorRegressionService

        def _raise_value_error(self, symbol, lookback_days):  # noqa: ANN001
            raise ValueError("insufficient history")

        monkeypatch.setattr(FactorRegressionService, "analyze_stock", _raise_value_error)
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 422

    def test_factor_regression_maps_other_value_error_to_400(self, analytics_client, monkeypatch):
        from src.server.services.factor_regression_service import FactorRegressionService

        def _raise_value_error(self, symbol, lookback_days):  # noqa: ANN001
            raise ValueError("bad input")

        monkeypatch.setattr(FactorRegressionService, "analyze_stock", _raise_value_error)
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 400

    def test_factor_regression_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.server.services.factor_regression_service import FactorRegressionService

        def _raise_runtime_error(self, symbol, lookback_days):  # noqa: ANN001
            raise RuntimeError("factor boom")

        monkeypatch.setattr(FactorRegressionService, "analyze_stock", _raise_runtime_error)
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 500
        assert "Failed to analyze" in str(resp.json())

    def test_screening_maps_value_error_to_422(self, analytics_client, monkeypatch):
        from src.server.services.screening_service import ScreeningService

        def _raise_value_error(self, **_kwargs):  # noqa: ANN001
            raise ValueError("invalid strategy")

        monkeypatch.setattr(ScreeningService, "run_screening", _raise_value_error)
        resp = analytics_client.get("/api/analytics/screening")
        assert resp.status_code == 422
        assert "invalid strategy" in str(resp.json())

    def test_screening_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.server.services.screening_service import ScreeningService

        def _raise_runtime_error(self, **_kwargs):  # noqa: ANN001
            raise RuntimeError("screening boom")

        monkeypatch.setattr(ScreeningService, "run_screening", _raise_runtime_error)
        resp = analytics_client.get("/api/analytics/screening")
        assert resp.status_code == 500
        assert "Failed to run screening" in str(resp.json())

    def test_portfolio_factor_regression_maps_missing_reader_to_422(self):
        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = None
            app.state.portfolio_db = object()
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 422

    def test_portfolio_factor_regression_maps_missing_portfolio_db_to_422(self):
        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = object()
            app.state.portfolio_db = None
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 422

    def test_portfolio_factor_regression_maps_value_error_variants(
        self,
        analytics_db_path,
        monkeypatch,
    ):
        from src.config.settings import reload_settings
        from src.server.services.portfolio_factor_regression_service import PortfolioFactorRegressionService

        monkeypatch.setenv("MARKET_DB_PATH", analytics_db_path)
        monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
        monkeypatch.setenv("JQUANTS_PLAN", "free")
        reload_settings()

        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = object()
            app.state.portfolio_db = object()

            def _raise_not_found(self, portfolio_id, lookback_days):  # noqa: ANN001
                raise ValueError("portfolio not found")

            monkeypatch.setattr(PortfolioFactorRegressionService, "analyze", _raise_not_found)
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 404

            def _raise_insufficient(self, portfolio_id, lookback_days):  # noqa: ANN001
                raise ValueError("insufficient samples")

            monkeypatch.setattr(PortfolioFactorRegressionService, "analyze", _raise_insufficient)
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 422

            def _raise_other(self, portfolio_id, lookback_days):  # noqa: ANN001
                raise ValueError("unexpected input")

            monkeypatch.setattr(PortfolioFactorRegressionService, "analyze", _raise_other)
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 400

            def _raise_runtime(self, portfolio_id, lookback_days):  # noqa: ANN001
                raise RuntimeError("portfolio boom")

            monkeypatch.setattr(PortfolioFactorRegressionService, "analyze", _raise_runtime)
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 500

        reload_settings()
