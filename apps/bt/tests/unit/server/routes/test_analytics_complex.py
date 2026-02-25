"""
Complex Analytics Routes Unit Tests

Ranking, Factor Regression, Screening のルートテスト。
"""

import sqlite3
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.screening_job import ScreeningJobRequest


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

    # statements data for fundamental ranking
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?,?,?)
        """,
        ("72030", "2024-05-10", 100.0, "FY", 120.0, 118.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("72030", "2024-08-10", "1Q", 130.0, 100.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, earnings_per_share, type_of_current_period,
            next_year_forecast_earnings_per_share, shares_outstanding
        )
        VALUES (?,?,?,?,?,?)
        """,
        ("67580", "2024-05-12", 180.0, "FY", 210.0, 200.0),
    )
    conn.execute(
        """
        INSERT INTO statements (
            code, disclosed_date, type_of_current_period, forecast_eps, shares_outstanding
        )
        VALUES (?,?,?,?,?)
        """,
        ("67580", "2024-08-12", "Q1", 225.0, 200.0),
    )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def analytics_client(analytics_db_path, monkeypatch):
    """analytics テスト用クライアント"""
    monkeypatch.setenv("MARKET_DB_PATH", analytics_db_path)
    monkeypatch.setenv("JQUANTS_API_KEY", "dummy_token_value_0000")
    monkeypatch.setenv("JQUANTS_PLAN", "free")
    from src.shared.config.settings import reload_settings
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


class TestFundamentalRanking:
    def test_200_default(self, analytics_client):
        resp = analytics_client.get("/api/analytics/fundamental-ranking")
        assert resp.status_code == 200
        data = resp.json()
        assert "date" in data
        assert "markets" in data
        assert data["metricKey"] == "eps_forecast_to_actual"
        assert "rankings" in data
        assert "lastUpdated" in data
        rankings = data["rankings"]
        assert "ratioHigh" in rankings
        assert "ratioLow" in rankings

    def test_with_limit(self, analytics_client):
        resp = analytics_client.get("/api/analytics/fundamental-ranking?limit=1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["rankings"]["ratioHigh"]) <= 1
        assert len(data["rankings"]["ratioLow"]) <= 1

    def test_item_shape(self, analytics_client):
        resp = analytics_client.get("/api/analytics/fundamental-ranking")
        assert resp.status_code == 200
        data = resp.json()
        if data["rankings"]["ratioHigh"]:
            item = data["rankings"]["ratioHigh"][0]
            assert "code" in item
            assert "companyName" in item
            assert "epsValue" in item
            assert "source" in item
            assert item["source"] in {"fy", "revised"}

    def test_422_unsupported_metric_key(self, analytics_client):
        resp = analytics_client.get("/api/analytics/fundamental-ranking?metricKey=roe_forecast_to_actual")
        assert resp.status_code == 422

    def test_422_no_db(self):
        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = None
            resp = client.get("/api/analytics/fundamental-ranking")
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
    @staticmethod
    def _make_job(job_id: str, status: JobStatus, raw_result: dict | None = None):
        job = MagicMock()
        job.job_id = job_id
        job.job_type = "screening"
        job.status = status
        job.progress = 1.0 if status == JobStatus.COMPLETED else 0.0
        job.message = None
        job.created_at = datetime(2026, 1, 1)
        job.started_at = datetime(2026, 1, 1)
        job.completed_at = datetime(2026, 1, 1) if status in {JobStatus.COMPLETED, JobStatus.CANCELLED} else None
        job.error = None
        job.raw_result = raw_result
        return job

    def test_legacy_get_returns_410(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening")
        assert resp.status_code == 410

    def test_create_job_202(self, analytics_client):
        with (
            patch("src.entrypoints.http.routes.analytics_complex.screening_job_service") as mock_service,
            patch("src.entrypoints.http.routes.analytics_complex.screening_job_manager") as mock_manager,
        ):
            mock_service.submit_screening = AsyncMock(return_value="job-1")
            mock_service.get_job_request.return_value = ScreeningJobRequest()
            mock_manager.get_job.return_value = self._make_job("job-1", JobStatus.PENDING)

            resp = analytics_client.post("/api/analytics/screening/jobs", json={})

        assert resp.status_code == 202
        data = resp.json()
        assert data["job_id"] == "job-1"
        assert data["status"] == "pending"
        assert data["sortBy"] == "matchedDate"
        assert data["order"] == "desc"

    def test_rejects_removed_backtest_metric_query(self, analytics_client):
        resp = analytics_client.post(
            "/api/analytics/screening/jobs",
            json={"backtestMetric": "sharpe_ratio"},
        )
        assert resp.status_code == 422

    def test_get_job_status(self, analytics_client):
        with (
            patch("src.entrypoints.http.routes.analytics_complex.screening_job_service") as mock_service,
            patch("src.entrypoints.http.routes.analytics_complex.screening_job_manager") as mock_manager,
        ):
            mock_service.get_job_request.return_value = ScreeningJobRequest(
                markets="prime,standard",
                recentDays=5,
                sortBy="matchedDate",
                order="asc",
            )
            mock_manager.get_job.return_value = self._make_job("job-2", JobStatus.RUNNING)

            resp = analytics_client.get("/api/analytics/screening/jobs/job-2")

        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == "job-2"
        assert data["status"] == "running"
        assert data["recentDays"] == 5
        assert data["markets"] == "prime,standard"

    def test_cancel_job_conflict_for_completed(self, analytics_client):
        with patch("src.entrypoints.http.routes.analytics_complex.screening_job_manager") as mock_manager:
            mock_manager.get_job.return_value = self._make_job("job-3", JobStatus.COMPLETED)
            mock_manager.cancel_job = AsyncMock(return_value=None)

            resp = analytics_client.post("/api/analytics/screening/jobs/job-3/cancel")

        assert resp.status_code == 409

    def test_get_result(self, analytics_client):
        raw_result = {
            "response": {
                "results": [],
                "summary": {
                    "totalStocksScreened": 2,
                    "matchCount": 0,
                    "skippedCount": 0,
                    "byStrategy": {"range_break_v15": 0},
                    "strategiesEvaluated": ["range_break_v15"],
                    "strategiesWithoutBacktestMetrics": [],
                    "warnings": [],
                },
                "markets": ["prime"],
                "recentDays": 10,
                "referenceDate": None,
                "sortBy": "matchedDate",
                "order": "desc",
                "lastUpdated": "2026-01-01T00:00:00Z",
            }
        }

        with patch("src.entrypoints.http.routes.analytics_complex.screening_job_manager") as mock_manager:
            mock_manager.get_job.return_value = self._make_job(
                "job-4",
                JobStatus.COMPLETED,
                raw_result=raw_result,
            )

            resp = analytics_client.get("/api/analytics/screening/result/job-4")

        assert resp.status_code == 200
        data = resp.json()
        assert data["sortBy"] == "matchedDate"
        assert "backtestMetric" not in data

    def test_422_no_db(self):
        app = create_app()
        with TestClient(app) as client:
            app.state.market_reader = None
            resp = client.post("/api/analytics/screening/jobs", json={})
            assert resp.status_code == 422


class TestAnalyticsRouteErrorMapping:
    def test_ranking_maps_value_error_to_422(self, analytics_client, monkeypatch):
        from src.application.services.ranking_service import RankingService

        def _raise_value_error(self, **_kwargs):  # noqa: ANN001
            raise ValueError("invalid ranking params")

        monkeypatch.setattr(RankingService, "get_rankings", _raise_value_error)
        resp = analytics_client.get("/api/analytics/ranking")
        assert resp.status_code == 422
        assert "invalid ranking params" in str(resp.json())

    def test_ranking_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.application.services.ranking_service import RankingService

        def _raise_runtime_error(self, **_kwargs):  # noqa: ANN001
            raise RuntimeError("ranking boom")

        monkeypatch.setattr(RankingService, "get_rankings", _raise_runtime_error)
        resp = analytics_client.get("/api/analytics/ranking")
        assert resp.status_code == 500
        assert "Failed to get rankings" in str(resp.json())

    def test_fundamental_ranking_maps_value_error_to_422(self, analytics_client, monkeypatch):
        from src.application.services.ranking_service import RankingService

        def _raise_value_error(self, **_kwargs):  # noqa: ANN001
            raise ValueError("invalid fundamental ranking params")

        monkeypatch.setattr(RankingService, "get_fundamental_rankings", _raise_value_error)
        resp = analytics_client.get("/api/analytics/fundamental-ranking")
        assert resp.status_code == 422
        assert "invalid fundamental ranking params" in str(resp.json())

    def test_fundamental_ranking_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.application.services.ranking_service import RankingService

        def _raise_runtime_error(self, **_kwargs):  # noqa: ANN001
            raise RuntimeError("fundamental ranking boom")

        monkeypatch.setattr(RankingService, "get_fundamental_rankings", _raise_runtime_error)
        resp = analytics_client.get("/api/analytics/fundamental-ranking")
        assert resp.status_code == 500
        assert "Failed to get fundamental rankings" in str(resp.json())

    def test_factor_regression_maps_insufficient_to_422(self, analytics_client, monkeypatch):
        from src.application.services.factor_regression_service import FactorRegressionService

        def _raise_value_error(self, symbol, lookback_days):  # noqa: ANN001
            raise ValueError("insufficient history")

        monkeypatch.setattr(FactorRegressionService, "analyze_stock", _raise_value_error)
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 422

    def test_factor_regression_maps_other_value_error_to_400(self, analytics_client, monkeypatch):
        from src.application.services.factor_regression_service import FactorRegressionService

        def _raise_value_error(self, symbol, lookback_days):  # noqa: ANN001
            raise ValueError("bad input")

        monkeypatch.setattr(FactorRegressionService, "analyze_stock", _raise_value_error)
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 400

    def test_factor_regression_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.application.services.factor_regression_service import FactorRegressionService

        def _raise_runtime_error(self, symbol, lookback_days):  # noqa: ANN001
            raise RuntimeError("factor boom")

        monkeypatch.setattr(FactorRegressionService, "analyze_stock", _raise_runtime_error)
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 500
        assert "Failed to analyze" in str(resp.json())

    def test_screening_job_submit_maps_value_error_to_422(self, analytics_client, monkeypatch):
        from src.application.services.screening_job_service import ScreeningJobService

        async def _raise_value_error(self, reader, request):  # noqa: ANN001
            raise ValueError("invalid strategy")

        monkeypatch.setattr(ScreeningJobService, "submit_screening", _raise_value_error)
        resp = analytics_client.post("/api/analytics/screening/jobs", json={})
        assert resp.status_code == 422
        assert "invalid strategy" in str(resp.json())

    def test_screening_job_submit_maps_unexpected_error_to_500(self, analytics_client, monkeypatch):
        from src.application.services.screening_job_service import ScreeningJobService

        async def _raise_runtime_error(self, reader, request):  # noqa: ANN001
            raise RuntimeError("screening boom")

        monkeypatch.setattr(ScreeningJobService, "submit_screening", _raise_runtime_error)
        resp = analytics_client.post("/api/analytics/screening/jobs", json={})
        assert resp.status_code == 500
        assert "Failed to start screening job" in str(resp.json())

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
        from src.shared.config.settings import reload_settings
        from src.application.services.portfolio_factor_regression_service import PortfolioFactorRegressionService

        monkeypatch.setenv("MARKET_DB_PATH", analytics_db_path)
        monkeypatch.setenv("JQUANTS_API_KEY", "dummy_token_value_0000")
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
