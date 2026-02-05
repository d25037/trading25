"""
FundamentalsService テスト

財務指標計算サービスのテスト
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.api.jquants_client import JQuantsStatement, StockInfo
from src.server.schemas.fundamentals import (
    DailyValuationDataPoint,
    FundamentalDataPoint,
    FundamentalsComputeRequest,
    FundamentalsComputeResponse,
)
from src.models.types import normalize_period_type
from src.server.services.fundamentals_service import (
    FundamentalsService,
    FYDataPoint,
    fundamentals_service,
)


class TestFundamentalsServiceInit:
    """FundamentalsService 初期化テスト"""

    def test_init(self):
        """インスタンス初期化"""
        service = FundamentalsService()
        assert service._jquants_client is None
        assert service._market_client is None

    def test_lazy_client_initialization(self):
        """遅延初期化でクライアントが生成される"""
        service = FundamentalsService()
        with patch(
            "src.server.services.fundamentals_service.JQuantsAPIClient"
        ) as mock_jquants:
            mock_jquants.return_value = MagicMock()
            _ = service.jquants_client
            mock_jquants.assert_called_once()

    def test_close_clients(self):
        """close()でクライアントがクローズされる"""
        service = FundamentalsService()
        mock_jquants = MagicMock()
        mock_market = MagicMock()
        service._jquants_client = mock_jquants
        service._market_client = mock_market

        service.close()

        mock_jquants.close.assert_called_once()
        mock_market.close.assert_called_once()
        assert service._jquants_client is None
        assert service._market_client is None


class TestMetricCalculations:
    """指標計算メソッドのテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    @pytest.fixture
    def sample_statement(self) -> JQuantsStatement:
        """サンプル財務諸表データ"""
        return JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="有価証券報告書・連結",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt="2024-04-01",
            NxtFYEn="2025-03-31",
            Sales=45000000000000,  # 45兆円
            OP=5000000000000,  # 5兆円
            OdP=5500000000000,
            NP=4000000000000,  # 4兆円
            EPS=300.0,  # 300円
            DEPS=290.0,
            TA=90000000000000,
            Eq=30000000000000,
            EqAR=33.33,
            BPS=2250.0,
            CFO=6000000000000,
            CFI=-2000000000000,
            CFF=-1000000000000,
            CashEq=8000000000000,
            ShOutFY=13333333333,
            TrShFY=0,
            AvgSh=13333333333,
            FEPS=320.0,
            NxFEPS=350.0,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )

    def test_calculate_eps_consolidated(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """連結EPS計算"""
        eps = service._calculate_eps(sample_statement, prefer_consolidated=True)
        assert eps == 300.0

    def test_calculate_eps_non_consolidated(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """非連結EPS計算 (連結のみの場合はフォールバック)"""
        eps = service._calculate_eps(sample_statement, prefer_consolidated=False)
        # NCEPSがNoneなのでEPSにフォールバック
        assert eps == 300.0

    def test_calculate_bps(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """BPS計算"""
        bps = service._calculate_bps(sample_statement, prefer_consolidated=True)
        assert bps == 2250.0

    def test_calculate_roe(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """ROE計算"""
        roe = service._calculate_roe(sample_statement, prefer_consolidated=True)
        # ROE = (NP / Eq) * 100 = (4兆 / 30兆) * 100 = 13.33%
        assert roe is not None
        assert abs(roe - 13.33) < 0.1

    def test_calculate_roe_quarterly(self, service: FundamentalsService):
        """四半期ROEの年率換算"""
        quarterly_stmt = JQuantsStatement(
            DiscDate="2024-02-15",
            Code="7203",
            DocType="四半期報告書・連結",
            CurPerType="3Q",
            CurPerSt="2023-04-01",
            CurPerEn="2023-12-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=3000000000000,  # 9ヶ月で3兆円
            EPS=225.0,
            DEPS=None,
            TA=None,
            Eq=30000000000000,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=None,
            NxFEPS=None,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        roe = service._calculate_roe(quarterly_stmt, prefer_consolidated=True)
        # 3Qの年率換算: (3兆 * 4/3) / 30兆 * 100 = 13.33%
        assert roe is not None
        assert abs(roe - 13.33) < 0.1

    def test_calculate_per(self, service: FundamentalsService):
        """PER計算"""
        per = service._calculate_per(eps=300.0, stock_price=6000.0)
        assert per == 20.0

    def test_calculate_per_zero_eps(self, service: FundamentalsService):
        """EPS=0の場合PERはNone"""
        per = service._calculate_per(eps=0.0, stock_price=6000.0)
        assert per is None

    def test_calculate_per_none_values(self, service: FundamentalsService):
        """None値の場合PERはNone"""
        assert service._calculate_per(eps=None, stock_price=6000.0) is None
        assert service._calculate_per(eps=300.0, stock_price=None) is None

    def test_calculate_pbr(self, service: FundamentalsService):
        """PBR計算"""
        pbr = service._calculate_pbr(bps=2250.0, stock_price=6750.0)
        assert pbr == 3.0

    def test_calculate_pbr_zero_bps(self, service: FundamentalsService):
        """BPS<=0の場合PBRはNone"""
        assert service._calculate_pbr(bps=0.0, stock_price=6000.0) is None
        assert service._calculate_pbr(bps=-100.0, stock_price=6000.0) is None

    def test_calculate_roa(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """ROA計算"""
        roa = service._calculate_roa(sample_statement, prefer_consolidated=True)
        # ROA = (NP / TA) * 100 = (4兆 / 90兆) * 100 = 4.44%
        assert roa is not None
        assert abs(roa - 4.44) < 0.1

    def test_calculate_operating_margin(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """営業利益率計算"""
        margin = service._calculate_operating_margin(
            sample_statement, prefer_consolidated=True
        )
        # = (OP / Sales) * 100 = (5兆 / 45兆) * 100 = 11.11%
        assert margin is not None
        assert abs(margin - 11.11) < 0.1

    def test_calculate_net_margin(
        self, service: FundamentalsService, sample_statement: JQuantsStatement
    ):
        """純利益率計算"""
        margin = service._calculate_net_margin(
            sample_statement, prefer_consolidated=True
        )
        # = (NP / Sales) * 100 = (4兆 / 45兆) * 100 = 8.89%
        assert margin is not None
        assert abs(margin - 8.89) < 0.1

    def test_calculate_simple_fcf(self, service: FundamentalsService):
        """FCF計算"""
        fcf = service._calculate_simple_fcf(cfo=6000000000000, cfi=-2000000000000)
        assert fcf == 4000000000000

    def test_calculate_simple_fcf_none(self, service: FundamentalsService):
        """FCF計算 (None値)"""
        assert service._calculate_simple_fcf(cfo=None, cfi=-2000000000000) is None
        assert service._calculate_simple_fcf(cfo=6000000000000, cfi=None) is None

    def test_calculate_fcf_margin(self, service: FundamentalsService):
        """FCFマージン計算"""
        fcf_margin = service._calculate_fcf_margin(
            fcf=4000000000000, net_sales=45000000000000
        )
        # = (4兆 / 45兆) * 100 = 8.89%
        assert fcf_margin is not None
        assert abs(fcf_margin - 8.89) < 0.1


class TestHelperMethods:
    """ヘルパーメソッドのテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_round_or_none(self, service: FundamentalsService):
        """数値の丸め処理"""
        assert service._round_or_none(None) is None
        assert service._round_or_none(13.333333) == 13.33
        assert service._round_or_none(100.0) == 100.0

    def test_to_millions(self, service: FundamentalsService):
        """百万円単位への変換"""
        assert service._to_millions(None) is None
        assert service._to_millions(1000000000000) == 1000000.0
        assert service._to_millions(0) == 0.0

    def test_is_consolidated_statement(self, service: FundamentalsService):
        """連結/非連結判定"""
        consolidated = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="有価証券報告書・連結",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=100,
            EPS=None,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=None,
            NxFEPS=None,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        assert service._is_consolidated_statement(consolidated) is True

        non_consolidated = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="有価証券報告書・非連結",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=None,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=None,
            NxFEPS=None,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        assert service._is_consolidated_statement(non_consolidated) is False

    def test_get_accounting_standard(self, service: FundamentalsService):
        """会計基準判定"""
        ifrs = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="四半期報告書・連結・IFRS",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=None,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=None,
            NxFEPS=None,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        assert service._get_accounting_standard(ifrs) == "IFRS"

        us_gaap = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="有価証券報告書・連結・US GAAP",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=None,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=None,
            NxFEPS=None,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        assert service._get_accounting_standard(us_gaap) == "US GAAP"


class TestFilterStatements:
    """財務諸表フィルタリングのテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    @pytest.fixture
    def statements(self) -> list[JQuantsStatement]:
        """テスト用財務諸表リスト"""
        base = {
            "DiscDate": "2024-05-15",
            "Code": "7203",
            "DocType": "連結",
            "CurPerSt": "2023-04-01",
            "CurFYSt": "2023-04-01",
            "CurFYEn": "2024-03-31",
            "NxtFYSt": None,
            "NxtFYEn": None,
            "Sales": None,
            "OP": None,
            "OdP": None,
            "NP": None,
            "EPS": None,
            "DEPS": None,
            "TA": None,
            "Eq": None,
            "EqAR": None,
            "BPS": None,
            "CFO": None,
            "CFI": None,
            "CFF": None,
            "CashEq": None,
            "ShOutFY": None,
            "TrShFY": None,
            "AvgSh": None,
            "FEPS": None,
            "NxFEPS": None,
            "NCSales": None,
            "NCOP": None,
            "NCOdP": None,
            "NCNP": None,
            "NCEPS": None,
            "NCTA": None,
            "NCEq": None,
            "NCEqAR": None,
            "NCBPS": None,
            "FNCEPS": None,
            "NxFNCEPS": None,
        }
        return [
            JQuantsStatement(**{**base, "CurPerType": "FY", "CurPerEn": "2024-03-31"}),
            JQuantsStatement(**{**base, "CurPerType": "3Q", "CurPerEn": "2023-12-31"}),
            JQuantsStatement(**{**base, "CurPerType": "2Q", "CurPerEn": "2023-09-30"}),
            JQuantsStatement(**{**base, "CurPerType": "1Q", "CurPerEn": "2023-06-30"}),
            JQuantsStatement(**{**base, "CurPerType": "FY", "CurPerEn": "2023-03-31"}),
        ]

    def test_filter_by_period_type_all(
        self, service: FundamentalsService, statements: list[JQuantsStatement]
    ):
        """全期間タイプ"""
        filtered = service._filter_statements(statements, "all", None, None)
        assert len(filtered) == 5

    def test_filter_by_period_type_fy(
        self, service: FundamentalsService, statements: list[JQuantsStatement]
    ):
        """FYのみ"""
        filtered = service._filter_statements(statements, "FY", None, None)
        assert len(filtered) == 2

    def test_filter_by_period_type_q1(
        self, service: FundamentalsService, statements: list[JQuantsStatement]
    ):
        """1Qのみ"""
        filtered = service._filter_statements(statements, "1Q", None, None)
        assert len(filtered) == 1

    def test_filter_by_date_range(
        self, service: FundamentalsService, statements: list[JQuantsStatement]
    ):
        """日付範囲フィルタ"""
        filtered = service._filter_statements(
            statements, "all", "2023-07-01", "2024-01-31"
        )
        # 2023-09-30, 2023-12-31
        assert len(filtered) == 2


class TestDailyValuation:
    """日次バリュエーションのテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_find_price_at_date_exact(self, service: FundamentalsService):
        """完全一致日付の価格検索"""
        sorted_dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
        price_map = {"2024-01-01": 100.0, "2024-01-02": 110.0, "2024-01-03": 105.0}
        price = service._find_price_at_date("2024-01-02", sorted_dates, price_map)
        assert price == 110.0

    def test_find_price_at_date_before(self, service: FundamentalsService):
        """祝日等で価格がない場合、直前の価格を使用"""
        sorted_dates = ["2024-01-01", "2024-01-02", "2024-01-04"]
        price_map = {"2024-01-01": 100.0, "2024-01-02": 110.0, "2024-01-04": 105.0}
        price = service._find_price_at_date("2024-01-03", sorted_dates, price_map)
        assert price == 110.0

    def test_find_price_at_date_no_prior(self, service: FundamentalsService):
        """対象日より前に価格がない場合"""
        sorted_dates = ["2024-01-02", "2024-01-03"]
        price_map = {"2024-01-02": 110.0, "2024-01-03": 105.0}
        price = service._find_price_at_date("2024-01-01", sorted_dates, price_map)
        assert price is None

    def test_get_applicable_fy_data(self, service: FundamentalsService):
        """FYデータポイント抽出"""
        base = {
            "DiscDate": "2024-05-15",
            "Code": "7203",
            "DocType": "連結",
            "CurPerSt": "2023-04-01",
            "CurFYSt": "2023-04-01",
            "CurFYEn": "2024-03-31",
            "NxtFYSt": None,
            "NxtFYEn": None,
            "Sales": None,
            "OP": None,
            "OdP": None,
            "NP": None,
            "DEPS": None,
            "TA": None,
            "Eq": None,
            "EqAR": None,
            "CFO": None,
            "CFI": None,
            "CFF": None,
            "CashEq": None,
            "ShOutFY": None,
            "TrShFY": None,
            "AvgSh": None,
            "FEPS": None,
            "NxFEPS": None,
            "NCSales": None,
            "NCOP": None,
            "NCOdP": None,
            "NCNP": None,
            "NCEPS": None,
            "NCTA": None,
            "NCEq": None,
            "NCEqAR": None,
            "NCBPS": None,
            "FNCEPS": None,
            "NxFNCEPS": None,
        }
        statements = [
            JQuantsStatement(
                **{
                    **base,
                    "CurPerType": "FY",
                    "CurPerEn": "2024-03-31",
                    "DiscDate": "2024-05-15",
                    "EPS": 300.0,
                    "BPS": 2250.0,
                }
            ),
            JQuantsStatement(
                **{
                    **base,
                    "CurPerType": "3Q",
                    "CurPerEn": "2023-12-31",
                    "DiscDate": "2024-02-15",
                    "EPS": 225.0,
                    "BPS": 2200.0,
                }
            ),
            JQuantsStatement(
                **{
                    **base,
                    "CurPerType": "FY",
                    "CurPerEn": "2023-03-31",
                    "DiscDate": "2023-05-15",
                    "EPS": 280.0,
                    "BPS": 2100.0,
                }
            ),
        ]
        fy_data = service._get_applicable_fy_data(
            statements, prefer_consolidated=True, baseline_shares=None
        )
        assert len(fy_data) == 2  # Only FY statements
        assert fy_data[0].disclosed_date == "2023-05-15"  # Sorted ascending
        assert fy_data[1].disclosed_date == "2024-05-15"

    def test_find_applicable_fy(self, service: FundamentalsService):
        """適用FYの検索"""
        fy_data = [
            FYDataPoint(disclosed_date="2023-05-15", eps=280.0, bps=2100.0),
            FYDataPoint(disclosed_date="2024-05-15", eps=300.0, bps=2250.0),
        ]

        # Before any FY disclosure
        assert service._find_applicable_fy(fy_data, "2023-05-14") is None

        # After first FY disclosure, before second
        result = service._find_applicable_fy(fy_data, "2024-01-01")
        assert result is not None
        assert result.eps == 280.0

        # After second FY disclosure
        result = service._find_applicable_fy(fy_data, "2024-06-01")
        assert result is not None
        assert result.eps == 300.0


class TestComputeFundamentals:
    """compute_fundamentals統合テスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_no_statements(self, service: FundamentalsService):
        """財務諸表が見つからない場合"""
        mock_jquants = MagicMock()
        mock_jquants.get_statements.return_value = []

        service._jquants_client = mock_jquants
        service._market_client = MagicMock()

        request = FundamentalsComputeRequest(symbol="9999")
        result = service.compute_fundamentals(request)

        assert result.symbol == "9999"
        assert result.data == []
        assert result.latestMetrics is None

    def test_with_statements(self, service: FundamentalsService):
        """財務諸表がある場合の計算"""
        base = {
            "Code": "7203",
            "DocType": "連結",
            "CurPerSt": "2023-04-01",
            "CurFYSt": "2023-04-01",
            "CurFYEn": "2024-03-31",
            "NxtFYSt": None,
            "NxtFYEn": None,
            "Sales": 45000000000000,
            "OP": 5000000000000,
            "OdP": 5500000000000,
            "NP": 4000000000000,
            "EPS": 300.0,
            "DEPS": 290.0,
            "TA": 90000000000000,
            "Eq": 30000000000000,
            "EqAR": 33.33,
            "BPS": 2250.0,
            "CFO": 6000000000000,
            "CFI": -2000000000000,
            "CFF": -1000000000000,
            "CashEq": 8000000000000,
            "ShOutFY": 13333333333,
            "TrShFY": 0,
            "AvgSh": 13333333333,
            "FEPS": 320.0,
            "NxFEPS": 350.0,
            "NCSales": None,
            "NCOP": None,
            "NCOdP": None,
            "NCNP": None,
            "NCEPS": None,
            "NCTA": None,
            "NCEq": None,
            "NCEqAR": None,
            "NCBPS": None,
            "FNCEPS": None,
            "NxFNCEPS": None,
        }

        statements = [
            JQuantsStatement(
                **{
                    **base,
                    "DiscDate": "2024-05-15",
                    "CurPerType": "FY",
                    "CurPerEn": "2024-03-31",
                }
            ),
        ]

        mock_stock_info = StockInfo(
            code="7203",
            companyName="トヨタ自動車",
            companyNameEnglish="Toyota Motor Corporation",
            marketCode="0111",
            marketName="プライム",
            sector17Code="16",
            sector17Name="自動車・輸送機",
            sector33Code="3250",
            sector33Name="自動車・輸送機",
            scaleCategory="TOPIX Large70",
            listedDate="1949-05-01",
        )

        mock_prices_df = pd.DataFrame(
            {
                "Close": [6000.0, 6100.0, 6050.0],
            },
            index=pd.to_datetime(["2024-05-14", "2024-05-15", "2024-05-16"]),
        )

        mock_jquants = MagicMock()
        mock_jquants.get_statements.return_value = statements
        mock_jquants.get_stock_info.return_value = mock_stock_info

        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = mock_prices_df

        service._jquants_client = mock_jquants
        service._market_client = mock_market

        request = FundamentalsComputeRequest(symbol="7203")
        result = service.compute_fundamentals(request)

        assert result.symbol == "7203"
        assert result.companyName == "トヨタ自動車"
        assert len(result.data) == 1
        assert result.data[0].eps == 300.0
        assert result.data[0].bps == 2250.0

    def test_share_adjusted_metrics(self, service: FundamentalsService):
        """発行済株式数でEPS/BPS/予想EPSを調整する"""
        base = {
            "Code": "7203",
            "DocType": "連結",
            "CurPerSt": "2022-04-01",
            "CurFYSt": "2022-04-01",
            "CurFYEn": "2023-03-31",
            "NxtFYSt": None,
            "NxtFYEn": None,
            "Sales": 45000000000000,
            "OP": 5000000000000,
            "OdP": 5500000000000,
            "NP": 4000000000000,
            "TA": 90000000000000,
            "Eq": 30000000000000,
            "EqAR": 33.33,
            "CFO": 6000000000000,
            "CFI": -2000000000000,
            "CFF": -1000000000000,
            "CashEq": 8000000000000,
            "TrShFY": 0,
            "AvgSh": 0,
            "NCSales": None,
            "NCOP": None,
            "NCOdP": None,
            "NCNP": None,
            "NCEPS": None,
            "NCTA": None,
            "NCEq": None,
            "NCEqAR": None,
            "NCBPS": None,
            "FNCEPS": None,
            "NxFNCEPS": None,
        }

        statements = [
            JQuantsStatement(
                **{
                    **base,
                    "DiscDate": "2024-06-01",
                    "CurPerType": "FY",
                    "CurPerEn": "2024-03-31",
                    "EPS": 200.0,
                    "DEPS": 195.0,
                    "BPS": 2000.0,
                    "ShOutFY": 200.0,
                    "FEPS": 210.0,
                    "NxFEPS": 220.0,
                }
            ),
            JQuantsStatement(
                **{
                    **base,
                    "DiscDate": "2023-06-01",
                    "CurPerType": "FY",
                    "CurPerEn": "2023-03-31",
                    "EPS": 100.0,
                    "DEPS": 98.0,
                    "BPS": 1000.0,
                    "ShOutFY": 100.0,
                    "FEPS": 105.0,
                    "NxFEPS": 110.0,
                }
            ),
        ]

        mock_prices_df = pd.DataFrame(
            {
                "Close": [6000.0, 6100.0, 6050.0],
            },
            index=pd.to_datetime(["2024-05-14", "2024-05-15", "2024-05-16"]),
        )

        mock_jquants = MagicMock()
        mock_jquants.get_statements.return_value = statements
        mock_jquants.get_stock_info.return_value = None

        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = mock_prices_df

        service._jquants_client = mock_jquants
        service._market_client = mock_market

        request = FundamentalsComputeRequest(symbol="7203", period_type="FY")
        result = service.compute_fundamentals(request)

        latest = next(d for d in result.data if d.date == "2024-03-31")
        older = next(d for d in result.data if d.date == "2023-03-31")

        assert latest.adjustedEps == latest.eps
        assert latest.adjustedForecastEps == latest.forecastEps
        assert latest.adjustedBps == latest.bps

        assert older.adjustedEps == 50.0
        # FY uses NxFEPS (=110.0), adjusted = 110.0 * (100/200) = 55.0
        assert older.adjustedForecastEps == 55.0
        assert older.adjustedBps == 500.0


class TestComputeAdjustedValue:
    """_compute_adjusted_value のエッジケーステスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_none_value(self, service: FundamentalsService):
        assert service._compute_adjusted_value(None, 100.0, 200.0) is None

    def test_none_current_shares(self, service: FundamentalsService):
        assert service._compute_adjusted_value(100.0, None, 200.0) is None

    def test_none_base_shares(self, service: FundamentalsService):
        assert service._compute_adjusted_value(100.0, 100.0, None) is None

    def test_zero_current_shares(self, service: FundamentalsService):
        assert service._compute_adjusted_value(100.0, 0.0, 200.0) is None

    def test_zero_base_shares(self, service: FundamentalsService):
        assert service._compute_adjusted_value(100.0, 100.0, 0.0) is None

    def test_nan_current_shares(self, service: FundamentalsService):
        assert service._compute_adjusted_value(100.0, float("nan"), 200.0) is None

    def test_nan_base_shares(self, service: FundamentalsService):
        assert service._compute_adjusted_value(100.0, 100.0, float("nan")) is None

    def test_same_shares_identity(self, service: FundamentalsService):
        result = service._compute_adjusted_value(100.0, 200.0, 200.0)
        assert result == 100.0

    def test_split_halves_eps(self, service: FundamentalsService):
        # Pre-split shares=100, post-split baseline=200 → EPS halved
        result = service._compute_adjusted_value(100.0, 100.0, 200.0)
        assert result == 50.0


class TestNormalizePeriodType:
    """normalize_period_type のテスト (shared utility in models.types)"""

    def test_legacy_q1(self):
        assert normalize_period_type("Q1") == "1Q"

    def test_legacy_q2(self):
        assert normalize_period_type("Q2") == "2Q"

    def test_legacy_q3(self):
        assert normalize_period_type("Q3") == "3Q"

    def test_already_normalized(self):
        assert normalize_period_type("1Q") == "1Q"
        assert normalize_period_type("2Q") == "2Q"
        assert normalize_period_type("3Q") == "3Q"

    def test_fy(self):
        assert normalize_period_type("FY") == "FY"

    def test_all(self):
        assert normalize_period_type("all") == "all"

    def test_none(self):
        assert normalize_period_type(None) is None

    def test_unknown_passthrough(self):
        assert normalize_period_type("X1") == "X1"


class TestAnnualizeQuarterlyProfit:
    """四半期利益の年率換算テスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_q1_annualization(self, service: FundamentalsService):
        """1Q: 4倍"""
        result = service._annualize_quarterly_profit(1000.0, "1Q")
        assert result == 4000.0

    def test_q2_annualization(self, service: FundamentalsService):
        """2Q: 2倍 (半期累計)"""
        result = service._annualize_quarterly_profit(2000.0, "2Q")
        assert result == 4000.0

    def test_q3_annualization(self, service: FundamentalsService):
        """3Q: 4/3倍 (9ヶ月累計)"""
        result = service._annualize_quarterly_profit(3000.0, "3Q")
        assert result == 4000.0

    def test_fy_no_adjustment(self, service: FundamentalsService):
        """FY: 調整なし"""
        result = service._annualize_quarterly_profit(4000.0, "FY")
        assert result == 4000.0


class TestGlobalServiceInstance:
    """グローバルサービスインスタンスのテスト"""

    def test_global_instance_exists(self):
        """グローバルインスタンスが存在する"""
        assert fundamentals_service is not None
        assert isinstance(fundamentals_service, FundamentalsService)


class TestForecastEps:
    """Forecast EPS関連のテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_get_forecast_eps_fy_consolidated(self, service: FundamentalsService):
        """FYの場合はNxFEPSが優先"""
        stmt = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="連結",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=300.0,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=320.0,
            NxFEPS=350.0,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        forecast, change_rate = service._get_forecast_eps(stmt, 300.0, True)
        assert forecast == 350.0  # NxFEPS優先
        assert change_rate is not None
        assert abs(change_rate - 16.67) < 0.1

    def test_get_forecast_eps_q_consolidated(self, service: FundamentalsService):
        """四半期の場合はFEPSが優先"""
        stmt = JQuantsStatement(
            DiscDate="2024-02-15",
            Code="7203",
            DocType="連結",
            CurPerType="3Q",
            CurPerSt="2023-04-01",
            CurPerEn="2023-12-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=225.0,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=320.0,
            NxFEPS=350.0,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        forecast, change_rate = service._get_forecast_eps(stmt, 225.0, True)
        assert forecast == 320.0  # FEPS優先

    def test_get_forecast_eps_non_consolidated(self, service: FundamentalsService):
        """非連結の場合"""
        stmt = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="非連結",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=None,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=None,
            NxFEPS=None,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=100.0,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=110.0,
            NxFNCEPS=120.0,
        )
        forecast, _ = service._get_forecast_eps(stmt, 100.0, False)
        assert forecast == 120.0  # NxFNCEPS優先

    def test_get_forecast_eps_zero_actual(self, service: FundamentalsService):
        """actual EPS=0の場合、change_rateはNone"""
        stmt = JQuantsStatement(
            DiscDate="2024-05-15",
            Code="7203",
            DocType="連結",
            CurPerType="FY",
            CurPerSt="2023-04-01",
            CurPerEn="2024-03-31",
            CurFYSt="2023-04-01",
            CurFYEn="2024-03-31",
            NxtFYSt=None,
            NxtFYEn=None,
            Sales=None,
            OP=None,
            OdP=None,
            NP=None,
            EPS=0.0,
            DEPS=None,
            TA=None,
            Eq=None,
            EqAR=None,
            BPS=None,
            CFO=None,
            CFI=None,
            CFF=None,
            CashEq=None,
            ShOutFY=None,
            TrShFY=None,
            AvgSh=None,
            FEPS=320.0,
            NxFEPS=350.0,
            NCSales=None,
            NCOP=None,
            NCOdP=None,
            NCNP=None,
            NCEPS=None,
            NCTA=None,
            NCEq=None,
            NCEqAR=None,
            NCBPS=None,
            FNCEPS=None,
            NxFNCEPS=None,
        )
        forecast, change_rate = service._get_forecast_eps(stmt, 0.0, True)
        assert forecast == 350.0
        assert change_rate is None


class TestFCFYield:
    """FCF Yield計算のテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_calculate_fcf_yield(self, service: FundamentalsService):
        """正常なFCF Yield計算"""
        # FCF = 4兆円, 株価 = 6000円, 発行済株式 = 133.33億株
        # Market Cap = 6000 * 13333333333 = 80兆円
        # FCF Yield = (4兆 / 80兆) * 100 = 5%
        result = service._calculate_fcf_yield(
            fcf=4000000000000,
            stock_price=6000.0,
            shares_outstanding=13333333333,
            treasury_shares=0,
        )
        assert result is not None
        assert abs(result - 5.0) < 0.1

    def test_calculate_fcf_yield_with_treasury(self, service: FundamentalsService):
        """自己株式を考慮したFCF Yield"""
        result = service._calculate_fcf_yield(
            fcf=4000000000000,
            stock_price=6000.0,
            shares_outstanding=13333333333,
            treasury_shares=3333333333,  # 25%が自己株式
        )
        assert result is not None
        # actual_shares = 10000000000
        # Market Cap = 6000 * 10000000000 = 60兆円
        # FCF Yield = (4兆 / 60兆) * 100 = 6.67%
        assert abs(result - 6.67) < 0.1

    def test_calculate_fcf_yield_no_shares(self, service: FundamentalsService):
        """発行済株式数がない場合"""
        result = service._calculate_fcf_yield(
            fcf=4000000000000,
            stock_price=6000.0,
            shares_outstanding=None,
            treasury_shares=None,
        )
        assert result is None

    def test_calculate_fcf_yield_zero_price(self, service: FundamentalsService):
        """株価が0の場合"""
        result = service._calculate_fcf_yield(
            fcf=4000000000000,
            stock_price=0.0,
            shares_outstanding=13333333333,
            treasury_shares=None,
        )
        assert result is None

    def test_calculate_fcf_yield_negative_actual_shares(
        self, service: FundamentalsService
    ):
        """自己株式が発行済株式より多い場合"""
        result = service._calculate_fcf_yield(
            fcf=4000000000000,
            stock_price=6000.0,
            shares_outstanding=1000,
            treasury_shares=2000,
        )
        assert result is None


class TestHasActualFinancialData:
    """財務データ存在チェックのテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_has_actual_data_with_roe(self, service: FundamentalsService):
        """ROEがある場合"""
        data = FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-15",
            periodType="FY",
            isConsolidated=True,
            accountingStandard=None,
            roe=13.33,
            eps=None,
            dilutedEps=None,
            bps=None,
            per=None,
            pbr=None,
            roa=None,
            operatingMargin=None,
            netMargin=None,
            stockPrice=None,
            netProfit=None,
            equity=None,
            totalAssets=None,
            netSales=None,
            operatingProfit=None,
            cashFlowOperating=None,
            cashFlowInvesting=None,
            cashFlowFinancing=None,
            cashAndEquivalents=None,
            fcf=None,
            fcfYield=None,
            fcfMargin=None,
            forecastEps=None,
            forecastEpsChangeRate=None,
            revisedForecastEps=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )
        assert service._has_actual_financial_data(data) is True

    def test_has_actual_data_with_nonzero_eps(self, service: FundamentalsService):
        """0でないEPSがある場合"""
        data = FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-15",
            periodType="FY",
            isConsolidated=True,
            accountingStandard=None,
            roe=None,
            eps=300.0,
            dilutedEps=None,
            bps=None,
            per=None,
            pbr=None,
            roa=None,
            operatingMargin=None,
            netMargin=None,
            stockPrice=None,
            netProfit=None,
            equity=None,
            totalAssets=None,
            netSales=None,
            operatingProfit=None,
            cashFlowOperating=None,
            cashFlowInvesting=None,
            cashFlowFinancing=None,
            cashAndEquivalents=None,
            fcf=None,
            fcfYield=None,
            fcfMargin=None,
            forecastEps=None,
            forecastEpsChangeRate=None,
            revisedForecastEps=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )
        assert service._has_actual_financial_data(data) is True

    def test_has_actual_data_with_zero_eps(self, service: FundamentalsService):
        """EPS=0の場合はFalse（純資産と利益で判断）"""
        data = FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-15",
            periodType="FY",
            isConsolidated=True,
            accountingStandard=None,
            roe=None,
            eps=0.0,
            dilutedEps=None,
            bps=None,
            per=None,
            pbr=None,
            roa=None,
            operatingMargin=None,
            netMargin=None,
            stockPrice=None,
            netProfit=None,
            equity=None,
            totalAssets=None,
            netSales=None,
            operatingProfit=None,
            cashFlowOperating=None,
            cashFlowInvesting=None,
            cashFlowFinancing=None,
            cashAndEquivalents=None,
            fcf=None,
            fcfYield=None,
            fcfMargin=None,
            forecastEps=None,
            forecastEpsChangeRate=None,
            revisedForecastEps=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )
        assert service._has_actual_financial_data(data) is False

    def test_has_actual_data_with_net_profit(self, service: FundamentalsService):
        """純利益がある場合"""
        data = FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-15",
            periodType="FY",
            isConsolidated=True,
            accountingStandard=None,
            roe=None,
            eps=None,
            dilutedEps=None,
            bps=None,
            per=None,
            pbr=None,
            roa=None,
            operatingMargin=None,
            netMargin=None,
            stockPrice=None,
            netProfit=4000000.0,
            equity=None,
            totalAssets=None,
            netSales=None,
            operatingProfit=None,
            cashFlowOperating=None,
            cashFlowInvesting=None,
            cashFlowFinancing=None,
            cashAndEquivalents=None,
            fcf=None,
            fcfYield=None,
            fcfMargin=None,
            forecastEps=None,
            forecastEpsChangeRate=None,
            revisedForecastEps=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )
        assert service._has_actual_financial_data(data) is True

    def test_has_actual_data_with_equity(self, service: FundamentalsService):
        """純資産がある場合"""
        data = FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-15",
            periodType="FY",
            isConsolidated=True,
            accountingStandard=None,
            roe=None,
            eps=None,
            dilutedEps=None,
            bps=None,
            per=None,
            pbr=None,
            roa=None,
            operatingMargin=None,
            netMargin=None,
            stockPrice=None,
            netProfit=None,
            equity=30000000.0,
            totalAssets=None,
            netSales=None,
            operatingProfit=None,
            cashFlowOperating=None,
            cashFlowInvesting=None,
            cashFlowFinancing=None,
            cashAndEquivalents=None,
            fcf=None,
            fcfYield=None,
            fcfMargin=None,
            forecastEps=None,
            forecastEpsChangeRate=None,
            revisedForecastEps=None,
            revisedForecastSource=None,
            prevCashFlowOperating=None,
            prevCashFlowInvesting=None,
            prevCashFlowFinancing=None,
            prevCashAndEquivalents=None,
        )
        assert service._has_actual_financial_data(data) is True


class TestHasValidValuationMetrics:
    """バリュエーション指標有効性チェックのテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_valid_eps_only(self, service: FundamentalsService):
        """EPSのみ有効"""
        assert service._has_valid_valuation_metrics(eps=100.0, bps=None) is True

    def test_valid_bps_only(self, service: FundamentalsService):
        """BPSのみ有効"""
        assert service._has_valid_valuation_metrics(eps=None, bps=1000.0) is True

    def test_zero_eps(self, service: FundamentalsService):
        """EPS=0は無効"""
        assert service._has_valid_valuation_metrics(eps=0.0, bps=None) is False

    def test_negative_bps(self, service: FundamentalsService):
        """BPS<0は無効"""
        assert service._has_valid_valuation_metrics(eps=None, bps=-100.0) is False

    def test_both_none(self, service: FundamentalsService):
        """両方Noneは無効"""
        assert service._has_valid_valuation_metrics(eps=None, bps=None) is False


class TestPreviousPeriodCashFlow:
    """前期キャッシュフロー取得のテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    @pytest.fixture
    def statements(self) -> list[JQuantsStatement]:
        """テスト用財務諸表"""
        base = {
            "DiscDate": "2024-05-15",
            "Code": "7203",
            "DocType": "連結",
            "CurPerSt": "2023-04-01",
            "CurFYSt": "2023-04-01",
            "CurFYEn": "2024-03-31",
            "NxtFYSt": None,
            "NxtFYEn": None,
            "Sales": None,
            "OP": None,
            "OdP": None,
            "NP": None,
            "EPS": None,
            "DEPS": None,
            "TA": None,
            "Eq": None,
            "EqAR": None,
            "BPS": None,
            "ShOutFY": None,
            "TrShFY": None,
            "AvgSh": None,
            "FEPS": None,
            "NxFEPS": None,
            "NCSales": None,
            "NCOP": None,
            "NCOdP": None,
            "NCNP": None,
            "NCEPS": None,
            "NCTA": None,
            "NCEq": None,
            "NCEqAR": None,
            "NCBPS": None,
            "FNCEPS": None,
            "NxFNCEPS": None,
        }
        return [
            JQuantsStatement(
                **{
                    **base,
                    "CurPerType": "FY",
                    "CurPerEn": "2024-03-31",
                    "CFO": 6000000000000,
                    "CFI": -2000000000000,
                    "CFF": -1000000000000,
                    "CashEq": 8000000000000,
                }
            ),
            JQuantsStatement(
                **{
                    **base,
                    "CurPerType": "FY",
                    "CurPerEn": "2023-03-31",
                    "CFO": 5000000000000,
                    "CFI": -1500000000000,
                    "CFF": -800000000000,
                    "CashEq": 7000000000000,
                }
            ),
        ]

    def test_get_previous_period_cash_flow(
        self, service: FundamentalsService, statements: list[JQuantsStatement]
    ):
        """前期CFを正しく取得"""
        result = service._get_previous_period_cash_flow(
            "2024-03-31", "FY", statements
        )
        assert result["prevCashFlowOperating"] == 5000000.0
        assert result["prevCashFlowInvesting"] == -1500000.0

    def test_get_previous_period_no_match(
        self, service: FundamentalsService, statements: list[JQuantsStatement]
    ):
        """該当する前期がない場合"""
        result = service._get_previous_period_cash_flow(
            "2022-03-31", "FY", statements
        )
        assert result["prevCashFlowOperating"] is None

    def test_get_previous_period_invalid_date(self, service: FundamentalsService):
        """無効な日付"""
        result = service._get_previous_period_cash_flow(
            "invalid-date", "FY", []
        )
        assert result["prevCashFlowOperating"] is None


class TestCalculateDailyValuation:
    """日次バリュエーション計算の追加テスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_empty_prices(self, service: FundamentalsService):
        """価格データがない場合"""
        result = service._calculate_daily_valuation([], {}, True)
        assert result == []

    def test_empty_fy_data(self, service: FundamentalsService):
        """FYデータがない場合"""
        prices = {"2024-01-01": 6000.0}
        result = service._calculate_daily_valuation([], prices, True)
        assert result == []


class TestGetStockInfo:
    """銘柄情報取得のテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_get_stock_info_error_handling(self, service: FundamentalsService):
        """エラー時はNoneを返す"""
        mock_jquants = MagicMock()
        mock_jquants.get_stock_info.side_effect = Exception("Test error")
        service._jquants_client = mock_jquants

        result = service._get_stock_info("7203")
        assert result is None


class TestGetDailyStockPrices:
    """日次株価取得のテスト"""

    @pytest.fixture
    def service(self):
        return FundamentalsService()

    def test_get_daily_prices_empty_df(self, service: FundamentalsService):
        """空のDataFrameの場合"""
        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = pd.DataFrame()
        service._market_client = mock_market

        result = service._get_daily_stock_prices("7203")
        assert result == {}

    def test_get_daily_prices_error(self, service: FundamentalsService):
        """エラー時は空dictを返す"""
        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.side_effect = Exception("Test error")
        service._market_client = mock_market

        result = service._get_daily_stock_prices("7203")
        assert result == {}
