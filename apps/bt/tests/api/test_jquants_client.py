"""
JQuantsAPIClient テスト

JQuants API クライアントの単体テスト
"""

from unittest.mock import patch

import pytest

from src.api.exceptions import APIConnectionError, APINotFoundError, APITimeoutError
from src.api.jquants_client import JQuantsAPIClient, JQuantsStatement, StockInfo


class TestJQuantsStatementModel:
    """JQuantsStatement Pydanticモデルのテスト"""

    def test_full_statement_parsing(self):
        """全フィールドが正しくパースされる"""
        data = {
            "DiscDate": "2024-05-15",
            "Code": "7203",
            "DocType": "有価証券報告書・連結",
            "CurPerType": "FY",
            "CurPerSt": "2023-04-01",
            "CurPerEn": "2024-03-31",
            "CurFYSt": "2023-04-01",
            "CurFYEn": "2024-03-31",
            "NxtFYSt": "2024-04-01",
            "NxtFYEn": "2025-03-31",
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
        stmt = JQuantsStatement.model_validate(data)

        assert stmt.DiscDate == "2024-05-15"
        assert stmt.Code == "7203"
        assert stmt.CurPerType == "FY"
        assert stmt.EPS == 300.0
        assert stmt.BPS == 2250.0
        assert stmt.NP == 4000000000000

    def test_minimal_statement_parsing(self):
        """必須フィールドのみでパースできる"""
        data = {
            "DiscDate": "2024-05-15",
            "Code": "7203",
            "DocType": "連結",
            "CurPerType": "FY",
            "CurPerSt": "2023-04-01",
            "CurPerEn": "2024-03-31",
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
        stmt = JQuantsStatement.model_validate(data)

        assert stmt.DiscDate == "2024-05-15"
        assert stmt.Code == "7203"
        assert stmt.EPS is None
        assert stmt.BPS is None

    def test_empty_string_converted_to_none(self):
        """空文字列がNoneに変換される（JQuants APIが空文字列を返す場合がある）"""
        data = {
            "DiscDate": "2024-05-15",
            "Code": "7203",
            "DocType": "連結",
            "CurPerType": "FY",
            "CurPerSt": "2023-04-01",
            "CurPerEn": "2024-03-31",
            "CurFYSt": "2023-04-01",
            "CurFYEn": "2024-03-31",
            "NxtFYSt": None,
            "NxtFYEn": None,
            "Sales": "",  # Empty string from JQuants API
            "OP": "",
            "OdP": None,
            "NP": 4000000000000,
            "EPS": "",  # Empty string
            "DEPS": None,
            "TA": None,
            "Eq": None,
            "EqAR": None,
            "BPS": 2250.0,
            "CFO": None,
            "CFI": None,
            "CFF": None,
            "CashEq": None,
            "ShOutFY": None,
            "TrShFY": None,
            "AvgSh": None,
            "FEPS": "",  # Empty string
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
            "FNCEPS": "",  # Empty string
            "NxFNCEPS": "",  # Empty string (this was causing the original error)
        }
        stmt = JQuantsStatement.model_validate(data)

        # Empty strings should be converted to None
        assert stmt.Sales is None
        assert stmt.OP is None
        assert stmt.EPS is None
        assert stmt.FEPS is None
        assert stmt.FNCEPS is None
        assert stmt.NxFNCEPS is None
        # Valid values should remain
        assert stmt.NP == 4000000000000
        assert stmt.BPS == 2250.0


class TestStockInfoModel:
    """StockInfo Pydanticモデルのテスト"""

    def test_stock_info_parsing(self):
        """StockInfoが正しくパースされる"""
        data = {
            "code": "7203",
            "companyName": "トヨタ自動車",
            "companyNameEnglish": "Toyota Motor Corporation",
            "marketCode": "0111",
            "marketName": "プライム",
            "sector17Code": "16",
            "sector17Name": "自動車・輸送機",
            "sector33Code": "3250",
            "sector33Name": "自動車・輸送機",
            "scaleCategory": "TOPIX Large70",
            "listedDate": "1949-05-01",
        }
        info = StockInfo.model_validate(data)

        assert info.code == "7203"
        assert info.companyName == "トヨタ自動車"
        assert info.marketName == "プライム"
        assert info.sector17Name == "自動車・輸送機"


class TestJQuantsAPIClientInit:
    """JQuantsAPIClient初期化テスト"""

    def test_inheritance(self):
        """BaseAPIClientを継承している"""
        from src.api.client import BaseAPIClient

        client = JQuantsAPIClient()
        assert isinstance(client, BaseAPIClient)

    def test_context_manager(self):
        """コンテキストマネージャーとして使用可能"""
        with JQuantsAPIClient() as client:
            assert client is not None


class TestGetStatements:
    """get_statementsメソッドのテスト"""

    def test_get_statements_success(self):
        """正常にステートメントを取得"""
        mock_response = {
            "data": [
                {
                    "DiscDate": "2024-05-15",
                    "Code": "7203",
                    "DocType": "連結",
                    "CurPerType": "FY",
                    "CurPerSt": "2023-04-01",
                    "CurPerEn": "2024-03-31",
                    "CurFYSt": "2023-04-01",
                    "CurFYEn": "2024-03-31",
                    "NxtFYSt": None,
                    "NxtFYEn": None,
                    "Sales": 1000000,
                    "OP": None,
                    "OdP": None,
                    "NP": None,
                    "EPS": 100.0,
                    "DEPS": None,
                    "TA": None,
                    "Eq": None,
                    "EqAR": None,
                    "BPS": 1000.0,
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
            ]
        }

        with patch.object(JQuantsAPIClient, "_get", return_value=mock_response):
            client = JQuantsAPIClient()
            statements = client.get_statements("7203")

            assert len(statements) == 1
            assert statements[0].Code == "7203"
            assert statements[0].EPS == 100.0

    def test_get_statements_empty(self):
        """データが空の場合"""
        with patch.object(JQuantsAPIClient, "_get", return_value={"data": []}):
            client = JQuantsAPIClient()
            statements = client.get_statements("9999")

            assert statements == []

    def test_get_statements_invalid_response(self):
        """不正なレスポンス形式"""
        with patch.object(JQuantsAPIClient, "_get", return_value="invalid"):
            client = JQuantsAPIClient()
            statements = client.get_statements("7203")

            assert statements == []

    def test_get_statements_no_data_key(self):
        """dataキーがない場合"""
        with patch.object(JQuantsAPIClient, "_get", return_value={"other": []}):
            client = JQuantsAPIClient()
            statements = client.get_statements("7203")

            assert statements == []


class TestGetStockInfo:
    """get_stock_infoメソッドのテスト"""

    def test_get_stock_info_success(self):
        """正常に銘柄情報を取得"""
        mock_response = {
            "code": "7203",
            "companyName": "トヨタ自動車",
            "companyNameEnglish": "Toyota Motor Corporation",
            "marketCode": "0111",
            "marketName": "プライム",
            "sector17Code": "16",
            "sector17Name": "自動車・輸送機",
            "sector33Code": "3250",
            "sector33Name": "自動車・輸送機",
            "scaleCategory": "TOPIX Large70",
            "listedDate": "1949-05-01",
        }

        with patch.object(JQuantsAPIClient, "_get", return_value=mock_response):
            client = JQuantsAPIClient()
            info = client.get_stock_info("7203")

            assert info is not None
            assert info.code == "7203"
            assert info.companyName == "トヨタ自動車"

    def test_get_stock_info_not_found(self):
        """銘柄が見つからない場合"""
        with patch.object(
            JQuantsAPIClient, "_get", side_effect=APINotFoundError("Not found")
        ):
            client = JQuantsAPIClient()
            info = client.get_stock_info("9999")

            assert info is None

    def test_get_stock_info_connection_error_propagates(self):
        """接続エラーは伝播する"""
        with patch.object(
            JQuantsAPIClient, "_get", side_effect=APIConnectionError("Connection failed")
        ):
            client = JQuantsAPIClient()

            with pytest.raises(APIConnectionError):
                client.get_stock_info("7203")

    def test_get_stock_info_timeout_error_propagates(self):
        """タイムアウトエラーは伝播する"""
        with patch.object(
            JQuantsAPIClient, "_get", side_effect=APITimeoutError("Timeout")
        ):
            client = JQuantsAPIClient()

            with pytest.raises(APITimeoutError):
                client.get_stock_info("7203")

    def test_get_stock_info_invalid_response(self):
        """不正なレスポンス形式 (dictでない)"""
        with patch.object(JQuantsAPIClient, "_get", return_value="invalid"):
            client = JQuantsAPIClient()
            info = client.get_stock_info("7203")

            assert info is None


class TestGetMarginInterest:
    """get_margin_interestメソッドのテスト"""

    def test_get_margin_interest_success(self):
        """正常に信用残データを取得"""
        mock_response = {
            "symbol": "7203",
            "marginInterest": [
                {
                    "date": "2024-01-05",
                    "code": "7203",
                    "shortMarginTradeVolume": 100000,
                    "longMarginTradeVolume": 500000,
                },
                {
                    "date": "2024-01-12",
                    "code": "7203",
                    "shortMarginTradeVolume": 120000,
                    "longMarginTradeVolume": 480000,
                },
            ],
            "lastUpdated": "2024-01-15T10:00:00Z",
        }

        with patch.object(JQuantsAPIClient, "_get", return_value=mock_response):
            client = JQuantsAPIClient()
            df = client.get_margin_interest("7203")

            assert len(df) == 2
            assert "longMarginVolume" in df.columns
            assert "shortMarginVolume" in df.columns
            assert df.iloc[0]["longMarginVolume"] == 500000
            assert df.iloc[0]["shortMarginVolume"] == 100000

    def test_get_margin_interest_with_date_range(self):
        """日付範囲を指定して取得"""
        mock_response = {
            "symbol": "7203",
            "marginInterest": [
                {
                    "date": "2024-01-05",
                    "code": "7203",
                    "shortMarginTradeVolume": 100000,
                    "longMarginTradeVolume": 500000,
                },
            ],
            "lastUpdated": "2024-01-15T10:00:00Z",
        }

        with patch.object(JQuantsAPIClient, "_get", return_value=mock_response) as mock_get:
            client = JQuantsAPIClient()
            df = client.get_margin_interest("7203", "2024-01-01", "2024-01-31")

            # Check that the request includes date parameters
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert call_args[1]["params"]["from"] == "2024-01-01"
            assert call_args[1]["params"]["to"] == "2024-01-31"
            assert len(df) == 1

    def test_get_margin_interest_empty(self):
        """データが空の場合"""
        mock_response = {
            "symbol": "9999",
            "marginInterest": [],
            "lastUpdated": "2024-01-15T10:00:00Z",
        }

        with patch.object(JQuantsAPIClient, "_get", return_value=mock_response):
            client = JQuantsAPIClient()
            df = client.get_margin_interest("9999")

            assert df.empty

    def test_get_margin_interest_invalid_response(self):
        """不正なレスポンス形式"""
        with patch.object(JQuantsAPIClient, "_get", return_value="invalid"):
            client = JQuantsAPIClient()
            df = client.get_margin_interest("7203")

            assert df.empty

    def test_get_margin_interest_no_margin_key(self):
        """marginInterestキーがない場合"""
        with patch.object(JQuantsAPIClient, "_get", return_value={"other": []}):
            client = JQuantsAPIClient()
            df = client.get_margin_interest("7203")

            assert df.empty
