"""
OHLCV Resample API Endpoint Tests
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app


@pytest.fixture
def client():
    """テスト用クライアント"""
    app = create_app()
    return TestClient(app)


@pytest.fixture
def mock_ohlcv_data() -> pd.DataFrame:
    """テスト用OHLCVデータ"""
    dates = pd.date_range("2024-01-08", periods=10, freq="B")  # 営業日
    return pd.DataFrame({
        "Open": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        "High": [105, 106, 107, 108, 109, 110, 111, 112, 113, 114],
        "Low": [95, 96, 97, 98, 99, 100, 101, 102, 103, 104],
        "Close": [102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
        "Volume": [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900],
    }, index=dates).astype(float)


@pytest.fixture
def mock_benchmark_data() -> pd.DataFrame:
    """テスト用ベンチマークデータ"""
    dates = pd.date_range("2024-01-08", periods=10, freq="B")
    return pd.DataFrame({
        "Open": [2000, 2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090],
        "High": [2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130, 2140],
        "Low": [1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020, 2030, 2040],
        "Close": [2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100],
        "Volume": [100000, 110000, 120000, 130000, 140000, 150000, 160000, 170000, 180000, 190000],
    }, index=dates).astype(float)


class TestResampleEndpoint:
    """POST /api/ohlcv/resample テスト"""

    def test_resample_weekly(self, client, mock_ohlcv_data):
        """週足リサンプルが正しく動作すること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            mock_service = MockService.return_value
            mock_service.load_ohlcv.return_value = mock_ohlcv_data
            mock_service.resample_timeframe.return_value = mock_ohlcv_data.resample("W").agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }).dropna(subset=["Close"])

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "7203",
                "source": "market",
                "timeframe": "weekly",
            })

            assert response.status_code == 200
            data = response.json()
            assert data["stock_code"] == "7203"
            assert data["timeframe"] == "weekly"
            assert "data" in data
            assert "meta" in data

    def test_resample_with_dates(self, client, mock_ohlcv_data):
        """日付範囲指定が正しく動作すること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            mock_service = MockService.return_value
            mock_service.load_ohlcv.return_value = mock_ohlcv_data
            mock_service.resample_timeframe.return_value = mock_ohlcv_data.resample("W").agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }).dropna(subset=["Close"])

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "7203",
                "source": "market",
                "timeframe": "weekly",
                "start_date": "2024-01-01",
                "end_date": "2024-02-01",
            })

            assert response.status_code == 200
            # load_ohlcvに日付が渡されることを確認
            call_args = mock_service.load_ohlcv.call_args
            assert call_args[0][0] == "7203"

    def test_resample_with_benchmark(self, client, mock_ohlcv_data, mock_benchmark_data):
        """ベンチマーク指定（相対OHLC）が正しく動作すること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            with patch("src.server.routes.ohlcv.calculate_relative_ohlcv") as mock_relative:
                mock_service = MockService.return_value
                mock_service.load_ohlcv.return_value = mock_ohlcv_data
                mock_service.load_benchmark_ohlcv.return_value = mock_benchmark_data

                # 相対OHLC計算後のデータ
                relative_df = mock_ohlcv_data.copy()
                relative_df["Open"] = mock_ohlcv_data["Open"] / mock_benchmark_data["Open"]
                relative_df["High"] = mock_ohlcv_data["High"] / mock_benchmark_data["High"]
                relative_df["Low"] = mock_ohlcv_data["Low"] / mock_benchmark_data["Low"]
                relative_df["Close"] = mock_ohlcv_data["Close"] / mock_benchmark_data["Close"]
                mock_relative.return_value = relative_df

                mock_service.resample_timeframe.return_value = relative_df.resample("W").agg({
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                    "Volume": "sum",
                }).dropna(subset=["Close"])

                response = client.post("/api/ohlcv/resample", json={
                    "stock_code": "7203",
                    "source": "market",
                    "timeframe": "weekly",
                    "benchmark_code": "topix",
                })

                assert response.status_code == 200
                data = response.json()
                assert data["benchmark_code"] == "topix"
                # 相対OHLCが計算されたことを確認
                mock_relative.assert_called_once()

    def test_resample_daily_no_resample(self, client, mock_ohlcv_data):
        """日足指定時は変換なしで返却されること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            mock_service = MockService.return_value
            mock_service.load_ohlcv.return_value = mock_ohlcv_data
            mock_service.resample_timeframe.return_value = mock_ohlcv_data

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "7203",
                "source": "market",
                "timeframe": "daily",
            })

            assert response.status_code == 200
            data = response.json()
            assert data["timeframe"] == "daily"
            assert len(data["data"]) == 10  # 元のデータ件数

    def test_resample_stock_not_found(self, client):
        """銘柄が見つからない場合に404を返すこと"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            mock_service = MockService.return_value
            mock_service.load_ohlcv.side_effect = ValueError("銘柄 9999 のOHLCVデータが取得できません")

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "9999",
                "source": "market",
                "timeframe": "weekly",
            })

            assert response.status_code == 404
            assert "取得できません" in response.json()["detail"]

    def test_resample_invalid_benchmark(self, client):
        """無効なベンチマークコードで422を返すこと"""
        response = client.post("/api/ohlcv/resample", json={
            "stock_code": "7203",
            "source": "market",
            "timeframe": "weekly",
            "benchmark_code": "invalid",
        })

        assert response.status_code == 422
        # Pydanticバリデーションエラー


class TestResampleResponse:
    """レスポンス形式のテスト"""

    def test_response_structure(self, client, mock_ohlcv_data):
        """レスポンス構造が正しいこと"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            mock_service = MockService.return_value
            mock_service.load_ohlcv.return_value = mock_ohlcv_data
            mock_service.resample_timeframe.return_value = mock_ohlcv_data.head(2)

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "7203",
                "source": "market",
                "timeframe": "weekly",
            })

            assert response.status_code == 200
            data = response.json()

            # 必須フィールド
            assert "stock_code" in data
            assert "timeframe" in data
            assert "meta" in data
            assert "data" in data

            # metaフィールド
            assert "source_bars" in data["meta"]
            assert "resampled_bars" in data["meta"]

            # dataフィールド
            if len(data["data"]) > 0:
                record = data["data"][0]
                assert "date" in record
                assert "open" in record
                assert "high" in record
                assert "low" in record
                assert "close" in record
                assert "volume" in record

    def test_ohlcv_values_rounded(self, client, mock_ohlcv_data):
        """OHLCV値が適切に丸められること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            # 小数点を含むデータ
            mock_data = mock_ohlcv_data.copy()
            mock_data["Open"] = mock_data["Open"] + 0.123456789

            mock_service = MockService.return_value
            mock_service.load_ohlcv.return_value = mock_data
            mock_service.resample_timeframe.return_value = mock_data.head(1)

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "7203",
                "source": "market",
                "timeframe": "daily",
            })

            assert response.status_code == 200
            data = response.json()

            # 4桁に丸められていること
            record = data["data"][0]
            open_str = str(record["open"])
            if "." in open_str:
                decimals = len(open_str.split(".")[1])
                assert decimals <= 4


class TestRelativeOptions:
    """相対OHLCオプションのテスト"""

    def test_handle_zero_division_skip(self, client, mock_ohlcv_data, mock_benchmark_data):
        """handleZeroDivision: skipが正しく渡されること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            with patch("src.server.routes.ohlcv.calculate_relative_ohlcv") as mock_relative:
                mock_service = MockService.return_value
                mock_service.load_ohlcv.return_value = mock_ohlcv_data
                mock_service.load_benchmark_ohlcv.return_value = mock_benchmark_data
                mock_relative.return_value = mock_ohlcv_data
                mock_service.resample_timeframe.return_value = mock_ohlcv_data

                response = client.post("/api/ohlcv/resample", json={
                    "stock_code": "7203",
                    "source": "market",
                    "timeframe": "weekly",
                    "benchmark_code": "topix",
                    "relative_options": {
                        "handle_zero_division": "skip",
                    },
                })

                assert response.status_code == 200
                # calculate_relative_ohlcvに正しいオプションが渡されること
                mock_relative.assert_called_once()
                call_args = mock_relative.call_args
                assert call_args[0][2] == "skip"

    def test_handle_zero_division_zero(self, client, mock_ohlcv_data, mock_benchmark_data):
        """handleZeroDivision: zeroが正しく渡されること"""
        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            with patch("src.server.routes.ohlcv.calculate_relative_ohlcv") as mock_relative:
                mock_service = MockService.return_value
                mock_service.load_ohlcv.return_value = mock_ohlcv_data
                mock_service.load_benchmark_ohlcv.return_value = mock_benchmark_data
                mock_relative.return_value = mock_ohlcv_data
                mock_service.resample_timeframe.return_value = mock_ohlcv_data

                response = client.post("/api/ohlcv/resample", json={
                    "stock_code": "7203",
                    "source": "market",
                    "timeframe": "weekly",
                    "benchmark_code": "topix",
                    "relative_options": {
                        "handle_zero_division": "zero",
                    },
                })

                assert response.status_code == 200
                call_args = mock_relative.call_args
                assert call_args[0][2] == "zero"


class TestCleanValueFunction:
    """_clean_value関数のテスト"""

    def test_nan_to_none(self, client, mock_ohlcv_data):
        """NaN値がNoneに変換されること"""
        from src.server.services.indicator_service import _clean_value

        assert _clean_value(np.nan) is None

    def test_inf_to_none(self, client, mock_ohlcv_data):
        """Inf値がNoneに変換されること"""
        from src.server.services.indicator_service import _clean_value

        assert _clean_value(np.inf) is None
        assert _clean_value(-np.inf) is None

    def test_valid_value_rounded(self, client, mock_ohlcv_data):
        """有効な値が4桁に丸められること"""
        from src.server.services.indicator_service import _clean_value

        # _clean_valueは常に4桁で丸める（indicator_serviceで定義）
        assert _clean_value(1.23456789) == 1.2346
        assert _clean_value(100.0) == 100.0


class TestResampleMonthly:
    """月足リサンプルのエンドポイントテスト"""

    def test_resample_monthly(self, client, mock_ohlcv_data):
        """月足リサンプルが正しく動作すること"""
        # 30日分のデータを作成
        dates = pd.date_range("2024-01-02", periods=30, freq="B")
        monthly_data = pd.DataFrame({
            "Open": [100.0 + i for i in range(30)],
            "High": [105.0 + i for i in range(30)],
            "Low": [95.0 + i for i in range(30)],
            "Close": [102.0 + i for i in range(30)],
            "Volume": [1000.0 + i*100 for i in range(30)],
        }, index=dates).astype(float)

        with patch("src.server.routes.ohlcv.IndicatorService") as MockService:
            mock_service = MockService.return_value
            mock_service.load_ohlcv.return_value = monthly_data
            mock_service.resample_timeframe.return_value = monthly_data.resample("ME").agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }).dropna(subset=["Close"])

            response = client.post("/api/ohlcv/resample", json={
                "stock_code": "7203",
                "source": "market",
                "timeframe": "monthly",
            })

            assert response.status_code == 200
            data = response.json()
            assert data["timeframe"] == "monthly"
