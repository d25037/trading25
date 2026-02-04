"""
Signals API Tests

/api/signals/compute エンドポイントのテスト
"""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.server.app import app
from src.server.schemas.signals import (
    SignalComputeRequest,
    SignalComputeResponse,
    SignalResult,
    SignalSpec,
)


@pytest.fixture
def client() -> TestClient:
    """FastAPIテストクライアント"""
    return TestClient(app)


class TestSignalComputeRequestSchema:
    """SignalComputeRequestのバリデーションテスト"""

    def test_default_values(self) -> None:
        """デフォルト値が設定される"""
        req = SignalComputeRequest(
            stock_code="7203",
            signals=[SignalSpec(type="volume")],
        )
        assert req.stock_code == "7203"
        assert req.source == "market"
        assert req.timeframe == "daily"
        assert req.start_date is None
        assert req.end_date is None

    def test_max_signals_validation(self) -> None:
        """シグナル数の上限バリデーション"""
        # 5個以内は有効
        req = SignalComputeRequest(
            stock_code="7203",
            signals=[SignalSpec(type="volume") for _ in range(5)],
        )
        assert len(req.signals) == 5

    def test_signal_spec_default_mode(self) -> None:
        """SignalSpecのデフォルトmode"""
        spec = SignalSpec(type="rsi_threshold")
        assert spec.mode == "entry"
        assert spec.params == {}


class TestSignalComputeResponseSchema:
    """SignalComputeResponseのバリデーションテスト"""

    def test_valid_response(self) -> None:
        """有効なレスポンス"""
        response = SignalComputeResponse(
            stock_code="7203",
            timeframe="daily",
            signals={
                "volume": SignalResult(trigger_dates=["2025-01-15"], count=1),
            },
        )
        assert response.stock_code == "7203"
        assert response.signals["volume"].count == 1

    def test_signal_result_with_error(self) -> None:
        """エラーを含むSignalResult"""
        result = SignalResult(trigger_dates=[], count=0, error="Phase 1では未対応")
        assert result.error == "Phase 1では未対応"


class TestSignalComputeEndpoint:
    """POST /api/signals/compute エンドポイントのテスト"""

    @pytest.fixture
    def mock_ohlcv(self) -> pd.DataFrame:
        """モックOHLCVデータ"""
        return pd.DataFrame({
            "Open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "High": [105.0, 106.0, 107.0, 108.0, 109.0],
            "Low": [95.0, 96.0, 97.0, 98.0, 99.0],
            "Close": [102.0, 103.0, 104.0, 105.0, 106.0],
            "Volume": [1000, 1100, 1200, 1300, 1400],
        }, index=pd.date_range("2025-01-01", periods=5))

    def test_compute_empty_signals_returns_empty(self, client: TestClient) -> None:
        """空のシグナルリストは空の結果を返す"""
        response = client.post(
            "/api/signals/compute",
            json={
                "stock_code": "7203",
                "signals": [],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["stock_code"] == "7203"
        assert data["signals"] == {}

    def test_compute_invalid_signal_type(self, client: TestClient, mock_ohlcv: pd.DataFrame) -> None:
        """Phase 1非対応シグナルはエラー情報を含む"""
        with patch(
            "src.server.services.signal_service.SignalService.load_ohlcv",
            return_value=mock_ohlcv,
        ):
            response = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "signals": [{"type": "per", "params": {}}],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert "per" in data["signals"]
            assert "error" in data["signals"]["per"]
            assert "Phase 1では未対応" in data["signals"]["per"]["error"]

    def test_compute_valid_signal(self, client: TestClient, mock_ohlcv: pd.DataFrame) -> None:
        """有効なシグナル計算"""
        with patch(
            "src.server.services.signal_service.SignalService.load_ohlcv",
            return_value=mock_ohlcv,
        ):
            response = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "timeframe": "daily",
                    # volume threshold は 0-10 の範囲なので、適切な値を指定
                    "signals": [{"type": "volume", "params": {"threshold": 1.5}}],
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["stock_code"] == "7203"
            assert data["timeframe"] == "daily"
            assert "volume" in data["signals"]
            # エラーがないことを確認
            assert "error" not in data["signals"]["volume"] or data["signals"]["volume"]["error"] is None

    def test_compute_missing_stock_code(self, client: TestClient) -> None:
        """stock_codeなしでリクエストするとバリデーションエラー"""
        response = client.post(
            "/api/signals/compute",
            json={
                "signals": [{"type": "volume"}],
            },
        )
        assert response.status_code == 422

    def test_compute_invalid_timeframe(self, client: TestClient) -> None:
        """無効なtimeframeはバリデーションエラー"""
        response = client.post(
            "/api/signals/compute",
            json={
                "stock_code": "7203",
                "timeframe": "invalid",
                "signals": [{"type": "volume"}],
            },
        )
        assert response.status_code == 422


class TestSignalReferenceEndpoint:
    """GET /api/signals/reference エンドポイントのテスト"""

    def test_get_reference(self, client: TestClient) -> None:
        """シグナルリファレンス取得"""
        response = client.get("/api/signals/reference")
        assert response.status_code == 200
        data = response.json()
        assert "signals" in data
        assert len(data["signals"]) > 0


class TestSignalSchemaEndpoint:
    """GET /api/signals/schema エンドポイントのテスト"""

    def test_get_schema(self, client: TestClient) -> None:
        """SignalParams JSON Schema取得"""
        response = client.get("/api/signals/schema")
        assert response.status_code == 200
        data = response.json()
        assert "type" in data or "properties" in data
