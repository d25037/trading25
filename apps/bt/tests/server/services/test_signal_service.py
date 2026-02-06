"""
SignalService テスト

OHLCV系シグナルの計算テスト
"""

import threading
from unittest.mock import patch

import pandas as pd
import pytest

from datetime import date

from src.server.services.signal_service import (
    PHASE1_SIGNAL_NAMES,
    SignalService,
    _SIGNAL_DEFINITION_MAP,
    _build_signal_definition_map,
    _extract_trigger_dates,
    _get_signal_definition,
)


class TestGetSignalDefinition:
    """_get_signal_definition関数のテスト"""

    def test_volume_signal(self):
        """出来高シグナルの定義取得"""
        sig_def = _get_signal_definition("volume")
        assert sig_def is not None
        assert sig_def.name == "出来高"
        assert sig_def.category == "volume"

    def test_rsi_threshold_signal(self):
        """RSI閾値シグナルの定義取得"""
        sig_def = _get_signal_definition("rsi_threshold")
        assert sig_def is not None
        assert sig_def.name == "RSI閾値"
        assert sig_def.category == "oscillator"

    def test_unknown_signal(self):
        """未知のシグナル"""
        sig_def = _get_signal_definition("unknown_signal")
        assert sig_def is None


class TestPhase1SignalNames:
    """Phase 1対象シグナル名のテスト"""

    def test_contains_expected_signals(self):
        """必須シグナルが含まれていることを確認"""
        expected = {
            "rsi_threshold",
            "rsi_spread",
            "period_breakout",
            "ma_breakout",
            "volume",
            "trading_value",
            "bollinger_bands",
        }
        assert expected.issubset(PHASE1_SIGNAL_NAMES)

    def test_excludes_fundamental_signals(self):
        """ファンダメンタルシグナルが除外されていることを確認"""
        fundamental_signals = {"per", "roe", "pbr"}
        assert not fundamental_signals.intersection(PHASE1_SIGNAL_NAMES)


class TestSignalService:
    """SignalServiceのテスト"""

    @pytest.fixture
    def service(self):
        return SignalService()

    def test_init(self, service):
        """インスタンス初期化"""
        assert service._market_client is None

    def test_phase1_signal_validation(self, service):
        """Phase 1非対応シグナルの拒否テスト"""
        import pandas as pd

        # ダミーデータ
        data = {
            "ohlc_data": pd.DataFrame({
                "Open": [100.0],
                "High": [105.0],
                "Low": [95.0],
                "Close": [102.0],
            }),
            "close": pd.Series([102.0]),
            "execution_close": pd.Series([102.0]),
            "volume": pd.Series([1000]),
        }

        # Phase 1非対応シグナル（例: per）はエラーになるべき
        with pytest.raises(ValueError, match="Phase 1では未対応"):
            service.compute_signal("per", {}, "entry", data)


class TestSignalDefinitionMap:
    """シグナル定義マッピングのテスト"""

    def test_map_is_populated(self):
        """マッピングが構築されていることを確認"""
        assert len(_SIGNAL_DEFINITION_MAP) > 0

    def test_build_signal_definition_map(self):
        """_build_signal_definition_map関数のテスト"""
        mapping = _build_signal_definition_map()
        assert isinstance(mapping, dict)
        assert "volume" in mapping
        assert "rsi_threshold" in mapping

    def test_nested_param_key_extraction(self):
        """ネストされたparam_key（例: fundamental.per）からsignal_typeを正しく抽出"""
        # fundamental.per -> "per" として登録されているか確認
        mapping = _build_signal_definition_map()
        assert "per" in mapping
        assert mapping["per"].param_key == "fundamental.per"


class TestThreadSafety:
    """スレッドセーフティのテスト"""

    def test_market_client_lazy_initialization(self):
        """market_clientの遅延初期化テスト"""
        service = SignalService()
        assert service._market_client is None

    def test_market_client_double_check_locking(self):
        """ダブルチェックロッキングのテスト"""
        service = SignalService()
        results = []
        errors = []

        def access_client():
            try:
                client = service.market_client
                results.append(client)
            except Exception as e:
                errors.append(e)

        # 複数スレッドから同時にアクセス
        threads = [threading.Thread(target=access_client) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # エラーがないことを確認
        assert len(errors) == 0
        # すべて同じインスタンスを取得していることを確認
        assert all(r is results[0] for r in results)


class TestEmptySignalsValidation:
    """空シグナルリストのバリデーションテスト"""

    def test_empty_signals_returns_early(self):
        """空シグナルリストの場合、OHLCVロードせずに早期リターン"""
        service = SignalService()

        # load_ohlcvが呼ばれないことを確認
        with patch.object(service, "load_ohlcv") as mock_load:
            result = service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[],
            )

            # load_ohlcvは呼ばれていない
            mock_load.assert_not_called()

            # 結果は空のsignals辞書を含む
            assert result["stock_code"] == "7203"
            assert result["timeframe"] == "daily"
            assert result["signals"] == {}


class TestExceptionHandling:
    """例外ハンドリングのテスト"""

    def test_signal_computation_error_logged_with_traceback(self):
        """シグナル計算エラー時にスタックトレースがログ出力される"""
        service = SignalService()

        # OHLCVデータをモック
        mock_ohlcv = pd.DataFrame({
            "Open": [100.0, 101.0, 102.0],
            "High": [105.0, 106.0, 107.0],
            "Low": [95.0, 96.0, 97.0],
            "Close": [102.0, 103.0, 104.0],
            "Volume": [1000, 1100, 1200],
        }, index=pd.date_range("2025-01-01", periods=3))

        with patch.object(service, "load_ohlcv", return_value=mock_ohlcv):
            with patch("src.server.services.signal_service.logger") as mock_logger:
                # 未知のシグナルを含むリクエスト
                result = service.compute_signals(
                    stock_code="7203",
                    source="market",
                    timeframe="daily",
                    signals=[{"type": "unknown_signal", "params": {}}],
                )

                # logger.exceptionが呼ばれたことを確認
                mock_logger.exception.assert_called()
                call_args = mock_logger.exception.call_args[0][0]
                assert "unknown_signal" in call_args

                # エラー情報が結果に含まれる
                assert "error" in result["signals"]["unknown_signal"]


class TestExtractTriggerDates:
    """_extract_trigger_dates関数のテスト（NaN/Inf検証対応）"""

    def test_extracts_true_values(self):
        """Trueの日付を正しく抽出"""
        series = pd.Series(
            [True, False, True, False],
            index=pd.date_range("2025-01-01", periods=4),
        )
        result = _extract_trigger_dates(series)
        assert result == ["2025-01-01", "2025-01-03"]

    def test_handles_nan_values(self):
        """NaN値を安全にスキップ"""
        series = pd.Series(
            [True, None, True, False],
            index=pd.date_range("2025-01-01", periods=4),
        )
        result = _extract_trigger_dates(series)
        # NaN（None）はスキップされ、Trueのみ抽出
        assert result == ["2025-01-01", "2025-01-03"]

    def test_handles_all_nan(self):
        """全てNaNの場合は空リストを返す"""
        series = pd.Series(
            [None, None, None],
            index=pd.date_range("2025-01-01", periods=3),
        )
        result = _extract_trigger_dates(series)
        assert result == []

    def test_handles_empty_series(self):
        """空のシリーズの場合は空リストを返す"""
        series = pd.Series([], dtype=bool)
        result = _extract_trigger_dates(series)
        assert result == []

    def test_handles_all_false(self):
        """全てFalseの場合は空リストを返す"""
        series = pd.Series(
            [False, False, False],
            index=pd.date_range("2025-01-01", periods=3),
        )
        result = _extract_trigger_dates(series)
        assert result == []


class TestDateRangeValidation:
    """日付範囲バリデーションのテスト"""

    def test_invalid_date_range_raises_error(self):
        """start_date > end_dateの場合エラー"""
        service = SignalService()

        with pytest.raises(ValueError, match="無効な日付範囲"):
            service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[{"type": "volume", "params": {}}],
                start_date=date(2025, 12, 31),
                end_date=date(2025, 1, 1),
            )

    def test_valid_date_range_no_error(self):
        """有効な日付範囲はエラーにならない"""
        service = SignalService()

        # OHLCVデータをモック
        mock_ohlcv = pd.DataFrame({
            "Open": [100.0, 101.0, 102.0],
            "High": [105.0, 106.0, 107.0],
            "Low": [95.0, 96.0, 97.0],
            "Close": [102.0, 103.0, 104.0],
            "Volume": [1000, 1100, 1200],
        }, index=pd.date_range("2025-01-01", periods=3))

        with patch.object(service, "load_ohlcv", return_value=mock_ohlcv):
            # エラーが発生しないことを確認
            result = service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[],  # 空シグナルで早期リターン
                start_date=date(2025, 1, 1),
                end_date=date(2025, 12, 31),
            )
            assert result is not None

    def test_none_dates_no_validation(self):
        """Noneの日付はバリデーションをスキップ"""
        service = SignalService()

        with patch.object(service, "load_ohlcv"):
            result = service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[],
                start_date=None,
                end_date=None,
            )
            # エラーなく早期リターン
            assert result["signals"] == {}


class TestResampleDataQualityValidation:
    """リサンプル後のデータ品質検証テスト"""

    def test_empty_resampled_data_raises_error(self):
        """リサンプル後に空になった場合エラー"""
        service = SignalService()

        # 元データは存在するが、リサンプル後に空になるケース
        mock_ohlcv = pd.DataFrame({
            "Open": [100.0],
            "High": [105.0],
            "Low": [95.0],
            "Close": [None],  # NaNでClose全欠損
            "Volume": [1000],
        }, index=pd.date_range("2025-01-01", periods=1))

        with patch.object(service, "load_ohlcv", return_value=mock_ohlcv):
            with pytest.raises(ValueError, match="リサンプル後データが不足"):
                service.compute_signals(
                    stock_code="7203",
                    source="market",
                    timeframe="weekly",  # リサンプルでCloseがNaNになりdropnaで空に
                    signals=[{"type": "volume", "params": {}}],
                )

    def test_valid_resampled_data_no_error(self):
        """有効なリサンプルデータはエラーにならない"""
        service = SignalService()

        # 十分なデータがあるケース
        mock_ohlcv = pd.DataFrame({
            "Open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "High": [105.0, 106.0, 107.0, 108.0, 109.0],
            "Low": [95.0, 96.0, 97.0, 98.0, 99.0],
            "Close": [102.0, 103.0, 104.0, 105.0, 106.0],
            "Volume": [1000, 1100, 1200, 1300, 1400],
        }, index=pd.date_range("2025-01-01", periods=5))

        with patch.object(service, "load_ohlcv", return_value=mock_ohlcv):
            # エラーが発生しないことを確認
            result = service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[],  # 空シグナルで早期リターンを回避するためsignalsを空に
            )
            assert result is not None
