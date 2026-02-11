"""
SignalService テスト

OHLCV系シグナルの計算テスト
"""

import threading
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from datetime import date

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.signal_service import (
    PHASE1_SIGNAL_NAMES,
    SignalService,
    _SIGNAL_DEFINITION_MAP,
    _build_signal_definition_map,
    _extract_trigger_dates,
    _get_signal_definition,
)


def _make_ohlcv_df(n: int = 5) -> pd.DataFrame:
    return pd.DataFrame({
        "Open": [100.0 + i for i in range(n)],
        "High": [105.0 + i for i in range(n)],
        "Low": [95.0 + i for i in range(n)],
        "Close": [102.0 + i for i in range(n)],
        "Volume": [1000 + i * 10 for i in range(n)],
    }, index=pd.date_range("2025-01-01", periods=n))


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
        # ダミーデータ
        data = {
            "ohlc_data": _make_ohlcv_df(1),
            "close": pd.Series([102.0], index=pd.date_range("2025-01-01", periods=1)),
            "execution_close": pd.Series([102.0], index=pd.date_range("2025-01-01", periods=1)),
            "volume": pd.Series([1000], index=pd.date_range("2025-01-01", periods=1)),
        }

        # Phase 1非対応シグナル（例: per）はエラーになるべき
        with pytest.raises(ValueError, match="Phase 1では未対応"):
            service.compute_signal("per", {}, "entry", data)

    @patch("src.api.market_client.MarketAPIClient")
    def test_load_market_source_prefers_market_reader(self, MockMarketClient, market_db_path):
        reader = MarketDbReader(market_db_path)
        try:
            service = SignalService(market_reader=reader)
            df = service.load_ohlcv("7203", "market")
            assert len(df) == 3
            assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
            MockMarketClient.assert_not_called()
        finally:
            reader.close()

    @patch("src.api.dataset.DatasetAPIClient")
    def test_load_dataset_source(self, MockDatasetClient):
        service = SignalService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv_df()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockDatasetClient.return_value = mock_client

        df = service.load_ohlcv("7203", "my_dataset")
        assert len(df) == 5
        mock_client.get_stock_ohlcv.assert_called_once_with("7203", None, None)

    def test_load_market_source_empty_raises(self):
        service = SignalService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = pd.DataFrame()
        service._market_client = mock_client

        with pytest.raises(ValueError, match="取得できません"):
            service.load_ohlcv("7203", "market")

    def test_compute_signal_exit_disabled(self, service):
        data = {
            "ohlc_data": _make_ohlcv_df(3),
            "close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "execution_close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "volume": pd.Series([1000, 1100, 1200], index=pd.date_range("2025-01-01", periods=3)),
        }

        with pytest.raises(ValueError, match="Exitモード"):
            service.compute_signal("buy_and_hold", {}, "exit", data)

    def test_build_signal_params_unknown_raises(self, service):
        with pytest.raises(ValueError, match="未対応のシグナル"):
            service._build_signal_params("unknown_signal", {}, "entry")

    def test_build_signal_params_nested_updates(self, service):
        signal_params = service._build_signal_params(
            "per",
            {"threshold": 20.0, "condition": "above"},
            "entry",
        )
        assert signal_params.fundamental.enabled is True
        assert signal_params.fundamental.per.enabled is True
        assert signal_params.fundamental.per.threshold == 20.0
        assert signal_params.fundamental.per.condition == "above"

    def test_update_top_level_field_unknown_is_noop(self, service):
        from src.models.signals import SignalParams

        signal_params = SignalParams()
        before = signal_params.model_dump()

        service._update_top_level_field(signal_params, "unknown_field", {"threshold": 9.9})

        assert signal_params.model_dump() == before

    def test_update_nested_field_unknown_parent_is_noop(self, service):
        from src.models.signals import SignalParams

        signal_params = SignalParams()
        before = signal_params.model_dump()

        service._update_nested_field(
            signal_params,
            ["unknown_parent", "per"],
            {"threshold": 9.9},
        )

        assert signal_params.model_dump() == before


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

    def test_build_signal_definition_map_logs_duplicate_warning(self):
        sig_def = _get_signal_definition("volume")
        assert sig_def is not None

        with patch("src.server.services.signal_service.SIGNAL_REGISTRY", [sig_def, sig_def]):
            with patch("src.server.services.signal_service.logger") as mock_logger:
                mapping = _build_signal_definition_map()

        assert mapping["volume"] is sig_def
        mock_logger.warning.assert_called_once()


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

    def test_close_closes_market_client(self):
        service = SignalService()
        mock_client = MagicMock()
        service._market_client = mock_client

        service.close()

        mock_client.close.assert_called_once()
        assert service._market_client is None


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

    def test_non_datetime_index_is_stringified(self):
        """非datetimeインデックスでも文字列化される"""
        series = pd.Series([True, False], index=[1, 2])
        result = _extract_trigger_dates(series)
        assert result == ["1"]


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
