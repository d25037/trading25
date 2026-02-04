"""
SignalProcessor unit tests

SignalProcessorクラスの基本機能・エラーハンドリング・統合処理をテスト
"""

import pytest
import pandas as pd
from unittest.mock import patch

from src.strategies.signals.processor import SignalProcessor
from src.models.signals import SignalParams, Signals


class TestSignalProcessor:
    """SignalProcessor テストクラス"""

    def setup_method(self):
        """テストセットアップ"""
        self.processor = SignalProcessor()

        # テスト用データ作成
        self.test_data = pd.DataFrame(
            {
                "Close": [100.0, 101.0, 102.0, 103.0, 104.0],
                "High": [101.0, 102.0, 103.0, 104.0, 105.0],
                "Low": [99.0, 100.0, 101.0, 102.0, 103.0],
                "Volume": [1000.0, 1100.0, 1200.0, 1300.0, 1400.0],
            },
            index=pd.date_range("2023-01-01", periods=5),
        )

        self.base_signal = pd.Series(
            [True, False, True, False, True], index=self.test_data.index
        )
        self.signal_params = SignalParams()

    def test_processor_initialization(self):
        """プロセッサー初期化テスト"""
        processor = SignalProcessor()
        assert processor is not None

    def test_apply_entry_signals_basic(self):
        """エントリーシグナル基本処理テスト"""
        result = self.processor.apply_entry_signals(
            base_signal=self.base_signal,
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
        )

        assert isinstance(result, pd.Series)
        assert len(result) == len(self.base_signal)
        assert result.dtype == bool

    def test_apply_exit_signals_basic(self):
        """エグジットシグナル基本処理テスト"""
        result = self.processor.apply_exit_signals(
            base_exits=self.base_signal,
            data=self.test_data,
            signal_params=self.signal_params,
        )

        assert isinstance(result, pd.Series)
        assert len(result) == len(self.base_signal)
        assert result.dtype == bool

    def test_generate_signals_integration(self):
        """統合シグナル生成テスト"""
        result = self.processor.generate_signals(
            strategy_entries=self.base_signal,
            strategy_exits=self.base_signal,
            ohlc_data=self.test_data,
            entry_signal_params=self.signal_params,
            exit_signal_params=self.signal_params,
        )

        assert isinstance(result, Signals)
        assert len(result.entries) == len(self.base_signal)
        assert len(result.exits) == len(self.base_signal)
        assert result.entries.dtype == bool
        assert result.exits.dtype == bool

    def test_apply_signals_entry_type(self):
        """エントリータイプシグナル処理テスト"""
        result = self.processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="entry",
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_apply_signals_exit_type(self):
        """エグジットタイプシグナル処理テスト"""
        result = self.processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="exit",
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_missing_ohlc_data_validation(self):
        """OHLCデータ不足時のバリデーションテスト"""
        with pytest.raises(ValueError, match="OHLCデータが提供されていません"):
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=None,
                signal_params=self.signal_params,
            )

    def test_missing_required_columns_validation(self):
        """必須カラム不足時のバリデーションテスト"""
        invalid_data = pd.DataFrame({"Price": [100, 101, 102]})

        with pytest.raises(ValueError, match="必須カラムが不足しています"):
            self.processor.apply_signals(
                base_signal=pd.Series([True, False, True]),
                signal_type="entry",
                ohlc_data=invalid_data,
                signal_params=self.signal_params,
            )

    def test_error_handling_in_signal_processing(self):
        """シグナル処理でのエラーハンドリングテスト"""
        # エラーが発生する可能性のあるシグナルパラメータを有効化
        params = SignalParams()
        params.volume.enabled = True
        params.volume.direction = "surge"

        # モックでエラーを発生させる（データ駆動設計対応: registry経由のシグナル関数）
        with patch("src.strategies.signals.volume.volume_signal") as mock_volume:
            mock_volume.side_effect = Exception("テストエラー")

            # エラーが発生してもプロセシングが継続することを確認
            result = self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=params,
            )

            assert isinstance(result, pd.Series)
            assert result.dtype == bool

    def test_with_optional_data(self):
        """オプションデータ付きシグナル処理テスト"""
        # ダミーの財務データ作成
        statements_data = pd.DataFrame(
            {
                "EPS": [10.0, 11.0, 12.0, 13.0, 14.0],
                "ROE": [15.0, 16.0, 17.0, 18.0, 19.0],
            },
            index=self.test_data.index,
        )

        # ダミーのベンチマークデータ作成
        benchmark_data = pd.DataFrame(
            {"Close": [2000.0, 2010.0, 2020.0, 2030.0, 2040.0]},
            index=self.test_data.index,
        )

        # ダミーの信用残高データ作成
        margin_data = pd.DataFrame(
            {"margin_balance": [1000000, 1100000, 1200000, 1300000, 1400000]},
            index=self.test_data.index,
        )

        result = self.processor.apply_signals(
            base_signal=self.base_signal,
            signal_type="entry",
            ohlc_data=self.test_data,
            signal_params=self.signal_params,
            statements_data=statements_data,
            benchmark_data=benchmark_data,
            margin_data=margin_data,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool

    def test_signal_logging(self):
        """シグナルログ記録テスト"""
        # ボリュームシグナルを有効化してログテスト
        params = SignalParams()
        params.volume.enabled = True
        params.volume.direction = "surge"

        with patch("src.strategies.signals.processor.logger") as mock_logger:
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=params,
            )

            # ログが呼ばれたことを確認
            assert mock_logger.debug.called

    def test_relative_mode_skips_execution_required_signals(self):
        """相対価格モード時に実価格必須シグナルがスキップされることを確認"""
        data = pd.DataFrame(
            {
                "Close": [1.0, 1.0, 1.0],
                "Volume": [1.0, 1.0, 1.0],
            },
            index=pd.date_range("2023-01-01", periods=3),
        )
        base_signal = pd.Series([True, True, True], index=data.index)

        params = SignalParams()
        params.trading_value.enabled = True
        params.trading_value.period = 1
        params.trading_value.threshold_value = 1.0  # 1e-8 << 1.0 => Falseになる
        params.trading_value.direction = "above"

        # 通常モード: シグナルが適用され、全てFalseになる
        result_normal = self.processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=data,
            signal_params=params,
            relative_mode=False,
        )
        assert result_normal.sum() == 0

        # 相対価格モード（実価格なし）: 実価格必須シグナルはスキップされ、baseが維持される
        result_relative = self.processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=data,
            signal_params=params,
            relative_mode=True,
        )
        assert result_relative.equals(base_signal)


if __name__ == "__main__":
    pytest.main([__file__])
