"""
SignalProcessor unit tests

SignalProcessorクラスの基本機能・エラーハンドリング・統合処理をテスト
"""

import pytest
import pandas as pd
from types import SimpleNamespace
from unittest.mock import patch

from src.domains.strategy.signals.processor import SignalProcessor
from src.shared.models.signals import SignalParams, Signals


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
        with patch("src.domains.strategy.signals.volume.volume_signal") as mock_volume:
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

        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
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

    def test_missing_data_warning_reports_requirement_name(self):
        """必須データ不足ログが実際の要件を示すことを確認"""
        dummy_signal = SimpleNamespace(
            name="Forward EPS成長率",
            signal_func=lambda **_kwargs: self.base_signal,
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="",
            exit_purpose="",
            category="test",
            description="",
            param_key="test.forward",
            data_checker=lambda _data: False,
            exit_disabled=False,
            data_requirements=["statements:ForwardForecastEPS"],
        )

        with (
            patch("src.domains.strategy.signals.processor.SIGNAL_REGISTRY", [dummy_signal]),
            patch("src.domains.strategy.signals.processor.logger") as mock_logger,
        ):
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=self.signal_params,
            )

        warning_messages = [call.args[0] for call in mock_logger.warning.call_args_list]
        assert any("statements:ForwardForecastEPS" in message for message in warning_messages)
        assert all("ベンチマークデータ" not in message for message in warning_messages)

    def test_apply_signals_raises_when_close_all_nan(self):
        data = self.test_data.copy()
        data["Close"] = [float("nan")] * len(data)
        with pytest.raises(ValueError, match="Close価格データが全てNaN"):
            self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=data,
                signal_params=self.signal_params,
            )

    def test_apply_signals_warns_when_volume_all_nan(self):
        data = self.test_data.copy()
        data["Volume"] = [float("nan")] * len(data)
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            result = self.processor.apply_signals(
                base_signal=self.base_signal,
                signal_type="entry",
                ohlc_data=data,
                signal_params=self.signal_params,
            )

        assert result.dtype == bool
        assert any("Volumeデータが全てNaN" in str(call.args[0]) for call in mock_logger.warning.call_args_list)

    def test_requirement_satisfied_covers_supported_requirements(self):
        benchmark = pd.DataFrame({"Close": [1.0, 2.0]}, index=self.test_data.index[:2])
        statements = pd.DataFrame({"EPS": [1.0, 2.0]}, index=self.test_data.index[:2])
        margin = pd.DataFrame({"x": [1.0]}, index=self.test_data.index[:1])
        sector_data = {"情報・通信業": pd.DataFrame({"Close": [1.0]}, index=self.test_data.index[:1])}
        ohlc = self.test_data.copy()
        ohlc["Open"] = ohlc["Close"]
        sources = {
            "benchmark_data": benchmark,
            "statements_data": statements,
            "margin_data": margin,
            "sector_data": sector_data,
            "stock_sector_name": "情報・通信業",
            "ohlc_data": ohlc[["Open", "High", "Low", "Close"]],
            "volume": self.test_data["Volume"],
        }

        assert self.processor._is_requirement_satisfied("benchmark", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("statements", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("statements:EPS", sources)  # noqa: SLF001
        assert not self.processor._is_requirement_satisfied("statements:ForwardForecastEPS", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("margin", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("sector", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("ohlc", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("volume", sources)  # noqa: SLF001
        assert self.processor._is_requirement_satisfied("unknown", sources)  # noqa: SLF001

    def test_describe_missing_requirements_fallback_message(self):
        no_requirements = SimpleNamespace(data_requirements=[])
        assert (
            self.processor._describe_missing_requirements(no_requirements, {"volume": self.test_data["Volume"]})  # noqa: SLF001
            == "data checker returned False"
        )

        satisfied = SimpleNamespace(data_requirements=["volume"])
        assert (
            self.processor._describe_missing_requirements(  # noqa: SLF001
                satisfied,
                {"volume": self.test_data["Volume"]},
            )
            == "data checker returned False"
        )

    def test_apply_unified_signal_exit_disabled_is_skipped(self):
        dummy_signal = SimpleNamespace(
            name="BuyAndHold",
            signal_func=lambda **_kwargs: self.base_signal,
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test",
            data_checker=None,
            exit_disabled=True,
            data_requirements=[],
        )

        signal_conditions = [self.base_signal]
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=dummy_signal,
                signal_conditions=signal_conditions,
                signal_type="exit",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
            )

        assert len(signal_conditions) == 1
        assert any("Exit用途では使用不可" in str(call.args[0]) for call in mock_logger.warning.call_args_list)

    def test_apply_unified_signal_reindexes_when_index_mismatch(self):
        result_index = self.base_signal.index[2:]
        dummy_signal = SimpleNamespace(
            name="MismatchSignal",
            signal_func=lambda **_kwargs: pd.Series([True, False, True], index=result_index),
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test",
            data_checker=None,
            exit_disabled=False,
            data_requirements=[],
        )

        signal_conditions = [self.base_signal]
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=dummy_signal,
                signal_conditions=signal_conditions,
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
            )

        assert len(signal_conditions) == 2
        aligned = signal_conditions[1]
        assert aligned.index.equals(self.base_signal.index)
        assert aligned.isna().sum() == 2
        assert any("日付不一致" in str(call.args[0]) for call in mock_logger.warning.call_args_list)

    def test_apply_unified_signal_keyerror_is_reraised(self):
        dummy_signal = SimpleNamespace(
            name="KeyErrorSignal",
            signal_func=lambda **_kwargs: (_ for _ in ()).throw(KeyError("missing key")),
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test",
            data_checker=None,
            exit_disabled=False,
            data_requirements=[],
        )

        with pytest.raises(KeyError):
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=dummy_signal,
                signal_conditions=[self.base_signal],
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
            )

    def test_apply_unified_signal_value_and_unknown_errors_are_swallowed(self):
        value_error_signal = SimpleNamespace(
            name="ValueErrorSignal",
            signal_func=lambda **_kwargs: (_ for _ in ()).throw(ValueError("invalid")),
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test",
            data_checker=None,
            exit_disabled=False,
            data_requirements=[],
        )
        unexpected_error_signal = SimpleNamespace(
            name="UnexpectedSignal",
            signal_func=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test",
            data_checker=None,
            exit_disabled=False,
            data_requirements=[],
        )

        signal_conditions = [self.base_signal]
        with patch("src.domains.strategy.signals.processor.logger") as mock_logger:
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=value_error_signal,
                signal_conditions=signal_conditions,
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
            )
            self.processor._apply_unified_signal(  # noqa: SLF001
                signal_def=unexpected_error_signal,
                signal_conditions=signal_conditions,
                signal_type="entry",
                signal_params=self.signal_params,
                base_signal=self.base_signal,
                data_sources={"is_relative_mode": False},
            )

        assert len(signal_conditions) == 1
        warning_messages = [str(call.args[0]) for call in mock_logger.warning.call_args_list]
        assert any("ValueError" in message for message in warning_messages)
        assert any("予期しないエラー" in message for message in warning_messages)

    def test_apply_signals_entry_early_stop_when_recent_window_exhausted(self):
        base_signal = pd.Series([True, True, True, True, True], index=self.test_data.index)

        first_signal = SimpleNamespace(
            name="FirstGate",
            signal_func=lambda **_kwargs: pd.Series(
                [True, True, False, False, False],
                index=self.test_data.index,
            ),
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test.first",
            data_checker=None,
            exit_disabled=False,
            data_requirements=[],
        )

        second_called = {"value": False}

        def _second_signal(**_kwargs):
            second_called["value"] = True
            return pd.Series([True, True, True, True, True], index=self.test_data.index)

        second_signal = SimpleNamespace(
            name="SecondGate",
            signal_func=_second_signal,
            enabled_checker=lambda _params: True,
            param_builder=lambda _params, _data: {},
            entry_purpose="entry",
            exit_purpose="exit",
            category="test",
            description="",
            param_key="test.second",
            data_checker=None,
            exit_disabled=False,
            data_requirements=[],
        )

        with patch(
            "src.domains.strategy.signals.processor.SIGNAL_REGISTRY",
            [first_signal, second_signal],
        ):
            result = self.processor.apply_signals(
                base_signal=base_signal,
                signal_type="entry",
                ohlc_data=self.test_data,
                signal_params=self.signal_params,
                entry_recent_days_for_early_stop=2,
            )

        assert second_called["value"] is False
        assert not result.tail(2).any()

    def test_generate_signals_skips_exit_when_recent_entry_is_empty(self):
        index = self.test_data.index
        entry_only_old = pd.Series([True, True, False, False, False], index=index)

        with (
            patch.object(
                self.processor,
                "apply_entry_signals",
                return_value=entry_only_old,
            ) as mock_entry,
            patch.object(self.processor, "apply_exit_signals") as mock_exit,
        ):
            result = self.processor.generate_signals(
                strategy_entries=self.base_signal,
                strategy_exits=self.base_signal,
                ohlc_data=self.test_data,
                entry_signal_params=self.signal_params,
                exit_signal_params=self.signal_params,
                screening_recent_days=2,
                skip_exit_when_no_recent_entry=True,
            )

        assert mock_entry.called
        assert mock_entry.call_args.kwargs["screening_recent_days"] == 2
        assert not mock_exit.called
        assert result.entries.equals(entry_only_old)
        assert result.exits.dtype == bool
        assert result.exits.sum() == 0


if __name__ == "__main__":
    pytest.main([__file__])
