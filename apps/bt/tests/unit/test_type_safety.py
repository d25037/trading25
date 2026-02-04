"""
型安全性テスト（最新アーキテクチャ対応版）

Pydantic実装の型安全性を検証（Signals + SignalParams統合システム）
"""

import pytest
import pandas as pd
from pydantic import ValidationError

from src.models.config import SharedConfig
from src.models.signals import Signals, SignalParams
from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy


class TestSignalsTypeSafety:
    """Signalsクラスの型安全性テスト"""

    def test_signals_creation_with_valid_boolean_series(self):
        """正しいboolean Seriesでのignals作成"""
        dates = pd.date_range("2023-01-01", periods=5)
        entries = pd.Series([True, False, True, False, True], index=dates)
        exits = pd.Series([False, True, False, True, False], index=dates)

        signals = Signals(entries=entries, exits=exits)

        assert isinstance(signals.entries, pd.Series)
        assert isinstance(signals.exits, pd.Series)
        assert signals.entries.dtype == bool
        assert signals.exits.dtype == bool
        assert len(signals) == 5

    def test_signals_validation_non_series(self):
        """非Series型でのバリデーションエラー"""
        with pytest.raises(ValidationError) as exc_info:
            Signals(
                entries=[True, False, True],  # リスト型は無効
                exits=pd.Series([False, True, False]),
            )
        # Pydantic v2のエラーメッセージに対応
        error_msg = str(exc_info.value)
        assert (
            "Input should be an instance of Series" in error_msg
            or "Must be pd.Series" in error_msg
        )

    def test_signals_validation_non_boolean(self):
        """非boolean型でのバリデーションエラー"""
        with pytest.raises(ValidationError) as exc_info:
            Signals(
                entries=pd.Series([1, 0, 1]),  # int型は無効
                exits=pd.Series([False, True, False]),
            )
        assert "Must be boolean Series" in str(exc_info.value)

    def test_signals_index_consistency_validation(self):
        """インデックス一致性バリデーション"""
        dates1 = pd.date_range("2023-01-01", periods=5)
        dates2 = pd.date_range("2023-01-06", periods=5)

        entries = pd.Series([True, False, True, False, True], index=dates1)
        exits = pd.Series([False, True, False, True, False], index=dates2)

        with pytest.raises(ValidationError) as exc_info:
            Signals(entries=entries, exits=exits)
        assert "entries and exits must have identical indices" in str(exc_info.value)

    def test_signals_methods(self):
        """Signalsメソッドの型安全性"""
        dates = pd.date_range("2023-01-01", periods=5)
        entries = pd.Series([True, False, True, False, False], index=dates)
        exits = pd.Series([False, True, False, True, False], index=dates)

        signals = Signals(entries=entries, exits=exits)

        # any_entries() / any_exits()
        assert isinstance(signals.any_entries(), bool)
        assert signals.any_entries() is True
        assert isinstance(signals.any_exits(), bool)
        assert signals.any_exits() is True

        # summary()
        summary = signals.summary()
        assert isinstance(summary, dict)
        assert "total_length" in summary
        assert "entry_signals" in summary
        assert "exit_signals" in summary
        assert summary["total_length"] == 5
        assert summary["entry_signals"] == 2
        assert summary["exit_signals"] == 2


class TestSignalParamsTypeSafety:
    """SignalParamsクラスの型安全性テスト"""

    def test_signal_params_creation_with_defaults(self):
        """デフォルト値でのSignalParams作成"""
        params = SignalParams()

        # 全シグナルがデフォルトで無効
        assert params.volume.enabled is False
        assert params.crossover.enabled is False
        assert params.mean_reversion.enabled is False
        assert params.bollinger_bands.enabled is False
        assert params.period_breakout.enabled is False
        assert params.fundamental.per.enabled is False
        assert params.beta.enabled is False
        assert params.atr_support_break.enabled is False

    def test_signal_params_volume_validation(self):
        """出来高シグナルパラメータのバリデーション"""
        params = SignalParams()

        # 正常な設定
        params.volume.enabled = True
        params.volume.threshold = 1.5
        params.volume.short_period = 20
        params.volume.long_period = 100

        assert params.volume.enabled is True
        assert params.volume.threshold == 1.5

        # 異常な設定（期間順序）
        # Pydanticバリデーションは初期化時に実行されるため、新規オブジェクト作成が必要
        with pytest.raises(ValidationError):
            from src.models.signals import VolumeSignalParams

            VolumeSignalParams(
                enabled=True,
                short_period=100,
                long_period=20,  # short_period > long_period でエラー
            )

    def test_signal_params_crossover_validation(self):
        """クロスオーバーシグナルパラメータのバリデーション"""
        params = SignalParams()

        # 正常な設定
        params.crossover.enabled = True
        params.crossover.type = "sma"
        params.crossover.direction = "golden"
        params.crossover.fast_period = 10
        params.crossover.slow_period = 30

        assert params.crossover.enabled is True
        assert params.crossover.type == "sma"
        assert params.crossover.fast_period == 10
        assert params.crossover.slow_period == 30

    def test_signal_params_has_any_enabled(self):
        """has_any_enabled()メソッドの型安全性"""
        params = SignalParams()

        # 全無効時
        assert isinstance(params.has_any_enabled(), bool)
        assert params.has_any_enabled() is False

        # 1つ有効化
        params.volume.enabled = True
        assert params.has_any_enabled() is True


class TestSharedConfigTypeSafety:
    """SharedConfigクラスの型安全性テスト"""

    def test_shared_config_creation_with_valid_params(self):
        """正しいパラメータでのSharedConfig作成"""
        config = SharedConfig(
            initial_cash=200000,
            fees=0.002,
            dataset="sampleA",
            stock_codes=["17190", "23010"],
        )

        assert isinstance(config.initial_cash, (int, float))
        assert isinstance(config.fees, float)
        assert isinstance(config.dataset, str)
        assert isinstance(config.stock_codes, list)
        assert config.initial_cash == 200000
        assert config.fees == 0.002

    def test_shared_config_validation_initial_cash(self):
        """initial_cashバリデーション"""
        with pytest.raises(ValidationError) as exc_info:
            SharedConfig(
                initial_cash=-1000,  # 負の値は無効
                dataset="sampleA",
            )
        assert "初期資金は正の値である必要があります" in str(exc_info.value)

    def test_shared_config_validation_fees(self):
        """feesバリデーション"""
        with pytest.raises(ValidationError) as exc_info:
            SharedConfig(
                fees=1.5,  # 1以上は無効
                dataset="sampleA",
            )
        assert "手数料は0以上1未満である必要があります" in str(exc_info.value)


class TestYamlConfigurableStrategyTypeSafety:
    """YamlConfigurableStrategy型安全性テスト"""

    def test_base_strategy_initialization_with_typed_params(self):
        """型付きパラメータでのYamlConfigurableStrategy初期化"""
        shared_config = SharedConfig(
            initial_cash=100000,
            fees=0.001,
            dataset="sampleA",
            stock_codes=["test_stock"],
            printlog=False,
        )

        entry_filter_params = SignalParams()
        entry_filter_params.volume.enabled = True

        exit_trigger_params = SignalParams()
        exit_trigger_params.atr_support_break.enabled = True

        strategy = YamlConfigurableStrategy(
            shared_config=shared_config,
            entry_filter_params=entry_filter_params,
            exit_trigger_params=exit_trigger_params,
        )

        # 型安全性確認
        assert isinstance(strategy.entry_filter_params, SignalParams)
        assert isinstance(strategy.exit_trigger_params, SignalParams)
        assert strategy.entry_filter_params.volume.enabled is True
        assert strategy.exit_trigger_params.atr_support_break.enabled is True

    def test_base_strategy_generate_signals_returns_signals(self):
        """generate_signals()がSignalsオブジェクトを返すことを確認"""
        shared_config = SharedConfig(
            initial_cash=100000,
            fees=0.001,
            dataset="sampleA",
            stock_codes=["test_stock"],
            printlog=False,
        )

        strategy = YamlConfigurableStrategy(
            shared_config=shared_config,
            entry_filter_params=SignalParams(),
            exit_trigger_params=SignalParams(),
        )

        # テストデータ作成
        dates = pd.date_range("2023-01-01", periods=10)
        data = pd.DataFrame(
            {
                "Open": [100] * 10,
                "High": [105] * 10,
                "Low": [95] * 10,
                "Close": [102] * 10,
                "Volume": [1000] * 10,
            },
            index=dates,
        )

        signals = strategy.generate_signals(data)

        # 型安全性確認
        assert isinstance(signals, Signals)
        assert isinstance(signals.entries, pd.Series)
        assert isinstance(signals.exits, pd.Series)
        assert signals.entries.dtype == bool
        assert signals.exits.dtype == bool


class TestIDECompletionSupport:
    """IDE補完サポートテスト（型安全性の恩恵）"""

    def test_signal_params_attribute_completion(self):
        """SignalParamsの属性補完テスト"""
        params = SignalParams()

        # これらの属性は全て型が確定しており、IDEで補完される
        assert hasattr(params, "volume")
        assert hasattr(params, "crossover")
        assert hasattr(params, "mean_reversion")
        assert hasattr(params, "bollinger_bands")
        assert hasattr(params, "period_breakout")
        assert hasattr(params, "fundamental")
        assert hasattr(params, "beta")
        assert hasattr(params, "atr_support_break")

        # ネストされた属性も補完される
        assert hasattr(params.volume, "enabled")
        assert hasattr(params.volume, "threshold")
        assert hasattr(params.fundamental, "per")
        assert hasattr(params.fundamental.per, "enabled")

    def test_signals_attribute_completion(self):
        """Signalsの属性補完テスト"""
        dates = pd.date_range("2023-01-01", periods=5)
        signals = Signals(
            entries=pd.Series([True, False, True, False, True], index=dates),
            exits=pd.Series([False, True, False, True, False], index=dates),
        )

        # これらのメソッドは全て型が確定しており、IDEで補完される
        assert hasattr(signals, "entries")
        assert hasattr(signals, "exits")
        assert hasattr(signals, "any_entries")
        assert hasattr(signals, "any_exits")
        assert hasattr(signals, "summary")


if __name__ == "__main__":
    pytest.main([__file__])
