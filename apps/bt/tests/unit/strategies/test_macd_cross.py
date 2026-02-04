"""
MACDクロス戦略のユニットテスト（最新アーキテクチャ対応版）

YamlConfigurableStrategy + YAML設定 + SignalParams（統一Signalsシステム）対応
"""

import pandas as pd
import pytest

from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.models.config import SharedConfig
from src.models.signals import SignalParams


class TestMACDStrategy:
    """MACDクロス戦略テストクラス（YamlConfigurableStrategy + YAML設定ベース）"""

    def setup_method(self):
        """各テスト前のセットアップ"""
        self.shared_config = SharedConfig(
            initial_cash=100000,
            fees=0.001,
            dataset="sampleA",
            stock_codes=["test_stock"],
            printlog=False,
        )

        # MACD戦略パラメータ（YAMLから読み込まれる想定）

        # エントリーフィルターパラメータ（統一SignalParams）
        self.entry_filter_params = SignalParams()
        self.entry_filter_params.crossover.enabled = True
        self.entry_filter_params.crossover.type = "macd"
        self.entry_filter_params.crossover.direction = "golden"
        self.entry_filter_params.crossover.fast_period = 12
        self.entry_filter_params.crossover.slow_period = 26
        self.entry_filter_params.crossover.signal_period = 9

        # エグジットトリガーパラメータ（統一SignalParams）
        self.exit_trigger_params = SignalParams()
        self.exit_trigger_params.crossover.enabled = True
        self.exit_trigger_params.crossover.type = "macd"
        self.exit_trigger_params.crossover.direction = "dead"
        self.exit_trigger_params.crossover.fast_period = 12
        self.exit_trigger_params.crossover.slow_period = 26
        self.exit_trigger_params.crossover.signal_period = 9

        self.strategy = YamlConfigurableStrategy(
            shared_config=self.shared_config,
            entry_filter_params=self.entry_filter_params,
            exit_trigger_params=self.exit_trigger_params,
        )

    def create_test_data(self) -> pd.DataFrame:
        """テスト用OHLCVデータを作成"""
        dates = pd.date_range(start="2023-01-01", periods=100, freq="D")

        # MACDクロスシナリオをテスト用に作成
        base_prices = []

        # 最初の50日間: 下降トレンド（100 -> 90）
        for i in range(50):
            price = 100 - (i * 0.2)
            base_prices.append(price)

        # 次の50日間: 上昇トレンド（90 -> 110）
        for i in range(50):
            price = 90 + (i * 0.4)
            base_prices.append(price)

        data = pd.DataFrame(
            {
                "Open": [p * 0.99 for p in base_prices],
                "High": [p * 1.02 for p in base_prices],
                "Low": [p * 0.98 for p in base_prices],
                "Close": base_prices,
                "Volume": [100000] * 100,
            },
            index=dates,
        )

        return data

    def test_strategy_initialization(self):
        """戦略初期化テスト（YamlConfigurableStrategy使用）"""
        assert self.strategy.entry_filter_params is not None
        assert self.strategy.exit_trigger_params is not None
        assert self.strategy.signal_processor is not None

    def test_generate_signals_basic(self):
        """基本シグナル生成テスト（YAML完全制御版）"""
        data = self.create_test_data()
        signals = self.strategy.generate_signals(data)

        # Signalsオブジェクトが返されることを確認
        assert hasattr(signals, "entries")
        assert hasattr(signals, "exits")
        assert isinstance(signals.entries, pd.Series)
        assert isinstance(signals.exits, pd.Series)
        assert len(signals.entries) == len(data)
        assert len(signals.exits) == len(data)
        assert signals.entries.dtype == bool
        assert signals.exits.dtype == bool

    def test_macd_crossover_signals(self):
        """MACDクロスオーバーシグナルテスト"""
        data = self.create_test_data()
        signals = self.strategy.generate_signals(data)

        # ゴールデンクロス（エントリー）シグナルが発生することを確認
        assert signals.entries.any(), "ゴールデンクロスシグナルが全くない"

        # デッドクロス（エグジット）シグナルが発生することを確認
        assert signals.exits.any(), "デッドクロスシグナルが全くない"

    def test_generate_signals_with_filters(self):
        """フィルター統合シグナル生成テスト"""
        # 出来高フィルター追加
        self.entry_filter_params.volume.enabled = True
        self.entry_filter_params.volume.direction = "surge"
        self.entry_filter_params.volume.threshold = 1.5
        self.entry_filter_params.volume.short_period = 20
        self.entry_filter_params.volume.long_period = 100

        strategy_with_filters = YamlConfigurableStrategy(
            shared_config=self.shared_config,
            entry_filter_params=self.entry_filter_params,
            exit_trigger_params=self.exit_trigger_params,
        )

        data = self.create_test_data()
        signals = strategy_with_filters.generate_signals(data)

        # フィルター適用されても正常にシグナル生成
        assert isinstance(signals.entries, pd.Series)
        assert isinstance(signals.exits, pd.Series)

    def test_signal_params_validation(self):
        """SignalParamsバリデーションテスト"""
        # 正常なパラメータ
        valid_params = SignalParams()
        valid_params.crossover.enabled = True
        valid_params.crossover.type = "macd"
        valid_params.crossover.fast_period = 12
        valid_params.crossover.slow_period = 26

        assert valid_params.crossover.enabled is True
        assert valid_params.crossover.fast_period == 12

        # 異常なパラメータ（Pydanticバリデーション）
        with pytest.raises(ValueError):
            from src.models.signals import CrossoverSignalParams

            CrossoverSignalParams(fast_period=26, slow_period=12)  # fast >= slowは無効

    def test_boundary_conditions(self):
        """境界値テスト"""
        data = self.create_test_data()

        # 極端なパラメータでもエラーが発生しないことを確認
        extreme_filter_params = SignalParams()
        extreme_filter_params.crossover.enabled = True
        extreme_filter_params.crossover.type = "macd"
        extreme_filter_params.crossover.fast_period = 1
        extreme_filter_params.crossover.slow_period = 2
        extreme_filter_params.crossover.signal_period = 1

        extreme_strategy = YamlConfigurableStrategy(
            shared_config=self.shared_config,
            entry_filter_params=extreme_filter_params,
            exit_trigger_params=self.exit_trigger_params,
        )

        signals = extreme_strategy.generate_signals(data)
        assert isinstance(signals.entries, pd.Series)
        assert isinstance(signals.exits, pd.Series)

    def test_empty_data_handling(self):
        """空データ処理テスト"""
        # 空データはValueErrorを発生させる（OHLCデータ検証）
        empty_data = pd.DataFrame(
            {"Open": [], "High": [], "Low": [], "Close": [], "Volume": []}
        )

        with pytest.raises(ValueError, match="OHLCデータが提供されていません"):
            self.strategy.generate_signals(empty_data)

    def test_signal_summary(self):
        """シグナル概要テスト（Signalsモデル機能）"""
        data = self.create_test_data()
        signals = self.strategy.generate_signals(data)

        summary = signals.summary()

        assert "total_length" in summary
        assert "entry_signals" in summary
        assert "exit_signals" in summary
        assert "has_entries" in summary
        assert "has_exits" in summary
        assert summary["total_length"] == len(data)


if __name__ == "__main__":
    pytest.main([__file__])
