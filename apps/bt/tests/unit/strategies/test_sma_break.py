"""
SMAブレイク戦略のユニットテスト（最新アーキテクチャ対応版）

YamlConfigurableStrategy + YAML設定 + SignalParams（統一Signalsシステム）対応
"""

import pandas as pd
import pytest
import numpy as np

from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.models.config import SharedConfig
from src.models.signals import SignalParams


class TestSMABreakStrategy:
    """SMAブレイク戦略テストクラス（YamlConfigurableStrategy + YAML設定ベース）"""

    def setup_method(self):
        """各テスト前のセットアップ"""
        self.shared_config = SharedConfig(
            initial_cash=100000,
            fees=0.001,
            dataset="sampleA",
            stock_codes=["test_stock"],
            printlog=False,
        )

        # SMAブレイク戦略パラメータ（YAMLから読み込まれる想定）

        # エントリーフィルターパラメータ（統一SignalParams）
        self.entry_filter_params = SignalParams()
        self.entry_filter_params.period_breakout.enabled = True
        self.entry_filter_params.period_breakout.direction = "high"
        self.entry_filter_params.period_breakout.condition = "break"
        self.entry_filter_params.period_breakout.period = 20

        # エグジットトリガーパラメータ（統一SignalParams）
        self.exit_trigger_params = SignalParams()
        self.exit_trigger_params.period_breakout.enabled = True
        self.exit_trigger_params.period_breakout.direction = "low"
        self.exit_trigger_params.period_breakout.condition = "break"
        self.exit_trigger_params.period_breakout.period = 20

        self.strategy = YamlConfigurableStrategy(
            shared_config=self.shared_config,
            entry_filter_params=self.entry_filter_params,
            exit_trigger_params=self.exit_trigger_params,
        )

    def create_test_data(self) -> pd.DataFrame:
        """テスト用OHLCVデータを作成"""
        dates = pd.date_range(start="2023-01-01", periods=100, freq="D")

        # SMAブレイクシナリオをテスト用に作成
        base_prices = []

        # 最初の30日間: SMA周辺で推移（95-105）
        for i in range(30):
            price = 100 + np.sin(i * 0.2) * 5
            base_prices.append(price)

        # 次の35日間: 上昇トレンド（SMAを上抜け）
        for i in range(35):
            price = 100 + (i * 0.5)
            base_prices.append(price)

        # 次の35日間: 下降トレンド（SMAを下抜け）
        for i in range(35):
            price = 117.5 - (i * 0.8)
            base_prices.append(price)

        data = pd.DataFrame(
            {
                "Open": [p * 0.99 for p in base_prices],
                "High": [p * 1.03 for p in base_prices],
                "Low": [p * 0.97 for p in base_prices],
                "Close": base_prices,
                "Volume": [
                    100000 + np.random.randint(-10000, 10000) for _ in range(100)
                ],
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

    def test_sma_breakout_signals(self):
        """SMAブレイクアウトシグナルテスト"""
        data = self.create_test_data()
        signals = self.strategy.generate_signals(data)

        # ブレイクアウト（エントリー）シグナルが発生することを確認
        assert signals.entries.any(), "ブレイクアウトシグナルが全くない"

        # ブレイクダウン（エグジット）シグナルが発生することを確認
        assert signals.exits.any(), "ブレイクダウンシグナルが全くない"

    def test_generate_signals_with_filters(self):
        """フィルター統合シグナル生成テスト"""
        # 出来高フィルター追加
        self.entry_filter_params.volume.enabled = True
        self.entry_filter_params.volume.direction = "surge"
        self.entry_filter_params.volume.threshold = 1.5
        self.entry_filter_params.volume.short_period = 10
        self.entry_filter_params.volume.long_period = 50

        # ボラティリティフィルター追加
        self.entry_filter_params.volatility.enabled = True
        self.entry_filter_params.volatility.lookback_period = 100
        self.entry_filter_params.volatility.threshold_multiplier = 1.0

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
        valid_params.period_breakout.enabled = True
        valid_params.period_breakout.period = 20

        assert valid_params.period_breakout.enabled is True
        assert valid_params.period_breakout.period == 20

        # 異常なパラメータ（Pydanticバリデーション）
        with pytest.raises(ValueError):
            from src.models.signals import VolumeSignalParams

            VolumeSignalParams(threshold=0.0)  # <=0.1は無効

    def test_boundary_conditions(self):
        """境界値テスト"""
        data = self.create_test_data()

        # 極端なパラメータでもエラーが発生しないことを確認
        extreme_filter_params = SignalParams()
        extreme_filter_params.period_breakout.enabled = True
        extreme_filter_params.period_breakout.period = 5  # 短期間

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
