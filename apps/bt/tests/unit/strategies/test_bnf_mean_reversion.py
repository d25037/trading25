"""
BNF逆張り戦略のユニットテスト（最新アーキテクチャ対応版）

YamlConfigurableStrategy + YAML設定 + SignalParams（統一Signalsシステム）対応
"""

import pandas as pd
import pytest

from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


class TestBnfBaselineDeviationStrategy:
    """BNF逆張り戦略テストクラス（YamlConfigurableStrategy + YAML設定ベース）"""

    def setup_method(self):
        """各テスト前のセットアップ"""
        self.shared_config = SharedConfig(
            initial_cash=100000,
            fees=0.001,
            universe_preset="sampleA",
            stock_codes=["test_stock"],
            printlog=False,
        )

        # BNF平均回帰戦略パラメータ（YAMLから読み込まれる想定）

        # エントリーフィルターパラメータ（統一SignalParams）
        self.entry_filter_params = SignalParams()
        self.entry_filter_params.baseline_deviation.enabled = True
        self.entry_filter_params.baseline_deviation.baseline_type = "sma"
        self.entry_filter_params.baseline_deviation.baseline_period = 25
        self.entry_filter_params.baseline_deviation.deviation_threshold = 0.2
        self.entry_filter_params.baseline_deviation.direction = "below"

        # エグジットトリガーパラメータ（統一SignalParams）
        self.exit_trigger_params = SignalParams()
        # 時間切れロジック等は戦略固有実装として残す想定

        self.strategy = YamlConfigurableStrategy(
            shared_config=self.shared_config,
            entry_filter_params=self.entry_filter_params,
            exit_trigger_params=self.exit_trigger_params,
        )

    def create_test_data(self) -> pd.DataFrame:
        """テスト用OHLCVデータを作成"""
        dates = pd.date_range(start="2023-01-01", periods=50, freq="D")

        # SMA25 = 100となるような価格データを作成
        base_prices = [100] * 50

        # 逆張りシナリオをテスト
        # Day 30で大幅下落（20%以上の乖離）
        # Day 35で回復
        for i in range(30, 35):
            base_prices[i] = 75  # 25%下落
        for i in range(35, 40):
            base_prices[i] = 105  # 回復

        data = pd.DataFrame(
            {
                "Open": base_prices,
                "High": [price * 1.02 for price in base_prices],
                "Low": [price * 0.98 for price in base_prices],
                "Close": base_prices,
                "Volume": [1000] * 50,
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

    def test_baseline_deviation_entry_signal(self):
        """基準線乖離エントリーシグナルテスト"""
        data = self.create_test_data()
        signals = self.strategy.generate_signals(data)

        # 基準線乖離シグナル（下落時）が発生していることを確認
        # 注: SignalProcessorによる絞り込み処理のため、
        # 全Trueから開始→baseline_deviationフィルターでAND結合
        assert signals.entries.any(), "エントリーシグナルが全くない"

    def test_generate_signals_with_filters(self):
        """フィルター統合シグナル生成テスト"""
        # 出来高フィルター追加
        self.entry_filter_params.volume_ratio_above.enabled = True
        self.entry_filter_params.volume_ratio_above.ratio_threshold = 1.5
        self.entry_filter_params.volume_ratio_above.short_period = 10
        self.entry_filter_params.volume_ratio_above.long_period = 50

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
        valid_params.baseline_deviation.enabled = True
        valid_params.baseline_deviation.baseline_period = 25
        valid_params.baseline_deviation.deviation_threshold = 0.2

        assert valid_params.baseline_deviation.enabled is True
        assert valid_params.baseline_deviation.baseline_period == 25

        # 異常なパラメータ（Pydanticバリデーション）
        with pytest.raises(ValueError):
            from src.shared.models.signals import BaselineDeviationSignalParams

            BaselineDeviationSignalParams(deviation_threshold=1.5)  # >1.0は無効

    def test_boundary_conditions(self):
        """境界値テスト"""
        data = self.create_test_data()

        # 極端なパラメータでもエラーが発生しないことを確認
        extreme_filter_params = SignalParams()
        extreme_filter_params.baseline_deviation.enabled = True
        extreme_filter_params.baseline_deviation.baseline_period = 5
        extreme_filter_params.baseline_deviation.deviation_threshold = 0.01  # 最小値

        extreme_strategy = YamlConfigurableStrategy(
            shared_config=self.shared_config,
            entry_filter_params=extreme_filter_params,
            exit_trigger_params=self.exit_trigger_params,
        )

        signals = extreme_strategy.generate_signals(data)
        assert isinstance(signals.entries, pd.Series)
        assert isinstance(signals.exits, pd.Series)
        assert len(signals.entries) == len(data)
        assert len(signals.exits) == len(data)

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
