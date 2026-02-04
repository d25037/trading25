"""
BacktestExecutorMixin ユニットテスト

β値シグナルのベンチマークデータロード機能のテスト
"""

from src.strategies.core.mixins.backtest_executor_mixin import BacktestExecutorMixin
from src.models.signals import SignalParams, BetaSignalParams


class MockStrategy(BacktestExecutorMixin):
    """テスト用のモック戦略クラス"""

    def __init__(self):
        self.entry_filter_params = None
        self.exit_trigger_params = None
        self.benchmark_data = None

    def _log(self, message: str, level: str = "info") -> None:
        """テスト用ログメソッド（何もしない）"""
        pass


class TestBacktestExecutorMixin:
    """BacktestExecutorMixin テストクラス"""

    def test_should_load_benchmark_when_beta_enabled_in_entry(self):
        """エントリーフィルターでβ値シグナルが有効な場合、ベンチマークロードが必要"""
        strategy = MockStrategy()

        # β値シグナルを有効化
        strategy.entry_filter_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True, lookback_period=250, min_beta=1.0, max_beta=2.0
            )
        )

        assert strategy._should_load_benchmark() is True

    def test_should_load_benchmark_when_beta_enabled_in_exit(self):
        """エグジットトリガーでβ値シグナルが有効な場合、ベンチマークロードが必要"""
        strategy = MockStrategy()

        # β値シグナルを有効化（エグジット）
        strategy.exit_trigger_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True, lookback_period=250, min_beta=1.0, max_beta=2.0
            )
        )

        assert strategy._should_load_benchmark() is True

    def test_should_not_load_benchmark_when_beta_disabled(self):
        """β値シグナルが無効な場合、ベンチマークロード不要"""
        strategy = MockStrategy()

        # β値シグナルを無効化
        strategy.entry_filter_params = SignalParams(
            beta=BetaSignalParams(
                enabled=False, lookback_period=250, min_beta=1.0, max_beta=2.0
            )
        )

        assert strategy._should_load_benchmark() is False

    def test_should_not_load_benchmark_when_no_params(self):
        """パラメータが未設定の場合、ベンチマークロード不要"""
        strategy = MockStrategy()

        # パラメータなし
        strategy.entry_filter_params = None
        strategy.exit_trigger_params = None

        assert strategy._should_load_benchmark() is False

    def test_should_load_benchmark_when_both_entry_and_exit_enabled(self):
        """エントリー・エグジット両方でβ値シグナルが有効な場合、ベンチマークロードが必要"""
        strategy = MockStrategy()

        # 両方でβ値シグナルを有効化
        strategy.entry_filter_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True, lookback_period=250, min_beta=1.0, max_beta=2.0
            )
        )
        strategy.exit_trigger_params = SignalParams(
            beta=BetaSignalParams(
                enabled=True, lookback_period=250, min_beta=0.5, max_beta=1.5
            )
        )

        assert strategy._should_load_benchmark() is True
