"""
BacktestExecutorMixin ユニットテスト

β値シグナルのベンチマークデータロード機能のテスト
"""

from src.strategies.core.mixins.backtest_executor_mixin import BacktestExecutorMixin
from src.models.signals import (
    BetaSignalParams,
    FundamentalSignalParams,
    MarginSignalParams,
    SignalParams,
)


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

    def test_should_load_margin_data_when_margin_signal_enabled(self):
        """信用残高シグナル有効時は信用残高データロードが必要"""
        strategy = MockStrategy()
        strategy.entry_filter_params = SignalParams(
            margin=MarginSignalParams(enabled=True)
        )

        assert strategy._should_load_margin_data() is True

    def test_should_not_load_margin_data_when_margin_signal_disabled(self):
        """信用残高シグナル無効時は信用残高データロード不要"""
        strategy = MockStrategy()
        strategy.entry_filter_params = SignalParams(
            margin=MarginSignalParams(enabled=False)
        )

        assert strategy._should_load_margin_data() is False

    def test_should_load_statements_data_when_fundamental_signal_enabled(self):
        """財務シグナル有効時は財務諸表データロードが必要"""
        strategy = MockStrategy()
        strategy.entry_filter_params = SignalParams(
            fundamental=FundamentalSignalParams(
                enabled=True,
                per={"enabled": True, "threshold": 15.0, "condition": "below"},
            )
        )

        assert strategy._should_load_statements_data() is True

    def test_should_not_load_statements_data_when_no_fundamental_subsignal_enabled(self):
        """fundamental.enabled=True でもサブシグナル無効なら財務諸表データは不要"""
        strategy = MockStrategy()
        strategy.entry_filter_params = SignalParams(
            fundamental=FundamentalSignalParams(enabled=True)
        )

        assert strategy._should_load_statements_data() is False
