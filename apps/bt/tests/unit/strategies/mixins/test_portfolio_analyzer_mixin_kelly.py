"""
ケリー基準ポートフォリオ最適化ミックスイン テスト

PortfolioAnalyzerKellyMixinクラスの統合ポートフォリオケリー基準計算・配分最適化機能をテスト
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.strategies.core.mixins.portfolio_analyzer_mixin_kelly import (
    PortfolioAnalyzerKellyMixin,
)


class MockStrategy(PortfolioAnalyzerKellyMixin):
    """テスト用モックストラテジークラス"""

    def __init__(self, stock_codes=None):
        self.stock_codes = stock_codes or ["1234", "5678", "9012"]
        self.combined_portfolio = None
        self.portfolio = None
        self.group_by = True
        self.initial_cash = 1000000.0
        self.fees = 0.001
        self.log_messages = []

    def _log(self, message: str, level: str = "info") -> None:
        """ログメッセージを記録"""
        self.log_messages.append((message, level))

    def run_multi_backtest(self, allocation_pct=None):
        """モックバックテスト実行"""
        mock_portfolio = MagicMock()
        mock_portfolio.total_return.return_value = 0.15  # 15%リターン
        return mock_portfolio


class TestPortfolioAnalyzerKellyMixin:
    """ケリー基準ポートフォリオ最適化ミックスイン テストクラス"""

    def setup_method(self):
        """テストセットアップ"""
        self.strategy = MockStrategy()

    def test_mixin_initialization(self):
        """ミックスイン初期化テスト"""
        assert isinstance(self.strategy, PortfolioAnalyzerKellyMixin)
        assert hasattr(self.strategy, "optimize_allocation_kelly")
        assert hasattr(self.strategy, "_calculate_kelly_for_portfolio")

    def test_calculate_kelly_for_portfolio_basic(self):
        """基本的な統合ポートフォリオケリー基準計算テスト"""
        # モックポートフォリオ作成
        mock_portfolio = MagicMock()
        mock_trades = MagicMock()

        # 統合ポートフォリオのトレードデータ作成（勝率60%）
        trades_df = pd.DataFrame(
            {
                "PnL": [100.0, -50.0, 150.0, -30.0, 200.0],  # 勝率60%
                "Entry Price": [1000.0, 1100.0, 1050.0, 1200.0, 1150.0],
                "Exit Price": [1100.0, 1050.0, 1200.0, 1170.0, 1350.0],
            }
        )

        mock_trades.records_readable = trades_df
        mock_portfolio.trades = mock_trades

        # ケリー基準計算
        kelly_value, stats = self.strategy._calculate_kelly_for_portfolio(
            mock_portfolio
        )

        # 勝率60%、平均勝ち=150、平均負け=40、b=3.75
        # Kelly = (0.6 * 3.75 - 0.4) / 3.75 = 0.493
        assert isinstance(kelly_value, float)
        assert kelly_value > 0  # 正のケリー値
        assert stats["win_rate"] == pytest.approx(0.6, rel=0.01)
        assert stats["total_trades"] == 5

    def test_calculate_kelly_zero_win_rate(self):
        """勝率0%のケリー基準計算テスト"""
        mock_portfolio = MagicMock()
        mock_trades = MagicMock()

        # すべて負けトレード
        trades_df = pd.DataFrame(
            {
                "PnL": [-50.0, -30.0, -100.0],
            }
        )

        mock_trades.records_readable = trades_df
        mock_portfolio.trades = mock_trades

        kelly_value, stats = self.strategy._calculate_kelly_for_portfolio(
            mock_portfolio
        )

        # 勝率0%の場合、ケリー値は負になる
        assert kelly_value < 0
        assert stats["win_rate"] == 0.0

    def test_calculate_kelly_perfect_win_rate(self):
        """勝率100%のケリー基準計算テスト"""
        mock_portfolio = MagicMock()
        mock_trades = MagicMock()

        # すべて勝ちトレード
        trades_df = pd.DataFrame(
            {
                "PnL": [100.0, 150.0, 200.0],
            }
        )

        mock_trades.records_readable = trades_df
        mock_portfolio.trades = mock_trades

        kelly_value, stats = self.strategy._calculate_kelly_for_portfolio(
            mock_portfolio
        )

        # 勝率100%の場合、ケリー値は1.0になる
        assert kelly_value > 0
        assert stats["win_rate"] == 1.0
        assert stats["avg_loss"] == 0.0

    def test_calculate_kelly_no_trades(self):
        """トレードなしのケリー基準計算テスト"""
        mock_portfolio = MagicMock()
        mock_trades = MagicMock()

        # トレードなし
        trades_df = pd.DataFrame({"PnL": []})

        mock_trades.records_readable = trades_df
        mock_portfolio.trades = mock_trades

        kelly_value, stats = self.strategy._calculate_kelly_for_portfolio(
            mock_portfolio
        )

        # トレードなしの場合、ケリー値は0
        assert kelly_value == 0.0
        assert stats["total_trades"] == 0

    def test_optimize_allocation_kelly_basic(self):
        """基本的なケリー基準配分最適化テスト"""
        # モックポートフォリオ作成
        mock_portfolio = MagicMock()
        self.strategy.combined_portfolio = mock_portfolio

        # _calculate_kelly_for_portfolioをモック
        def mock_calculate_kelly(portfolio):
            return (
                0.4,
                {
                    "win_rate": 0.6,
                    "avg_win": 150.0,
                    "avg_loss": 50.0,
                    "total_trades": 10,
                },
            )

        with patch.object(
            self.strategy,
            "_calculate_kelly_for_portfolio",
            side_effect=mock_calculate_kelly,
        ):
            allocation, stats = self.strategy.optimize_allocation_kelly(
                mock_portfolio, kelly_fraction=0.5
            )

            # 配分率が正しく計算されているか確認（float型）
            assert isinstance(allocation, float)
            assert allocation == pytest.approx(0.2, rel=0.01)  # 0.4 * 0.5 = 0.2

            # 統計情報が取得できているか確認
            assert stats["win_rate"] == 0.6

    def test_optimize_allocation_kelly_min_max_constraints(self):
        """最小・最大配分率制約テスト"""
        mock_portfolio = MagicMock()
        self.strategy.combined_portfolio = mock_portfolio

        # 高いケリー値を返すモック
        def mock_calculate_kelly(portfolio):
            return (
                0.9,
                {
                    "win_rate": 0.8,
                    "avg_win": 300.0,
                    "avg_loss": 50.0,
                    "total_trades": 15,
                },
            )

        with patch.object(
            self.strategy,
            "_calculate_kelly_for_portfolio",
            side_effect=mock_calculate_kelly,
        ):
            # 最大配分率制約テスト
            allocation_max, _ = self.strategy.optimize_allocation_kelly(
                mock_portfolio,
                kelly_fraction=1.0,
                max_allocation=0.5,
            )

            # 配分率が制約内に収まっているか確認
            assert allocation_max <= 0.5

            # 最小配分率制約テスト
            allocation_min, _ = self.strategy.optimize_allocation_kelly(
                mock_portfolio,
                kelly_fraction=0.01,
                min_allocation=0.1,
            )

            assert allocation_min >= 0.1

    def test_optimize_allocation_kelly_negative_kelly(self):
        """負のケリー値処理テスト"""
        mock_portfolio = MagicMock()
        self.strategy.combined_portfolio = mock_portfolio

        # 負のケリー値を返すモック（勝率が低い）
        def mock_calculate_kelly(portfolio):
            return (
                -0.2,
                {
                    "win_rate": 0.3,
                    "avg_win": 50.0,
                    "avg_loss": 100.0,
                    "total_trades": 5,
                },
            )

        with patch.object(
            self.strategy,
            "_calculate_kelly_for_portfolio",
            side_effect=mock_calculate_kelly,
        ):
            allocation, _ = self.strategy.optimize_allocation_kelly(
                mock_portfolio, kelly_fraction=0.5, min_allocation=0.01
            )

            # 負のケリー値は最小配分になっているか確認
            assert allocation == 0.01

    def test_optimize_allocation_kelly_error_handling(self):
        """エラーハンドリングテスト"""
        # ポートフォリオが存在しない場合、均等配分を返す
        self.strategy.combined_portfolio = None
        self.strategy.portfolio = None

        allocation, stats = self.strategy.optimize_allocation_kelly(MagicMock())

        # エラー時は均等配分を返すことを確認
        expected_allocation = 1.0 / len(self.strategy.stock_codes)
        assert allocation == pytest.approx(expected_allocation, rel=0.01)

    def test_optimize_allocation_kelly_half_kelly(self):
        """Half Kelly係数テスト"""
        mock_portfolio = MagicMock()
        self.strategy.combined_portfolio = mock_portfolio

        # 固定ケリー値を返すモック
        def mock_calculate_kelly(portfolio):
            return (
                0.4,
                {
                    "win_rate": 0.6,
                    "avg_win": 100.0,
                    "avg_loss": 50.0,
                    "total_trades": 10,
                },
            )

        with patch.object(
            self.strategy,
            "_calculate_kelly_for_portfolio",
            side_effect=mock_calculate_kelly,
        ):
            # Half Kelly
            allocation_half, _ = self.strategy.optimize_allocation_kelly(
                mock_portfolio, kelly_fraction=0.5
            )

            # Full Kelly
            allocation_full, _ = self.strategy.optimize_allocation_kelly(
                mock_portfolio, kelly_fraction=1.0
            )

            # Half KellyがFull Kellyの約半分になっているか確認
            ratio = allocation_half / allocation_full
            assert ratio == pytest.approx(0.5, rel=0.01)

    def test_run_optimized_backtest_kelly_integration(self):
        """ケリー基準2段階最適化バックテスト統合テスト"""
        # モックメソッド準備
        mock_portfolio_1 = MagicMock()
        mock_portfolio_1.total_return.return_value = 0.10  # 10%

        mock_portfolio_2 = MagicMock()
        mock_portfolio_2.total_return.return_value = 0.20  # 20%

        # run_multi_backtest は (portfolio, entries) のタプルを返す
        mock_entries = MagicMock()

        optimized_allocation = 0.14  # 14%配分

        stats = {
            "win_rate": 0.55,
            "avg_win": 120.0,
            "avg_loss": 60.0,
            "total_trades": 100,
        }

        with (
            patch.object(
                self.strategy,
                "run_multi_backtest",
                side_effect=[
                    (mock_portfolio_1, mock_entries),
                    (mock_portfolio_2, mock_entries),
                ],
            ),
            patch.object(
                self.strategy,
                "optimize_allocation_kelly",
                return_value=(optimized_allocation, stats),
            ),
        ):
            (
                initial_pf,
                final_pf,
                allocation,
                result_stats,
                all_entries,
            ) = self.strategy.run_optimized_backtest_kelly()

            # 2段階バックテストが実行されているか確認
            assert initial_pf == mock_portfolio_1
            assert final_pf == mock_portfolio_2
            assert allocation == optimized_allocation
            assert result_stats == stats
            assert all_entries == mock_entries

            # リターンが改善しているか確認
            initial_return = initial_pf.total_return()
            final_return = final_pf.total_return()
            assert final_return >= initial_return

    def test_logging_messages(self):
        """ログメッセージ記録テスト"""
        mock_portfolio = MagicMock()
        self.strategy.combined_portfolio = mock_portfolio

        def mock_calculate_kelly(portfolio):
            return (
                0.3,
                {
                    "win_rate": 0.6,
                    "avg_win": 100.0,
                    "avg_loss": 50.0,
                    "total_trades": 10,
                },
            )

        with patch.object(
            self.strategy,
            "_calculate_kelly_for_portfolio",
            side_effect=mock_calculate_kelly,
        ):
            self.strategy.optimize_allocation_kelly(mock_portfolio)

            # ログメッセージが記録されているか確認
            assert len(self.strategy.log_messages) > 0

            # 特定のメッセージが含まれているか確認
            messages = [msg for msg, level in self.strategy.log_messages]
            assert any("ケリー基準" in msg for msg in messages)
            assert any("戦略全体勝率" in msg for msg in messages)

    def test_portfolio_allocation_output_format(self):
        """配分率出力形式テスト（単一値）"""
        mock_portfolio = MagicMock()
        self.strategy.combined_portfolio = mock_portfolio

        def mock_calculate_kelly(portfolio):
            return (
                0.275,
                {
                    "win_rate": 0.55,
                    "avg_win": 120.0,
                    "avg_loss": 60.0,
                    "total_trades": 50,
                },
            )

        with patch.object(
            self.strategy,
            "_calculate_kelly_for_portfolio",
            side_effect=mock_calculate_kelly,
        ):
            allocation, stats = self.strategy.optimize_allocation_kelly(
                mock_portfolio, kelly_fraction=0.5
            )

            # 配分率が単一のfloat値であることを確認
            assert isinstance(allocation, float)
            assert not isinstance(allocation, dict)

            # 実運用で使える形式（各銘柄に同じ配分率）
            assert allocation == pytest.approx(0.1375, rel=0.01)  # 0.275 * 0.5

    def test_zero_trades_handling(self):
        """トレード0件時の処理テスト"""
        mock_portfolio = MagicMock()
        mock_trades = MagicMock()
        mock_trades.records_readable = pd.DataFrame()  # 空のDataFrame
        mock_portfolio.trades = mock_trades

        self.strategy.combined_portfolio = mock_portfolio

        allocation, stats = self.strategy.optimize_allocation_kelly(mock_portfolio)

        # トレード0件時はデフォルト配分を返す
        expected_allocation = 1.0 / len(self.strategy.stock_codes)
        assert allocation == pytest.approx(expected_allocation, rel=0.01)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
