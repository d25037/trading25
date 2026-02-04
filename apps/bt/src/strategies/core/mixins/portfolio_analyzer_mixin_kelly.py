"""
ケリー基準ポートフォリオ最適化ミックスイン

YamlConfigurableStrategy用のケリー基準を用いたポートフォリオ最適化機能を提供します。
統合ポートフォリオ全体の統計から最適配分率を計算します。
"""

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import pandas as pd
import vectorbt as vbt

if TYPE_CHECKING:
    from .protocols import StrategyProtocol


class PortfolioAnalyzerKellyMixin:
    """ケリー基準ポートフォリオ最適化ミックスイン"""

    def optimize_allocation_kelly(
        self: "StrategyProtocol",
        portfolio: vbt.Portfolio,
        kelly_fraction: float = 0.5,
        min_allocation: float = 0.0,
        max_allocation: float = 1.0,
    ) -> Tuple[float, Dict[str, float]]:
        """
        統合ポートフォリオ全体からケリー基準配分率を計算

        Args:
            portfolio: 第1段階で実行されたポートフォリオ
            kelly_fraction: ケリー基準の適用率（0.5 = Half Kelly, 1.0 = Full Kelly）
            min_allocation: 最小配分率
            max_allocation: 最大配分率

        Returns:
            Tuple[float, Dict[str, float]]:
                - 各銘柄への配分率（単一値）
                - 統計情報辞書
        """
        try:
            self._log("🔧 ケリー基準による配分最適化開始", "info")

            # ポートフォリオ参照を設定
            _ = portfolio  # 引数使用の明示

            # 統合ポートフォリオ全体のケリー基準を計算
            if self.combined_portfolio is not None:
                portfolio = self.combined_portfolio
                self._log("📊 統合ポートフォリオ全体から統計を計算", "debug")
            elif self.portfolio is not None:
                portfolio = self.portfolio
                self._log("📊 個別ポートフォリオから統計を計算", "debug")
            else:
                self._log("バックテストを先に実行してください", "error")
                # エラー時はデフォルト配分を返す
                default_allocation = 1.0 / len(self.stock_codes)
                return default_allocation, {}

            # 統合ポートフォリオ全体のケリー計算
            kelly_value, stats = self._calculate_kelly_for_portfolio(portfolio)

            # ケリー基準適用
            if kelly_value > 0:
                optimized_allocation = kelly_value * kelly_fraction
                # 制約適用
                optimized_allocation = max(
                    min_allocation, min(max_allocation, optimized_allocation)
                )
            elif kelly_value == 0:
                # トレード0件などでケリー値が0の場合は均等配分
                self._log("ケリー値が0のため均等配分を使用", "warning")
                optimized_allocation = 1.0 / len(self.stock_codes)
            else:
                # 負のケリー値の場合は最小配分
                self._log(
                    f"負のケリー値のため最小配分を使用: {kelly_value:.3f}", "warning"
                )
                optimized_allocation = min_allocation

            # 結果サマリー
            self._log("✅ ケリー基準配分最適化完了", "info")
            self._log(f"  - 戦略全体勝率: {stats['win_rate']:.1%}", "info")
            self._log(f"  - 平均勝ちトレード: {stats['avg_win']:.2f}", "info")
            self._log(f"  - 平均負けトレード: {stats['avg_loss']:.2f}", "info")
            self._log(f"  - 全トレード数: {stats['total_trades']}", "info")
            self._log(f"  - Full Kelly: {kelly_value:.1%}", "info")
            self._log(
                f"  - Kelly係数: {kelly_fraction} ({'Half Kelly' if kelly_fraction == 0.5 else 'Full Kelly' if kelly_fraction == 1.0 else f'{kelly_fraction}x Kelly'})",
                "info",
            )
            self._log(f"  - 最適配分率: {optimized_allocation:.1%}", "info")
            self._log(
                "  - 実運用: シグナルが出た銘柄にこの配分率で投資",
                "info",
            )

            return optimized_allocation, stats

        except Exception as e:
            self._log(f"ケリー基準配分最適化エラー: {e}", "error")
            # エラー時はデフォルト配分を返す
            default_allocation = 1.0 / len(self.stock_codes)
            return default_allocation, {}

    def _calculate_kelly_for_portfolio(
        self, portfolio: vbt.Portfolio
    ) -> Tuple[float, Dict[str, float]]:
        """
        統合ポートフォリオ全体のケリー基準を計算

        Args:
            portfolio: VectorBTポートフォリオ

        Returns:
            Tuple[float, Dict[str, float]]: (ケリー基準値, 統計情報辞書)
        """
        try:
            # トレード記録を取得
            trades: Any = portfolio.trades  # VectorBT動的型のため型推論回避

            # trades.records_readable から全トレード統計を計算
            if hasattr(trades, "records_readable"):
                trades_df: Any = trades.records_readable  # VectorBT動的型

                # トレードがない場合
                if len(trades_df) == 0:
                    return 0.0, {
                        "win_rate": 0.0,
                        "avg_win": 0.0,
                        "avg_loss": 0.0,
                        "total_trades": 0,
                    }

                # 全トレードのPnL（銘柄フィルタなし）
                pnl_series = trades_df["PnL"]

                # 戦略全体の統計計算
                win_rate = (
                    (pnl_series > 0).sum() / len(pnl_series)
                    if len(pnl_series) > 0
                    else 0.0
                )

                # 平均勝ちトレード
                avg_win = (
                    pnl_series[pnl_series > 0].mean() if (pnl_series > 0).any() else 0.0
                )

                # 平均負けトレード（絶対値）
                avg_loss = (
                    abs(pnl_series[pnl_series < 0].mean())
                    if (pnl_series < 0).any()
                    else 0.0
                )

                # ケリー基準計算
                # Full Kelly: f* = (win_rate * b - (1 - win_rate)) / b
                # where b = avg_win / avg_loss
                if avg_loss > 0 and avg_win > 0:
                    b = avg_win / avg_loss  # オッズ比
                    # b が 0 でないことを確認（avg_win > 0 で保証されるが明示的にチェック）
                    if b > 0:
                        kelly = (win_rate * b - (1 - win_rate)) / b
                    else:
                        # b が 0 の場合（起こり得ないがゼロ除算防止）
                        kelly = 0.0
                elif avg_loss > 0 and avg_win == 0:
                    # 勝ちトレードがない場合（すべて負け）
                    # ケリー基準は負になる（ポジションを取るべきでない）
                    kelly = -1.0
                else:
                    # 負けトレードがない場合（すべて勝ち）
                    kelly = win_rate if win_rate > 0 else 0.0

                stats = {
                    "win_rate": win_rate,
                    "avg_win": avg_win,
                    "avg_loss": avg_loss,
                    "total_trades": len(pnl_series),
                    "kelly": kelly,
                }

                return kelly, stats

            else:
                # records_readableがない場合
                return 0.0, {
                    "win_rate": 0.0,
                    "avg_win": 0.0,
                    "avg_loss": 0.0,
                    "total_trades": 0,
                }

        except Exception as e:
            self._log(f"統合ポートフォリオのケリー計算エラー: {e}", "debug")
            return 0.0, {
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "total_trades": 0,
            }

    def run_optimized_backtest_kelly(
        self,
        kelly_fraction: float = 0.5,
        min_allocation: float = 0.01,
        max_allocation: float = 0.5,
    ) -> Tuple[
        vbt.Portfolio, vbt.Portfolio, float, Dict[str, float], Optional[pd.DataFrame]
    ]:
        """
        ケリー基準を用いた2段階最適化バックテストを実行

        Args:
            kelly_fraction: ケリー基準の適用率（0.5 = Half Kelly推奨）
            min_allocation: 最小配分率
            max_allocation: 最大配分率

        Returns:
            Tuple[vbt.Portfolio, vbt.Portfolio, float, Dict[str, float], Optional[pd.DataFrame]]:
                (第1段階結果, 第2段階最適化結果, 各銘柄への配分率, 統計情報, エントリーシグナルDataFrame)
        """
        self._log("🚀 ケリー基準2段階最適化バックテスト開始", "info")

        try:
            # 第1段階：探索的実行（均等配分）
            self._log("📊 第1段階：探索的バックテスト実行（均等配分）", "info")
            initial_portfolio, all_entries = self.run_multi_backtest()

            # ポートフォリオ参照を設定
            if self.group_by:
                self.combined_portfolio = initial_portfolio
            else:
                self.portfolio = initial_portfolio

            # ケリー基準で最適配分率を計算（統合ポートフォリオ全体）
            self._log("🔧 ケリー基準配分最適化計算開始", "info")
            optimized_allocation, stats = self.optimize_allocation_kelly(
                initial_portfolio,
                kelly_fraction=kelly_fraction,
                min_allocation=min_allocation,
                max_allocation=max_allocation,
            )

            # 第2段階：最適化実行（各銘柄に同じ配分率を適用）
            self._log(
                f"⚡ 第2段階：ケリー最適化バックテスト実行（配分率={optimized_allocation:.1%}）",
                "info",
            )

            kelly_portfolio, _ = self.run_multi_backtest(
                allocation_pct=optimized_allocation,
            )

            # 結果比較ログ
            self._log("✅ ケリー基準2段階最適化バックテスト完了", "info")
            self._log("📈 最適化効果:", "info")
            try:
                initial_return = initial_portfolio.total_return()
                kelly_return = kelly_portfolio.total_return()

                # NaN/Inf チェックと安全な改善倍率計算
                if initial_return != 0 and not (
                    pd.isna(initial_return) or pd.isna(kelly_return)
                ):
                    improvement = kelly_return / initial_return
                    # Inf/-Inf チェック
                    if not pd.isinf(improvement):
                        self._log(f"  - 第1段階リターン: {initial_return:.1%}", "info")
                        self._log(f"  - 第2段階リターン: {kelly_return:.1%}", "info")
                        self._log(f"  - 改善倍率: {improvement:.2f}x", "info")
                    else:
                        self._log(f"  - 第1段階リターン: {initial_return:.1%}", "info")
                        self._log(f"  - 第2段階リターン: {kelly_return:.1%}", "info")
                        self._log("  - 改善倍率: 計算不可（無限大）", "warning")
                else:
                    self._log(f"  - 第1段階リターン: {initial_return:.1%}", "info")
                    self._log(f"  - 第2段階リターン: {kelly_return:.1%}", "info")
                    self._log("  - 改善倍率: 計算不可（基準値が0またはNaN）", "warning")
            except Exception as e:
                self._log(f"リターン比較計算エラー: {e}", "debug")

            return (
                initial_portfolio,
                kelly_portfolio,
                optimized_allocation,
                stats,
                all_entries,
            )

        except Exception as e:
            self._log(f"ケリー基準2段階最適化バックテストエラー: {e}", "error")
            raise RuntimeError(f"ケリー基準2段階最適化バックテスト実行失敗: {e}")
