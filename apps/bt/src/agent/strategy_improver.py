"""
戦略改善モジュール

既存戦略の弱点分析と改善提案を生成
"""

import copy
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.strategy_config.loader import ConfigLoader

from .models import Improvement, WeaknessReport
from .strategy_generator import AVAILABLE_SIGNALS


class StrategyImprover:
    """
    戦略改善クラス

    既存戦略の弱点を分析し、改善提案を生成
    """

    # 弱点パターンと対応するシグナル提案
    WEAKNESS_SIGNAL_MAP: dict[str, list[str]] = {
        "high_drawdown": ["atr_support_break", "rsi_threshold"],
        "low_win_rate": ["volume", "bollinger_bands"],
        "low_sharpe": ["beta", "trading_value_range"],
        "few_trades": ["period_breakout", "ma_breakout"],
        "market_sensitivity": ["index_daily_change", "index_macd_histogram"],
    }

    def __init__(self, shared_config_dict: dict[str, Any] | None = None):
        """
        初期化

        Args:
            shared_config_dict: 共有設定辞書
        """
        self.shared_config_dict = shared_config_dict

    def analyze(self, strategy_name: str) -> WeaknessReport:
        """
        戦略の弱点を分析

        Args:
            strategy_name: 戦略名

        Returns:
            弱点分析レポート
        """
        logger.info(f"Analyzing strategy: {strategy_name}")

        # 戦略ロード
        config_loader = ConfigLoader()
        strategy_config = config_loader.load_strategy_config(strategy_name)

        # shared_configをマージ
        if self.shared_config_dict is None:
            self.shared_config_dict = config_loader.merge_shared_config(strategy_config)

        # SignalParams構築
        entry_params = SignalParams(**strategy_config.get("entry_filter_params", {}))
        exit_params = SignalParams(**strategy_config.get("exit_trigger_params", {}))
        shared_config = SharedConfig(**self.shared_config_dict)

        # 戦略インスタンス作成
        strategy = YamlConfigurableStrategy(
            shared_config=shared_config,
            entry_filter_params=entry_params,
            exit_trigger_params=exit_params,
        )

        # バックテスト実行
        _, portfolio, _, _, _ = strategy.run_optimized_backtest_kelly(
            kelly_fraction=shared_config.kelly_fraction,
            min_allocation=shared_config.min_allocation,
            max_allocation=shared_config.max_allocation,
        )

        # 弱点分析
        report = self._analyze_portfolio(portfolio, strategy_name)

        # 改善提案生成
        report.suggested_improvements = self._generate_improvement_suggestions(
            report, strategy_config
        )

        return report

    def _analyze_portfolio(
        self, portfolio: Any, strategy_name: str
    ) -> WeaknessReport:
        """
        ポートフォリオを分析

        Args:
            portfolio: VectorBTポートフォリオ
            strategy_name: 戦略名

        Returns:
            弱点レポート
        """
        report = WeaknessReport(strategy_name=strategy_name)

        # 最大ドローダウン
        try:
            max_dd = float(portfolio.max_drawdown())
            if pd.notna(max_dd) and np.isfinite(max_dd):
                report.max_drawdown = max_dd
            else:
                report.max_drawdown = 0.0

            # ドローダウン期間分析
            drawdown_series = portfolio.drawdown()
            if hasattr(drawdown_series, "values"):
                dd_values = drawdown_series.values
                if len(dd_values) > 0:
                    # 最大ドローダウン開始・終了を推定
                    min_idx = np.argmin(dd_values)
                    if hasattr(drawdown_series, "index"):
                        report.max_drawdown_end = str(drawdown_series.index[min_idx])

                        # 開始点を探索（ドローダウンが始まった点）
                        start_idx = min_idx
                        for i in range(min_idx - 1, -1, -1):
                            if dd_values[i] >= -0.001:  # ほぼゼロ
                                start_idx = i
                                break
                        dd_start = str(drawdown_series.index[start_idx])
                        report.max_drawdown_start = dd_start
                        report.max_drawdown_duration_days = min_idx - start_idx

        except Exception as e:
            logger.warning(f"Drawdown analysis failed: {e}")

        # 負けトレードパターン分析
        try:
            trades = portfolio.trades.records_readable
            if len(trades) > 0:
                losing_trades = trades[trades["Return"] < 0]
                if len(losing_trades) > 0:
                    # 最大損失トレード
                    worst_trade = losing_trades.loc[losing_trades["Return"].idxmin()]
                    report.losing_trade_patterns.append(
                        {
                            "type": "worst_trade",
                            "return": float(worst_trade["Return"]),
                            "entry_date": str(worst_trade.get("Entry Timestamp", "")),
                            "exit_date": str(worst_trade.get("Exit Timestamp", "")),
                        }
                    )

                    # 連続損失
                    consecutive_losses = 0
                    max_consecutive = 0
                    for _, trade in trades.iterrows():
                        if trade["Return"] < 0:
                            consecutive_losses += 1
                            max_consecutive = max(max_consecutive, consecutive_losses)
                        else:
                            consecutive_losses = 0

                    if max_consecutive >= 3:
                        report.losing_trade_patterns.append(
                            {
                                "type": "consecutive_losses",
                                "count": max_consecutive,
                            }
                        )
        except Exception as e:
            logger.warning(f"Trade pattern analysis failed: {e}")

        # 市場環境別パフォーマンス（簡易版）
        try:
            returns = portfolio.returns()
            if hasattr(returns, "values"):
                # 上昇相場・下落相場の判定（簡易）
                positive_returns = returns[returns > 0]
                negative_returns = returns[returns < 0]

                report.performance_by_market_condition = {
                    "bull_market_avg": float(positive_returns.mean())
                    if len(positive_returns) > 0
                    else 0.0,
                    "bear_market_avg": float(negative_returns.mean())
                    if len(negative_returns) > 0
                    else 0.0,
                    "bull_ratio": len(positive_returns) / len(returns)
                    if len(returns) > 0
                    else 0.0,
                }
        except Exception as e:
            logger.warning(f"Market condition analysis failed: {e}")

        return report

    def _generate_improvement_suggestions(
        self, report: WeaknessReport, strategy_config: dict[str, Any]
    ) -> list[str]:
        """
        改善提案を生成

        Args:
            report: 弱点レポート
            strategy_config: 戦略設定

        Returns:
            改善提案リスト
        """
        suggestions: list[str] = []

        # 高ドローダウン対策
        if report.max_drawdown > 0.3:  # 30%以上
            suggestions.append(
                f"最大ドローダウンが{report.max_drawdown:.1%}と高い。"
                "ATRサポートブレイクやRSI閾値によるエグジット条件追加を推奨"
            )

        # 連続損失対策
        for pattern in report.losing_trade_patterns:
            is_consecutive = pattern.get("type") == "consecutive_losses"
            if is_consecutive and pattern.get("count", 0) >= 5:
                suggestions.append(
                    f"連続{pattern['count']}回の損失が発生。"
                    "ボリュームフィルターやボリンジャーバンドの追加を推奨"
                )

        # 市場感応度対策
        market_perf = report.performance_by_market_condition
        if market_perf:
            bear_avg = market_perf.get("bear_market_avg", 0)
            if bear_avg < -0.02:  # 下落相場で2%以上損失
                suggestions.append(
                    "下落相場でのパフォーマンスが悪い。"
                    "指数前日比シグナルやINDEXヒストグラムシグナルの追加を推奨"
                )

        # 現在のシグナル確認
        entry_signals = strategy_config.get("entry_filter_params", {})
        exit_signals = strategy_config.get("exit_trigger_params", {})
        used_signals = set(entry_signals.keys()) | set(exit_signals.keys())

        # 未使用シグナルの提案
        for signal in AVAILABLE_SIGNALS:
            if signal.name not in used_signals:
                # 推奨組み合わせチェック
                for rec in signal.recommended_with:
                    if rec in used_signals:
                        suggestions.append(
                            f"{signal.name}シグナルの追加を検討。"
                            f"{rec}と相性が良い"
                        )
                        break

        return suggestions[:5]  # 最大5件

    def suggest_improvements(
        self, report: WeaknessReport, strategy_config: dict[str, Any]
    ) -> list[Improvement]:
        """
        具体的な改善案を生成

        Args:
            report: 弱点レポート
            strategy_config: 戦略設定

        Returns:
            改善案リスト
        """
        improvements: list[Improvement] = []

        entry_signals = strategy_config.get("entry_filter_params", {})
        exit_signals = strategy_config.get("exit_trigger_params", {})

        # 高ドローダウン対策
        if report.max_drawdown > 0.3:
            # ATRサポートブレイクをエグジットに追加
            if "atr_support_break" not in exit_signals:
                improvements.append(
                    Improvement(
                        improvement_type="add_signal",
                        target="exit",
                        signal_name="atr_support_break",
                        changes={
                            "enabled": True,
                            "direction": "break",
                            "lookback_period": 20,
                            "atr_multiplier": 3.0,
                            "price_column": "close",
                        },
                        reason=f"最大ドローダウン{report.max_drawdown:.1%}を軽減",
                        expected_impact="早期損切りによるドローダウン軽減",
                    )
                )

        # ボリュームフィルター追加
        if "volume" not in entry_signals:
            improvements.append(
                Improvement(
                    improvement_type="add_signal",
                    target="entry",
                    signal_name="volume",
                    changes={
                        "enabled": True,
                        "direction": "surge",
                        "threshold": 1.5,
                        "short_period": 50,
                        "long_period": 150,
                        "ma_type": "sma",
                    },
                    reason="エントリー精度向上",
                    expected_impact="出来高急増時のみエントリーで勝率向上",
                )
            )

        # 市場環境フィルター追加
        market_perf = report.performance_by_market_condition
        if market_perf and market_perf.get("bear_market_avg", 0) < -0.02:
            if "index_daily_change" not in entry_signals:
                improvements.append(
                    Improvement(
                        improvement_type="add_signal",
                        target="entry",
                        signal_name="index_daily_change",
                        changes={
                            "enabled": True,
                            "max_daily_change_pct": 1.0,
                            "direction": "below",
                        },
                        reason="下落相場でのパフォーマンス改善",
                        expected_impact="市場過熱時のエントリー回避",
                    )
                )

        return improvements

    def apply_improvements(
        self,
        strategy_config: dict[str, Any],
        improvements: list[Improvement],
    ) -> dict[str, Any]:
        """
        改善案を戦略設定に適用

        Args:
            strategy_config: 元の戦略設定
            improvements: 改善案リスト

        Returns:
            改善後の戦略設定
        """
        improved = copy.deepcopy(strategy_config)

        for improvement in improvements:
            if improvement.improvement_type == "add_signal":
                target_key = (
                    "entry_filter_params"
                    if improvement.target == "entry"
                    else "exit_trigger_params"
                )

                if target_key not in improved:
                    improved[target_key] = {}

                improved[target_key][improvement.signal_name] = improvement.changes

            elif improvement.improvement_type == "remove_signal":
                target_key = (
                    "entry_filter_params"
                    if improvement.target == "entry"
                    else "exit_trigger_params"
                )

                signal = improvement.signal_name
                if target_key in improved and signal in improved[target_key]:
                    del improved[target_key][signal]

            elif improvement.improvement_type == "adjust_param":
                target_key = (
                    "entry_filter_params"
                    if improvement.target == "entry"
                    else "exit_trigger_params"
                )

                signal = improvement.signal_name
                if target_key in improved and signal in improved[target_key]:
                    improved[target_key][signal].update(improvement.changes)

        return improved
