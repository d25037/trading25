"""
戦略自動生成モジュール

シグナルの組み合わせを自動生成し、戦略候補を作成
"""

import random
import uuid
from typing import Any

from loguru import logger

from .models import GeneratorConfig, SignalConstraints, StrategyCandidate
from .parameter_evolver import ParameterEvolver

# 利用可能なシグナル定義
# SignalParamsの属性から自動抽出し、制約情報を追加
AVAILABLE_SIGNALS: list[SignalConstraints] = [
    SignalConstraints(
        name="period_breakout",
        required_data=[],
        usage="both",
        recommended_with=["volume", "bollinger_bands"],
    ),
    SignalConstraints(
        name="ma_breakout",
        required_data=[],
        usage="both",
        recommended_with=["volume", "rsi_threshold"],
    ),
    SignalConstraints(
        name="crossover",
        required_data=[],
        usage="both",
        recommended_with=["volume"],
    ),
    SignalConstraints(
        name="mean_reversion",
        required_data=[],
        usage="both",
        mutually_exclusive=["period_breakout"],  # トレンドフォローと平均回帰は相反
    ),
    SignalConstraints(
        name="bollinger_bands",
        required_data=[],
        usage="both",
        recommended_with=["rsi_threshold"],
    ),
    SignalConstraints(
        name="atr_support_break",
        required_data=[],
        usage="both",
    ),
    SignalConstraints(
        name="rsi_threshold",
        required_data=[],
        usage="both",
        recommended_with=["volume"],
    ),
    SignalConstraints(
        name="rsi_spread",
        required_data=[],
        usage="both",
        mutually_exclusive=["rsi_threshold"],  # RSI系は1つで十分
    ),
    SignalConstraints(
        name="volume",
        required_data=[],
        usage="both",
    ),
    SignalConstraints(
        name="trading_value",
        required_data=[],
        usage="both",
        mutually_exclusive=["trading_value_range"],
    ),
    SignalConstraints(
        name="trading_value_range",
        required_data=[],
        usage="both",
        mutually_exclusive=["trading_value"],
    ),
    SignalConstraints(
        name="beta",
        required_data=["benchmark_data"],
        usage="entry",  # Entryフィルターとして使用
        recommended_with=["volume"],
    ),
    SignalConstraints(
        name="margin",
        required_data=["margin_data"],
        usage="entry",
    ),
    SignalConstraints(
        name="index_daily_change",
        required_data=["benchmark_data"],
        usage="both",
    ),
    SignalConstraints(
        name="index_macd_histogram",
        required_data=["benchmark_data"],
        usage="both",
    ),
    # fundamentalは複雑なネスト構造のため除外（将来対応）
    # retracement, buy_and_hold も特殊用途のため除外
]

# シグナル名→制約のマッピング
SIGNAL_CONSTRAINTS_MAP: dict[str, SignalConstraints] = {
    s.name: s for s in AVAILABLE_SIGNALS
}


class StrategyGenerator:
    """
    戦略自動生成クラス

    シグナルの組み合わせをランダムに生成し、戦略候補を作成する
    """

    def __init__(self, config: GeneratorConfig | None = None):
        """
        初期化

        Args:
            config: 生成設定（省略時はデフォルト）
        """
        self.config = config or GeneratorConfig()

        # 乱数シード設定
        if self.config.seed is not None:
            random.seed(self.config.seed)

        # 利用可能シグナルをフィルタリング
        self.entry_signals = self._filter_signals("entry")
        self.exit_signals = self._filter_signals("exit")

        logger.info(
            f"StrategyGenerator initialized: "
            f"entry_signals={len(self.entry_signals)}, "
            f"exit_signals={len(self.exit_signals)}"
        )

    def _filter_signals(self, usage_type: str) -> list[SignalConstraints]:
        """
        使用タイプでシグナルをフィルタリング

        Args:
            usage_type: "entry" or "exit"

        Returns:
            フィルタリング済みシグナルリスト
        """
        filtered = []
        for signal in AVAILABLE_SIGNALS:
            # 除外シグナルをスキップ
            if signal.name in self.config.exclude_signals:
                continue

            # 使用タイプでフィルタ
            if signal.usage == "both" or signal.usage == usage_type:
                filtered.append(signal)

        return filtered

    def generate(self, n_strategies: int | None = None) -> list[StrategyCandidate]:
        """
        戦略候補を生成

        Args:
            n_strategies: 生成数（省略時はconfig値）

        Returns:
            戦略候補リスト
        """
        n = n_strategies or self.config.n_strategies
        candidates: list[StrategyCandidate] = []

        for i in range(n):
            try:
                candidate = self._generate_single_candidate(i)
                candidates.append(candidate)
            except Exception as e:
                logger.warning(f"Failed to generate candidate {i}: {e}")
                continue

        logger.info(f"Generated {len(candidates)} strategy candidates")
        return candidates

    def _generate_single_candidate(self, index: int) -> StrategyCandidate:
        """
        単一の戦略候補を生成

        Args:
            index: 候補インデックス

        Returns:
            戦略候補
        """
        # Entryシグナル選択
        n_entry = random.randint(
            self.config.entry_signal_min, self.config.entry_signal_max
        )
        entry_signals = self._select_signals(self.entry_signals, n_entry, "entry")

        # Exitシグナル選択
        n_exit = random.randint(
            self.config.exit_signal_min, self.config.exit_signal_max
        )
        exit_signals = self._select_signals(self.exit_signals, n_exit, "exit")

        # 必須シグナルを追加
        for required in self.config.required_signals:
            if required not in [s.name for s in entry_signals]:
                constraint = SIGNAL_CONSTRAINTS_MAP.get(required)
                if constraint and (constraint.usage in ["both", "entry"]):
                    entry_signals.append(constraint)

        # パラメータ辞書を構築
        entry_params = self._build_signal_params(entry_signals, "entry")
        exit_params = self._build_signal_params(exit_signals, "exit")

        # 戦略IDを生成
        strategy_id = f"auto_{uuid.uuid4().hex[:8]}"

        return StrategyCandidate(
            strategy_id=strategy_id,
            entry_filter_params=entry_params,
            exit_trigger_params=exit_params,
            metadata={
                "generation_index": index,
                "entry_signals": [s.name for s in entry_signals],
                "exit_signals": [s.name for s in exit_signals],
            },
        )

    def _select_signals(
        self,
        available: list[SignalConstraints],
        n: int,
        usage_type: str,
    ) -> list[SignalConstraints]:
        """
        制約を考慮してシグナルを選択

        Args:
            available: 利用可能シグナル
            n: 選択数
            usage_type: "entry" or "exit"

        Returns:
            選択されたシグナルリスト
        """
        selected: list[SignalConstraints] = []
        candidates = available.copy()
        random.shuffle(candidates)

        for signal in candidates:
            if len(selected) >= n:
                break

            # 相互排他チェック
            if self._is_mutually_exclusive(signal, selected):
                continue

            selected.append(signal)

        return selected

    def _is_mutually_exclusive(
        self, signal: SignalConstraints, selected: list[SignalConstraints]
    ) -> bool:
        """
        相互排他チェック

        Args:
            signal: チェック対象シグナル
            selected: 既に選択されたシグナル

        Returns:
            True if mutually exclusive
        """
        selected_names = {s.name for s in selected}
        return bool(set(signal.mutually_exclusive) & selected_names)

    def _build_signal_params(
        self, signals: list[SignalConstraints], usage_type: str
    ) -> dict[str, Any]:
        """
        シグナルパラメータ辞書を構築

        Args:
            signals: シグナルリスト
            usage_type: "entry" or "exit"

        Returns:
            パラメータ辞書
        """
        params: dict[str, Any] = {}

        for signal in signals:
            # デフォルトパラメータを取得
            default_params = self._get_default_params(signal.name, usage_type)
            default_params["enabled"] = True
            # パラメータをランダム化
            randomized_params = self._randomize_params(signal.name, default_params)
            params[signal.name] = randomized_params

        return params

    def _randomize_params(
        self, signal_name: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        """
        パラメータをPARAM_RANGES内でランダム化

        Args:
            signal_name: シグナル名
            params: デフォルトパラメータ

        Returns:
            ランダム化されたパラメータ
        """
        randomized = params.copy()
        ranges = ParameterEvolver.PARAM_RANGES.get(signal_name, {})

        for param_name in params:
            # enabled, カテゴリカルパラメータはスキップ
            if param_name == "enabled":
                continue

            if param_name in ranges:
                min_val, max_val, param_type = ranges[param_name]

                if param_type == "int":
                    randomized[param_name] = random.randint(int(min_val), int(max_val))
                else:  # float
                    randomized[param_name] = random.uniform(min_val, max_val)

        return randomized

    def _get_default_params(self, signal_name: str, usage_type: str) -> dict[str, Any]:
        """
        シグナルのデフォルトパラメータを取得

        Args:
            signal_name: シグナル名
            usage_type: "entry" or "exit"

        Returns:
            デフォルトパラメータ辞書
        """
        # シグナル固有のデフォルト値を抽出
        signal_defaults: dict[str, dict[str, Any]] = {
            "period_breakout": {
                "direction": "high" if usage_type == "entry" else "low",
                "condition": "break",
                "period": 200,
                "lookback_days": 10 if usage_type == "entry" else 1,
            },
            "ma_breakout": {
                "period": 200,
                "ma_type": "sma",
                "direction": "above" if usage_type == "entry" else "below",
                "lookback_days": 1,
            },
            "crossover": {
                "type": "sma",
                "direction": "golden" if usage_type == "entry" else "dead",
                "fast_period": 10,
                "slow_period": 30,
                "signal_period": 9,
                "lookback_days": 1,
            },
            "mean_reversion": {
                "baseline_type": "sma",
                "baseline_period": 25,
                "deviation_threshold": 0.2 if usage_type == "entry" else 0.0,
                "deviation_direction": "below" if usage_type == "entry" else "above",
                "recovery_price": "none" if usage_type == "entry" else "high",
                "recovery_direction": "above",
            },
            "bollinger_bands": {
                "window": 50,
                "alpha": 2.0,
                "position": "below_upper" if usage_type == "entry" else "above_upper",
            },
            "atr_support_break": {
                "direction": "recovery" if usage_type == "entry" else "break",
                "lookback_period": 20,
                "atr_multiplier": 3.0,
                "price_column": "close",
            },
            "rsi_threshold": {
                "period": 14,
                "threshold": 40.0 if usage_type == "entry" else 70.0,
                "condition": "above" if usage_type == "entry" else "below",
            },
            "rsi_spread": {
                "fast_period": 9,
                "slow_period": 14,
                "threshold": 10.0,
                "condition": "above" if usage_type == "entry" else "below",
            },
            "volume": {
                "direction": "surge" if usage_type == "entry" else "drop",
                "threshold": 1.5 if usage_type == "entry" else 0.5,
                "short_period": 50,
                "long_period": 150,
                "ma_type": "sma",
            },
            "trading_value": {
                "direction": "above",
                "period": 15,
                "threshold_value": 1.0,
            },
            "trading_value_range": {
                "period": 15,
                "min_threshold": 1.0,
                "max_threshold": 75.0,
            },
            "beta": {
                "lookback_period": 50,
                "min_beta": 0.2,
                "max_beta": 3.0,
            },
            "margin": {
                "lookback_period": 150,
                "percentile_threshold": 0.2,
            },
            "index_daily_change": {
                "max_daily_change_pct": 1.0,
                "direction": "below" if usage_type == "entry" else "above",
            },
            "index_macd_histogram": {
                "fast_period": 12,
                "slow_period": 26,
                "signal_period": 9,
                "direction": "positive" if usage_type == "entry" else "negative",
            },
        }

        return signal_defaults.get(signal_name, {})

    def generate_from_template(
        self, template_signals: dict[str, list[str]], n_variations: int = 10
    ) -> list[StrategyCandidate]:
        """
        テンプレートからバリエーションを生成

        Args:
            template_signals: {"entry": [...], "exit": [...]}
            n_variations: 生成するバリエーション数

        Returns:
            戦略候補リスト
        """
        candidates: list[StrategyCandidate] = []

        for i in range(n_variations):
            # テンプレートをベースにパラメータを変動
            entry_params = {}
            exit_params = {}

            for signal_name in template_signals.get("entry", []):
                params = self._get_default_params(signal_name, "entry")
                params["enabled"] = True
                # パラメータに若干の変動を加える
                params = self._add_parameter_variation(params)
                entry_params[signal_name] = params

            for signal_name in template_signals.get("exit", []):
                params = self._get_default_params(signal_name, "exit")
                params["enabled"] = True
                params = self._add_parameter_variation(params)
                exit_params[signal_name] = params

            strategy_id = f"template_{uuid.uuid4().hex[:8]}"

            candidates.append(
                StrategyCandidate(
                    strategy_id=strategy_id,
                    entry_filter_params=entry_params,
                    exit_trigger_params=exit_params,
                    metadata={
                        "generation_method": "template",
                        "variation_index": i,
                        "template": template_signals,
                    },
                )
            )

        return candidates

    def _add_parameter_variation(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        パラメータに変動を追加

        Args:
            params: 元パラメータ

        Returns:
            変動を加えたパラメータ
        """
        varied = params.copy()

        for key, value in params.items():
            if key == "enabled":
                continue

            if isinstance(value, int):
                # ±20%の変動（整数）
                delta = max(1, int(value * 0.2))
                varied[key] = value + random.randint(-delta, delta)
                varied[key] = max(1, varied[key])  # 最小1

            elif isinstance(value, float):
                # ±20%の変動（小数）
                delta = value * 0.2
                varied[key] = value + random.uniform(-delta, delta)
                varied[key] = max(0.01, varied[key])  # 最小0.01

        return varied
