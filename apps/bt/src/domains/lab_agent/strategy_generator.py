"""
戦略自動生成モジュール

シグナルの組み合わせを自動生成し、戦略候補を作成
"""

import random
import uuid
import copy
from typing import Any, cast

from loguru import logger

from src.shared.models.signals import SignalParams

from .models import (
    GeneratorConfig,
    SignalConstraints,
    StrategyCandidate,
)
from .signal_catalog import AVAILABLE_SIGNALS, SIGNAL_CONSTRAINTS_MAP
from .signal_search_space import PARAM_RANGES, ParamType


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

        categories = ",".join(self.config.allowed_categories) or "all"
        logger.info(
            f"StrategyGenerator initialized: "
            f"entry_signals={len(self.entry_signals)}, "
            f"exit_signals={len(self.exit_signals)}, "
            f"entry_filter_only={self.config.entry_filter_only}, "
            f"allowed_categories={categories}"
        )

    def _filter_signals(self, usage_type: str) -> list[SignalConstraints]:
        """
        使用タイプでシグナルをフィルタリング

        Args:
            usage_type: "entry" or "exit"

        Returns:
            フィルタリング済みシグナルリスト
        """
        if usage_type == "exit" and self.config.entry_filter_only:
            return []

        allowed_categories = set(self.config.allowed_categories)
        filtered = []
        for signal in AVAILABLE_SIGNALS:
            # 除外シグナルをスキップ
            if signal.name in self.config.exclude_signals:
                continue

            # カテゴリ制約
            if allowed_categories and signal.category not in allowed_categories:
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
        if self.config.entry_filter_only:
            exit_signals: list[SignalConstraints] = []
        else:
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
                "entry_filter_only": self.config.entry_filter_only,
                "allowed_categories": self.config.allowed_categories,
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
        if signal_name == "fundamental":
            return self._randomize_fundamental_params(params)

        randomized = copy.deepcopy(params)
        ranges = PARAM_RANGES.get(signal_name, {})
        if not ranges:
            return randomized

        self._randomize_nested_params(randomized, ranges)
        return randomized

    def _randomize_fundamental_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """fundamental ネスト構造のしきい値をランダム化"""
        randomized = copy.deepcopy(params)
        ranges = PARAM_RANGES.get("fundamental", {})
        self._randomize_nested_params(randomized, ranges, enabled_gated=True)
        return randomized

    def _randomize_nested_params(
        self,
        params: dict[str, Any],
        ranges: dict[str, tuple[float, float, ParamType]],
        prefix: str = "",
        enabled_gated: bool = False,
    ) -> None:
        for key, value in params.items():
            if key == "enabled":
                continue

            param_name = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                if enabled_gated and "enabled" in value and not bool(value["enabled"]):
                    continue
                self._randomize_nested_params(
                    value,
                    ranges,
                    prefix=param_name,
                    enabled_gated=enabled_gated,
                )
                continue

            if param_name not in ranges:
                continue

            min_val, max_val, param_type = ranges[param_name]
            if param_type == "int":
                params[key] = random.randint(int(min_val), int(max_val))
            else:
                params[key] = random.uniform(min_val, max_val)

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
            "fundamental": self._get_default_fundamental_params(usage_type),
        }

        if signal_name in signal_defaults:
            return signal_defaults[signal_name]

        # 新規シグナルは SignalParams のデフォルト定義から自動取得
        return self._get_signal_model_defaults(signal_name)

    def _get_default_fundamental_params(self, usage_type: str) -> dict[str, Any]:
        """fundamental 用のネストパラメータを生成"""
        defaults = self._get_signal_model_defaults("fundamental")
        if not defaults:
            return {}

        child_keys = self._list_enable_children(defaults)
        for key in child_keys:
            child = defaults.get(key)
            if isinstance(child, dict):
                child["enabled"] = False

        if usage_type == "entry" and child_keys:
            selected_key = random.choice(child_keys)
            selected = defaults.get(selected_key)
            if isinstance(selected, dict):
                selected["enabled"] = True

        return defaults

    def _get_signal_model_defaults(self, signal_name: str) -> dict[str, Any]:
        """SignalParams からシグナルのデフォルト辞書を取得する。"""
        field_info = SignalParams.model_fields.get(signal_name)
        if field_info is None:
            return {}

        default_value = field_info.get_default(call_default_factory=True)
        if default_value is None:
            return {}
        if isinstance(default_value, dict):
            return copy.deepcopy(default_value)
        if hasattr(default_value, "model_dump"):
            return cast(dict[str, Any], default_value.model_dump())
        return {}

    def _list_enable_children(self, params: dict[str, Any]) -> list[str]:
        keys: list[str] = []
        for key, value in params.items():
            if isinstance(value, dict) and "enabled" in value:
                keys.append(key)
        return keys

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
