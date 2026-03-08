"""
戦略自動生成モジュール

シグナルの組み合わせを自動生成し、戦略候補を作成
"""

import random
import uuid
from typing import Any

from loguru import logger

from .models import (
    GeneratorConfig,
    SignalConstraints,
    StrategyCandidate,
)
from .signal_catalog import AVAILABLE_SIGNALS, SIGNAL_CONSTRAINTS_MAP
from .signal_param_factory import UsageType, build_signal_params


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

    def _filter_signals(self, usage_type: UsageType) -> list[SignalConstraints]:
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
        usage_type: UsageType,
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
        self, signals: list[SignalConstraints], usage_type: UsageType
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
            params[signal.name] = build_signal_params(signal.name, usage_type, random)

        return params

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
                entry_params[signal_name] = build_signal_params(
                    signal_name, "entry", random
                )

            for signal_name in template_signals.get("exit", []):
                exit_params[signal_name] = build_signal_params(
                    signal_name, "exit", random
                )

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
