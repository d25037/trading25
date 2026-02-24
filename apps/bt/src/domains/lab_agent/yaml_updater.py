"""
YAML自動更新モジュール

戦略設定YAMLファイルの自動生成・更新
"""

import os
from datetime import datetime
from typing import Any

from io import StringIO
from loguru import logger
from ruamel.yaml import YAML

from .models import Improvement, StrategyCandidate


class YamlUpdater:
    """
    YAML自動更新クラス

    戦略設定をYAMLファイルに書き出し
    external 管理カテゴリは外部ディレクトリ（~/.local/share/trading25）に保存
    """

    def __init__(self, base_dir: str | None = None, use_external: bool = True):
        """
        初期化

        Args:
            base_dir: 戦略設定ベースディレクトリ（Noneでデフォルト使用）
            use_external: Trueで外部ディレクトリを使用（external 管理カテゴリ用）
        """
        if base_dir is None:
            self.base_dir = "config/strategies"
        else:
            self.base_dir = base_dir
        self.use_external = use_external

    def save_candidate(
        self,
        candidate: StrategyCandidate,
        output_path: str | None = None,
        category: str = "experimental",
    ) -> str:
        """
        戦略候補をYAMLファイルに保存

        external 管理カテゴリは外部ディレクトリ（~/.local/share/trading25）に保存

        Args:
            candidate: 戦略候補
            output_path: 出力パス（省略時は自動生成）
            category: カテゴリ（experimental/production/legacy）

        Returns:
            保存したファイルパス
        """
        if output_path is None:
            # 自動生成
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{candidate.strategy_id}_{timestamp}.yaml"
            output_dir = self._resolve_auto_output_dir(category)
            output_path = os.path.join(output_dir, filename)

        # ディレクトリ作成
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # YAML構造を構築
        yaml_content = self._build_yaml_content(candidate)

        # ファイル書き込み
        ruamel_yaml = YAML()
        ruamel_yaml.default_flow_style = False
        ruamel_yaml.allow_unicode = True
        with open(output_path, "w", encoding="utf-8") as f:
            ruamel_yaml.dump(yaml_content, f)

        logger.info(f"Strategy saved to: {output_path}")
        return output_path

    def _resolve_auto_output_dir(self, category: str) -> str:
        """auto 出力時のカテゴリ別ディレクトリを解決"""
        if self.use_external:
            from src.shared.paths import get_strategies_dir
            from src.shared.paths.constants import EXTERNAL_CATEGORIES

            if category in EXTERNAL_CATEGORIES:
                return os.path.join(str(get_strategies_dir(category)), "auto")

        return os.path.join(self.base_dir, category, "auto")

    def apply_improvements(
        self,
        strategy_name: str,
        improvements: list[Improvement],
        output_path: str | None = None,
    ) -> str:
        """
        改善案を適用して新しいYAMLファイルを生成

        Args:
            strategy_name: 元の戦略名
            improvements: 改善案リスト
            output_path: 出力パス（省略時は自動生成）

        Returns:
            保存したファイルパス
        """
        from src.domains.strategy.runtime.loader import ConfigLoader

        # 元の戦略をロード
        config_loader = ConfigLoader()
        original_config = config_loader.load_strategy_config(strategy_name)

        # 改善を適用
        from .strategy_improver import StrategyImprover

        improver = StrategyImprover()
        improved_config = improver.apply_improvements(original_config, improvements)

        # 出力パス決定
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            strategy_basename = self._resolve_strategy_basename(strategy_name)
            filename = f"{strategy_basename}_improved_{timestamp}.yaml"

            # experimentalは外部ディレクトリに保存
            if self.use_external:
                from src.shared.paths import get_strategies_dir
                base_dir = str(get_strategies_dir("experimental"))
                output_path = os.path.join(base_dir, filename)
            else:
                output_path = os.path.join(self.base_dir, "experimental", filename)

        # ディレクトリ作成
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # コメント付きYAML生成
        yaml_content = self._build_improved_yaml(
            improved_config, strategy_name, improvements
        )

        # ファイル書き込み
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(yaml_content)

        logger.info(f"Improved strategy saved to: {output_path}")
        return output_path

    def _build_yaml_content(self, candidate: StrategyCandidate) -> dict[str, Any]:
        """
        戦略候補からYAML用辞書を構築

        Args:
            candidate: 戦略候補

        Returns:
            YAML用辞書
        """
        content: dict[str, Any] = {}

        # entry_filter_params
        if candidate.entry_filter_params:
            content["entry_filter_params"] = self._format_signal_params(
                candidate.entry_filter_params
            )

        # exit_trigger_params
        if candidate.exit_trigger_params:
            content["exit_trigger_params"] = self._format_signal_params(
                candidate.exit_trigger_params
            )

        # shared_config（空でなければ）
        if candidate.shared_config:
            content["shared_config"] = candidate.shared_config

        return content

    def _format_signal_params(
        self, params: dict[str, Any]
    ) -> dict[str, Any]:
        """
        シグナルパラメータをYAML用にフォーマット

        Args:
            params: パラメータ辞書

        Returns:
            フォーマット済み辞書
        """
        formatted: dict[str, Any] = {}

        for signal_name, signal_params in params.items():
            if isinstance(signal_params, dict):
                # 有効なシグナルのみ出力
                if signal_params.get("enabled", False):
                    formatted[signal_name] = signal_params
            else:
                formatted[signal_name] = signal_params

        return formatted

    def _build_improved_yaml(
        self,
        config: dict[str, Any],
        original_name: str,
        improvements: list[Improvement],
    ) -> str:
        """
        改善内容をコメント付きでYAML生成

        Args:
            config: 改善後の設定
            original_name: 元の戦略名
            improvements: 適用した改善案

        Returns:
            YAML文字列
        """
        lines: list[str] = []

        # ヘッダーコメント
        lines.append(f"# Improved strategy based on: {original_name}")
        lines.append(f"# Generated at: {datetime.now().isoformat()}")
        lines.append("#")
        lines.append("# Applied improvements:")
        for imp in improvements:
            lines.append(
                f"#   - [{imp.improvement_type}] {imp.signal_name}: {imp.reason}"
            )
        lines.append("#")
        lines.append("")

        # YAML本体
        ruamel_yaml = YAML()
        ruamel_yaml.default_flow_style = False
        ruamel_yaml.allow_unicode = True
        stream = StringIO()
        ruamel_yaml.dump(config, stream)
        yaml_str = stream.getvalue()
        lines.append(yaml_str)

        return "\n".join(lines)

    def _resolve_strategy_basename(self, strategy_name: str) -> str:
        """
        戦略名からファイル名用のベース名を解決

        category付き戦略名（例: production/range_break_v15）でも
        不要なディレクトリを作らないよう basename のみを返す。
        """
        normalized = strategy_name.replace("\\", "/").strip("/")
        if not normalized:
            return "strategy"
        return normalized.rsplit("/", 1)[-1]

    def save_evolution_result(
        self,
        best_candidate: StrategyCandidate,
        history: list[dict[str, Any]],
        base_strategy_name: str,
        output_dir: str | None = None,
    ) -> tuple[str, str]:
        """
        進化結果を保存（戦略YAML + 履歴YAML）

        Args:
            best_candidate: 最良候補
            history: 進化履歴
            base_strategy_name: ベース戦略名（ファイル名に使用）
            output_dir: 出力ディレクトリ（省略時は自動生成）

        Returns:
            (戦略YAMLパス, 履歴YAMLパス)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        strategy_basename = self._resolve_strategy_basename(base_strategy_name)

        if output_dir is None:
            # experimentalは外部ディレクトリに保存
            if self.use_external:
                from src.shared.paths import get_strategies_dir
                output_dir = str(get_strategies_dir("experimental") / "evolved")
            else:
                output_dir = os.path.join(self.base_dir, "experimental", "evolved")

        os.makedirs(output_dir, exist_ok=True)

        # 戦略YAML保存
        strategy_path = os.path.join(output_dir, f"{strategy_basename}_{timestamp}.yaml")
        self.save_candidate(best_candidate, strategy_path)

        # 履歴YAML保存
        history_path = os.path.join(
            output_dir, f"{strategy_basename}_{timestamp}_history.yaml"
        )
        ruamel_yaml = YAML()
        ruamel_yaml.default_flow_style = False
        ruamel_yaml.allow_unicode = True
        with open(history_path, "w", encoding="utf-8") as f:
            ruamel_yaml.dump(
                {
                    "strategy_id": best_candidate.strategy_id,
                    "metadata": best_candidate.metadata,
                    "evolution_history": history,
                },
                f,
            )

        logger.info(
            f"Evolution results saved: strategy={strategy_path}, history={history_path}"
        )
        return strategy_path, history_path

    def save_optuna_result(
        self,
        best_candidate: StrategyCandidate,
        study_history: list[dict[str, Any]],
        base_strategy_name: str,
        output_dir: str | None = None,
    ) -> tuple[str, str]:
        """
        Optuna結果を保存（戦略YAML + 履歴YAML）

        Args:
            best_candidate: 最良候補
            study_history: Optuna履歴
            base_strategy_name: ベース戦略名（ファイル名に使用）
            output_dir: 出力ディレクトリ（省略時は自動生成）

        Returns:
            (戦略YAMLパス, 履歴YAMLパス)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        strategy_basename = self._resolve_strategy_basename(base_strategy_name)

        if output_dir is None:
            # experimentalは外部ディレクトリに保存
            if self.use_external:
                from src.shared.paths import get_strategies_dir
                output_dir = str(get_strategies_dir("experimental") / "optuna")
            else:
                output_dir = os.path.join(self.base_dir, "experimental", "optuna")

        os.makedirs(output_dir, exist_ok=True)

        # 戦略YAML保存
        strategy_path = os.path.join(output_dir, f"{strategy_basename}_{timestamp}.yaml")
        self.save_candidate(best_candidate, strategy_path)

        # 履歴YAML保存
        history_path = os.path.join(
            output_dir, f"{strategy_basename}_{timestamp}_history.yaml"
        )
        ruamel_yaml = YAML()
        ruamel_yaml.default_flow_style = False
        ruamel_yaml.allow_unicode = True
        with open(history_path, "w", encoding="utf-8") as f:
            ruamel_yaml.dump(
                {
                    "strategy_id": best_candidate.strategy_id,
                    "metadata": best_candidate.metadata,
                    "optuna_history": study_history,
                },
                f,
            )

        logger.info(
            f"Optuna results saved: strategy={strategy_path}, history={history_path}"
        )
        return strategy_path, history_path
