"""
Strategy Factory

戦略の動的インポートとインスタンス化を行うファクトリークラス
"""

import os
from typing import Any, Dict, Tuple, Union

from loguru import logger
from rich.console import Console

from src.models.config import SharedConfig

# データアクセス（SharedConfigで自動解決されるため直接使用不要）
from src.utils.logger_config import setup_logger


class StrategyFactory:
    """
    戦略ファクトリークラス

    戦略名から適切な戦略クラスを動的にインポートし、
    インスタンスを生成する
    """

    @classmethod
    def create_strategy(
        cls,
        shared_config: Union[SharedConfig, Dict[str, Any]],
        entry_filter_params: Union[Dict[str, Any], Any, None] = None,
        exit_trigger_params: Union[Dict[str, Any], Any, None] = None,
    ) -> Any:
        """
        戦略インスタンスを作成（YamlConfigurableStrategy直接使用）

        全戦略でYamlConfigurableStrategyを直接インスタンス化。
        戦略固有ロジックは完全にYAML制御。

        Args:
            shared_config: 共通設定
            entry_filter_params: エントリーフィルターパラメータ
            exit_trigger_params: エグジットトリガーパラメータ

        Returns:
            YamlConfigurableStrategy: 戦略インスタンス
        """
        from src.strategies.core.yaml_configurable_strategy import (
            YamlConfigurableStrategy,
        )

        # SharedConfig変換
        if isinstance(shared_config, dict):
            shared_config_obj = SharedConfig(**shared_config)
        else:
            shared_config_obj = shared_config

        # SignalParams変換
        entry_filter_params_obj, exit_trigger_params_obj = (
            cls._convert_to_signal_params(entry_filter_params, exit_trigger_params)
        )

        # YamlConfigurableStrategy直接インスタンス化（SharedConfigが既にstock_codes解決済み）
        strategy = YamlConfigurableStrategy(
            shared_config=shared_config_obj,
            entry_filter_params=entry_filter_params_obj,
            exit_trigger_params=exit_trigger_params_obj,
        )

        logger.info("戦略インスタンス作成成功 (YamlConfigurableStrategy)")
        return strategy

    @classmethod
    def get_available_strategies(cls) -> Dict[str, str]:
        """
        利用可能な戦略の一覧を取得（YAML一覧）

        Returns:
            戦略名と説明のマッピング
        """
        from pathlib import Path

        from ruamel.yaml import YAML

        strategies_dir = Path("config/strategies")
        if not strategies_dir.exists():
            return {}

        strategies = {}
        for yaml_file in strategies_dir.glob("*.yaml"):
            strategy_name = yaml_file.stem
            # template.yamlは除外
            if strategy_name == "template":
                continue

            # YAMLからdescription取得（あれば）
            try:
                ruamel_yaml = YAML()
                ruamel_yaml.preserve_quotes = True
                with open(yaml_file, "r", encoding="utf-8") as f:
                    config = ruamel_yaml.load(f)
                    desc = config.get("strategy_params", {}).get("description", "")
                    strategies[strategy_name] = desc or strategy_name
            except Exception:
                strategies[strategy_name] = strategy_name

        return strategies

    @classmethod
    def is_supported_strategy(cls, strategy_name: str) -> bool:
        """
        戦略がサポートされているかチェック（YAML存在確認）

        Args:
            strategy_name: 戦略名

        Returns:
            サポート状況
        """
        from pathlib import Path

        yaml_path = Path(f"config/strategies/{strategy_name}.yaml")
        return yaml_path.exists()

    @classmethod
    def _convert_to_signal_params(
        cls,
        entry_filter_params: Union[Dict[str, Any], Any, None],
        exit_trigger_params: Union[Dict[str, Any], Any, None],
    ) -> Tuple[Any, Any]:
        """
        YAML辞書からSignalParamsオブジェクトに変換

        Args:
            entry_filter_params: エントリーフィルターパラメータ辞書
            exit_trigger_params: エグジットトリガーパラメータ辞書

        Returns:
            Tuple[SignalParams, SignalParams]: エントリー・エグジット用SignalParams
        """
        from src.models.signals import SignalParams

        # エントリーフィルター用SignalParams生成
        entry_filter_params_obj = None
        if entry_filter_params:
            if isinstance(entry_filter_params, dict):
                entry_filter_params_obj = SignalParams(**entry_filter_params)
            else:
                entry_filter_params_obj = entry_filter_params

        # エグジットトリガー用SignalParams生成
        exit_trigger_params_obj = None
        if exit_trigger_params:
            if isinstance(exit_trigger_params, dict):
                exit_trigger_params_obj = SignalParams(**exit_trigger_params)
            else:
                exit_trigger_params_obj = exit_trigger_params

        return entry_filter_params_obj, exit_trigger_params_obj

    @classmethod
    def execute_strategy_with_config(
        cls,
        shared_config: Union[SharedConfig, Dict[str, Any]],
        entry_filter_params: Union[Dict[str, Any], Any, None] = None,
        exit_trigger_params: Union[Dict[str, Any], Any, None] = None,
    ) -> Dict[str, Any]:
        """
        統一戦略実行関数（strategy_executor.pyの代替）

        Args:
            shared_config: 共通設定（initial_cash, fees, db_path等）
            entry_filter_params: エントリーフィルターパラメータ
            exit_trigger_params: エグジットトリガーパラメータ（オプション）

        Returns:
            Dict[str, Any]: 実行結果（portfolio等）
        """
        console = Console()

        # printlog設定に基づくログレベル制御
        if isinstance(shared_config, dict):
            printlog = shared_config.get("printlog", True)
        else:
            printlog = getattr(shared_config, "printlog", True)

        # printlog=Falseの場合はERRORレベル以上のみ出力（INFO/DEBUG/WARNING抑制）
        if not printlog:
            os.environ["LOG_LEVEL"] = "ERROR"
            setup_logger(level_override="ERROR")

        try:
            # SharedConfigの変換・バリデーション
            if isinstance(shared_config, dict):
                shared_config_obj = SharedConfig(**shared_config)
            else:
                shared_config_obj = shared_config

            # 戦略インスタンスを作成（YamlConfigurableStrategy直接使用）
            strategy = cls.create_strategy(
                shared_config=shared_config_obj,
                entry_filter_params=entry_filter_params,
                exit_trigger_params=exit_trigger_params,
            )

            # stock_codesはSharedConfigから取得（自動解決済み）
            stock_codes = shared_config_obj.stock_codes

            # 実行状況の表示（SharedConfigが既にstock_codes解決済み）
            stock_count = len(stock_codes)
            if stock_count == 1:
                console.print(f"🎯 個別銘柄実行: {stock_codes[0]}")
            else:
                console.print(f"🎯 統合ポートフォリオ実行: {stock_count}銘柄")

            # 戦略実行のコンソール出力
            relative_mode = getattr(shared_config_obj, "relative_mode", False)
            relative_status = " (Relative Mode)" if relative_mode else ""
            console.print(f"📈 戦略実行中...{relative_status}")

            # 戦略実行（2段階最適化）
            initial_portfolio, kelly_portfolio, allocation_info = (
                strategy.run_optimized_backtest(group_by=True)
            )

            # 2段階最適化結果の検証
            if initial_portfolio is not None and kelly_portfolio is not None:
                console.print("✅ 戦略実行完了（2段階最適化）")

                # 後方互換性のため max_concurrent も含める（AllocationInfoから取得）
                max_concurrent = 0  # デフォルト値

                # all_entriesを戦略インスタンスから取得
                all_entries = getattr(strategy, "all_entries", None)

                return {
                    "initial_portfolio": initial_portfolio,
                    "kelly_portfolio": kelly_portfolio,
                    "max_concurrent": max_concurrent,  # 後方互換性
                    "allocation_info": allocation_info,  # 詳細統計情報
                    "all_entries": all_entries,  # エントリーシグナルDataFrame
                }
            else:
                console.print("❌ 2段階最適化ポートフォリオ作成エラー")
                raise Exception("2段階最適化ポートフォリオ作成に失敗しました")

        except Exception as e:
            console.print(f"❌ [red]戦略実行エラー: {e}[/red]")
            console.print("[yellow]💡 対処方法:[/yellow]")
            console.print("  - 銘柄コードが正しいか確認してください")
            console.print("  - データ期間に十分なレコードがあるか確認してください")
            console.print(
                "  - 戦略設定ファイル（YAML）が正しく設定されているか確認してください"
            )
            raise
