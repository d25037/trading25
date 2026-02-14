"""
Backtest Runner

CLI/Streamlit両対応のバックテスト実行ロジック
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from loguru import logger
from pydantic import BaseModel, Field

from src.data.access.mode import (
    DATA_ACCESS_MODE_ENV,
    data_access_mode_context,
    normalize_data_access_mode,
)
from src.lib.backtest_core.marimo_executor import MarimoExecutor
from src.lib.strategy_runtime.loader import ConfigLoader


class BacktestResult(BaseModel):
    """バックテスト実行結果"""

    html_path: Path = Field(description="出力HTMLファイルのパス")
    elapsed_time: float = Field(gt=0, description="実行時間（秒）")
    summary: dict[str, Any] = Field(description="実行サマリー")
    strategy_name: str = Field(min_length=1, description="戦略名")
    dataset_name: str = Field(min_length=1, description="データセット名")

    model_config = {"arbitrary_types_allowed": True}


class BacktestRunner:
    """
    CLI/Streamlit両対応のバックテスト実行ラッパー

    進捗コールバック対応により、Rich/Streamlitどちらでも使用可能
    """

    def __init__(self) -> None:
        """初期化"""
        self.config_loader = ConfigLoader()

    def execute(
        self,
        strategy: str,
        progress_callback: Callable[[str, float], None] | None = None,
        config_override: dict[str, Any] | None = None,
        data_access_mode: str | None = "direct",
    ) -> BacktestResult:
        """
        バックテストを実行

        Args:
            strategy: 戦略名（例: "range_break_v5", "production/range_break_v5"）
            progress_callback: 進捗通知コールバック(status_text, elapsed_time)
            config_override: 設定オーバーライド辞書（オプション）
            data_access_mode: データアクセスモード（"http" | "direct"）
                未指定時は "direct"（内部HTTPを使わない）

        Returns:
            BacktestResult（html_path, elapsed_time, summary含む）

        Raises:
            ValueError: 戦略名が不正な場合
            FileNotFoundError: 戦略ファイルが見つからない場合
            RuntimeError: バックテスト実行に失敗した場合
        """
        start_time = time.time()

        def notify(status: str) -> None:
            if progress_callback:
                elapsed = time.time() - start_time
                progress_callback(status, elapsed)

        strategy_name_only = strategy.split("/")[-1]
        resolved_mode = normalize_data_access_mode(
            data_access_mode if data_access_mode is not None else "direct"
        )

        with data_access_mode_context(resolved_mode):
            notify("設定を読み込み中...")

            strategy_config = self.config_loader.load_strategy_config(strategy)

            parameters = self._build_parameters(strategy_config, config_override)

            shared_config = parameters.get("shared_config", {})
            dataset_name = shared_config.get("dataset", "unknown")

            notify("バックテストを実行中...")

            executor_output_dir = self.config_loader.get_output_directory(strategy_config)
            executor = MarimoExecutor(str(executor_output_dir))
            template_path = str(self.config_loader.get_template_notebook_path(strategy_config))

            logger.debug(f"バックテスト実行開始: strategy={strategy_name_only}")
            logger.debug(f"出力ディレクトリ: {executor_output_dir}")
            logger.debug(f"テンプレートパス: {template_path}")

            html_path = executor.execute_notebook(
                template_path=template_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                extra_env={DATA_ACCESS_MODE_ENV: resolved_mode},
            )

            elapsed_time = time.time() - start_time

            if not html_path.exists():
                logger.error(f"HTMLファイルが見つかりません: {html_path}")
                raise RuntimeError(f"HTML file not found after execution: {html_path}")

            logger.info(f"バックテスト完了: {html_path} (elapsed: {elapsed_time:.2f}s)")

            notify("完了！")

            summary = executor.get_execution_summary(html_path)
            summary["execution_time"] = elapsed_time
            summary["html_path"] = str(html_path)
            walk_forward_result = self._run_walk_forward(parameters)
            if walk_forward_result:
                summary["walk_forward"] = walk_forward_result

            manifest_path = self._write_manifest(
                html_path=html_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
                elapsed_time=elapsed_time,
                walk_forward=walk_forward_result,
            )
            summary["manifest_path"] = str(manifest_path)

            return BacktestResult(
                html_path=html_path,
                elapsed_time=elapsed_time,
                summary=summary,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
            )

    def build_parameters_for_strategy(
        self,
        strategy: str,
        config_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        戦略名から実行パラメータを構築

        Args:
            strategy: 戦略名
            config_override: 設定オーバーライド辞書（オプション）

        Returns:
            実行パラメータ辞書
        """
        strategy_config = self.config_loader.load_strategy_config(strategy)
        return self._build_parameters(strategy_config, config_override)

    def _write_manifest(
        self,
        html_path: Path,
        parameters: dict[str, Any],
        strategy_name: str,
        dataset_name: str,
        elapsed_time: float,
        walk_forward: dict[str, Any] | None = None,
    ) -> Path:
        """実行マニフェストをJSONで保存"""
        manifest = {
            "generated_at": datetime.now().isoformat(),
            "strategy_name": strategy_name,
            "dataset_name": dataset_name,
            "html_path": str(html_path),
            "execution_time": elapsed_time,
            "parameters": parameters,
            "versions": {
                "python": sys.version.split()[0],
                "vectorbt": self._get_package_version("vectorbt"),
                "marimo": self._get_package_version("marimo"),
                "pydantic": self._get_package_version("pydantic"),
            },
            "git_commit": self._get_git_commit(),
        }
        if walk_forward:
            manifest["walk_forward"] = walk_forward

        manifest_path = html_path.with_suffix(".manifest.json")
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return manifest_path

    @staticmethod
    def _get_package_version(package: str) -> str | None:
        try:
            from importlib.metadata import version

            return version(package)
        except Exception:
            return None

    @staticmethod
    def _get_git_commit() -> str | None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() or None
        except Exception:
            return None

    def _run_walk_forward(self, parameters: dict[str, Any]) -> dict[str, Any] | None:
        """ウォークフォワード分析を実行（設定有効時のみ）"""
        shared_config = parameters.get("shared_config", {})
        walk_forward = shared_config.get("walk_forward", {})
        if not isinstance(walk_forward, dict) or not walk_forward.get("enabled", False):
            return None

        train_window = int(walk_forward.get("train_window", 252))
        test_window = int(walk_forward.get("test_window", 63))
        step = walk_forward.get("step")
        max_splits = walk_forward.get("max_splits")

        from src.data import get_stock_list
        from src.data.loaders.stock_loaders import load_stock_data
        from src.lib.backtest_core.walkforward import generate_walkforward_splits
        from src.strategies.core.factory import StrategyFactory

        stock_codes = shared_config.get("stock_codes", [])
        if stock_codes == ["all"]:
            try:
                stock_codes = get_stock_list(shared_config.get("dataset", ""))
            except Exception as e:
                logger.warning(f"ウォークフォワード用の銘柄取得失敗: {e}")
                return None

        if not stock_codes:
            logger.warning("ウォークフォワード用の銘柄が取得できませんでした")
            return None

        dataset = shared_config.get("dataset", "")
        start_date = shared_config.get("start_date") or None
        end_date = shared_config.get("end_date") or None
        timeframe = shared_config.get("timeframe", "daily")

        try:
            price_data = load_stock_data(
                dataset=dataset,
                stock_code=stock_codes[0],
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe,
            )
        except Exception as e:
            logger.warning(f"ウォークフォワード用の価格データ取得失敗: {e}")
            return None

        splits = generate_walkforward_splits(
            price_data.index, train_window, test_window, step
        )
        if max_splits is not None:
            splits = splits[: int(max_splits)]

        results = []
        for split in splits:
            test_shared = dict(shared_config)
            test_shared["start_date"] = split.test_start
            test_shared["end_date"] = split.test_end

            result = StrategyFactory.execute_strategy_with_config(
                test_shared,
                parameters.get("entry_filter_params"),
                parameters.get("exit_trigger_params"),
            )
            kelly_portfolio = result.get("kelly_portfolio")

            metrics = self._collect_portfolio_metrics(kelly_portfolio)
            results.append(
                {
                    "train": {
                        "start": split.train_start,
                        "end": split.train_end,
                    },
                    "test": {
                        "start": split.test_start,
                        "end": split.test_end,
                    },
                    "metrics": metrics,
                }
            )

        if not results:
            return None

        aggregate = self._aggregate_walk_forward_metrics(results)

        return {
            "count": len(results),
            "splits": results,
            "aggregate": aggregate,
        }

    @staticmethod
    def _coerce_metric(value: Any) -> float | None:
        try:
            if hasattr(value, "mean"):
                value = value.mean()
            return float(value)
        except Exception:
            return None

    def _collect_portfolio_metrics(self, portfolio: Any) -> dict[str, float | None]:
        if portfolio is None:
            return {}

        return {
            "total_return": self._coerce_metric(portfolio.total_return()),
            "sharpe_ratio": self._coerce_metric(portfolio.sharpe_ratio()),
            "calmar_ratio": self._coerce_metric(portfolio.calmar_ratio()),
        }

    @staticmethod
    def _aggregate_walk_forward_metrics(
        results: list[dict[str, Any]],
    ) -> dict[str, float]:
        totals = {"total_return": [], "sharpe_ratio": [], "calmar_ratio": []}
        for result in results:
            metrics = result.get("metrics", {})
            for key in totals:
                value = metrics.get(key)
                if value is not None:
                    totals[key].append(value)

        aggregate = {}
        for key, values in totals.items():
            if values:
                aggregate[key] = sum(values) / len(values)
        return aggregate

    def _build_parameters(
        self,
        strategy_config: dict[str, Any],
        config_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        戦略設定からパラメータを構築

        Args:
            strategy_config: 戦略設定辞書
            config_override: 設定オーバーライド辞書（オプション）

        Returns:
            実行パラメータ辞書
        """
        from src.lib.strategy_runtime.parameter_extractor import _deep_merge_dict

        parameters = self.config_loader.default_config.get("parameters", {}).copy()

        merged_shared_config = self.config_loader.merge_shared_config(strategy_config)
        parameters["shared_config"] = merged_shared_config

        if "entry_filter_params" in strategy_config:
            parameters["entry_filter_params"] = strategy_config["entry_filter_params"]
        if "exit_trigger_params" in strategy_config:
            parameters["exit_trigger_params"] = strategy_config["exit_trigger_params"]

        if config_override:
            for key in ("shared_config", "entry_filter_params", "exit_trigger_params"):
                if key in config_override:
                    override_value = config_override[key]
                    if not isinstance(override_value, dict):
                        raise ValueError(
                            f"config_override['{key}'] must be a dict, got {type(override_value).__name__}"
                        )
                    if key in parameters:
                        parameters[key] = _deep_merge_dict(
                            parameters[key], override_value
                        )
                    else:
                        parameters[key] = override_value

            from src.models.signals import SignalParams

            for signal_key in ("entry_filter_params", "exit_trigger_params"):
                if signal_key in parameters and parameters[signal_key]:
                    try:
                        SignalParams(**parameters[signal_key])
                    except Exception as e:
                        raise ValueError(
                            f"Invalid {signal_key} after config_override merge: {e}"
                        ) from e

        return parameters

    def get_execution_info(self, strategy: str) -> dict[str, Any]:
        """
        実行前の情報を取得（プレビュー用）

        Args:
            strategy: 戦略名

        Returns:
            実行情報辞書
        """
        try:
            strategy_config = self.config_loader.load_strategy_config(strategy)
            parameters = self._build_parameters(strategy_config)
            shared_config = parameters.get("shared_config", {})

            return {
                "display_name": strategy_config.get("display_name", strategy),
                "description": strategy_config.get("description", ""),
                "dataset": shared_config.get("dataset", "unknown"),
                "initial_cash": shared_config.get("initial_cash", 0),
                "fees": shared_config.get("fees", 0),
                "kelly_fraction": shared_config.get("kelly_fraction", 1.0),
            }
        except Exception as e:
            logger.error(f"実行情報取得エラー: {e}")
            return {"error": str(e)}


__all__ = ["BacktestResult", "BacktestRunner"]
