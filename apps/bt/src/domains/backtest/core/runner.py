"""
Backtest Runner

CLI/Streamlit両対応のバックテスト実行ロジック
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from loguru import logger
from pydantic import BaseModel, Field

from src.domains.backtest.core.artifacts import BacktestArtifactPaths, BacktestArtifactWriter
from src.domains.backtest.core.report_payload import (
    build_backtest_report_payload,
    write_backtest_report_payload,
)
from src.domains.backtest.core.simulation import (
    BacktestSimulationExecutor,
    BacktestSimulationResult,
)
from src.infrastructure.data_access.mode import (
    DATA_ACCESS_MODE_ENV,
    data_access_mode_context,
    normalize_data_access_mode,
)
from src.domains.backtest.vectorbt_adapter import canonical_metrics_from_portfolio
from src.domains.backtest.core.marimo_executor import MarimoExecutor
from src.domains.strategy.runtime.loader import ConfigLoader


class BacktestResult(BaseModel):
    """バックテスト実行結果"""

    html_path: Path | None = Field(default=None, description="出力HTMLファイルのパス")
    expected_html_path: Path = Field(description="期待されるHTML成果物のパス")
    metrics_path: Path | None = Field(default=None, description="出力metrics JSONのパス")
    manifest_path: Path = Field(description="出力manifest JSONのパス")
    elapsed_time: float = Field(gt=0, description="実行時間（秒）")
    summary: dict[str, Any] = Field(description="実行サマリー")
    strategy_name: str = Field(min_length=1, description="戦略名")
    dataset_name: str = Field(min_length=1, description="データセット名")
    render_error: str | None = Field(default=None, description="レポート生成時のエラー")

    model_config = {"arbitrary_types_allowed": True}


class BacktestRunner:
    """
    CLI/Streamlit両対応のバックテスト実行ラッパー

    進捗コールバック対応により、Rich/Streamlitどちらでも使用可能
    """

    def __init__(self) -> None:
        """初期化"""
        self.config_loader = ConfigLoader()
        self.artifact_writer = BacktestArtifactWriter()
        self.simulation_executor = BacktestSimulationExecutor()

    def execute(
        self,
        strategy: str,
        progress_callback: Callable[[str, float], None] | None = None,
        config_override: dict[str, Any] | None = None,
        data_access_mode: str | None = "direct",
        simulation_artifacts_callback: Callable[[dict[str, Any]], None] | None = None,
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

            notify("シミュレーションを実行中...")

            executor_output_dir = self.config_loader.get_output_directory(strategy_config)
            executor = MarimoExecutor(str(executor_output_dir))
            template_path = str(self.config_loader.get_template_notebook_path(strategy_config))

            logger.debug(f"バックテスト実行開始: strategy={strategy_name_only}")
            logger.debug(f"出力ディレクトリ: {executor_output_dir}")
            logger.debug(f"テンプレートパス: {template_path}")

            artifact_paths = executor.resolve_output_paths(
                parameters,
                strategy_name=strategy_name_only,
            )

            simulation_started_at = time.time()
            simulation_result = self.simulation_executor.execute(parameters)
            simulation_elapsed = time.time() - simulation_started_at
            metrics_path = self._write_metrics(
                html_path=artifact_paths.html_path,
                simulation_result=simulation_result,
            )
            report_data_path = self._write_report_payload(
                artifact_paths=artifact_paths,
                simulation_result=simulation_result,
            )
            walk_forward_result = self._run_walk_forward(parameters)
            initial_manifest_path = self._write_manifest(
                html_path=artifact_paths.html_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
                elapsed_time=simulation_elapsed,
                walk_forward=walk_forward_result,
                metrics_path=metrics_path,
                html_generated=False,
                render_error=None,
                render_status="pending",
                report_data_path=report_data_path,
            )
            simulation_summary = self._build_execution_summary(
                executor=executor,
                simulation_result=simulation_result,
                metrics_path=metrics_path,
                html_path=None,
                render_error=None,
                simulation_time=simulation_elapsed,
                render_time=0.0,
            )
            if walk_forward_result:
                simulation_summary["walk_forward"] = walk_forward_result
            simulation_summary = self._annotate_execution_summary(
                summary=simulation_summary,
                manifest_path=initial_manifest_path,
                metrics_path=metrics_path,
                expected_html_path=artifact_paths.html_path,
                report_data_path=report_data_path,
                render_status="pending",
            )
            if simulation_artifacts_callback is not None:
                simulation_artifacts_callback(dict(simulation_summary))

            notify("レポートを生成中...")

            render_started_at = time.time()
            html_path, render_error = self._execute_report(
                executor=executor,
                template_path=template_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                resolved_mode=resolved_mode,
                artifact_paths=artifact_paths,
            )
            render_elapsed = time.time() - render_started_at

            elapsed_time = time.time() - start_time

            log_path = html_path or artifact_paths.html_path
            logger.info(f"バックテスト完了: {log_path} (elapsed: {elapsed_time:.2f}s)")

            notify("完了！" if render_error is None else "完了（レポート生成は警告あり）")

            summary = self._build_execution_summary(
                executor=executor,
                simulation_result=simulation_result,
                metrics_path=metrics_path,
                html_path=html_path,
                render_error=render_error,
                simulation_time=simulation_elapsed,
                render_time=render_elapsed,
            )
            summary["execution_time"] = elapsed_time
            if walk_forward_result:
                summary["walk_forward"] = walk_forward_result

            manifest_path = self._write_manifest(
                html_path=artifact_paths.html_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
                elapsed_time=elapsed_time,
                walk_forward=walk_forward_result,
                metrics_path=metrics_path,
                html_generated=html_path is not None,
                render_error=render_error,
                render_status="completed" if render_error is None else "failed",
                report_data_path=report_data_path,
            )
            summary = self._annotate_execution_summary(
                summary=summary,
                manifest_path=manifest_path,
                metrics_path=metrics_path,
                expected_html_path=artifact_paths.html_path,
                report_data_path=report_data_path,
                render_status="completed" if render_error is None else "failed",
            )

            return BacktestResult(
                html_path=html_path,
                expected_html_path=artifact_paths.html_path,
                metrics_path=metrics_path,
                manifest_path=manifest_path,
                elapsed_time=elapsed_time,
                summary=summary,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
                render_error=render_error,
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
        metrics_path: Path | None = None,
        html_generated: bool | None = None,
        render_error: str | None = None,
        render_status: str | None = None,
        report_data_path: Path | None = None,
    ) -> Path:
        """実行マニフェストをJSONで保存"""
        return self.artifact_writer.write_manifest(
            html_path=html_path,
            parameters=parameters,
            strategy_name=strategy_name,
            dataset_name=dataset_name,
            elapsed_time=elapsed_time,
            walk_forward=walk_forward,
            metrics_path=metrics_path,
            html_generated=html_generated,
            render_error=render_error,
            render_status=render_status,
            report_data_path=report_data_path,
        )

    def _write_metrics(
        self,
        *,
        html_path: Path,
        simulation_result: BacktestSimulationResult,
    ) -> Path:
        return self.artifact_writer.write_metrics(
            html_path=html_path,
            metrics_payload=simulation_result.metrics_payload,
        )

    def _write_report_payload(
        self,
        *,
        artifact_paths: BacktestArtifactPaths,
        simulation_result: BacktestSimulationResult,
    ) -> Path:
        return write_backtest_report_payload(
            path=artifact_paths.report_data_path,
            payload=build_backtest_report_payload(simulation_result),
        )

    def _execute_report(
        self,
        *,
        executor: MarimoExecutor,
        template_path: str,
        parameters: dict[str, Any],
        strategy_name: str,
        resolved_mode: str,
        artifact_paths: BacktestArtifactPaths,
    ) -> tuple[Path | None, str | None]:
        try:
            render_parameters = dict(parameters)
            execution_meta = render_parameters.get("_execution")
            if not isinstance(execution_meta, dict):
                execution_meta = {}
            render_parameters["_execution"] = {
                **execution_meta,
                "report_data_path": str(artifact_paths.report_data_path),
            }
            html_path = executor.execute_notebook(
                template_path=template_path,
                parameters=render_parameters,
                strategy_name=strategy_name,
                extra_env={DATA_ACCESS_MODE_ENV: resolved_mode},
            )
            if html_path.exists():
                return html_path, None
            if artifact_paths.metrics_path.exists():
                error = f"HTML file not found after execution: {html_path}"
                logger.warning(
                    "HTML render metadata is missing but simulation artifacts exist; preserving run result",
                    htmlPath=str(html_path),
                    metricsPath=str(artifact_paths.metrics_path),
                    error=error,
                )
                return None, error
            raise RuntimeError(f"HTML file not found after execution: {html_path}")
        except RuntimeError as exc:
            if artifact_paths.metrics_path.exists():
                logger.warning(
                    "HTML render failed after canonical artifacts were persisted; preserving run result",
                    htmlPath=str(artifact_paths.html_path),
                    metricsPath=str(artifact_paths.metrics_path),
                    error=str(exc),
                )
                return None, str(exc)
            raise

    def _get_package_version(self, package: str) -> str | None:
        return BacktestArtifactWriter._get_package_version(package)

    def _get_git_commit(self) -> str | None:
        return BacktestArtifactWriter._get_git_commit()

    def _build_execution_summary(
        self,
        *,
        executor: MarimoExecutor,
        simulation_result: BacktestSimulationResult,
        metrics_path: Path,
        html_path: Path | None,
        render_error: str | None,
        simulation_time: float,
        render_time: float,
    ) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "generated_at": datetime.now().isoformat(),
            "simulation_time": simulation_time,
            "render_time": render_time,
            "metrics_path": str(metrics_path),
        }
        if html_path is not None:
            summary.update(executor.get_execution_summary(html_path))
            summary["html_path"] = str(html_path)
        else:
            summary["file_size"] = 0

        summary.update(
            {
                key: value
                for key, value in simulation_result.metrics_payload.items()
                if key != "total_trades" and value is not None
            }
        )
        trade_count = simulation_result.metrics_payload.get("total_trades")
        if trade_count is not None:
            summary["trade_count"] = int(trade_count)
        if render_error is not None:
            summary["render_error"] = render_error
        return summary

    @staticmethod
    def _annotate_execution_summary(
        *,
        summary: dict[str, Any],
        manifest_path: Path,
        metrics_path: Path,
        expected_html_path: Path,
        report_data_path: Path,
        render_status: str,
    ) -> dict[str, Any]:
        summary["manifest_path"] = str(manifest_path)
        summary["_manifest_path"] = str(manifest_path)
        summary["metrics_path"] = str(metrics_path)
        summary["_metrics_path"] = str(metrics_path)
        summary["expected_html_path"] = str(expected_html_path)
        summary["_expected_html_path"] = str(expected_html_path)
        summary["_report_data_path"] = str(report_data_path)
        summary["render_status"] = render_status
        return summary

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

        from src.infrastructure.data_access.loaders import get_stock_list
        from src.infrastructure.data_access.loaders.stock_loaders import load_stock_data
        from src.domains.backtest.core.walkforward import generate_walkforward_splits
        from src.domains.strategy.core.factory import StrategyFactory

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

        metrics = canonical_metrics_from_portfolio(portfolio)
        if metrics is not None:
            return {
                "total_return": metrics.total_return,
                "sharpe_ratio": metrics.sharpe_ratio,
                "calmar_ratio": metrics.calmar_ratio,
            }

        return {
            "total_return": self._coerce_metric(getattr(portfolio, "total_return", lambda: None)()),
            "sharpe_ratio": self._coerce_metric(getattr(portfolio, "sharpe_ratio", lambda: None)()),
            "calmar_ratio": self._coerce_metric(getattr(portfolio, "calmar_ratio", lambda: None)()),
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
        from src.domains.strategy.runtime.parameter_extractor import _deep_merge_dict

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

            from src.shared.models.signals import SignalParams

            for signal_key in ("entry_filter_params", "exit_trigger_params"):
                if signal_key in parameters and parameters[signal_key]:
                    try:
                        SignalParams(**parameters[signal_key])
                    except Exception as e:
                        raise ValueError(
                            f"Invalid {signal_key} after config_override merge: {e}"
                        ) from e

        from src.domains.strategy.runtime.compiler import (
            compile_runtime_strategy,
            resolve_round_trip_execution_mode_name,
        )
        from src.shared.models.config import SharedConfig
        from src.shared.models.signals import SignalParams

        try:
            shared_config_model = SharedConfig.model_validate(
                parameters.get("shared_config", {}),
                context={"resolve_stock_codes": False},
            )
        except Exception as e:
            raise ValueError(f"Invalid shared_config after config merge: {e}") from e

        compiled_strategy = compile_runtime_strategy(
            strategy_name=strategy_config.get("name", "runtime"),
            shared_config=shared_config_model,
            entry_signal_params=SignalParams.model_validate(
                parameters.get("entry_filter_params", {})
            ),
            exit_signal_params=SignalParams.model_validate(
                parameters.get("exit_trigger_params", {})
            ),
        )
        mode_name = resolve_round_trip_execution_mode_name(compiled_strategy)
        if mode_name is not None and parameters.get("exit_trigger_params") not in (None, {}):
            raise ValueError(
                "exit_trigger_params must be empty when "
                f"shared_config.{mode_name} is true"
            )

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
