"""
Backtest Runner

CLI/Streamlit両対応のバックテスト実行ロジック
"""

import json
import math
import pickle
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from loguru import logger
from pydantic import BaseModel, Field

from src.infrastructure.data_access.mode import (
    DATA_ACCESS_MODE_ENV,
    data_access_mode_context,
    normalize_data_access_mode,
)
from src.domains.backtest.core.artifacts import (
    BacktestArtifactWriter,
    build_metrics_payload as build_backtest_metrics_payload,
)
from src.domains.backtest.vectorbt_adapter import canonical_metrics_from_portfolio
from src.domains.backtest.core.marimo_executor import BacktestReportPaths, MarimoExecutor
from src.domains.backtest.core.report_payload import (
    build_backtest_report_payload,
    write_backtest_report_payload,
)
from src.domains.backtest.core.simulation import (
    BacktestSimulationExecutor,
    BacktestSimulationResult,
)
from src.domains.strategy.runtime.loader import ConfigLoader


class BacktestResult(BaseModel):
    """バックテスト実行結果"""

    html_path: Path | None = Field(default=None, description="出力HTMLファイルのパス")
    metrics_path: Path | None = Field(default=None, description="core metrics artifact path")
    manifest_path: Path | None = Field(default=None, description="run manifest artifact path")
    simulation_payload_path: Path | None = Field(
        default=None,
        description="serialized simulation payload artifact path",
    )
    report_payload_path: Path | None = Field(
        default=None,
        description="serialized report payload artifact path",
    )
    elapsed_time: float = Field(gt=0, description="完了ジョブの総実行時間（秒）")
    simulation_elapsed_time: float | None = Field(
        default=None,
        description="simulation phase execution time in seconds",
    )
    summary: dict[str, Any] = Field(description="実行サマリー")
    strategy_name: str = Field(min_length=1, description="戦略名")
    dataset_name: str = Field(min_length=1, description="データセット名")
    render_error: str | None = Field(default=None, description="report render error")

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

            executor_output_dir = self.config_loader.get_output_directory(strategy_config)
            executor = MarimoExecutor(str(executor_output_dir))
            template_path = str(self.config_loader.get_template_notebook_path(strategy_config))
            report_paths = executor.plan_report_paths(parameters, strategy_name_only)

            logger.debug(f"バックテスト実行開始: strategy={strategy_name_only}")
            logger.debug(f"出力ディレクトリ: {executor_output_dir}")
            logger.debug(f"テンプレートパス: {template_path}")

            notify("バックテストを実行中...")

            simulation_result = self._normalize_simulation_result(
                self._execute_simulation(parameters)
            )
            simulation_elapsed_time = time.time() - start_time

            self._write_metrics_artifact(
                report_paths.metrics_path,
                simulation_result.metrics_payload,
            )
            self._write_simulation_payload(
                report_paths.simulation_payload_path,
                simulation_result,
            )
            self._write_report_payload(
                report_paths.report_payload_path,
                simulation_result,
            )

            walk_forward_result: dict[str, Any] | None = None
            manifest_path = self._write_manifest(
                html_path=None,
                manifest_path=report_paths.manifest_path,
                metrics_path=report_paths.metrics_path,
                simulation_payload_path=report_paths.simulation_payload_path,
                report_payload_path=report_paths.report_payload_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
                elapsed_time=simulation_elapsed_time,
                total_elapsed_time=simulation_elapsed_time,
                walk_forward=walk_forward_result,
                report_status="pending",
                report_render_time=None,
                render_error=None,
            )

            try:
                walk_forward_result = self._run_walk_forward(parameters)
            except Exception as exc:
                logger.warning(f"ウォークフォワード分析失敗: {exc}")

            notify("レポートを描画中...")

            html_path, report_status, render_error, report_render_time = self._render_report(
                executor=executor,
                template_path=template_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                data_access_mode=resolved_mode,
                report_paths=report_paths,
            )
            total_elapsed_time = time.time() - start_time

            manifest_path = self._write_manifest(
                html_path=html_path,
                manifest_path=report_paths.manifest_path,
                metrics_path=report_paths.metrics_path,
                simulation_payload_path=report_paths.simulation_payload_path,
                report_payload_path=report_paths.report_payload_path,
                parameters=parameters,
                strategy_name=strategy_name_only,
                dataset_name=dataset_name,
                elapsed_time=simulation_elapsed_time,
                total_elapsed_time=total_elapsed_time,
                walk_forward=walk_forward_result,
                report_status=report_status,
                report_render_time=report_render_time,
                render_error=render_error,
            )

            summary = self._build_summary_payload(
                html_path=html_path,
                metrics_path=report_paths.metrics_path,
                manifest_path=manifest_path,
                simulation_payload_path=report_paths.simulation_payload_path,
                report_payload_path=report_paths.report_payload_path,
                simulation_elapsed_time=simulation_elapsed_time,
                total_elapsed_time=total_elapsed_time,
                report_status=report_status,
                render_error=render_error,
                walk_forward_result=walk_forward_result,
            )

            logger.info(
                "バックテスト完了: "
                f"metrics={report_paths.metrics_path}, html={html_path}, "
                f"simulation_elapsed={simulation_elapsed_time:.2f}s, total_elapsed={total_elapsed_time:.2f}s"
            )

            notify("完了！")

            return BacktestResult(
                html_path=html_path,
                metrics_path=report_paths.metrics_path,
                manifest_path=manifest_path,
                simulation_payload_path=report_paths.simulation_payload_path,
                report_payload_path=report_paths.report_payload_path,
                elapsed_time=total_elapsed_time,
                simulation_elapsed_time=simulation_elapsed_time,
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

    def _execute_simulation(
        self, parameters: dict[str, Any]
    ) -> BacktestSimulationResult | dict[str, Any]:
        return self.simulation_executor.execute(parameters)

    def _write_json_artifact(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        sanitized_payload = self._sanitize_json_payload(payload)
        path.write_text(
            json.dumps(
                sanitized_payload,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            ),
            encoding="utf-8",
        )

    def _write_metrics_artifact(self, metrics_path: Path, metrics_payload: dict[str, Any]) -> None:
        self.artifact_writer.write_metrics(
            metrics_path=metrics_path,
            metrics_payload=metrics_payload,
        )

    def _write_simulation_payload(
        self,
        payload_path: Path,
        simulation_result: BacktestSimulationResult | dict[str, Any],
    ) -> None:
        normalized = self._normalize_simulation_result(simulation_result)
        payload = {
            "initial_portfolio": self._serialize_simulation_portfolio(
                normalized.initial_portfolio
            ),
            "kelly_portfolio": self._serialize_simulation_portfolio(
                normalized.kelly_portfolio
            ),
            "allocation_info": normalized.allocation_info,
            "all_entries": normalized.all_entries,
        }
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        with payload_path.open("wb") as file:
            pickle.dump(payload, file)

    def _write_report_payload(
        self,
        payload_path: Path | None,
        simulation_result: BacktestSimulationResult | dict[str, Any],
    ) -> None:
        if payload_path is None:
            return
        normalized = self._normalize_simulation_result(simulation_result)
        write_backtest_report_payload(
            path=payload_path,
            payload=build_backtest_report_payload(normalized),
        )

    def _serialize_simulation_portfolio(self, portfolio: Any) -> Any:
        if portfolio is None:
            return None

        unwrap = getattr(portfolio, "unwrap", None)
        resolved_portfolio = portfolio
        if callable(unwrap):
            try:
                candidate = unwrap()
            except Exception as exc:
                logger.warning(f"simulation payload unwrap失敗: {exc}")
            else:
                if candidate is not None:
                    resolved_portfolio = candidate

        dumps = getattr(resolved_portfolio, "dumps", None)
        if callable(dumps):
            try:
                return {
                    "__serialization__": "vectorbt.dumps",
                    "payload": dumps(),
                }
            except Exception as exc:
                logger.warning(f"simulation payload portfolio serialize失敗: {exc}")
                return None

        return resolved_portfolio

    def _render_report(
        self,
        *,
        executor: MarimoExecutor,
        template_path: str,
        parameters: dict[str, Any],
        strategy_name: str,
        data_access_mode: str,
        report_paths: BacktestReportPaths,
    ) -> tuple[Path | None, str, str | None, float]:
        html_path: Path | None = None
        render_error: str | None = None
        report_status = "completed"
        render_started_at = time.time()
        try:
            rendered_html_path = executor.execute_notebook(
                template_path=template_path,
                parameters=parameters,
                strategy_name=strategy_name,
                extra_env={DATA_ACCESS_MODE_ENV: data_access_mode},
                html_path=report_paths.html_path,
                execution_metadata={
                    "simulation_payload_path": str(report_paths.simulation_payload_path),
                    "report_payload_path": str(report_paths.report_payload_path)
                    if report_paths.report_payload_path is not None
                    else "",
                },
            )
            if rendered_html_path.exists():
                html_path = rendered_html_path
        except Exception as exc:
            report_status = "failed"
            render_error = str(exc)
            logger.warning(f"バックテストHTML描画失敗: {exc}")

        return html_path, report_status, render_error, time.time() - render_started_at

    def _build_summary_payload(
        self,
        *,
        html_path: Path | None,
        metrics_path: Path,
        manifest_path: Path,
        simulation_payload_path: Path,
        report_payload_path: Path | None,
        simulation_elapsed_time: float,
        total_elapsed_time: float,
        report_status: str,
        render_error: str | None,
        walk_forward_result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "html_path": str(html_path) if html_path else None,
            "execution_time": total_elapsed_time,
            "simulation_elapsed_time": simulation_elapsed_time,
            "total_elapsed_time": total_elapsed_time,
            "report_status": report_status,
            "_metrics_path": str(metrics_path),
            "_manifest_path": str(manifest_path),
            "_simulation_payload_path": str(simulation_payload_path),
            "_report_payload_path": str(report_payload_path) if report_payload_path else None,
        }
        if render_error is not None:
            summary["_render_error"] = render_error
        if walk_forward_result:
            summary["walk_forward"] = walk_forward_result
        return summary

    def _write_manifest(
        self,
        *,
        html_path: Path | None,
        manifest_path: Path | None = None,
        metrics_path: Path | None = None,
        simulation_payload_path: Path | None = None,
        report_payload_path: Path | None = None,
        parameters: dict[str, Any],
        strategy_name: str,
        dataset_name: str,
        elapsed_time: float,
        total_elapsed_time: float | None = None,
        walk_forward: dict[str, Any] | None = None,
        report_status: str = "completed",
        report_render_time: float | None = None,
        render_error: str | None = None,
    ) -> Path:
        """実行マニフェストをJSONで保存する。"""
        return self.artifact_writer.write_manifest(
            html_path=html_path,
            manifest_path=manifest_path,
            metrics_path=metrics_path,
            simulation_payload_path=simulation_payload_path,
            report_payload_path=report_payload_path,
            parameters=parameters,
            strategy_name=strategy_name,
            dataset_name=dataset_name,
            elapsed_time=elapsed_time,
            total_elapsed_time=total_elapsed_time,
            walk_forward=walk_forward,
            report_status=report_status,
            report_render_time=report_render_time,
            render_error=render_error,
        )

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

    def _build_metrics_payload(
        self,
        *,
        kelly_portfolio: Any,
        allocation_info: Any,
    ) -> dict[str, Any]:
        return build_backtest_metrics_payload(
            portfolio=kelly_portfolio,
            allocation_info=allocation_info,
        )

    def _sanitize_json_payload(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._sanitize_json_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_json_payload(item) for item in value]
        if isinstance(value, tuple):
            return [self._sanitize_json_payload(item) for item in value]
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        return value

    def _normalize_simulation_result(
        self,
        simulation_result: BacktestSimulationResult | dict[str, Any],
    ) -> BacktestSimulationResult:
        if isinstance(simulation_result, BacktestSimulationResult):
            return simulation_result
        if not isinstance(simulation_result, dict):
            raise RuntimeError("strategy simulation returned an invalid payload")

        allocation_info = simulation_result.get(
            "allocation_info",
            simulation_result.get("max_concurrent"),
        )
        kelly_portfolio = simulation_result.get("kelly_portfolio")
        return BacktestSimulationResult(
            initial_portfolio=simulation_result.get("initial_portfolio"),
            kelly_portfolio=kelly_portfolio,
            allocation_info=allocation_info,
            all_entries=simulation_result.get("all_entries"),
            summary_metrics=canonical_metrics_from_portfolio(kelly_portfolio),
            metrics_payload=self._build_metrics_payload(
                kelly_portfolio=kelly_portfolio,
                allocation_info=allocation_info,
            ),
        )

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
            parsed = float(value)
        except Exception:
            return None
        return parsed if math.isfinite(parsed) else None

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
