"""
パラメータ最適化エンジン

既存ParameterOptimizerをラップし、YAML統合・自動パス推測を提供します。
"""

import os
import sys
import threading
import signal
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import pandas as pd

from loguru import logger
from ruamel.yaml import YAML

from src.shared.constants import OPTIMIZATION_TIMEOUT_SECONDS
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.domains.strategy.utils.optimization import OptimizationResult

from .grid_loader import (
    find_grid_config_path,
    generate_combinations,
    load_default_config,
    load_grid_config,
)
from .param_builder import build_signal_params
from .scoring import (
    calculate_composite_score,
    normalize_and_recalculate_scores,
)
from .metrics import collect_metrics as collect_optimization_metrics

# ワーカープロセス間データ共有用（initializer経由で設定）
_worker_shared_data: Optional[Dict[str, Dict[str, pd.DataFrame]]] = None
_worker_shared_benchmark: Optional[pd.DataFrame] = None

T = TypeVar("T")


@contextmanager
def _timeout_guard(seconds: int) -> Iterator[None]:  # type: ignore[override]
    """指定秒数でTimeoutErrorを発生させるガード（UNIXシグナルベース）"""
    if seconds <= 0:
        yield
        return
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def _handler(signum, frame):  # type: ignore[override]
        raise TimeoutError(f"Optimization step timed out after {seconds} seconds")

    previous_handler = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _run_with_timeout(seconds: int, func: Callable[[], T]) -> T:
    """タイムアウト付きで関数を実行"""
    with _timeout_guard(seconds):
        return func()


def _init_worker_data(
    shared_data: Dict[str, Dict[str, pd.DataFrame]],
    shared_benchmark: Optional[pd.DataFrame],
) -> None:
    """ProcessPoolExecutor initializer: 共有データをワーカーにセット"""
    global _worker_shared_data, _worker_shared_benchmark
    _worker_shared_data = shared_data
    _worker_shared_benchmark = shared_benchmark


class ParameterOptimizationEngine:
    """
    パラメータ最適化エンジン

    YAMLグリッド定義に基づき、並列最適化を実行
    """

    def __init__(
        self,
        strategy_name: str,
        grid_config_path: str | None = None,
        verbose: bool = False,
    ):
        """
        最適化エンジンの初期化

        Args:
            strategy_name: 戦略名（例: "range_break_v6"）
            grid_config_path: グリッドYAMLファイルパス（省略時は自動推測）
            verbose: 詳細ログ出力フラグ（Trueでinfo/debugログ表示）
        """
        # 戦略名を保存（カテゴリ付き可能）
        self.strategy_name = strategy_name
        # ベース名（カテゴリなし）を保存（ファイル名生成用）
        self.strategy_basename = strategy_name.split("/")[-1]
        self.verbose = verbose

        # loguruログレベル設定（親プロセス用）
        self._configure_logger(verbose)

        # グリッドYAML読み込み
        self.grid_config_path = find_grid_config_path(
            self.strategy_basename, grid_config_path
        )
        grid_config = load_grid_config(self.grid_config_path)

        self.parameter_ranges = grid_config.get("parameter_ranges", {})
        self.description = grid_config.get("description", "")

        # default.yaml読み込み
        default_config = load_default_config()

        # 最適化設定
        self.optimization_config = default_config["parameter_optimization"]

        # ベース戦略YAML（グリッドで指定 or ConfigLoaderで推測）
        from src.domains.strategy.runtime.loader import ConfigLoader

        config_loader = ConfigLoader()

        if "base_config" in grid_config:
            # グリッドYAMLで明示指定されている場合
            self.base_config_path = grid_config["base_config"]
            ruamel_yaml_base = YAML()
            ruamel_yaml_base.preserve_quotes = True
            with open(self.base_config_path) as f:
                base_strategy_config = ruamel_yaml_base.load(f)
        else:
            # 未指定の場合はConfigLoaderで自動推測
            base_strategy_config = config_loader.load_strategy_config(strategy_name)
            # パスを取得（ログ表示用）
            inferred_path = config_loader._infer_strategy_path(strategy_name)
            self.base_config_path = str(inferred_path)

        # 共通設定（戦略YAML override 対応）
        self.shared_config_dict = config_loader.merge_shared_config(
            base_strategy_config
        )

        # ベースSignalParamsを構築（最適化しない設定の基準値）
        self.base_entry_params = SignalParams(
            **base_strategy_config.get("entry_filter_params", {})
        )
        self.base_exit_params = SignalParams(
            **base_strategy_config.get("exit_trigger_params", {})
        )

    @property
    def total_combinations(self) -> int:
        """パラメータ組み合わせ総数を返す"""
        return len(generate_combinations(self.parameter_ranges))

    def optimize(self) -> OptimizationResult:
        """
        最適化実行

        Returns:
            OptimizationResult: 最適化結果
        """
        # 1. パラメータ組み合わせ生成
        combinations = generate_combinations(self.parameter_ranges)

        # バリデーション: パラメータ範囲が空でないかチェック
        if len(combinations) == 0:
            raise ValueError(
                "パラメータ範囲が空です。グリッドYAMLファイルを確認してください。\n"
                f"グリッドファイル: {self.grid_config_path if hasattr(self, 'grid_config_path') else '不明'}"
            )

        logger.info(f"パラメータ組み合わせ数: {len(combinations)}")

        # 2. 各組み合わせに対してSignalParamsを構築
        strategy_kwargs_list = []
        for combo in combinations:
            # SignalParams動的構築
            entry_params = build_signal_params(
                combo, "entry_filter_params", self.base_entry_params
            )
            exit_params = build_signal_params(
                combo, "exit_trigger_params", self.base_exit_params
            )

            # SharedConfig構築
            shared_config = SharedConfig(**self.shared_config_dict)

            # 戦略パラメータ辞書（ParameterOptimizerに渡す）
            strategy_kwargs_list.append(
                {
                    "shared_config": shared_config,
                    "entry_filter_params": entry_params,
                    "exit_trigger_params": exit_params,
                }
            )

        # 2.5. データ事前取得（全ワーカー共有）
        self._prefetched_data, self._prefetched_benchmark = self._prefetch_data()

        # 3. カスタム最適化実行
        results = self._run_custom_optimization(strategy_kwargs_list, combinations)

        # 5. 正規化処理＆複合スコア再計算
        results = normalize_and_recalculate_scores(
            results, self.optimization_config["scoring_weights"]
        )

        # バリデーション: 結果が空でないかチェック
        if not results:
            raise RuntimeError(
                "最適化結果が空です。全てのパラメータ組み合わせでエラーが発生した可能性があります。\n"
                "ログを確認してください。"
            )

        # 6. 複合スコアランキング順にソート
        sorted_results = sorted(results, key=lambda x: x["score"], reverse=True)

        # 7. 最良パラメータで再バックテスト実行（best_portfolio取得）
        best_params = sorted_results[0]["params"]
        best_entry_params = build_signal_params(
            best_params, "entry_filter_params", self.base_entry_params
        )
        best_exit_params = build_signal_params(
            best_params, "exit_trigger_params", self.base_exit_params
        )
        best_shared_config = SharedConfig(**self.shared_config_dict)

        best_strategy = YamlConfigurableStrategy(
            shared_config=best_shared_config,
            entry_filter_params=best_entry_params,
            exit_trigger_params=best_exit_params,
        )
        # 事前取得データを注入（API呼出スキップ）
        best_strategy.multi_data_dict = self._prefetched_data
        best_strategy.benchmark_data = self._prefetched_benchmark

        _, best_portfolio, _, _, _ = best_strategy.run_optimized_backtest_kelly(
            kelly_fraction=best_shared_config.kelly_fraction,
            min_allocation=best_shared_config.min_allocation,
            max_allocation=best_shared_config.max_allocation,
        )

        # 8. 可視化Notebook生成
        notebook_path = self._generate_visualization_notebook(
            sorted_results, combinations
        )

        # 9. 最適化結果を返却
        return OptimizationResult(
            best_params=sorted_results[0]["params"],
            best_score=sorted_results[0]["score"],
            best_portfolio=best_portfolio,  # 再構築したportfolio
            all_results=sorted_results,
            scoring_weights=self.optimization_config["scoring_weights"],
            notebook_path=notebook_path,
        )

    def _prefetch_data(
        self,
    ) -> Tuple[Dict[str, Dict[str, pd.DataFrame]], Optional[pd.DataFrame]]:
        """最適化ワーカー用にデータを事前取得（1回のみ）"""
        from src.infrastructure.data_access.loaders import prepare_multi_data

        shared_config = SharedConfig(**self.shared_config_dict)
        include_forecast_revision = self._should_include_forecast_revision()

        logger.info("最適化用データ事前取得開始")
        multi_data_dict = prepare_multi_data(
            dataset=shared_config.dataset,
            stock_codes=shared_config.stock_codes,
            start_date=shared_config.start_date,
            end_date=shared_config.end_date,
            include_margin_data=shared_config.include_margin_data,
            include_statements_data=shared_config.include_statements_data,
            timeframe=shared_config.timeframe,
            include_forecast_revision=include_forecast_revision,
        )
        logger.info(f"最適化用データ事前取得完了: {len(multi_data_dict)}銘柄")

        # ベンチマークデータも事前取得（betaシグナル等で使用）
        benchmark_data = None
        if shared_config.relative_mode or shared_config.benchmark_table:
            try:
                from src.infrastructure.data_access.loaders import load_topix_data

                benchmark_data = load_topix_data(
                    shared_config.dataset,
                    shared_config.start_date,
                    shared_config.end_date,
                )
                logger.info("ベンチマークデータ事前取得完了")
            except Exception as e:
                logger.warning(f"ベンチマークデータ事前取得失敗（続行）: {e}")

        return multi_data_dict, benchmark_data

    @staticmethod
    def _is_forecast_signal_enabled(signal_params: SignalParams) -> bool:
        fundamental = signal_params.fundamental
        return bool(
            fundamental.enabled
            and (
                fundamental.forward_eps_growth.enabled
                or fundamental.peg_ratio.enabled
                or fundamental.forward_dividend_growth.enabled
                or fundamental.forward_payout_ratio.enabled
            )
        )

    def _grid_may_enable_forecast_signals(self) -> bool:
        """Grid定義で予想系シグナルが有効化されうるか判定する。"""
        if not isinstance(self.parameter_ranges, dict):
            return False

        section_to_base = (
            ("entry_filter_params", self.base_entry_params),
            ("exit_trigger_params", self.base_exit_params),
        )

        for section_name, base_params in section_to_base:
            section_cfg = self.parameter_ranges.get(section_name)
            if not isinstance(section_cfg, dict):
                continue
            fundamental_cfg = section_cfg.get("fundamental")
            if not isinstance(fundamental_cfg, dict):
                continue

            base_fundamental = base_params.fundamental
            fundamental_enabled = bool(base_fundamental.enabled)
            if not fundamental_enabled:
                fundamental_enabled_values = fundamental_cfg.get("enabled")
                if isinstance(fundamental_enabled_values, list):
                    fundamental_enabled = any(bool(v) for v in fundamental_enabled_values)
                elif fundamental_enabled_values is not None:
                    fundamental_enabled = bool(fundamental_enabled_values)

            if not fundamental_enabled:
                continue

            for signal_name in (
                "forward_eps_growth",
                "peg_ratio",
                "forward_dividend_growth",
                "forward_payout_ratio",
            ):
                signal_cfg = fundamental_cfg.get(signal_name)
                if not isinstance(signal_cfg, dict):
                    continue

                signal_enabled = bool(getattr(base_fundamental, signal_name).enabled)
                if not signal_enabled:
                    signal_enabled_values = signal_cfg.get("enabled")
                    if isinstance(signal_enabled_values, list):
                        signal_enabled = any(bool(v) for v in signal_enabled_values)
                    elif signal_enabled_values is not None:
                        signal_enabled = bool(signal_enabled_values)

                if signal_enabled:
                    return True

        return False

    def _should_include_forecast_revision(self) -> bool:
        return (
            self._is_forecast_signal_enabled(self.base_entry_params)
            or self._is_forecast_signal_enabled(self.base_exit_params)
            or self._grid_may_enable_forecast_signals()
        )

    def _run_custom_optimization(
        self, strategy_kwargs_list: List[Dict], combinations: List[Dict]
    ) -> List[Dict[str, Any]]:
        """
        カスタム最適化実行（並列処理対応）

        Args:
            strategy_kwargs_list: 戦略パラメータリスト
            combinations: パラメータ組み合わせリスト

        Returns:
            List[Dict[str, Any]]: 最適化結果リスト
        """
        n_jobs = self.optimization_config["n_jobs"]
        max_workers = None if n_jobs == -1 else n_jobs

        self._log_parallel_mode(n_jobs, max_workers)

        if max_workers == 1:
            return self._run_optimization_single_process(
                strategy_kwargs_list, combinations
            )

        return self._run_optimization_parallel(
            strategy_kwargs_list, combinations, max_workers
        )

    def _log_parallel_mode(self, n_jobs: int, max_workers: int | None) -> None:
        """並列処理モードをログ出力"""
        if n_jobs == -1:
            actual_cores = os.cpu_count() or 1
            logger.info(f"並列処理: 全CPUコア使用 ({actual_cores} コア)")
        elif max_workers == 1:
            logger.info("シングルプロセス実行（デバッグモード）")
        else:
            logger.info(f"並列処理: {max_workers} ワーカー")

    def _run_optimization_single_process(
        self, strategy_kwargs_list: List[Dict], combinations: List[Dict]
    ) -> List[Dict[str, Any]]:
        """
        シングルプロセスで最適化を実行（デバッグモード用）

        Args:
            strategy_kwargs_list: 戦略パラメータリスト
            combinations: パラメータ組み合わせリスト

        Returns:
            List[Dict[str, Any]]: 最適化結果リスト
        """
        results: List[Dict[str, Any]] = []
        total = len(combinations)

        for i, (strategy_kwargs, combo) in enumerate(
            zip(strategy_kwargs_list, combinations), 1
        ):
            result = self._evaluate_single_params(strategy_kwargs, combo, self.verbose)
            if result:
                results.append(result)
                self._log_evaluation_result(i, total, result, combo)

        return results

    def _run_optimization_parallel(
        self,
        strategy_kwargs_list: List[Dict],
        combinations: List[Dict],
        max_workers: int | None,
    ) -> List[Dict[str, Any]]:
        """
        並列処理で最適化を実行（ProcessPoolExecutor使用）

        Args:
            strategy_kwargs_list: 戦略パラメータリスト
            combinations: パラメータ組み合わせリスト
            max_workers: 最大ワーカー数（Noneで全CPUコア使用）

        Returns:
            List[Dict[str, Any]]: 最適化結果リスト

        Note:
            - タイムアウト設定: 600秒（10分）per 組み合わせ
        """
        from concurrent.futures import ProcessPoolExecutor, as_completed

        results: List[Dict[str, Any]] = []
        total = len(combinations)

        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_init_worker_data,
            initargs=(self._prefetched_data, self._prefetched_benchmark),
        ) as executor:
            future_to_params = {
                executor.submit(
                    self._evaluate_single_params,
                    strategy_kwargs,
                    combo,
                    self.verbose,
                ): (strategy_kwargs, combo)
                for strategy_kwargs, combo in zip(strategy_kwargs_list, combinations)
            }

            for i, future in enumerate(as_completed(future_to_params), 1):
                _, combo = future_to_params[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        self._log_evaluation_result(i, total, result, result["params"])
                except TimeoutError:
                    logger.warning(
                        f"[{i}/{total}] "
                        f"TIMEOUT: {self._format_params(combo)} "
                        f"(10分でタイムアウト、スキップ)"
                    )
                except Exception as e:
                    logger.error(
                        f"[{i}/{total}] ERROR: {self._format_params(combo)}: {e}"
                    )

        return results

    def _log_evaluation_result(
        self, index: int, total: int, result: Dict[str, Any], params: Dict
    ) -> None:
        """評価結果をログ出力"""
        metrics = result["metric_values"]
        logger.info(
            f"[{index}/{total}] "
            f"Score: {result['score']:.4f}, "
            f"Params: {self._format_params(params)}"
        )
        logger.info(
            f"      Sharpe: {metrics['sharpe_ratio']:.4f}, "
            f"Calmar: {metrics['calmar_ratio']:.4f}, "
            f"Total Return: {metrics['total_return']:.2%}"
        )

    def _evaluate_single_params(
        self, strategy_kwargs: Dict, params: Dict, verbose: bool = False
    ) -> Dict[str, Any] | None:
        """
        単一パラメータ組み合わせを評価（2段階Kelly基準評価）

        Args:
            strategy_kwargs: 戦略パラメータ
            params: パラメータ組み合わせ（表示用）
            verbose: 詳細ログ出力フラグ（子プロセス用）

        Returns:
            Dict[str, Any] | None: 評価結果

        Note:
            - ProcessPoolExecutor使用時、子プロセスで実行される
            - 親プロセスのログ設定は引き継がれないため、ここで再設定
        """
        self._configure_logger(verbose)

        try:
            kelly_portfolio = _run_with_timeout(
                OPTIMIZATION_TIMEOUT_SECONDS,
                lambda: self._run_kelly_backtest(strategy_kwargs),
            )
            metric_values = self._collect_metrics(kelly_portfolio)
            weights = self.optimization_config["scoring_weights"]
            score = calculate_composite_score(kelly_portfolio, weights)

            return {
                "params": params,
                "score": score,
                "metric_values": metric_values,
            }

        except TimeoutError:
            logger.warning(f"TIMEOUT: {self._format_params(params)} (10分でタイムアウト)")
            return None
        except Exception as e:
            logger.exception(f"パラメータ評価エラー {params}: {e}")
            return None

    def _run_kelly_backtest(self, strategy_kwargs: Dict) -> Any:
        """
        Kelly基準2段階最適化バックテストを実行

        Args:
            strategy_kwargs: 戦略パラメータ

        Returns:
            Any: Kelly最適化後のポートフォリオ
        """
        strategy = YamlConfigurableStrategy(**strategy_kwargs)

        # 事前取得データを注入（API呼出スキップ）
        if _worker_shared_data is not None:
            # 並列処理モード: initializer経由で設定済みのグローバルデータを使用
            strategy.multi_data_dict = _worker_shared_data
            strategy.benchmark_data = _worker_shared_benchmark
        elif hasattr(self, "_prefetched_data") and self._prefetched_data is not None:
            # シングルプロセスモード: インスタンス変数から直接注入
            strategy.multi_data_dict = self._prefetched_data
            strategy.benchmark_data = self._prefetched_benchmark

        shared_config = strategy_kwargs["shared_config"]

        _, kelly_portfolio, _, _, _ = strategy.run_optimized_backtest_kelly(
            kelly_fraction=shared_config.kelly_fraction,
            min_allocation=shared_config.min_allocation,
            max_allocation=shared_config.max_allocation,
        )

        return kelly_portfolio

    def _collect_metrics(self, portfolio: Any) -> Dict[str, float | int]:
        """
        ポートフォリオからメトリクスを収集

        Args:
            portfolio: VectorBTポートフォリオオブジェクト

        Returns:
            Dict[str, float | int]: メトリクス名と値のマッピング
        """
        scoring_weights = self.optimization_config["scoring_weights"]
        return collect_optimization_metrics(portfolio, scoring_weights)

    def _generate_visualization_notebook(
        self, sorted_results: List[Dict], combinations: List[Dict]
    ) -> str:
        """
        可視化Notebook生成

        Args:
            sorted_results: 複合スコア順にソートされた最適化結果
            combinations: パラメータ組み合わせリスト

        Returns:
            str: 生成されたNotebookのパス
        """
        from datetime import datetime

        from .notebook_generator import generate_optimization_notebook

        # データセット名を抽出
        from pathlib import Path

        dataset = self.shared_config_dict.get("dataset", "")
        if dataset:
            # "primeExTopix500" または "dataset/primeExTopix500.db" → "primeExTopix500"
            dataset_name = Path(dataset).stem
        else:
            dataset_name = "unknown"

        # 出力パス生成（外部ディレクトリまたはプロジェクト内）
        from src.shared.paths import get_optimization_results_dir

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = get_optimization_results_dir(self.strategy_basename)
        output_path = str(output_dir / f"{dataset_name}_{timestamp}.html")

        # ディレクトリ作成
        output_dir.mkdir(parents=True, exist_ok=True)

        # Notebook生成（ベース名を使用）
        result = generate_optimization_notebook(
            results=sorted_results,
            output_path=output_path,
            strategy_name=self.strategy_basename,
            parameter_ranges=self.parameter_ranges,
            scoring_weights=self.optimization_config["scoring_weights"],
            n_combinations=len(combinations),
        )

        return str(result)

    def _format_params(self, params: Dict) -> str:
        """
        パラメータを読みやすくフォーマット

        Args:
            params: パラメータ辞書

        Returns:
            str: フォーマット済み文字列
        """
        formatted = []
        for key, value in params.items():
            # entry/exit接頭語を追加して区別
            parts = key.split(".")
            if parts[0] == "entry_filter_params":
                short_key = f"entry_{parts[-1]}"
            elif parts[0] == "exit_trigger_params":
                short_key = f"exit_{parts[-1]}"
            else:
                short_key = parts[-1]
            formatted.append(f"{short_key}={value}")

        return ", ".join(formatted)

    @staticmethod
    def _configure_logger(verbose: bool) -> None:
        """
        loguruログレベルを設定（DRY原則に基づく共通化）

        Args:
            verbose: 詳細ログ出力フラグ
                True: DEBUG以上全て表示
                False: WARNING以上のみ表示（INFO/DEBUGを抑制）

        Note:
            - 子プロセス（ProcessPoolExecutor）でも呼び出し必須
            - 親プロセスのログ設定は子プロセスに引き継がれないため、
              並列処理時は各子プロセスで再設定する必要がある
        """
        if not verbose:
            # verboseでない場合はWARNING以上のみ表示（INFO/DEBUGを抑制）
            logger.remove()  # 既存ハンドラを削除
            logger.add(
                sys.stderr,
                level="WARNING",  # WARNING, ERROR, CRITICAL のみ
                format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            )
        # verboseの場合はデフォルト設定（DEBUGから全て表示）をそのまま使用
