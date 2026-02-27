"""
戦略パラメータ最適化ユーティリティ

VectorBTベースの戦略パラメータ最適化機能を提供します。
"""

import itertools
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, NamedTuple

import pandas as pd

from src.domains.optimization.scoring import (
    is_valid_metric,
    normalize_and_recalculate_scores,
)

from ..core.yaml_configurable_strategy import YamlConfigurableStrategy


class OptimizationResult(NamedTuple):
    """最適化結果を格納するクラス"""

    best_params: dict[str, Any]
    best_score: float
    best_portfolio: Any
    all_results: list[dict[str, Any]]
    scoring_weights: dict[str, float]
    html_path: str = ""  # 可視化HTMLパス（オプション）


@dataclass
class ParameterRange:
    """パラメータ範囲定義"""

    name: str
    values: range | list[Any]
    default: Any = None


class ParameterOptimizer:
    """
    戦略パラメータ最適化クラス

    グリッドサーチ、ランダムサーチによるパラメータ最適化を提供。
    Kelly基準による2段階評価（等分配→Kelly配分）を必須とする。

    前提条件:
        - 戦略クラスは calculate_kelly_allocations() メソッドを実装していること
        - 戦略クラスの run_backtest() は custom_allocations 引数をサポートしていること
    """

    def __init__(
        self,
        strategy_class: type,
        strategy_kwargs: dict[str, Any],
        scoring_weights: dict[str, float],
        n_jobs: int = 1,
    ):
        """
        最適化器の初期化

        Args:
            strategy_class: 最適化する戦略クラス
            strategy_kwargs: 戦略の固定パラメータ
            scoring_weights: 複合スコアリング用の重み付け辞書（必須）
                例: {"sharpe_ratio": 0.6, "calmar_ratio": 0.3, "total_return": 0.1}
            n_jobs: 並列処理数
        """
        self.strategy_class = strategy_class
        self.strategy_kwargs = strategy_kwargs
        self.scoring_weights = scoring_weights
        self.n_jobs = n_jobs

        # サポートされる最適化指標
        self.supported_metrics = {
            "sharpe_ratio": self._extract_sharpe_ratio,
            "total_return": self._extract_total_return,
            "calmar_ratio": self._extract_calmar_ratio,
            "max_drawdown": self._extract_max_drawdown,
            "win_rate": self._extract_win_rate,
        }

        # 複合スコアリング設定の検証
        self._validate_scoring_weights()

        # 正規化用の指標値キャッシュ（全結果収集後に正規化）
        self.all_metric_values: dict[str, list[float]] = {
            metric: [] for metric in self.scoring_weights.keys()
        }

    def grid_search(
        self, param_ranges: list[ParameterRange], max_combinations: int | None = None
    ) -> OptimizationResult:
        """
        グリッドサーチによるパラメータ最適化

        Args:
            param_ranges: 最適化するパラメータ範囲のリスト
            max_combinations: 最大組み合わせ数（制限）

        Returns:
            OptimizationResult: 最適化結果
        """
        # パラメータの組み合わせを生成
        param_combinations = self._generate_param_combinations(param_ranges)

        if max_combinations and len(param_combinations) > max_combinations:
            # ランダムサンプリングで制限
            import random

            param_combinations = random.sample(param_combinations, max_combinations)

        print(f"グリッドサーチ開始: {len(param_combinations)} 組み合わせ")

        # 最適化実行
        results = self._run_optimization(param_combinations)

        # 正規化処理＆複合スコア再計算
        results = normalize_and_recalculate_scores(results, self.scoring_weights)

        return self._create_optimization_result(results)

    def random_search(
        self, param_ranges: list[ParameterRange], n_trials: int = 100
    ) -> OptimizationResult:
        """
        ランダムサーチによるパラメータ最適化

        Args:
            param_ranges: 最適化するパラメータ範囲のリスト
            n_trials: 試行回数

        Returns:
            OptimizationResult: 最適化結果
        """
        # ランダムなパラメータ組み合わせを生成
        param_combinations = self._generate_random_combinations(param_ranges, n_trials)

        print(f"ランダムサーチ開始: {n_trials} 試行")

        # 最適化実行
        results = self._run_optimization(param_combinations)

        # 正規化処理＆複合スコア再計算
        results = normalize_and_recalculate_scores(results, self.scoring_weights)

        return self._create_optimization_result(results)

    def _generate_param_combinations(
        self, param_ranges: list[ParameterRange]
    ) -> list[dict[str, Any]]:
        """パラメータの全組み合わせを生成"""
        param_names = [pr.name for pr in param_ranges]
        param_values = [list(pr.values) for pr in param_ranges]

        combinations = []
        for combination in itertools.product(*param_values):
            param_dict = dict(zip(param_names, combination))
            combinations.append(param_dict)

        return combinations

    def _generate_random_combinations(
        self, param_ranges: list[ParameterRange], n_trials: int
    ) -> list[dict[str, Any]]:
        """ランダムなパラメータ組み合わせを生成"""
        import random

        combinations = []
        for _ in range(n_trials):
            param_dict = {}
            for pr in param_ranges:
                param_dict[pr.name] = random.choice(list(pr.values))
            combinations.append(param_dict)

        return combinations

    def _run_optimization(
        self, param_combinations: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """最適化を実行"""
        if self.n_jobs == 1:
            # シングルプロセス実行
            results = []
            for i, params in enumerate(param_combinations):
                result = self._evaluate_single_params(params)
                if result:
                    results.append(result)
                    if (i + 1) % 10 == 0:
                        print(f"進捗: {i + 1}/{len(param_combinations)}")
        else:
            # 並列実行
            results = []
            with ProcessPoolExecutor(max_workers=self.n_jobs) as executor:
                future_to_params = {
                    executor.submit(self._evaluate_single_params, params): params
                    for params in param_combinations
                }

                for i, future in enumerate(as_completed(future_to_params)):
                    result = future.result()
                    if result:
                        results.append(result)

                    if (i + 1) % 10 == 0:
                        print(f"進捗: {i + 1}/{len(param_combinations)}")

        return results

    def _evaluate_single_params(
        self, params: dict[str, Any]
    ) -> dict[str, Any] | None:
        """
        単一パラメータ組み合わせを評価（2段階Kelly基準評価・必須）

        評価プロセス:
            1. 戦略インスタンスを作成（ベース設定 + 最適化パラメータ）
            2. 初回バックテスト実行（全銘柄等分配）
            3. Kelly基準による最適資金配分計算（必須）
            4. Kelly配分バックテスト実行（最終評価）
            5. 複合スコア計算（Kelly配分結果を使用・正規化前の生の値）

        Note:
            - Kelly基準が使用できない場合はエラーを発生させる
            - 複合スコアは全結果収集後に正規化される
        """
        try:
            # 戦略パラメータをマージ
            strategy_params = {**self.strategy_kwargs, **params}

            # 戦略インスタンスを作成
            strategy = self.strategy_class(**strategy_params, printlog=False)

            # 1. 初回バックテスト実行（等分配）
            # Kelly基準計算用に全銘柄均等配分でバックテスト
            portfolio_equal = strategy.run_backtest()

            # 2. Kelly基準による最適資金配分計算（必須）
            # SharedConfigからKelly設定を取得
            shared_config = strategy_params.get("shared_config")
            kelly_fraction = shared_config.kelly_fraction if shared_config else 0.5

            kelly_allocations = strategy.calculate_kelly_allocations(
                portfolio_equal, kelly_fraction=kelly_fraction
            )

            # 3. Kelly配分バックテスト実行（最終評価）
            portfolio_final = strategy.run_backtest(
                custom_allocations=kelly_allocations
            )

            # 4. 指標を計算（Kelly配分結果を使用）
            score = self._calculate_composite_score(portfolio_final, strategy)

            # 各指標の生の値を収集（後で正規化）
            metric_values = {}
            for metric in self.scoring_weights.keys():
                metric_values[metric] = self.supported_metrics[metric](
                    portfolio_final, strategy
                )

            return {
                "params": params,
                "score": score,
                "portfolio": portfolio_final,
                "strategy": strategy,
                "metric_values": metric_values,  # 正規化用
            }

        except Exception as e:
            print(f"パラメータ評価エラー {params}: {e}")
            import traceback

            traceback.print_exc()
            return None

    def _validate_scoring_weights(self) -> None:
        """複合スコアリング重み付けの検証"""
        if not self.scoring_weights:
            raise ValueError("scoring_weightsが設定されていません")

        # すべての指標がサポートされているか確認
        for metric in self.scoring_weights.keys():
            if metric not in self.supported_metrics:
                raise ValueError(f"Unsupported metric in scoring_weights: {metric}")

        # 重みの合計が1.0に近いか警告
        total_weight = sum(self.scoring_weights.values())
        if abs(total_weight - 1.0) > 0.01:
            print(f"警告: 重みの合計が1.0ではありません（合計={total_weight:.3f}）")

    def _calculate_composite_score(
        self, portfolio: Any, strategy: YamlConfigurableStrategy
    ) -> float:
        """
        複合スコアを計算（複数指標の重み付け合計・正規化前）

        Note:
            - ここでは正規化前の生の指標値を使用して計算
            - 全結果収集後に normalize_and_recalculate_scores() で
              Min-Max正規化を行い、複合スコアを再計算する

        Args:
            portfolio: VectorBTポートフォリオ（Kelly配分後）
            strategy: 戦略インスタンス

        Returns:
            float: 複合スコア（正規化前の生の値）
        """
        if not self.scoring_weights:
            raise ValueError("scoring_weights is not set")

        composite_score = 0.0
        for metric, weight in self.scoring_weights.items():
            metric_value = self.supported_metrics[metric](portfolio, strategy)
            composite_score += weight * metric_value

        return composite_score

    def _extract_sharpe_ratio(
        self, portfolio: Any, strategy: YamlConfigurableStrategy
    ) -> float:
        """シャープレシオを抽出"""
        try:
            sharpe = portfolio.sharpe_ratio()
            if not is_valid_metric(sharpe):
                return 0.0
            return float(sharpe)
        except Exception:
            return 0.0

    def _extract_total_return(
        self, portfolio: Any, strategy: YamlConfigurableStrategy
    ) -> float:
        """トータルリターンを抽出"""
        try:
            return float(portfolio.total_return())
        except Exception:
            return 0.0

    def _extract_calmar_ratio(
        self, portfolio: Any, strategy: YamlConfigurableStrategy
    ) -> float:
        """カルマーレシオを抽出"""
        try:
            calmar = portfolio.calmar_ratio()
            if not is_valid_metric(calmar):
                return 0.0
            return float(calmar)
        except Exception:
            return 0.0

    def _extract_max_drawdown(
        self, portfolio: Any, strategy: YamlConfigurableStrategy
    ) -> float:
        """
        最大ドローダウンを抽出（最小化のため負の値として返す）

        注意: 最適化では最大値を探すため、負の値として返す
        （ドローダウンが小さいほど良い = 負の値が大きいほど良い）
        """
        try:
            max_dd = portfolio.max_drawdown()
            if not is_valid_metric(max_dd):
                return -100.0
            # 負の値として返す（最小化のため）
            return -abs(float(max_dd))
        except Exception:
            return -100.0

    def _extract_win_rate(
        self, portfolio: Any, strategy: YamlConfigurableStrategy
    ) -> float:
        """勝率を抽出"""
        try:
            # VectorBTの組み込みメソッドを使用
            win_rate = portfolio.trades.win_rate()
            if not is_valid_metric(win_rate):
                return 0.0
            return float(win_rate)
        except Exception:
            # 代替手法: trades.records_readableから計算
            try:
                trades = portfolio.trades
                if hasattr(trades, "records_readable"):
                    trades_df = trades.records_readable
                    if len(trades_df) == 0:
                        return 0.0

                    pnl_series = trades_df["PnL"]
                    win_rate = (
                        (pnl_series > 0).sum() / len(pnl_series)
                        if len(pnl_series) > 0
                        else 0.0
                    )
                    return float(win_rate)
            except Exception:
                pass
            return 0.0

    def _create_optimization_result(
        self, results: list[dict[str, Any]]
    ) -> OptimizationResult:
        """最適化結果を作成"""
        if not results:
            raise ValueError("最適化結果が空です")

        # 最良結果を取得
        best_result = max(results, key=lambda x: x["score"])

        return OptimizationResult(
            best_params=best_result["params"],
            best_score=best_result["score"],
            best_portfolio=best_result["portfolio"],
            all_results=results,
            scoring_weights=self.scoring_weights,
        )

    def plot_optimization_surface(
        self,
        results: OptimizationResult,
        param_x: str,
        param_y: str,
        save_path: str | None = None,
    ) -> None:
        """最適化結果の表面プロット"""
        try:
            import matplotlib.pyplot as plt
            from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 # pyright: ignore[reportUnusedImport]

            # データを抽出（型安全性確保）
            x_values: list[float] = [
                float(r["params"][param_x]) for r in results.all_results
            ]
            y_values: list[float] = [
                float(r["params"][param_y]) for r in results.all_results
            ]
            z_values: list[float] = [float(r["score"]) for r in results.all_results]

            # 3Dプロット
            fig = plt.figure(figsize=(12, 8))
            ax = fig.add_subplot(111, projection="3d")

            scatter = ax.scatter(
                x_values,
                y_values,
                z_values,  # pyright: ignore[reportArgumentType]
                c=z_values,
                cmap="viridis",
            )
            ax.set_xlabel(param_x)
            ax.set_ylabel(param_y)
            ax.set_zlabel("Composite Score")
            ax.set_title("Optimization Surface: Composite Score")

            # カラーバーを追加
            plt.colorbar(scatter)

            if save_path:
                plt.savefig(save_path)
            else:
                plt.show()

        except ImportError:
            print("matplotlib が必要です: pip install matplotlib")

    def get_optimization_summary(self, results: OptimizationResult) -> pd.DataFrame:
        """最適化結果のサマリーを取得"""
        summary_data = []

        for result in results.all_results:
            row = result["params"].copy()
            row["score"] = result["score"]
            summary_data.append(row)

        df = pd.DataFrame(summary_data)
        return df.sort_values("score", ascending=False)
