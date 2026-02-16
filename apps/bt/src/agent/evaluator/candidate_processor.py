"""
単一候補評価ロジック

戦略候補を単体で評価する関数
"""

from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from src.data.access.mode import data_access_mode_context
from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy

from ..models import EvaluationResult, StrategyCandidate
from .data_preparation import convert_dict_to_dataframes


def _safe_float(value: float, default: float = 0.0) -> float:
    """NaN/Inf値を安全にfloatに変換"""
    if pd.notna(value) and np.isfinite(value):
        return float(value)
    return default


def evaluate_single_candidate(
    candidate: StrategyCandidate,
    shared_config_dict: dict[str, Any],
    scoring_weights: dict[str, float],
    pre_fetched_stock_codes: list[str] | None = None,
    pre_fetched_ohlcv_data: dict[str, dict[str, Any]] | None = None,
    pre_fetched_benchmark_data: dict[str, Any] | None = None,
) -> EvaluationResult:
    """
    単一候補の評価（並列処理用スタンドアロン関数）

    Args:
        candidate: 戦略候補
        shared_config_dict: 共有設定辞書
        scoring_weights: スコアリング重み
        pre_fetched_stock_codes: 事前取得済み銘柄リスト（並列実行でのAPI呼び出し削減用）
        pre_fetched_ohlcv_data: 事前取得済みOHLCVデータ（シリアライズ済み辞書形式）
        pre_fetched_benchmark_data: 事前取得済みベンチマークデータ（シリアライズ済み辞書形式）

    Returns:
        評価結果
    """
    try:
        # SignalParams構築
        entry_params = SignalParams(**candidate.entry_filter_params)
        exit_params = SignalParams(**candidate.exit_trigger_params)

        with data_access_mode_context("direct"):
            # SharedConfig構築（候補のshared_configをマージ）
            merged_config = {**shared_config_dict, **candidate.shared_config}

            # 事前取得した銘柄リストを設定（ProcessPoolExecutor対応）
            # 並列実行では各ワーカーが独自のメモリ空間を持つため、
            # DataCacheが機能せずAPIを何度も叩く問題を回避
            if pre_fetched_stock_codes is not None:
                merged_config["stock_codes"] = pre_fetched_stock_codes

            shared_config = SharedConfig(**merged_config)

            # 事前取得OHLCVデータをDataFrameに復元
            restored_ohlcv_data: dict[str, dict[str, pd.DataFrame]] | None = None
            if pre_fetched_ohlcv_data is not None:
                restored_ohlcv_data = convert_dict_to_dataframes(pre_fetched_ohlcv_data)

            # 事前取得ベンチマークデータをDataFrameに復元
            restored_benchmark_data: pd.DataFrame | None = None
            if pre_fetched_benchmark_data is not None:
                restored_benchmark_data = pd.DataFrame(
                    data=pre_fetched_benchmark_data["data"],
                    index=pd.to_datetime(pre_fetched_benchmark_data["index"]),
                    columns=pre_fetched_benchmark_data["columns"],
                )

            # 戦略インスタンス作成
            strategy = YamlConfigurableStrategy(
                shared_config=shared_config,
                entry_filter_params=entry_params,
                exit_trigger_params=exit_params,
            )

            # 事前取得OHLCVデータを設定（APIスキップ）
            if restored_ohlcv_data is not None:
                strategy.multi_data_dict = restored_ohlcv_data

            # 事前取得ベンチマークデータを設定（APIスキップ）
            if restored_benchmark_data is not None:
                strategy.benchmark_data = restored_benchmark_data

            # Kelly基準バックテスト実行
            _, portfolio, _, _, _ = strategy.run_optimized_backtest_kelly(
                kelly_fraction=shared_config.kelly_fraction,
                min_allocation=shared_config.min_allocation,
                max_allocation=shared_config.max_allocation,
            )

        # メトリクス抽出
        sharpe = portfolio.sharpe_ratio()
        calmar = portfolio.calmar_ratio()
        total_return = portfolio.total_return()
        max_dd = portfolio.max_drawdown()

        # NaN/Infチェック
        sharpe = _safe_float(sharpe)
        calmar = _safe_float(calmar)
        total_return = _safe_float(total_return)
        max_dd = _safe_float(max_dd)

        # 勝率・トレード数
        try:
            trades = portfolio.trades.records_readable  # type: ignore[attr-defined]
            if len(trades) > 0:
                win_rate = float((trades["Return"] > 0).mean())
                trade_count = len(trades)
            else:
                win_rate = 0.0
                trade_count = 0
        except Exception:
            win_rate = 0.0
            trade_count = 0

        # 複合スコア計算（仮、後で正規化）
        metrics = {"sharpe_ratio": sharpe, "calmar_ratio": calmar, "total_return": total_return}
        score = sum(
            scoring_weights.get(key, 0.0) * value for key, value in metrics.items()
        )

        return EvaluationResult(
            candidate=candidate,
            score=score,
            sharpe_ratio=sharpe,
            calmar_ratio=calmar,
            total_return=total_return,
            max_drawdown=max_dd,
            win_rate=win_rate,
            trade_count=trade_count,
            success=True,
        )

    except Exception as e:
        logger.warning(f"Evaluation failed for {candidate.strategy_id}: {e}")
        return EvaluationResult(
            candidate=candidate,
            score=-999.0,
            success=False,
            error_message=str(e),
        )


# 後方互換性のためのエイリアス（アンダースコア付き旧名）
_evaluate_single_candidate = evaluate_single_candidate
