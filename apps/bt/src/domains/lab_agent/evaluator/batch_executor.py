"""
バッチ実行モジュール

並列・シングルプロセスでのバッチ評価実行
"""

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

from loguru import logger

from src.infrastructure.data_access.loaders.data_preparation import prepare_multi_data
from src.infrastructure.data_access.loaders.index_loaders import load_topix_data
from src.infrastructure.data_access.loaders.stock_loaders import get_stock_list

from ..models import EvaluationResult, StrategyCandidate
from .candidate_processor import evaluate_single_candidate
from .data_preparation import BatchPreparedData, convert_dataframes_to_dict


def get_max_workers(n_jobs: int) -> int | None:
    """
    並列ワーカー数を決定してログ出力

    Args:
        n_jobs: 設定値（-1で全CPU）

    Returns:
        max_workers: ワーカー数（Noneは全CPU使用）
    """
    if n_jobs == -1:
        actual_cores = os.cpu_count() or 1
        logger.info(f"Parallel evaluation: using all {actual_cores} cores")
        return None

    if n_jobs == 1:
        logger.info("Single-process evaluation (debug mode)")
    else:
        logger.info(f"Parallel evaluation: using {n_jobs} workers")

    return n_jobs


def _is_forecast_signal_enabled(side_params: dict[str, Any]) -> bool:
    """候補パラメータ内で予想系シグナルが有効か判定する。"""
    if not isinstance(side_params, dict):
        return False
    fundamental = side_params.get("fundamental")
    if not isinstance(fundamental, dict):
        return False
    if not bool(fundamental.get("enabled", False)):
        return False

    forward = fundamental.get("forward_eps_growth")
    if isinstance(forward, dict) and bool(forward.get("enabled", False)):
        return True

    peg = fundamental.get("peg_ratio")
    if isinstance(peg, dict) and bool(peg.get("enabled", False)):
        return True

    forward_dividend = fundamental.get("forward_dividend_growth")
    if isinstance(forward_dividend, dict) and bool(forward_dividend.get("enabled", False)):
        return True

    forward_payout = fundamental.get("forward_payout_ratio")
    if isinstance(forward_payout, dict) and bool(forward_payout.get("enabled", False)):
        return True

    return False


def _should_include_forecast_revision(
    candidates: list[StrategyCandidate] | None,
) -> bool:
    if not candidates:
        return False
    return any(
        _is_forecast_signal_enabled(candidate.entry_filter_params)
        or _is_forecast_signal_enabled(candidate.exit_trigger_params)
        for candidate in candidates
    )


def prepare_batch_data(
    shared_config_dict: dict[str, Any],
    candidates: list[StrategyCandidate] | None = None,
) -> BatchPreparedData:
    """
    バッチ評価用データを事前取得

    並列実行では各ワーカープロセスが独自のメモリ空間を持つため、
    メインプロセスで1回だけデータを取得し、各ワーカーに渡す。

    Args:
        shared_config_dict: 共有設定辞書
        candidates: 評価対象候補（予想系シグナル有効時の追加データ取得判定に使用）

    Returns:
        BatchPreparedData: 事前取得データ（銘柄リスト、OHLCVデータ、ベンチマークデータ）
    """
    include_forecast_revision = _should_include_forecast_revision(candidates)
    stock_codes = fetch_stock_codes(shared_config_dict)
    ohlcv_data = fetch_ohlcv_data(
        shared_config_dict,
        stock_codes,
        include_forecast_revision=include_forecast_revision,
    )
    benchmark_data = fetch_benchmark_data(shared_config_dict)

    return BatchPreparedData(
        stock_codes=stock_codes,
        ohlcv_data=ohlcv_data,
        benchmark_data=benchmark_data,
    )


def fetch_stock_codes(shared_config_dict: dict[str, Any]) -> list[str] | None:
    """銘柄リストを事前取得"""
    dataset = shared_config_dict.get("dataset")
    if not dataset:
        return None

    try:
        stock_codes = get_stock_list(dataset)
        logger.info(
            f"Pre-fetched {len(stock_codes)} stock codes for parallel evaluation"
        )
        return stock_codes
    except Exception as e:
        logger.warning(f"Failed to pre-fetch stock codes: {e}")
        return None


def fetch_ohlcv_data(
    shared_config_dict: dict[str, Any],
    stock_codes: list[str] | None,
    include_forecast_revision: bool = False,
) -> dict[str, dict[str, Any]] | None:
    """OHLCVデータを事前取得してシリアライズ"""
    dataset = shared_config_dict.get("dataset")
    if not stock_codes or not dataset:
        return None

    try:
        start_date = shared_config_dict.get("start_date")
        end_date = shared_config_dict.get("end_date")
        timeframe = shared_config_dict.get("timeframe", "daily")
        include_margin = shared_config_dict.get("include_margin_data", False)
        include_statements = shared_config_dict.get("include_statements_data", False)

        logger.info(f"Pre-fetching OHLCV data for {len(stock_codes)} stocks...")
        raw_data = prepare_multi_data(
            dataset=dataset,
            stock_codes=stock_codes,
            start_date=start_date,
            end_date=end_date,
            include_margin_data=include_margin,
            include_statements_data=include_statements,
            timeframe=timeframe,
            include_forecast_revision=include_forecast_revision,
        )

        ohlcv_data = convert_dataframes_to_dict(raw_data)
        logger.info(f"Pre-fetched OHLCV data for {len(ohlcv_data)} stocks")
        return ohlcv_data

    except Exception as e:
        logger.warning(f"Failed to pre-fetch OHLCV data: {e}")
        return None


def fetch_benchmark_data(shared_config_dict: dict[str, Any]) -> dict[str, Any] | None:
    """ベンチマークデータ（TOPIX）を事前取得してシリアライズ"""
    dataset = shared_config_dict.get("dataset")
    if not dataset:
        return None

    try:
        start_date = shared_config_dict.get("start_date")
        end_date = shared_config_dict.get("end_date")

        logger.info("Pre-fetching benchmark (TOPIX) data...")
        benchmark_df = load_topix_data(dataset, start_date, end_date)

        benchmark_data = {
            "index": benchmark_df.index.astype(str).tolist(),
            "columns": benchmark_df.columns.tolist(),
            "data": benchmark_df.values.tolist(),
        }
        logger.info(f"Pre-fetched benchmark data: {len(benchmark_df)} records")
        return benchmark_data

    except Exception as e:
        logger.warning(f"Failed to pre-fetch benchmark data: {e}")
        return None


def execute_batch_evaluation(
    candidates: list[StrategyCandidate],
    max_workers: int | None,
    prepared_data: BatchPreparedData,
    shared_config_dict: dict[str, Any],
    scoring_weights: dict[str, float],
    timeout_seconds: int,
) -> list[EvaluationResult]:
    """
    バッチ評価を実行

    Args:
        candidates: 戦略候補リスト
        max_workers: 並列ワーカー数（Noneは全CPU、1はシングルプロセス）
        prepared_data: 事前取得データ
        shared_config_dict: 共有設定辞書
        scoring_weights: スコアリング重み
        timeout_seconds: タイムアウト秒数

    Returns:
        評価結果リスト（未ソート）
    """
    if max_workers == 1 or len(candidates) < 3:
        return execute_single_process(
            candidates, prepared_data, shared_config_dict, scoring_weights
        )
    return execute_parallel(
        candidates,
        max_workers,
        prepared_data,
        shared_config_dict,
        scoring_weights,
        timeout_seconds,
    )


def execute_single_process(
    candidates: list[StrategyCandidate],
    prepared_data: BatchPreparedData,
    shared_config_dict: dict[str, Any],
    scoring_weights: dict[str, float],
) -> list[EvaluationResult]:
    """シングルプロセスで評価を実行"""
    results: list[EvaluationResult] = []

    for i, candidate in enumerate(candidates, 1):
        result = evaluate_single_candidate(
            candidate,
            shared_config_dict,
            scoring_weights,
            prepared_data.stock_codes,
            prepared_data.ohlcv_data,
            prepared_data.benchmark_data,
        )
        results.append(result)

        if result.success:
            logger.info(
                f"[{i}/{len(candidates)}] {candidate.strategy_id}: "
                f"score={result.score:.4f}, sharpe={result.sharpe_ratio:.4f}"
            )
        else:
            logger.warning(f"[{i}/{len(candidates)}] {candidate.strategy_id}: FAILED")

    return results


def execute_parallel(
    candidates: list[StrategyCandidate],
    max_workers: int | None,
    prepared_data: BatchPreparedData,
    shared_config_dict: dict[str, Any],
    scoring_weights: dict[str, float],
    timeout_seconds: int,
) -> list[EvaluationResult]:
    """並列実行で評価を実行"""
    results: list[EvaluationResult] = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_candidate = {
            executor.submit(
                evaluate_single_candidate,
                candidate,
                shared_config_dict,
                scoring_weights,
                prepared_data.stock_codes,
                prepared_data.ohlcv_data,
                prepared_data.benchmark_data,
            ): candidate
            for candidate in candidates
        }

        for i, future in enumerate(as_completed(future_to_candidate), 1):
            candidate = future_to_candidate[future]
            result = handle_future_result(
                future, candidate, i, len(candidates), timeout_seconds
            )
            results.append(result)

    return results


def handle_future_result(
    future: Any,
    candidate: StrategyCandidate,
    index: int,
    total: int,
    timeout_seconds: int,
) -> EvaluationResult:
    """並列実行のFuture結果を処理"""
    try:
        result = future.result(timeout=timeout_seconds)
        if result.success:
            logger.info(
                f"[{index}/{total}] {candidate.strategy_id}: "
                f"score={result.score:.4f}"
            )
        else:
            logger.warning(f"[{index}/{total}] {candidate.strategy_id}: FAILED")
        return result

    except TimeoutError:
        logger.warning(f"[{index}/{total}] {candidate.strategy_id}: TIMEOUT")
        return EvaluationResult(
            candidate=candidate,
            score=-999.0,
            success=False,
            error_message="Timeout",
        )

    except Exception as e:
        logger.warning(f"[{index}/{total}] {candidate.strategy_id}: ERROR - {e}")
        return EvaluationResult(
            candidate=candidate,
            score=-999.0,
            success=False,
            error_message=str(e),
        )
