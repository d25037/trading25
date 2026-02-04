"""
戦略評価パッケージ

後方互換性のため、全クラス・関数を再エクスポート
"""

from .batch_executor import (
    execute_batch_evaluation,
    execute_parallel,
    execute_single_process,
    fetch_benchmark_data,
    fetch_ohlcv_data,
    fetch_stock_codes,
    get_max_workers,
    handle_future_result,
    prepare_batch_data,
)
from .candidate_processor import evaluate_single_candidate
from .data_preparation import (
    BatchPreparedData,
    convert_dataframes_to_dict,
    convert_dict_to_dataframes,
    load_default_shared_config,
)
from .evaluator import StrategyEvaluator
from .score_normalizer import normalize_scores, normalize_value

# 後方互換性のためのエイリアス（アンダースコア付き旧名）
_convert_dataframes_to_dict = convert_dataframes_to_dict
_convert_dict_to_dataframes = convert_dict_to_dataframes
_load_default_shared_config = load_default_shared_config
_evaluate_single_candidate = evaluate_single_candidate

__all__ = [
    # メインクラス
    "StrategyEvaluator",
    # データ準備
    "BatchPreparedData",
    "convert_dataframes_to_dict",
    "convert_dict_to_dataframes",
    "load_default_shared_config",
    # 候補評価
    "evaluate_single_candidate",
    # バッチ実行
    "execute_batch_evaluation",
    "execute_parallel",
    "execute_single_process",
    "fetch_benchmark_data",
    "fetch_ohlcv_data",
    "fetch_stock_codes",
    "get_max_workers",
    "handle_future_result",
    "prepare_batch_data",
    # スコア正規化
    "normalize_scores",
    "normalize_value",
    # 後方互換性エイリアス
    "_convert_dataframes_to_dict",
    "_convert_dict_to_dataframes",
    "_load_default_shared_config",
    "_evaluate_single_candidate",
]
