"""
データ準備ヘルパー関数

バッチ評価用のデータシリアライズ・デシリアライズ処理
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from ruamel.yaml import YAML

from src.shared.constants import (
    DEFAULT_FEES,
    DEFAULT_INITIAL_CASH,
    DEFAULT_KELLY_FRACTION,
    DEFAULT_MAX_ALLOCATION,
    DEFAULT_MIN_ALLOCATION,
)


@dataclass
class BatchPreparedData:
    """バッチ評価用の事前取得データ"""

    stock_codes: list[str] | None
    ohlcv_data: dict[str, dict[str, Any]] | None
    benchmark_data: dict[str, Any] | None


def convert_dataframes_to_dict(
    data: dict[str, dict[str, pd.DataFrame]]
) -> dict[str, dict[str, Any]]:
    """
    DataFrameを辞書形式に変換（ProcessPoolExecutorシリアライズ用）

    Args:
        data: {銘柄コード: {"daily": DataFrame, ...}}

    Returns:
        {銘柄コード: {"daily": {"index": [...], "columns": [...], "data": [...]}, ...}}
    """
    result: dict[str, dict[str, Any]] = {}
    for stock_code, timeframe_dict in data.items():
        result[stock_code] = {}
        for tf_name, df in timeframe_dict.items():
            result[stock_code][tf_name] = {
                "index": df.index.astype(str).tolist(),
                "columns": df.columns.tolist(),
                "data": df.values.tolist(),
            }
    return result


def convert_dict_to_dataframes(
    data: dict[str, dict[str, Any]]
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    辞書形式をDataFrameに復元（デシリアライズ用）

    Args:
        data: シリアライズされたデータ

    Returns:
        {銘柄コード: {"daily": DataFrame, ...}}
    """
    result: dict[str, dict[str, pd.DataFrame]] = {}
    for stock_code, timeframe_dict in data.items():
        result[stock_code] = {}
        for tf_name, serialized in timeframe_dict.items():
            df = pd.DataFrame(
                data=serialized["data"],
                index=pd.to_datetime(serialized["index"]),
                columns=serialized["columns"],
            )
            result[stock_code][tf_name] = df
    return result


def _get_fallback_shared_config() -> dict[str, Any]:
    """フォールバック用のデフォルト shared_config"""
    return {
        "initial_cash": DEFAULT_INITIAL_CASH,
        "fees": DEFAULT_FEES,
        "dataset": "primeExTopix500",
        "kelly_fraction": DEFAULT_KELLY_FRACTION,
        "min_allocation": DEFAULT_MIN_ALLOCATION,
        "max_allocation": DEFAULT_MAX_ALLOCATION,
        "group_by": True,
        "cash_sharing": True,
        "direction": "longonly",
        "timeframe": "daily",
    }


def load_default_shared_config() -> dict[str, Any]:
    """config/default.yaml から shared_config を読み込む"""
    default_yaml_path = Path("config/default.yaml")
    if not default_yaml_path.exists():
        return _get_fallback_shared_config()

    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    with open(default_yaml_path, encoding="utf-8") as f:
        config = ruamel_yaml.load(f)

    return config.get("default", {}).get("parameters", {}).get("shared_config", {})


# 後方互換性のためのエイリアス（アンダースコア付き旧名）
_convert_dataframes_to_dict = convert_dataframes_to_dict
_convert_dict_to_dataframes = convert_dict_to_dataframes
_load_default_shared_config = load_default_shared_config
