"""
株価データローダー

データアクセスクライアント経由で株価データを読み込み、
VectorBTで使用できる形式に変換します。
"""

from typing import Literal, Optional

import pandas as pd
from loguru import logger

from src.infrastructure.data_access.clients import get_dataset_client
from src.infrastructure.data_access.loaders.cache import DataCache, cached_loader
from src.infrastructure.data_access.loaders.utils import extract_dataset_name

# Backward-compatible symbol for tests patching module-local DatasetAPIClient.
DatasetAPIClient = get_dataset_client


def get_stock_list(dataset: str, min_records: int = 100) -> list[str]:
    """利用可能な銘柄リストを取得.

    Args:
        dataset: データセット名
        min_records: 最小レコード数

    Returns:
        List[str]: 銘柄コードのリスト
    """
    dataset_name = extract_dataset_name(dataset)

    # キャッシュチェック（DataCache有効時のみ）
    cache = DataCache.get_instance()
    cache_key = f"stock_list:{dataset_name}:{min_records}"

    if cache.is_enabled():
        cached = cache.get(cache_key)
        if cached is not None:
            return cached["stockCode"].tolist()

    # API呼び出し
    with DatasetAPIClient(dataset_name) as client:
        df = client.get_stock_list(min_records=min_records)

    if df.empty:
        return []

    # キャッシュに保存（DataCache有効時のみ）
    if cache.is_enabled():
        cache.set(cache_key, df)

    return df["stockCode"].tolist()


@cached_loader("stock:{dataset}:{stock_code}:{start_date}:{end_date}:{timeframe}")
def load_stock_data(
    dataset: str,
    stock_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    timeframe: Literal["daily", "weekly", "monthly"] = "daily",
) -> pd.DataFrame:
    """API経由で株価データを読み込み（VectorBT用）.

    Args:
        dataset: データセット名
        stock_code: 銘柄コード
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        timeframe: データの時間軸 ("daily", "weekly", "monthly")

    Returns:
        pandas.DataFrame: VectorBT用のOHLCVデータ（DatetimeIndex）

    Raises:
        ValueError: データが見つからない場合
    """
    dataset_name = extract_dataset_name(dataset)

    # API呼び出し
    with DatasetAPIClient(dataset_name) as client:
        df = client.get_stock_ohlcv(stock_code, start_date, end_date, timeframe)

    if df.empty:
        raise ValueError(f"No data found for stock code: {stock_code}")

    # NaNを除去
    df = df.dropna()

    logger.debug(f"株価データ読み込み成功: {stock_code} (timeframe={timeframe})")
    return df


def get_available_stocks(dataset: str, min_records: int = 1000) -> pd.DataFrame:
    """利用可能な銘柄一覧を取得（VectorBT用）.

    Args:
        dataset: データセット名
        min_records: 最小レコード数

    Returns:
        pandas.DataFrame: 銘柄一覧（レコード数順）
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_available_stocks(min_records=min_records)

    return df
