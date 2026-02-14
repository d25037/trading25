"""
信用残高データローダー

データアクセスクライアント経由で信用残高データを読み込み、
VectorBTで使用できる形式に変換します。
"""

from typing import Optional

import pandas as pd
from loguru import logger

from src.data.access.clients import get_dataset_client

# Backward-compatible symbol for tests patching module-local DatasetAPIClient.
DatasetAPIClient = get_dataset_client
from src.data.loaders.cache import DataCache
from src.data.loaders.utils import extract_dataset_name


def transform_margin_df(df: pd.DataFrame) -> pd.DataFrame:
    """Batch/個別共用: APIレスポンスDataFrameをVectorBT形式に変換

    カラム名のリネーム、MarginRatio/TotalMargin計算、NaN処理を行う。
    """
    df = df.rename(
        columns={"longMarginVolume": "LongMargin", "shortMarginVolume": "ShortMargin"}
    )
    # NaN処理を先に実行してから派生指標を計算
    df["LongMargin"] = df["LongMargin"].fillna(0)
    df["ShortMargin"] = df["ShortMargin"].fillna(0)
    total = df["LongMargin"] + df["ShortMargin"]
    df["MarginRatio"] = df["ShortMargin"].div(total).fillna(0)
    df["TotalMargin"] = total
    return df


def load_margin_data(
    dataset: str,
    stock_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    daily_index: Optional[pd.DatetimeIndex] = None,
) -> pd.DataFrame:
    """
    API経由で信用残高データを読み込み（VectorBT用）

    Args:
        dataset: データベースファイルパス
        stock_code: 銘柄コード
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        daily_index: 日次変換時に使用するインデックス

    Returns:
        pandas.DataFrame: VectorBT用の信用残高データ（DatetimeIndex）

    Raises:
        ValueError: データが見つからない場合
    """
    dataset_name = extract_dataset_name(dataset)

    # キャッシュチェック（daily_indexなしの場合のみキャッシュ利用）
    cache = DataCache.get_instance()
    cache_key = f"margin:{dataset_name}:{stock_code}:{start_date}:{end_date}"

    if cache.is_enabled() and daily_index is None:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    # API呼び出し
    with DatasetAPIClient(dataset_name) as client:
        df = client.get_margin(stock_code, start_date, end_date)

    if df.empty:
        raise ValueError(f"No margin data found for stock code: {stock_code}")

    # カラム名統一・派生指標計算・NaN処理
    df = transform_margin_df(df)

    # キャッシュに保存（daily_indexなしの場合のみ）
    if cache.is_enabled() and daily_index is None:
        cache.set(cache_key, df.copy())

    # 日次インデックスに合わせて補完（オプション）
    if daily_index is not None:
        # 週次データを日次インデックスに合わせてffillで補完
        df = df.reindex(daily_index).ffill()

        # NaN値が残っている場合は0で埋める
        df = df.fillna(0)

    logger.debug(f"信用残高データ読み込み成功: {stock_code}")
    return df


def get_margin_available_stocks(dataset: str, min_records: int = 10) -> pd.DataFrame:
    """
    信用残高データがある銘柄一覧を取得（VectorBT用）

    Args:
        dataset: データベースファイルパス
        min_records: 最小レコード数

    Returns:
        pandas.DataFrame: 信用残高データ保有銘柄一覧（レコード数順）
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_margin_list(min_records)

    return df
