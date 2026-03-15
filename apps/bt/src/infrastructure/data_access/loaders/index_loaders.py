"""
インデックス・ベンチマークデータローダー

データアクセスクライアント経由でインデックス・ベンチマークデータを読み込み、
VectorBTで使用できる形式に変換します。

TOPIXデータの二重ロードパス:
    1. load_topix_data() — resolved dataset snapshot の TOPIX データをロード（バックテスト専用）
       長期間の過去データを使用するバックテストシミュレーション向け。
    2. load_topix_data_from_market_db() — DuckDB の topix_data テーブルからロード
       日次更新の直近データを使用する ScreeningService の市場分析・
       portfolio factor regression API向け。
"""

from typing import Optional

import pandas as pd
from loguru import logger

from src.infrastructure.data_access.clients import get_dataset_client, get_market_client
from src.infrastructure.data_access.loaders.cache import cached_loader
from src.infrastructure.data_access.loaders.utils import extract_dataset_name

# Backward-compatible symbols for tests patching module-local client constructors.
DatasetAPIClient = get_dataset_client
MarketAPIClient = get_market_client


@cached_loader("topix:{dataset}:{start_date}:{end_date}")
def load_topix_data(
    dataset: str, start_date: Optional[str] = None, end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    TOPIXデータを resolved dataset snapshot から読み込み（バックテスト専用）

    長期間の過去データを使用するバックテストシミュレーション向け。
    現行 runtime では `dataset.duckdb + parquet + manifest.v2.json` を解決して利用する。

    Args:
        dataset: データセット名または legacy 互換パス表現
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)

    Returns:
        pandas.DataFrame: TOPIXのOHLCVデータ

    Raises:
        ValueError: TOPIXデータが見つからない場合
    """
    dataset_name = extract_dataset_name(dataset)

    # API呼び出し
    with DatasetAPIClient(dataset_name) as client:
        df = client.get_topix(start_date, end_date)

    if df.empty:
        raise ValueError("No TOPIX data found in topix table")

    # NaNを除去
    df = df.dropna()

    logger.debug("TOPIXデータ読み込み成功")
    return df


def load_topix_data_from_market_db(
    _dataset: str = "", start_date: Optional[str] = None, end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    市場データベース（DuckDB）からTOPIXデータを読み込み

    日次更新の直近データを使用する以下の用途向け:
        - ScreeningService: スクリーニング/分析のベンチマークデータ
        - analytics API: ポートフォリオ分析のベンチマークデータ

    Args:
        _dataset: 未使用（後方互換性のため残存、API経由のため不要）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)

    Returns:
        pandas.DataFrame: TOPIXのOHLCデータ（DatetimeIndex、VectorBT標準形式）

    Raises:
        ValueError: TOPIXデータが見つからない場合

    Note:
        - market DuckDB は topix_data テーブルを使用
        - 直近1年間の市場データ（日次更新）
        - バックテスト用の load_topix_data() とは data plane が異なる
    """
    with MarketAPIClient() as client:
        df = client.get_topix(start_date, end_date)

    if df.empty:
        raise ValueError("No TOPIX data found in topix_data table")

    # NaNを除去
    df = df.dropna()

    logger.debug("TOPIXデータ読み込み成功（DuckDB）")
    return df


@cached_loader("index:{dataset}:{index_code}:{start_date}:{end_date}")
def load_index_data(
    dataset: str,
    index_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    API経由でインデックスデータを読み込み（VectorBT用）

    Args:
        dataset: データセット名または legacy 互換パス表現
        index_code: インデックスコード
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)

    Returns:
        pandas.DataFrame: VectorBT用のOHLCデータ（DatetimeIndex）

    Raises:
        ValueError: データが見つからない場合
    """
    dataset_name = extract_dataset_name(dataset)

    # API呼び出し
    with DatasetAPIClient(dataset_name) as client:
        df = client.get_index(index_code, start_date, end_date)

    if df.empty:
        raise ValueError(f"No index data found for index code: {index_code}")

    # NaNを除去
    df = df.dropna()

    logger.debug(f"インデックスデータ読み込み成功: {index_code}")
    return df


def get_index_list(dataset: str, min_records: int = 100) -> list[str]:
    """
    データベースから利用可能なインデックスコードリストを取得

    Args:
        dataset: データセット名または legacy 互換パス表現
        min_records: 最小レコード数

    Returns:
        List[str]: インデックスコードのリスト
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_index_list(min_records)

    if df.empty:
        return []

    return df["indexCode"].tolist()


def get_available_indices(dataset: str, min_records: int = 100) -> pd.DataFrame:
    """
    利用可能なインデックス一覧を取得（VectorBT用）

    Args:
        dataset: データセット名または legacy 互換パス表現
        min_records: 最小レコード数

    Returns:
        pandas.DataFrame: インデックス一覧（レコード数順）
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_index_list(min_records)

    return df
