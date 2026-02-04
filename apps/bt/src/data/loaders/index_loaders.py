"""
インデックス・ベンチマークデータローダー

localhost:3001 API経由でインデックス・ベンチマークデータを読み込み、VectorBTで使用できる形式に変換します。

TOPIXデータの二重ロードパス:
    1. load_topix_data() — dataset.db の topix テーブルからロード（バックテスト専用）
       長期間の過去データを使用するバックテストシミュレーション向け。
    2. load_topix_data_from_market_db() — market.db の topix_data テーブルからロード
       日次更新の直近データを使用する signal_screening のβ値計算 および cli_portfolio のPCA分析向け。
"""

from typing import Optional

import pandas as pd
from loguru import logger

from src.api.dataset_client import DatasetAPIClient
from src.api.market_client import MarketAPIClient
from src.data.loaders.cache import cached_loader
from src.data.loaders.utils import extract_dataset_name


@cached_loader("topix:{dataset}:{start_date}:{end_date}")
def load_topix_data(
    dataset: str, start_date: Optional[str] = None, end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    TOPIXデータをdataset.dbのtopixテーブルから読み込み（バックテスト専用）

    長期間の過去データを使用するバックテストシミュレーション向け。
    dataset.db は J-Quants API から取得した過去データを格納。

    Args:
        dataset: データベースファイルパス
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
    市場データベース（market.db）からTOPIXデータを読み込み

    日次更新の直近データを使用する以下の用途向け:
        - signal_screening: β値シグナル計算のベンチマークデータ
        - cli_portfolio: PCA分析のベンチマークデータ

    Args:
        _dataset: 未使用（後方互換性のため残存、API経由のため不要）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)

    Returns:
        pandas.DataFrame: TOPIXのOHLCデータ（DatetimeIndex、VectorBT標準形式）

    Raises:
        ValueError: TOPIXデータが見つからない場合

    Note:
        - market.dbは topix_data テーブルを使用（dataset/*.dbの topix とは異なる）
        - 直近1年間の市場データ（日次更新）
        - バックテスト用のload_topix_data()とはテーブル名・データソースが異なる
    """
    with MarketAPIClient() as client:
        df = client.get_topix(start_date, end_date)

    if df.empty:
        raise ValueError("No TOPIX data found in topix_data table")

    # NaNを除去
    df = df.dropna()

    logger.debug("TOPIXデータ読み込み成功（market.db）")
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
        dataset: データベースファイルパス
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
        dataset: データベースファイルパス
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
        dataset: データベースファイルパス
        min_records: 最小レコード数

    Returns:
        pandas.DataFrame: インデックス一覧（レコード数順）
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_index_list(min_records)

    return df
