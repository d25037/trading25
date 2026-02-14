"""
セクター関連シグナル機能

33業種分類に基づくセクター分析・シグナル機能を提供
"""

from typing import Optional

import pandas as pd
from loguru import logger

from src.data import get_sector_mapping, load_index_data
from src.data.access.clients import get_dataset_client
from src.data.loaders.utils import extract_dataset_name

# Backward-compatible symbol for tests patching module-local DatasetAPIClient.
DatasetAPIClient = get_dataset_client


def get_sector_index_code(dataset: str, sector_name: str) -> str:
    """
    セクター名からインデックスコードを取得

    Args:
        dataset: データベースファイルパス
        sector_name: セクター名（例: "化学", "医薬品"）

    Returns:
        str: インデックスコード

    Raises:
        ValueError: セクター名が見つからない場合
    """
    mapping_df = get_sector_mapping(dataset)
    sector_data = mapping_df[mapping_df["sector_name"] == sector_name]

    if sector_data.empty:
        raise ValueError(f"セクター名が見つかりません: {sector_name}")

    return sector_data["index_code"].iloc[0]


def get_sector_stocks(dataset: str, sector_name: str) -> list[str]:
    """
    セクターに属する銘柄コードのリストを取得

    Args:
        dataset: データベースファイルパス
        sector_name: セクター名（例: "化学", "医薬品"）

    Returns:
        list[str]: 銘柄コードのリスト

    Raises:
        ValueError: セクター名が見つからない場合
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        stocks = client.get_sector_stocks(sector_name)

    if not stocks:
        raise ValueError(f"セクター '{sector_name}' に属する銘柄が見つかりません")

    return stocks


def get_all_sectors(dataset: str) -> pd.DataFrame:
    """
    全セクターの一覧を取得

    Args:
        dataset: データベースファイルパス

    Returns:
        pandas.DataFrame: セクター一覧
            - sector_code: セクターコード
            - sector_name: セクター名
            - index_code: インデックスコード
            - stock_count: 銘柄数
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_all_sectors()

    return df


def create_sector_signal_by_index_performance(
    dataset: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    top_n_sectors: int = 10,
    performance_period_days: int = 20,
) -> dict[str, list[str]]:
    """
    インデックス パフォーマンスに基づくセクターシグナル作成

    Args:
        dataset: データベースファイルパス
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        top_n_sectors: 上位N セクター
        performance_period_days: パフォーマンス計算期間（日数）

    Returns:
        dict[str, list[str]]: セクター別銘柄リスト
            - key: セクター名
            - value: 銘柄コードのリスト
    """
    logger.info(
        f"セクターパフォーマンスシグナル作成開始（上位{top_n_sectors}セクター）"
    )

    # 全セクター情報を取得
    sectors_df = get_all_sectors(dataset)
    sector_performance = {}

    for _, row in sectors_df.iterrows():
        sector_name = row["sector_name"]
        index_code = row["index_code"]

        try:
            # インデックスデータを読み込み
            index_data = load_index_data(dataset, index_code, start_date, end_date)

            if len(index_data) < performance_period_days:
                logger.warning(
                    f"セクター '{sector_name}' のデータが不足: {len(index_data)}日"
                )
                continue

            # パフォーマンス計算（期間リターン）
            period_return = (
                index_data["Close"].iloc[-1]
                / index_data["Close"].iloc[-performance_period_days]
                - 1
            ) * 100

            sector_performance[sector_name] = period_return

        except Exception as e:
            logger.warning(f"セクター '{sector_name}' のパフォーマンス計算エラー: {e}")
            continue

    # 上位セクターを選択
    sorted_sectors = sorted(
        sector_performance.items(), key=lambda x: x[1], reverse=True
    )[:top_n_sectors]

    # セクター別銘柄リストを作成
    result = {}
    for sector_name, performance in sorted_sectors:
        try:
            stocks = get_sector_stocks(dataset, sector_name)
            result[sector_name] = stocks
            logger.info(
                f"セクター '{sector_name}': {performance:.2f}% ({len(stocks)}銘柄)"
            )
        except Exception as e:
            logger.warning(f"セクター '{sector_name}' の銘柄取得エラー: {e}")
            continue

    logger.info(f"セクターパフォーマンスシグナル完了: {len(result)}セクター")
    return result


def validate_sector_name(dataset: str, sector_name: str) -> bool:
    """
    セクター名の妥当性を検証

    Args:
        dataset: データベースファイルパス
        sector_name: セクター名

    Returns:
        bool: 妥当性（True=有効, False=無効）
    """
    try:
        mapping_df = get_sector_mapping(dataset)
        return sector_name in mapping_df["sector_name"].values
    except Exception:
        return False


def get_sector_correlation_matrix(
    dataset: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    price_column: str = "Close",
) -> pd.DataFrame:
    """
    セクター間の相関関係マトリックスを計算

    Args:
        dataset: データベースファイルパス
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        price_column: 価格カラム

    Returns:
        pandas.DataFrame: 相関関係マトリックス
    """
    logger.info("セクター相関関係マトリックス計算開始")

    # 全セクター情報を取得
    sectors_df = get_all_sectors(dataset)
    sector_data = {}

    for _, row in sectors_df.iterrows():
        sector_name = row["sector_name"]
        index_code = row["index_code"]

        try:
            index_data = load_index_data(dataset, index_code, start_date, end_date)
            sector_data[sector_name] = index_data[price_column]
        except Exception as e:
            logger.warning(f"セクター '{sector_name}' データ読み込みエラー: {e}")
            continue

    if not sector_data:
        raise ValueError("セクターデータが取得できませんでした")

    # データフレーム作成と相関計算
    combined_df = pd.DataFrame(sector_data)
    correlation_matrix = combined_df.corr()

    logger.info(
        f"セクター相関関係マトリックス計算完了: {len(correlation_matrix)}x{len(correlation_matrix)}"
    )
    return correlation_matrix
