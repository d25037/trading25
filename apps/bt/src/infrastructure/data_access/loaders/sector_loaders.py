"""
セクター・業種別データローダー

データアクセスクライアント経由でセクター・業種別データを読み込み、
VectorBTで使用できる形式に変換します。
"""

import pandas as pd
from loguru import logger

from src.infrastructure.data_access.clients import get_dataset_client
from src.shared.exceptions import IndexDataLoadError, SectorDataLoadError

from .index_loaders import load_index_data
from .utils import extract_dataset_name

# Backward-compatible symbol for tests patching module-local DatasetAPIClient.
DatasetAPIClient = get_dataset_client

# セクターデータキャッシュ（同一セッション内での重複APIコール回避）
_sector_indices_cache: dict[str, dict[str, pd.DataFrame]] = {}
_stock_sector_mapping_cache: dict[str, dict[str, str]] = {}


def get_sector_mapping(dataset: str) -> pd.DataFrame:
    """
    セクター対応関係を取得

    Args:
        dataset: データベースファイルパス

    Returns:
        pandas.DataFrame: セクター対応関係
            - sector_code: 銘柄テーブルの33業種コード
            - sector_name: 33業種名
            - index_code: インデックステーブルのインデックスコード
            - index_name: インデックス名
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_sector_mapping()

    return df


def prepare_sector_data(
    dataset: str,
    sector_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    セクター別データを準備する関数

    Args:
        dataset: データベースファイルパス
        sector_name: セクター名（例: "化学", "医薬品"）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)

    Returns:
        Dict[str, pd.DataFrame]: セクターデータ
            - 'sector_index': セクターインデックスデータ
            - 'sector_mapping': セクター対応関係
    """
    logger.info(f"セクター別データ準備開始: {sector_name}")

    # セクター対応関係を取得
    mapping_df = get_sector_mapping(dataset)
    sector_mapping = mapping_df[mapping_df["sector_name"] == sector_name]

    if sector_mapping.empty:
        raise SectorDataLoadError(f"セクター名が見つかりません: {sector_name}")

    # インデックスコードを取得
    index_code = sector_mapping["index_code"].iloc[0]

    # セクターインデックスデータを読み込み
    try:
        sector_index_data = load_index_data(dataset, index_code, start_date, end_date)
        logger.info(
            f"セクターインデックスデータ読み込み完了: {len(sector_index_data)}レコード"
        )
    except (ValueError, KeyError, IndexDataLoadError) as e:
        logger.error(f"セクターインデックスデータ読み込みエラー: {e}")
        raise SectorDataLoadError(
            f"Failed to load sector index data for {sector_name}: {e}"
        ) from e

    # 結果構造を作成
    result = {"sector_index": sector_index_data, "sector_mapping": sector_mapping}

    logger.info(f"セクター別データ準備完了: {sector_name}")
    return result


def load_all_sector_indices(
    dataset: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    全33セクターインデックスのOHLCデータを一括取得

    Args:
        dataset: データベースファイルパス
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)

    Returns:
        Dict[str, pd.DataFrame]: セクター名をキーとしたOHLCデータ辞書
            例: {"電気機器": pd.DataFrame(OHLC), "化学": pd.DataFrame(OHLC), ...}
    """
    cache_key = f"{extract_dataset_name(dataset)}_{start_date}_{end_date}"
    if cache_key in _sector_indices_cache:
        logger.debug("セクターインデックスキャッシュヒット")
        return _sector_indices_cache[cache_key]

    logger.info("全セクターインデックスデータ一括ロード開始")

    # セクター対応関係を取得
    mapping_df = get_sector_mapping(dataset)
    if mapping_df.empty:
        logger.warning("セクターマッピングが空です")
        return {}

    sector_data: dict[str, pd.DataFrame] = {}
    for _, row in mapping_df.iterrows():
        sector_name = row["sector_name"]
        index_code = row["index_code"]
        try:
            index_data = load_index_data(dataset, index_code, start_date, end_date)
            if not index_data.empty:
                sector_data[sector_name] = index_data
                logger.debug(f"セクター '{sector_name}' ロード完了: {len(index_data)}レコード")
            else:
                logger.warning(f"セクター '{sector_name}' データが空です")
        except Exception as e:
            logger.warning(f"セクター '{sector_name}' ロード失敗: {e}")
            continue

    missing_count = len(mapping_df) - len(sector_data)
    msg = f"全セクターインデックスロード完了: {len(sector_data)}/{len(mapping_df)}セクター"
    if missing_count > 0:
        msg += f" ({missing_count}セクター欠損)"
        logger.warning(msg)
    else:
        logger.info(msg)

    # キャッシュ保存
    _sector_indices_cache[cache_key] = sector_data
    return sector_data


def get_stock_sector_mapping(dataset: str) -> dict[str, str]:
    """
    全銘柄のセクター名マッピングを取得

    Args:
        dataset: データベースファイルパス

    Returns:
        dict[str, str]: {stock_code: sector_name} マッピング
    """
    dataset_name = extract_dataset_name(dataset)

    if dataset_name in _stock_sector_mapping_cache:
        logger.debug("銘柄→セクターマッピングキャッシュヒット")
        return _stock_sector_mapping_cache[dataset_name]

    logger.info("銘柄→セクターマッピング取得開始")

    try:
        with DatasetAPIClient(dataset_name) as client:
            mapping = client.get_stock_sector_mapping()

        logger.info(f"銘柄→セクターマッピング取得完了: {len(mapping)}銘柄")

        _stock_sector_mapping_cache[dataset_name] = mapping
        return mapping

    except Exception as e:
        logger.warning(f"銘柄→セクターマッピング取得失敗: {e}")
        return {}
