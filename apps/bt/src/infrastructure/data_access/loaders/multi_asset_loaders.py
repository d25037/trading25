"""
マルチアセット・一括データローダー

複数の銘柄・インデックスを同時に読み込み、VectorBTで使用できる形式に変換します。
"""

from typing import List, Literal, Optional

import pandas as pd
from loguru import logger

from src.infrastructure.data_access.clients import get_dataset_client
from src.shared.exceptions import BatchAPIError, NoValidDataError

from src.infrastructure.external_api.dataset.statements_mixin import APIPeriodType
from src.shared.models.types import normalize_period_type

from .cache import DataCache
from .index_loaders import load_index_data
from .margin_loaders import load_margin_data, transform_margin_df
from .statements_loaders import (
    load_statements_data,
    merge_forward_forecast_revision,
    transform_statements_df,
)
from .stock_loaders import load_stock_data
from .utils import extract_dataset_name


def load_multiple_stocks(
    dataset: str,
    stock_codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    price_column: str = "Close",
    timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    use_batch_api: bool = True,
) -> pd.DataFrame:
    """
    複数銘柄のデータを同時に読み込み（VectorBT用）

    Args:
        dataset: データベースファイルパス
        stock_codes: 銘柄コードのリスト
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        price_column: 使用する価格カラム（'Close', 'Open', etc.）
        timeframe: データの時間軸 ("daily", "weekly", "monthly")
        use_batch_api: バッチAPIを使用するか（デフォルト: True）

    Returns:
        pandas.DataFrame: 銘柄をカラムとした価格データ（DatetimeIndex）
    """
    data_dict: dict[str, pd.Series[float]] = {}
    cache = DataCache.get_instance()

    # バッチAPIを試行（キャッシュ有効時は個別取得でキャッシュを利用）
    if use_batch_api and not cache.is_enabled():
        dataset_name = extract_dataset_name(dataset)
        try:
            with get_dataset_client(dataset_name) as client:
                batch_data = client.get_stocks_ohlcv_batch(
                    stock_codes, start_date, end_date, timeframe
                )
            for code, df in batch_data.items():
                if price_column in df.columns:
                    data_dict[code] = df[price_column]
            logger.debug(f"バッチAPI使用: {len(data_dict)}銘柄取得")
        except (ConnectionError, RuntimeError, BatchAPIError) as e:
            logger.debug(f"バッチAPI失敗、個別取得にフォールバック: {e}")
            use_batch_api = False

    # 個別取得（キャッシュ有効時 or バッチAPI失敗時）
    if not data_dict:
        for stock_code in stock_codes:
            try:
                df = load_stock_data(dataset, stock_code, start_date, end_date, timeframe)
                data_dict[stock_code] = df[price_column]
            except ValueError as e:
                logger.warning(f"データ読み込み警告: {e}")
                continue

    if not data_dict:
        raise NoValidDataError("No valid data found for any stock codes")

    # すべてのデータを結合
    result_df = pd.DataFrame(data_dict)

    # NaNを前方補間（VectorBTでは完全なデータが推奨）
    result_df = result_df.ffill().dropna()

    logger.debug(f"複数銘柄価格データ読み込み成功: {len(stock_codes)}銘柄 (timeframe={timeframe})")
    return result_df


def load_multiple_indices(
    dataset: str,
    index_codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    price_column: str = "Close",
) -> pd.DataFrame:
    """
    複数インデックスのデータを同時に読み込み（VectorBT用）

    Args:
        dataset: データベースファイルパス
        index_codes: インデックスコードのリスト
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        price_column: 使用する価格カラム（'Close', 'Open', etc.）

    Returns:
        pandas.DataFrame: インデックスをカラムとした価格データ（DatetimeIndex）
    """
    data_dict: dict[str, pd.Series[float]] = {}

    for index_code in index_codes:
        try:
            df = load_index_data(dataset, index_code, start_date, end_date)
            data_dict[index_code] = df[price_column]
        except ValueError as e:
            logger.warning(f"インデックスデータ読み込み警告: {e}")
            continue

    if not data_dict:
        raise ValueError("No valid index data found for any index codes")

    # すべてのデータを結合
    result_df = pd.DataFrame(data_dict)

    # NaNを前方補間（VectorBTでは完全なデータが推奨）
    result_df = result_df.ffill().dropna()

    logger.debug(
        f"複数インデックス価格データ読み込み成功: {len(index_codes)}インデックス"
    )
    return result_df


def load_multiple_margin_data(
    dataset: str,
    stock_codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    margin_column: str = "TotalMargin",
) -> pd.DataFrame:
    """
    複数銘柄の信用残高データを同時に読み込み（VectorBT用）

    バッチAPIを使用して効率的に複数銘柄のデータを取得します。
    バッチAPI失敗時は個別取得にフォールバックします。

    Args:
        dataset: データベースファイルパス
        stock_codes: 銘柄コードのリスト
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        margin_column: 使用する信用残高カラム（'LongMargin', 'ShortMargin', 'TotalMargin'）

    Returns:
        pandas.DataFrame: 銘柄をカラムとした信用残高データ（DatetimeIndex）
    """
    data_dict: dict[str, pd.Series[float]] = {}
    dataset_name = extract_dataset_name(dataset)

    # バッチAPIを試行
    try:
        with get_dataset_client(dataset_name) as client:
            batch_data = client.get_margin_batch(
                stock_codes, start_date, end_date
            )
        for code, df in batch_data.items():
            df = transform_margin_df(df)
            if margin_column in df.columns:
                data_dict[code] = df[margin_column]
        logger.debug(f"バッチAPI使用: {len(data_dict)}銘柄の信用残高データ取得")
    except (ConnectionError, RuntimeError, BatchAPIError) as e:
        logger.debug(f"信用残高バッチAPI失敗、個別取得にフォールバック: {e}")

    # バッチAPI失敗時は個別取得
    if not data_dict:
        for stock_code in stock_codes:
            try:
                df = load_margin_data(dataset, stock_code, start_date, end_date)
                data_dict[stock_code] = df[margin_column]
            except ValueError as e:
                logger.warning(f"信用残高データ読み込み警告: {e}")
                continue

    if not data_dict:
        raise ValueError("No valid margin data found for any stock codes")

    # すべてのデータを結合
    result_df = pd.DataFrame(data_dict)

    # NaNを前方補間（週次データなので0で埋める）
    result_df = result_df.fillna(0)

    logger.debug(f"複数銘柄信用残高データ読み込み成功: {len(stock_codes)}銘柄")
    return result_df


def load_multiple_statements_data(
    dataset: str,
    stock_codes: List[str],
    daily_index: pd.DatetimeIndex,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    statements_column: str = "eps",
    period_type: APIPeriodType = "FY",
    include_forecast_revision: bool = False,
) -> pd.DataFrame:
    """
    複数銘柄の財務諸表データを同時に読み込み（VectorBT用）

    バッチAPIを使用して効率的に複数銘柄のデータを取得します。
    バッチAPI失敗時は個別取得にフォールバックします。

    Args:
        dataset: データベースファイルパス
        stock_codes: 銘柄コードのリスト
        daily_index: 日次インデックス（株価データと同じにするため、必須）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        statements_column: 使用する財務指標カラム（'eps', 'profit', 'equity', 'roe'）
        period_type: 決算期間タイプ ('FY'=本決算のみ, 'all'=全四半期, '1Q'/'2Q'/'3Q'=特定四半期)
        include_forecast_revision: Trueの場合、period_type="FY"時に四半期修正を反映

    Returns:
        pandas.DataFrame: 銘柄をカラムとした財務諸表データ（DatetimeIndex）
    """
    data_dict: dict[str, pd.Series[float]] = {}
    dataset_name = extract_dataset_name(dataset)
    normalized_column = {
        "eps": "EPS",
        "profit": "Profit",
        "equity": "Equity",
        "roe": "ROE",
    }.get(statements_column, statements_column)

    should_merge_forecast_revision = (
        include_forecast_revision
        and normalize_period_type(period_type) == "FY"
        and normalized_column in {"ForwardForecastEPS", "AdjustedForwardForecastEPS"}
    )

    # バッチAPIを試行
    try:
        revision_batch: dict[str, pd.DataFrame] = {}
        with get_dataset_client(dataset_name) as client:
            batch_data = client.get_statements_batch(
                stock_codes, start_date, end_date,
                period_type=period_type, actual_only=True,
            )
            if should_merge_forecast_revision:
                try:
                    revision_batch = client.get_statements_batch(
                        stock_codes,
                        start_date,
                        end_date,
                        period_type="all",
                        actual_only=False,
                    )
                except (ConnectionError, RuntimeError, BatchAPIError) as e:
                    logger.warning(f"財務諸表四半期修正バッチ取得失敗、FYのみで続行: {e}")
        for code, df in batch_data.items():
            df = transform_statements_df(df)
            reindexed = df.reindex(daily_index, method="ffill")
            if (
                should_merge_forecast_revision
                and code in revision_batch
                and not revision_batch[code].empty
            ):
                revision_df = transform_statements_df(revision_batch[code])
                revision_reindexed = revision_df.reindex(daily_index, method="ffill")
                reindexed = merge_forward_forecast_revision(
                    reindexed,
                    revision_reindexed,
                )
            if normalized_column in reindexed.columns:
                data_dict[code] = reindexed[normalized_column]
        logger.debug(f"バッチAPI使用: {len(data_dict)}銘柄の財務諸表データ取得")
    except (ConnectionError, RuntimeError, BatchAPIError) as e:
        logger.debug(f"財務諸表バッチAPI失敗、個別取得にフォールバック: {e}")

    # バッチAPI失敗時は個別取得
    if not data_dict:
        for stock_code in stock_codes:
            try:
                df = load_statements_data(
                    dataset, stock_code, daily_index, start_date, end_date,
                    period_type=period_type,
                    include_forecast_revision=include_forecast_revision,
                )
                if normalized_column in df.columns:
                    data_dict[stock_code] = df[normalized_column]
                else:
                    logger.warning(
                        f"{normalized_column}カラムが見つかりません: {stock_code}"
                    )
                    continue
            except ValueError as e:
                logger.warning(f"財務諸表データ読み込み警告 {stock_code}: {e}")
                continue

    if not data_dict:
        raise ValueError(
            f"No valid statements data found for any stock codes with column: {normalized_column}"
        )

    # すべてのデータを結合
    result_df = pd.DataFrame(data_dict)

    # NaNを前方補間（年次データなので前の値を使用）
    result_df = result_df.ffill().fillna(0)

    logger.debug(f"複数銘柄財務諸表データ読み込み成功: {len(stock_codes)}銘柄")
    return result_df
