"""
データ準備・統合処理

VectorBT用のデータを準備する高レベル統合関数群
"""

from typing import Dict, List, Literal, Optional

import pandas as pd
from loguru import logger

from src.api.dataset.statements_mixin import APIPeriodType
from src.api.exceptions import APIError
from src.data.access.clients import get_dataset_client
from src.exceptions import (
    BatchAPIError,
    DataPreparationError,
    MarginDataLoadError,
    NoValidDataError,
    StatementsDataLoadError,
)
from src.models.types import normalize_period_type

from .margin_loaders import load_margin_data, transform_margin_df
from .multi_asset_loaders import (
    load_multiple_margin_data,
    load_multiple_statements_data,
    load_multiple_stocks,
)
from .statements_loaders import (
    load_statements_data,
    merge_forward_forecast_revision,
    transform_statements_df,
)
from .stock_loaders import get_available_stocks, load_stock_data
from .utils import extract_dataset_name

# Backward-compatible symbol for tests patching module-local DatasetAPIClient.
DatasetAPIClient = get_dataset_client


def prepare_all_stocks_data(
    dataset: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_margin_data: bool = False,
    include_statements_data: bool = False,
    min_records: int = 100,
    timeframe: Literal["daily", "weekly"] = "daily",
    period_type: APIPeriodType = "FY",
    include_forecast_revision: bool = False,
) -> Dict[str, pd.DataFrame]:
    """
    全銘柄のデータを準備する関数

    Args:
        dataset: データセット名
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        include_margin_data: 信用残高データを含めるか
        include_statements_data: 財務諸表データを含めるか
        min_records: 最小レコード数フィルタ
        timeframe: データの時間軸 ('daily'=日足, 'weekly'=週足)
        period_type: 決算期間タイプ ('FY'=本決算のみ, 'all'=全四半期, '1Q'/'2Q'/'3Q'=特定四半期)

    Returns:
        Dict[str, pd.DataFrame]: 各種データフィード（全銘柄統合）
            - 'daily': 全銘柄の日足データ（カラムが各銘柄）
            - 'margin_daily': 信用残高データ（include_margin_dataがTrueの場合）
            - 'statements_daily': 財務諸表データ（include_statements_dataがTrueの場合）
    """
    logger.info("全銘柄データ準備開始")

    # 利用可能な銘柄一覧を取得
    available_stocks = get_available_stocks(dataset, min_records)
    stock_codes = available_stocks["stockCode"].tolist()

    logger.info(f"対象銘柄数: {len(stock_codes)}銘柄")

    # 複数銘柄のデータを読み込み（Closeのみ、API側でtimeframe変換）
    try:
        combined_data = load_multiple_stocks(
            dataset, stock_codes, start_date, end_date, "Close", timeframe
        )

        logger.info(f"データ読み込み完了: {len(combined_data)}レコード (timeframe={timeframe})")

        # 結果構造を作成
        result = {"daily": combined_data}

        # 信用残高データの追加（オプション）
        if include_margin_data:
            try:
                margin_data = load_multiple_margin_data(
                    dataset, stock_codes, start_date, end_date, "LongMargin"
                )
                result["margin_daily"] = margin_data
                logger.info(f"全銘柄信用残高データ追加完了: {len(margin_data)}レコード")
            except (ValueError, MarginDataLoadError, APIError) as e:
                logger.warning(f"全銘柄信用残高データ読み込みエラー: {e}")
                logger.info("信用残高データなしで続行")

        # 財務諸表データの追加（オプション）
        if include_statements_data:
            try:
                # 株価データから日次インデックスを取得
                daily_index = pd.DatetimeIndex(combined_data.index)
                statements_data = load_multiple_statements_data(
                    dataset, stock_codes, daily_index, start_date, end_date, "eps",
                    period_type=period_type,
                    include_forecast_revision=include_forecast_revision,
                )
                result["statements_daily"] = statements_data
                logger.info(
                    f"全銘柄財務諸表データ追加完了: {len(statements_data)}レコード"
                )
            except (ValueError, StatementsDataLoadError, APIError) as e:
                logger.warning(f"全銘柄財務諸表データ読み込みエラー: {e}")
                logger.info("財務諸表データなしで続行")

        logger.info("全銘柄データ準備完了")
        return result

    except (ValueError, KeyError, RuntimeError) as e:
        logger.error(f"全銘柄データ準備エラー: {e}")
        raise DataPreparationError(f"Failed to prepare data for all stocks: {e}") from e


def prepare_data(
    dataset: str,
    stock_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_margin_data: bool = False,
    include_statements_data: bool = False,
    timeframe: Literal["daily", "weekly"] = "daily",
    period_type: APIPeriodType = "FY",
    include_forecast_revision: bool = False,
) -> Dict[str, pd.DataFrame]:
    """
    VectorBT用のデータを準備する統合関数

    Args:
        dataset: データベースファイルパス
        stock_code: 銘柄コード（'all'の場合は全銘柄対象）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        include_margin_data: 信用残高データを含めるか
        include_statements_data: 財務諸表データを含めるか
        timeframe: データの時間軸 ('daily'=日足, 'weekly'=週足)
        period_type: 決算期間タイプ ('FY'=本決算のみ, 'all'=全四半期, '1Q'/'2Q'/'3Q'=特定四半期)

    Returns:
        Dict[str, pd.DataFrame]: 各種データフィード
            - 'daily': 日足データ
            - 'margin_daily': 信用残高データ（include_margin_dataがTrueの場合）
            - 'statements_daily': 財務諸表データ（include_statements_dataがTrueの場合）
    """
    # 'all'の場合は全銘柄のデータを準備
    if stock_code.lower() == "all":
        return prepare_all_stocks_data(
            dataset,
            start_date,
            end_date,
            include_margin_data,
            include_statements_data,
            timeframe=timeframe,
            period_type=period_type,
            include_forecast_revision=include_forecast_revision,
        )

    # 基本データの読み込み（API側でtimeframe変換を実行）
    stock_data = load_stock_data(dataset, stock_code, start_date, end_date, timeframe)
    daily_index = pd.DatetimeIndex(stock_data.index)

    # データの作成
    result = {"daily": stock_data}

    # 信用残高データ（オプション）
    if include_margin_data:
        try:
            margin_data = load_margin_data(
                dataset=dataset,
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                daily_index=daily_index,
            )
            result["margin_daily"] = margin_data
            logger.info(f"信用残高データ追加完了: {len(margin_data)}レコード")
        except (ValueError, KeyError, MarginDataLoadError, APIError) as e:
            logger.warning(f"信用残高データ読み込みエラー: {e}")
            logger.info("信用残高データなしで続行")

    # 財務諸表データ（オプション）
    if include_statements_data:
        try:
            statements_data = load_statements_data(
                dataset=dataset,
                stock_code=stock_code,
                daily_index=daily_index,
                start_date=start_date,
                end_date=end_date,
                period_type=period_type,
                include_forecast_revision=include_forecast_revision,
            )
            result["statements_daily"] = statements_data
            logger.info(f"財務諸表データ追加完了: {len(statements_data)}レコード")
        except (ValueError, KeyError, StatementsDataLoadError, APIError) as e:
            logger.warning(f"財務諸表データ読み込みエラー: {e}")
            logger.info("財務諸表データなしで続行")

    return result


def prepare_multi_data(
    dataset: str,
    stock_codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_margin_data: bool = False,
    include_statements_data: bool = False,
    timeframe: Literal["daily", "weekly"] = "daily",
    period_type: APIPeriodType = "FY",
    include_forecast_revision: bool = False,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    複数銘柄のデータを一括でバッチ取得する関数

    Args:
        dataset: データベースファイルパス
        stock_codes: 銘柄コードのリスト
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        include_margin_data: 信用残高データを含めるか
        include_statements_data: 財務諸表データを含めるか
        timeframe: データの時間軸 ('daily'=日足, 'weekly'=週足)
        period_type: 決算期間タイプ ('FY'=本決算のみ, 'all'=全四半期, '1Q'/'2Q'/'3Q'=特定四半期)

    Returns:
        Dict[str, Dict[str, pd.DataFrame]]: {銘柄コード: {"daily": DataFrame, ...}}

    Note:
        バッチAPIを使用して複数銘柄を一括取得します。
        これにより、50銘柄の場合 50 API calls → 1 API call に削減できます。
    """
    if not stock_codes:
        return {}

    logger.info(f"複数銘柄データ一括準備開始: {len(stock_codes)}銘柄")

    dataset_name = extract_dataset_name(dataset)
    result: Dict[str, Dict[str, pd.DataFrame]] = {}

    # バッチAPIで株価データを一括取得
    ohlcv_batch: Dict[str, pd.DataFrame] = {}

    try:
        with DatasetAPIClient(dataset_name) as client:
            ohlcv_batch = client.get_stocks_ohlcv_batch(
                stock_codes, start_date, end_date, timeframe
            )
        logger.info(f"バッチAPI使用: {len(ohlcv_batch)}銘柄のOHLCVデータ取得")
    except (ConnectionError, RuntimeError, BatchAPIError) as e:
        logger.warning(f"バッチAPI失敗、個別取得にフォールバック: {e}")
        # フォールバック: 個別取得
        for stock_code in stock_codes:
            try:
                df = load_stock_data(dataset, stock_code, start_date, end_date, timeframe)
                ohlcv_batch[stock_code] = df
            except (ValueError, KeyError) as load_err:
                logger.warning(f"データ読み込み警告 {stock_code}: {load_err}")

    if not ohlcv_batch:
        raise NoValidDataError("No valid data found for any stock codes")

    # 各銘柄のデータ構造を作成
    for stock_code, stock_data in ohlcv_batch.items():
        result[stock_code] = {"daily": stock_data}

    # 信用残高データ（オプション・バッチAPI使用）
    if include_margin_data:
        logger.debug("信用残高データ読み込み開始（バッチAPI）")
        margin_codes = list(result.keys())
        try:
            with DatasetAPIClient(dataset_name) as client:
                margin_batch = client.get_margin_batch(
                    margin_codes, start_date, end_date
                )
            for stock_code, margin_df in margin_batch.items():
                if stock_code in result and not margin_df.empty:
                    margin_df = transform_margin_df(margin_df)
                    stock_data = result[stock_code]["daily"]
                    daily_index = pd.DatetimeIndex(stock_data.index)
                    margin_reindexed = margin_df.reindex(daily_index, method="ffill")
                    result[stock_code]["margin_daily"] = margin_reindexed
            logger.debug(f"バッチAPI使用: {len(margin_batch)}銘柄の信用残高データ取得")
        except (ConnectionError, RuntimeError, BatchAPIError, APIError) as e:
            logger.warning(f"信用残高バッチAPI失敗、個別取得にフォールバック: {e}")
            for stock_code in margin_codes:
                try:
                    stock_data = result[stock_code]["daily"]
                    margin_data = load_margin_data(
                        dataset=dataset,
                        stock_code=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        daily_index=pd.DatetimeIndex(stock_data.index),
                    )
                    result[stock_code]["margin_daily"] = margin_data
                except (ValueError, KeyError, MarginDataLoadError, APIError) as e2:
                    logger.debug(f"信用残高データ読み込み警告 {stock_code}: {e2}")

    # 財務諸表データ（オプション・バッチAPI使用）
    if include_statements_data:
        logger.debug("財務諸表データ読み込み開始（バッチAPI）")
        statements_codes = list(result.keys())
        should_merge_forecast_revision = (
            include_forecast_revision and normalize_period_type(period_type) == "FY"
        )
        try:
            revision_batch: dict[str, pd.DataFrame] = {}
            with DatasetAPIClient(dataset_name) as client:
                statements_batch = client.get_statements_batch(
                    statements_codes, start_date, end_date,
                    period_type=period_type, actual_only=True,
                )
                if should_merge_forecast_revision:
                    try:
                        revision_batch = client.get_statements_batch(
                            statements_codes,
                            start_date,
                            end_date,
                            period_type="all",
                            actual_only=True,
                        )
                    except (ConnectionError, RuntimeError, BatchAPIError, APIError) as e:
                        logger.warning(
                            f"財務諸表四半期修正バッチ取得失敗、FYのみで続行: {e}"
                        )
            for stock_code, stmt_df in statements_batch.items():
                if stock_code in result and not stmt_df.empty:
                    stmt_df = transform_statements_df(stmt_df)
                    stock_data = result[stock_code]["daily"]
                    daily_index = pd.DatetimeIndex(stock_data.index)
                    stmt_reindexed = stmt_df.reindex(daily_index, method="ffill")
                    if (
                        should_merge_forecast_revision
                        and stock_code in revision_batch
                        and not revision_batch[stock_code].empty
                    ):
                        revision_df = transform_statements_df(revision_batch[stock_code])
                        revision_reindexed = revision_df.reindex(
                            daily_index, method="ffill"
                        )
                        stmt_reindexed = merge_forward_forecast_revision(
                            stmt_reindexed, revision_reindexed
                        )
                    result[stock_code]["statements_daily"] = stmt_reindexed
            logger.debug(f"バッチAPI使用: {len(statements_batch)}銘柄の財務諸表データ取得")
        except (ConnectionError, RuntimeError, BatchAPIError, APIError) as e:
            logger.warning(f"財務諸表バッチAPI失敗、個別取得にフォールバック: {e}")
            for stock_code in statements_codes:
                try:
                    stock_data = result[stock_code]["daily"]
                    statements_data = load_statements_data(
                        dataset=dataset,
                        stock_code=stock_code,
                        daily_index=pd.DatetimeIndex(stock_data.index),
                        start_date=start_date,
                        end_date=end_date,
                        period_type=period_type,
                        include_forecast_revision=should_merge_forecast_revision,
                    )
                    result[stock_code]["statements_daily"] = statements_data
                except (ValueError, KeyError, StatementsDataLoadError, APIError) as e2:
                    logger.debug(f"財務諸表データ読み込み警告 {stock_code}: {e2}")

    logger.info(f"複数銘柄データ一括準備完了: {len(result)}銘柄")
    return result
