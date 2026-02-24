"""
財務諸表データローダー

データアクセスクライアント経由で財務諸表データを読み込み、
VectorBTで使用できる形式に変換します。
"""

from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from src.infrastructure.external_api.dataset.statements_mixin import APIPeriodType
from src.infrastructure.data_access.clients import get_dataset_client
from src.infrastructure.data_access.loaders.utils import extract_dataset_name
from src.shared.models.types import normalize_period_type

# Backward-compatible symbol for tests patching module-local DatasetAPIClient.
DatasetAPIClient = get_dataset_client

# カラム名マッピング（API -> VectorBT PascalCase）
_COLUMN_MAPPING = {
    "earningsPerShare": "EPS",
    "profit": "Profit",
    "equity": "Equity",
    "typeOfCurrentPeriod": "TypeOfCurrentPeriod",
    "nextYearForecastEarningsPerShare": "NextYearForecastEPS",
    "bps": "BPS",
    "sales": "Sales",
    "operatingProfit": "OperatingProfit",
    "ordinaryProfit": "OrdinaryProfit",
    "operatingCashFlow": "OperatingCashFlow",
    "investingCashFlow": "InvestingCashFlow",
    "dividendFY": "DividendFY",
    "dividendFy": "DividendFY",
    "forecastDividendFY": "ForecastDividendFY",
    "forecastDividendFy": "ForecastDividendFY",
    "nextYearForecastDividendFY": "NextYearForecastDividendFY",
    "nextYearForecastDividendFy": "NextYearForecastDividendFY",
    "payoutRatio": "PayoutRatio",
    "forecastPayoutRatio": "ForecastPayoutRatio",
    "nextYearForecastPayoutRatio": "NextYearForecastPayoutRatio",
    "forecastEps": "ForecastEPS",
    "totalAssets": "TotalAssets",
    "sharesOutstanding": "SharesOutstanding",
    "treasuryShares": "TreasuryShares",
}

# 数値型変換対象カラム
_NUMERIC_COLUMNS = [
    "EPS",
    "Profit",
    "Equity",
    "NextYearForecastEPS",
    "BPS",
    "Sales",
    "OperatingProfit",
    "OrdinaryProfit",
    "OperatingCashFlow",
    "InvestingCashFlow",
    "DividendFY",
    "ForecastDividendFY",
    "NextYearForecastDividendFY",
    "PayoutRatio",
    "ForecastPayoutRatio",
    "NextYearForecastPayoutRatio",
    "ForecastEPS",
    "TotalAssets",
    "SharesOutstanding",
    "TreasuryShares",
    # Adjusted fields (computed, but keep in numeric list for safety)
    "AdjustedEPS",
    "AdjustedBPS",
    "AdjustedForecastEPS",
    "AdjustedNextYearForecastEPS",
    "AdjustedDividendFY",
    "AdjustedForecastDividendFY",
    "AdjustedNextYearForecastDividendFY",
    # Forward forecast columns (used by forecast-based signals)
    "ForwardBaseEPS",
    "AdjustedForwardBaseEPS",
    "ForwardForecastEPS",
    "AdjustedForwardForecastEPS",
    "ForwardBaseDividendFY",
    "AdjustedForwardBaseDividendFY",
    "ForwardForecastDividendFY",
    "AdjustedForwardForecastDividendFY",
    "ForwardBasePayoutRatio",
    "ForwardForecastPayoutRatio",
]

# Raw -> Adjusted カラム名マッピング（株式数調整対象）
_ADJUSTED_COLUMN_MAP = {
    "EPS": "AdjustedEPS",
    "BPS": "AdjustedBPS",
    "ForecastEPS": "AdjustedForecastEPS",
    "NextYearForecastEPS": "AdjustedNextYearForecastEPS",
    "DividendFY": "AdjustedDividendFY",
    "ForecastDividendFY": "AdjustedForecastDividendFY",
    "NextYearForecastDividendFY": "AdjustedNextYearForecastDividendFY",
}

_FORWARD_FORECAST_COLUMNS = [
    "ForwardForecastEPS",
    "AdjustedForwardForecastEPS",
    "ForwardForecastDividendFY",
    "AdjustedForwardForecastDividendFY",
    "ForwardForecastPayoutRatio",
]


def _resolve_baseline_shares(df: pd.DataFrame) -> float | None:
    """Resolve baseline shares from the latest quarterly disclosure within range."""
    if df.empty or "SharesOutstanding" not in df.columns:
        return None

    df_sorted = df.sort_index()
    shares = df_sorted["SharesOutstanding"]

    if "TypeOfCurrentPeriod" in df_sorted.columns:
        period_types = df_sorted["TypeOfCurrentPeriod"].map(normalize_period_type)
        quarterly_mask = period_types.isin(["1Q", "2Q", "3Q"])
        quarterly_valid = quarterly_mask & shares.notna() & (shares != 0)
        if quarterly_valid.any():
            latest_idx = shares[quarterly_valid].index.max()
            return float(shares.loc[latest_idx])

    # Fallback: latest disclosure with shares (any period type)
    valid_any = shares.notna() & (shares != 0)
    if valid_any.any():
        latest_idx = shares[valid_any].index.max()
        return float(shares.loc[latest_idx])

    return None


def _compute_adjusted_series(
    raw: pd.Series, shares: pd.Series, baseline_shares: float | None
) -> pd.Series:
    """Compute adjusted series using share count ratio, fallback to raw when not possible."""
    if baseline_shares is None or baseline_shares == 0 or pd.isna(baseline_shares):
        return raw
    mask = raw.notna() & shares.notna() & (shares != 0)
    adjusted = raw.where(~mask, raw * (shares / baseline_shares))
    return adjusted


def _build_forward_eps_columns(df: pd.DataFrame) -> None:
    """Build forward-signal helper columns.

    Forward signal policy:
    - Base EPS denominator: latest disclosed FY EPS only
    - Forecast EPS numerator: FY uses NextYearForecastEPS, quarterly uses ForecastEPS
    """
    if df.empty:
        return

    if "TypeOfCurrentPeriod" in df.columns:
        period_types = df["TypeOfCurrentPeriod"].map(normalize_period_type)
    else:
        period_types = pd.Series("FY", index=df.index)

    is_fy = period_types == "FY"
    is_quarter = period_types.isin(["1Q", "2Q", "3Q"])

    if "EPS" in df.columns:
        df["ForwardBaseEPS"] = df["EPS"].where(is_fy).ffill()
    if "AdjustedEPS" in df.columns:
        df["AdjustedForwardBaseEPS"] = df["AdjustedEPS"].where(is_fy).ffill()

    if "NextYearForecastEPS" in df.columns:
        fy_forecast = df["NextYearForecastEPS"].where(is_fy)
    else:
        fy_forecast = pd.Series(np.nan, index=df.index)

    if "ForecastEPS" in df.columns:
        q_forecast = df["ForecastEPS"].where(is_quarter)
    else:
        q_forecast = pd.Series(np.nan, index=df.index)

    df["ForwardForecastEPS"] = fy_forecast.combine_first(q_forecast)

    if "AdjustedNextYearForecastEPS" in df.columns:
        adjusted_fy_forecast = df["AdjustedNextYearForecastEPS"].where(is_fy)
    else:
        adjusted_fy_forecast = pd.Series(np.nan, index=df.index)

    if "AdjustedForecastEPS" in df.columns:
        adjusted_q_forecast = df["AdjustedForecastEPS"].where(is_quarter)
    else:
        adjusted_q_forecast = pd.Series(np.nan, index=df.index)

    df["AdjustedForwardForecastEPS"] = adjusted_fy_forecast.combine_first(
        adjusted_q_forecast
    )

    # Dividend forecast helpers (same policy as EPS: FY uses next FY forecast, Q uses current FY forecast)
    if "DividendFY" in df.columns:
        df["ForwardBaseDividendFY"] = df["DividendFY"].where(is_fy).ffill()
    if "AdjustedDividendFY" in df.columns:
        df["AdjustedForwardBaseDividendFY"] = df["AdjustedDividendFY"].where(is_fy).ffill()

    if "NextYearForecastDividendFY" in df.columns:
        fy_div_forecast = df["NextYearForecastDividendFY"].where(is_fy)
    else:
        fy_div_forecast = pd.Series(np.nan, index=df.index)

    if "ForecastDividendFY" in df.columns:
        q_div_forecast = df["ForecastDividendFY"].where(is_quarter)
    else:
        q_div_forecast = pd.Series(np.nan, index=df.index)

    df["ForwardForecastDividendFY"] = fy_div_forecast.combine_first(q_div_forecast)

    if "AdjustedNextYearForecastDividendFY" in df.columns:
        adjusted_fy_div_forecast = df["AdjustedNextYearForecastDividendFY"].where(is_fy)
    else:
        adjusted_fy_div_forecast = pd.Series(np.nan, index=df.index)

    if "AdjustedForecastDividendFY" in df.columns:
        adjusted_q_div_forecast = df["AdjustedForecastDividendFY"].where(is_quarter)
    else:
        adjusted_q_div_forecast = pd.Series(np.nan, index=df.index)

    df["AdjustedForwardForecastDividendFY"] = adjusted_fy_div_forecast.combine_first(
        adjusted_q_div_forecast
    )

    if "PayoutRatio" in df.columns:
        df["ForwardBasePayoutRatio"] = df["PayoutRatio"].where(is_fy).ffill()

    if "NextYearForecastPayoutRatio" in df.columns:
        fy_payout_forecast = df["NextYearForecastPayoutRatio"].where(is_fy)
    else:
        fy_payout_forecast = pd.Series(np.nan, index=df.index)

    if "ForecastPayoutRatio" in df.columns:
        q_payout_forecast = df["ForecastPayoutRatio"].where(is_quarter)
    else:
        q_payout_forecast = pd.Series(np.nan, index=df.index)

    df["ForwardForecastPayoutRatio"] = fy_payout_forecast.combine_first(
        q_payout_forecast
    )


def merge_forward_forecast_revision(
    base_daily_df: pd.DataFrame, revision_daily_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge forward forecast columns, preferring revision values when available."""
    merged = base_daily_df.copy()
    for column in _FORWARD_FORECAST_COLUMNS:
        if column not in revision_daily_df.columns:
            continue
        if column in merged.columns:
            merged[column] = revision_daily_df[column].combine_first(merged[column])
        else:
            merged[column] = revision_daily_df[column]
    return merged


def transform_statements_df(df: pd.DataFrame) -> pd.DataFrame:
    """Batch/個別共用: APIレスポンスDataFrameをVectorBT形式に変換

    カラム名のリネーム、数値型変換、派生指標(ROE/ROA/OperatingMargin)計算、
    EPS/BPS/Forecast系の株式数調整を行う。
    """
    df = df.rename(columns=_COLUMN_MAPPING)
    for col in _NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Share-based adjustments (baseline within the available date range)
    baseline_shares = _resolve_baseline_shares(df)
    shares = df["SharesOutstanding"] if "SharesOutstanding" in df.columns else None
    for raw_col, adjusted_col in _ADJUSTED_COLUMN_MAP.items():
        if raw_col in df.columns:
            df[adjusted_col] = (
                _compute_adjusted_series(df[raw_col], shares, baseline_shares)
                if shares is not None
                else df[raw_col]
            )

    _build_forward_eps_columns(df)

    df["ROE"] = _calc_roe(df)
    df["ROA"] = _calc_roa(df)
    df["OperatingMargin"] = _calc_operating_margin(df)
    return df


def load_statements_data(
    dataset: str,
    stock_code: str,
    daily_index: pd.DatetimeIndex,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period_type: APIPeriodType = "FY",
    actual_only: bool = True,
    include_forecast_revision: bool = False,
) -> pd.DataFrame:
    """
    決算データを日次株価データに同期変換（VectorBTフィルター用）

    決算発表データを日次インデックスに前方補完し、各決算発表の数値を
    次の決算発表まで継続することでフィルター計算を可能にします。

    Args:
        dataset: データベースファイルパス
        stock_code: 銘柄コード
        daily_index: 株価データと同期するための日次インデックス（必須）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        period_type: 使用する決算期間タイプ（デフォルト: "FY"）
            - "all": 全四半期（1Q, 2Q, 3Q, FY）
            - "FY": 本決算のみ（推奨・年次比較に適切）
            - "1Q", "2Q", "3Q": 特定の四半期のみ
        actual_only: 実績データのみ取得（デフォルト: True）
            - True: 予想データ（EPS/Profit/Equityが全てnull）を除外
            - False: 予想データも含む
        include_forecast_revision: Trueの場合、period_type="FY"時に
            追加でperiod_type="all"を取得し、ForwardForecast系カラムへ
            四半期修正（FEPS）を反映する

    Returns:
        pandas.DataFrame: 日次同期された財務諸表データ
            基本指標: EPS, Profit, Equity, ROE
            拡張指標: BPS, Sales, OperatingProfit, OrdinaryProfit,
                     OperatingCashFlow, InvestingCashFlow,
                     DividendFY, ForecastDividendFY, NextYearForecastDividendFY,
                     PayoutRatio, ForecastPayoutRatio, NextYearForecastPayoutRatio,
                     ForecastEPS,
                     NextYearForecastEPS, TotalAssets,
                     ROA, OperatingMargin (派生指標)
            Adjusted指標: AdjustedEPS, AdjustedBPS, AdjustedForecastEPS,
                         AdjustedNextYearForecastEPS,
                         AdjustedDividendFY, AdjustedForecastDividendFY,
                         AdjustedNextYearForecastDividendFY

    Raises:
        ValueError: データが見つからない場合

    Note:
        各period_typeの推奨用途:
        - "FY": PER, PBR, ROE, EPS成長率, 配当利回り（年次ベースの比較）
        - "all": 最新情報を素早く反映したい場合（四半期ごとに更新）
    """
    dataset_name = extract_dataset_name(dataset)
    revision_df: pd.DataFrame | None = None

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_statements(
            stock_code,
            start_date,
            end_date,
            period_type=period_type,
            actual_only=actual_only,
        )
        if include_forecast_revision and normalize_period_type(period_type) == "FY":
            try:
                revision_df = client.get_statements(
                    stock_code,
                    start_date,
                    end_date,
                    period_type="all",
                    actual_only=False,
                )
            except Exception as e:  # noqa: BLE001 - continue with FY-only baseline
                logger.warning(
                    "四半期修正データ取得に失敗（FYのみで続行）: "
                    f"{stock_code}, error={e}"
                )

    if df.empty:
        raise ValueError(
            f"No statements data found for stock code: {stock_code} "
            f"with period_type: {period_type}"
        )

    # カラム名統一・数値変換・派生指標計算
    df = transform_statements_df(df)

    # 決算データを日次インデックスに前方補完（次の決算発表まで値を継続）
    df = df.reindex(daily_index).ffill()
    if revision_df is not None and not revision_df.empty:
        revision_daily = transform_statements_df(revision_df).reindex(daily_index).ffill()
        df = merge_forward_forecast_revision(df, revision_daily)

    logger.debug(f"財務諸表データ読み込み成功: {stock_code} (period_type={period_type})")
    return df


def _calc_roe(df: pd.DataFrame) -> np.ndarray:
    """
    ROE（自己資本利益率）を計算

    ROE = (Profit / Equity) * 100
    Equity=0またはNaNの場合は0を返す
    """
    return np.where(
        (df["Equity"].notna()) & (df["Equity"] != 0) & (df["Profit"].notna()),
        (df["Profit"] / df["Equity"]) * 100,
        0,
    )


def _calc_operating_margin(df: pd.DataFrame) -> np.ndarray:
    """
    営業利益率を計算

    OperatingMargin = (OperatingProfit / Sales) * 100
    Sales=0またはNaNの場合はNaNを返す
    """
    if "OperatingProfit" not in df.columns or "Sales" not in df.columns:
        return np.full(len(df), np.nan)

    return np.where(
        (df["Sales"].notna()) & (df["Sales"] != 0) & (df["OperatingProfit"].notna()),
        (df["OperatingProfit"] / df["Sales"]) * 100,
        np.nan,
    )


def _calc_roa(df: pd.DataFrame) -> np.ndarray:
    """
    ROA（総資産利益率）を計算

    ROA = (Profit / TotalAssets) * 100
    TotalAssets=0またはNaNの場合はNaNを返す
    """
    if "TotalAssets" not in df.columns or "Profit" not in df.columns:
        return np.full(len(df), np.nan)

    return np.where(
        (df["TotalAssets"].notna())
        & (df["TotalAssets"] != 0)
        & (df["Profit"].notna()),
        (df["Profit"] / df["TotalAssets"]) * 100,
        np.nan,
    )
