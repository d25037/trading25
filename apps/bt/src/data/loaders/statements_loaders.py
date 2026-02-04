"""
財務諸表データローダー

localhost:3001 API経由で財務諸表データを読み込み、VectorBTで使用できる形式に変換します。
"""

from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from src.api.dataset_client import DatasetAPIClient
from src.api.dataset.statements_mixin import APIPeriodType
from src.data.loaders.utils import extract_dataset_name

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
    "forecastEps": "ForecastEPS",
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
    "ForecastEPS",
    "SharesOutstanding",
    "TreasuryShares",
]


def transform_statements_df(df: pd.DataFrame) -> pd.DataFrame:
    """Batch/個別共用: APIレスポンスDataFrameをVectorBT形式に変換

    カラム名のリネーム、数値型変換、派生指標(ROE/OperatingMargin)計算を行う。
    """
    df = df.rename(columns=_COLUMN_MAPPING)
    for col in _NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ROE"] = _calc_roe(df)
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

    Returns:
        pandas.DataFrame: 日次同期された財務諸表データ
            基本指標: EPS, Profit, Equity, ROE
            拡張指標: BPS, Sales, OperatingProfit, OrdinaryProfit,
                     OperatingCashFlow, InvestingCashFlow, DividendFY, ForecastEPS,
                     NextYearForecastEPS, OperatingMargin (派生指標)

    Raises:
        ValueError: データが見つからない場合

    Note:
        各period_typeの推奨用途:
        - "FY": PER, PBR, ROE, EPS成長率, 配当利回り（年次ベースの比較）
        - "all": 最新情報を素早く反映したい場合（四半期ごとに更新）
    """
    dataset_name = extract_dataset_name(dataset)

    with DatasetAPIClient(dataset_name) as client:
        df = client.get_statements(
            stock_code,
            start_date,
            end_date,
            period_type=period_type,
            actual_only=actual_only,
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
