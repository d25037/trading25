"""
データ変換関数群

データの時間軸変換、相対強度計算などの変換処理を提供します。
"""

import pandas as pd
from loguru import logger


def create_relative_ohlc_data(
    stock_data: pd.DataFrame, benchmark_data: pd.DataFrame
) -> pd.DataFrame:
    """
    相対OHLC価格データを作成（relative mode用）

    Args:
        stock_data: 個別銘柄のOHLCVデータ
        benchmark_data: ベンチマーク（TOPIX等）のOHLCVデータ

    Returns:
        pandas.DataFrame: 相対OHLC価格データ（個別銘柄OHLC ÷ ベンチマークOHLC）

    Raises:
        ValueError: データの日付が一致しない場合
    """
    # 日付インデックスを合わせて結合
    common_dates = stock_data.index.intersection(benchmark_data.index)

    if len(common_dates) == 0:
        raise ValueError("株価データとベンチマークデータに共通する日付がありません")

    # 共通日付でデータを絞り込み
    stock_aligned = stock_data.reindex(common_dates)
    benchmark_aligned = benchmark_data.reindex(common_dates)

    # 相対価格データを作成
    relative_data = stock_aligned.copy()

    # OHLC各カラムで相対価格を計算
    ohlc_columns = ["Open", "High", "Low", "Close"]
    for col in ohlc_columns:
        if col in stock_aligned.columns and col in benchmark_aligned.columns:
            # 個別銘柄OHLC ÷ ベンチマークOHLC
            relative_data[col] = stock_aligned[col] / benchmark_aligned[col]

    # Volumeは個別銘柄のVolumeをそのまま使用（ベンチマークのVolumeは使わない）
    if "Volume" in stock_aligned.columns:
        relative_data["Volume"] = stock_aligned["Volume"]

    # NaN値を除去
    relative_data = relative_data.dropna()

    logger.info(
        f"相対OHLC作成完了: {len(relative_data)}レコード (共通日付: {len(common_dates)})"
    )

    return relative_data
