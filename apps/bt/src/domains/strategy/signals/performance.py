"""
パフォーマンスシグナル実装

ベンチマークとの相対パフォーマンス比較シグナルを提供
"""

import pandas as pd
from typing import Optional
from loguru import logger

from src.infrastructure.data_access.loaders import load_topix_data, load_index_data


def relative_performance_signal(
    price_data: pd.Series,
    benchmark_data: pd.Series,
    lookback_days: int = 60,
    performance_multiplier: float = 2.0,
) -> pd.Series:
    """
    相対パフォーマンスシグナル

    N日前と比較した最新値の上昇率が、ベンチマークに対してX倍かどうかを判定

    Args:
        price_data: 個別銘柄の価格データ (pd.Series with DatetimeIndex)
        benchmark_data: ベンチマークの価格データ (pd.Series with DatetimeIndex)
        lookback_days: 比較対象期間（N日前、デフォルト60日）
        performance_multiplier: ベンチマークとの倍率閾値（X倍、デフォルト2.0）

    Returns:
        pd.Series: 各日において条件を満たす場合にTrue

    Example:
        >>> stock_prices = pd.Series([100, 102, 105, 110], index=pd.date_range('2024-01-01', periods=4))
        >>> benchmark_prices = pd.Series([1000, 1005, 1008, 1010], index=pd.date_range('2024-01-01', periods=4))
        >>> signal_result = relative_performance_signal(stock_prices, benchmark_prices, lookback_days=2, performance_multiplier=2.0)
    """

    # 入力データの検証
    if price_data.empty or benchmark_data.empty:
        logger.warning("Price data or benchmark data is empty")
        return pd.Series(False, index=price_data.index)

    # 日付インデックスの整合性確認
    if not isinstance(price_data.index, pd.DatetimeIndex):
        logger.warning("Price data index is not DatetimeIndex")
        return pd.Series(False, index=price_data.index)

    if not isinstance(benchmark_data.index, pd.DatetimeIndex):
        logger.warning("Benchmark data index is not DatetimeIndex")
        return pd.Series(False, index=price_data.index)

    # 共通の日付範囲を取得
    common_dates = price_data.index.intersection(benchmark_data.index)

    if len(common_dates) < lookback_days + 1:
        logger.warning(
            f"Insufficient common dates: {len(common_dates)} < {lookback_days + 1}"
        )
        return pd.Series(False, index=price_data.index)

    # 共通日付のデータを抽出
    stock_aligned = price_data.reindex(common_dates)
    bench_aligned = benchmark_data.reindex(common_dates)

    # N日前との比較によるリターン計算
    stock_return = (stock_aligned / stock_aligned.shift(lookback_days) - 1).fillna(0)
    bench_return = (bench_aligned / bench_aligned.shift(lookback_days) - 1).fillna(0)

    # 相対パフォーマンス判定
    # 銘柄のリターンがベンチマークのリターン × 倍率閾値を上回る
    outperformance = stock_return > (bench_return * performance_multiplier)

    # 元のインデックスに合わせて結果を返す
    result = pd.Series(False, index=price_data.index)
    result.loc[common_dates] = outperformance

    logger.debug(
        f"Relative performance signal: {result.sum()}/{len(result)} periods passed (lookback={lookback_days}d, multiplier={performance_multiplier}x)"
    )

    return result


def create_relative_performance_signal_from_db(
    dataset: str,
    stock_price_data: pd.Series,
    benchmark_table: str = "topix",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = 60,
    performance_multiplier: float = 2.0,
) -> pd.Series:
    """
    データベースからベンチマークデータを読み込んで相対パフォーマンスシグナルを作成

    Args:
        dataset: データベースファイルパス
        stock_price_data: 個別銘柄の価格データ
        benchmark_table: ベンチマークテーブル名（'topix'または特定のインデックスコード）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
        lookback_days: 比較対象期間（N日前）
        performance_multiplier: ベンチマークとの倍率閾値（X倍）

    Returns:
        pd.Series: 相対パフォーマンスシグナル結果

    Raises:
        ValueError: ベンチマークデータが取得できない場合
    """

    try:
        # ベンチマークデータの読み込み
        if benchmark_table.lower() == "topix":
            # TOPIXデータを読み込み
            benchmark_df = load_topix_data(dataset, start_date, end_date)
            benchmark_prices = benchmark_df["Close"]
            logger.info(f"Loaded TOPIX data: {len(benchmark_prices)} records")

        else:
            # 指定されたインデックスデータを読み込み
            benchmark_df = load_index_data(
                dataset, benchmark_table, start_date, end_date
            )
            benchmark_prices = benchmark_df["Close"]
            logger.info(
                f"Loaded {benchmark_table} data: {len(benchmark_prices)} records"
            )

        # 相対パフォーマンスシグナルを適用
        return relative_performance_signal(
            price_data=stock_price_data,
            benchmark_data=benchmark_prices,
            lookback_days=lookback_days,
            performance_multiplier=performance_multiplier,
        )

    except Exception as e:
        logger.error(f"Failed to create relative performance signal from DB: {e}")
        raise ValueError(f"Could not load benchmark data for {benchmark_table}: {e}")


def multi_timeframe_relative_performance_signal(
    price_data: pd.Series,
    benchmark_data: pd.Series,
    timeframes: list[int] = [30, 60, 90],
    performance_multiplier: float = 2.0,
    require_all_timeframes: bool = False,
) -> pd.Series:
    """
    複数時間軸での相対パフォーマンスシグナル

    Args:
        price_data: 個別銘柄の価格データ
        benchmark_data: ベンチマークの価格データ
        timeframes: 検証する期間のリスト（日数）
        performance_multiplier: ベンチマークとの倍率閾値
        require_all_timeframes: True=全期間でパス必要、False=いずれか一つでパス

    Returns:
        pd.Series: 複数時間軸でのシグナル結果
    """

    results = []

    for timeframe in timeframes:
        timeframe_result = relative_performance_signal(
            price_data=price_data,
            benchmark_data=benchmark_data,
            lookback_days=timeframe,
            performance_multiplier=performance_multiplier,
        )
        results.append(timeframe_result)
        logger.debug(
            f"Timeframe {timeframe}d: {timeframe_result.sum()}/{len(timeframe_result)} passed"
        )

    if require_all_timeframes:
        # 全ての期間で条件を満たす必要
        combined_result = pd.concat(results, axis=1).all(axis=1)
        logger.info(
            f"Multi-timeframe (ALL required): {combined_result.sum()}/{len(combined_result)} passed"
        )
    else:
        # いずれかの期間で条件を満たせば良い
        combined_result = pd.concat(results, axis=1).any(axis=1)
        logger.info(
            f"Multi-timeframe (ANY): {combined_result.sum()}/{len(combined_result)} passed"
        )

    return combined_result
