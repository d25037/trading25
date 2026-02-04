"""
平均回帰シグナル実装

SMA等の基準線からの乖離を検出する汎用シグナル関数
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def deviation_signal(
    price: pd.Series,
    baseline: pd.Series,
    threshold: float,
    direction: str = "below",
) -> pd.Series:
    """
    乖離シグナル（汎用）

    価格が基準線（SMA等）から一定以上乖離した条件を検出する。
    既存シグナル（volume_surge_signal等）と同じく、条件判定のみを行う。

    Args:
        price: 価格データ（Close等）
        baseline: 基準線（SMA、EMA等）
        threshold: 乖離率閾値（例: 0.2 = 20%乖離）
        direction: 乖離方向
            - "below": 基準線より安い（(price - baseline) / baseline <= -threshold）
            - "above": 基準線より高い（(price - baseline) / baseline >= threshold）

    Returns:
        pd.Series[bool]: 乖離条件成立時にTrue

    Examples:
        >>> # SMAより20%以上安い（平均回帰エントリー）
        >>> sma = data["Close"].rolling(25).mean()
        >>> entry_signal = deviation_signal(data["Close"], sma, threshold=0.2, direction="below")
        >>>
        >>> # SMAより10%以上高い（オーバーシュートエグジット）
        >>> exit_signal = deviation_signal(data["Close"], sma, threshold=0.1, direction="above")
    """
    logger.debug(
        f"乖離シグナル: 処理開始 (閾値={threshold:.1%}, 方向={direction}, データ長={len(price)})"
    )

    # 乖離率計算
    deviation_ratio = (price - baseline) / baseline

    if direction == "below":
        signal = (deviation_ratio <= -threshold) & baseline.notna() & price.notna()
    elif direction == "above":
        signal = (deviation_ratio >= threshold) & baseline.notna() & price.notna()
    else:
        raise ValueError(f"不正なdirection: {direction} (below/aboveのみ)")

    result = signal.fillna(False)

    logger.debug(
        f"乖離シグナル: 処理完了 (閾値={threshold:.1%}, 方向={direction}, True: {result.sum()}/{len(result)})"
    )
    return result


def price_recovery_signal(
    price: pd.Series,
    baseline: pd.Series,
    direction: str = "above",
) -> pd.Series:
    """
    価格回復シグナル（汎用）

    価格が基準線（SMA等）を回復した条件を検出する。
    平均回帰戦略のエグジット条件に使用。

    Args:
        price: 価格データ（High、Close等）
        baseline: 基準線（SMA、EMA等）
        direction: 回復方向
            - "above": 基準線上抜け（price > baseline）
            - "below": 基準線下抜け（price < baseline）

    Returns:
        pd.Series[bool]: 回復条件成立時にTrue

    Examples:
        >>> # 高値がSMA上抜け（平均回帰エグジット）
        >>> sma = data["Close"].rolling(25).mean()
        >>> exit_signal = price_recovery_signal(data["High"], sma, direction="above")
    """
    logger.debug(
        f"価格回復シグナル: 処理開始 (方向={direction}, データ長={len(price)})"
    )

    if direction == "above":
        signal = (price > baseline) & baseline.notna() & price.notna()
    elif direction == "below":
        signal = (price < baseline) & baseline.notna() & price.notna()
    else:
        raise ValueError(f"不正なdirection: {direction} (above/belowのみ)")

    result = signal.fillna(False)

    logger.debug(
        f"価格回復シグナル: 処理完了 (方向={direction}, True: {result.sum()}/{len(result)})"
    )
    return result


def mean_reversion_entry_signal(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
    deviation_threshold: float,
    deviation_direction: str = "below",
) -> pd.Series:
    """
    平均回帰エントリーシグナル統合関数（ベースライン計算 + 乖離検出）

    VectorBTを使用してベースライン（SMA/EMA）を計算し、
    乖離エントリーシグナルを生成する。

    Args:
        ohlc_data: OHLCVデータ（Close含む）
        baseline_type: ベースラインタイプ（"sma" or "ema"）
        baseline_period: ベースライン期間
        deviation_threshold: 乖離率閾値（例: 0.2 = 20%乖離）
        deviation_direction: 乖離方向（"below" or "above"）

    Returns:
        pd.Series[bool]: 乖離条件成立時にTrue

    Examples:
        >>> # SMAより20%以上安い（買いエントリー）
        >>> entry = mean_reversion_entry_signal(
        ...     ohlc_data, "sma", baseline_period=25, deviation_threshold=0.2, deviation_direction="below"
        ... )
    """
    logger.debug(
        f"平均回帰エントリーシグナル: ベースライン={baseline_type}({baseline_period}), 閾値={deviation_threshold:.1%}"
    )

    close = ohlc_data["Close"]

    # ベースライン計算（VectorBT使用）
    if baseline_type == "sma":
        baseline = vbt.MA.run(close, baseline_period).ma
    elif baseline_type == "ema":
        baseline = vbt.MA.run(close, baseline_period, ewm=True).ma
    else:
        raise ValueError(f"未対応のベースラインタイプ: {baseline_type} (sma/emaのみ)")

    # 乖離シグナル生成
    return deviation_signal(close, baseline, deviation_threshold, deviation_direction)


def mean_reversion_exit_signal(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
    recovery_direction: str = "above",
    recovery_price: str = "close",
) -> pd.Series:
    """
    平均回帰エグジットシグナル統合関数（ベースライン計算 + 回復検出）

    VectorBTを使用してベースライン（SMA/EMA）を計算し、
    価格回復エグジットシグナルを生成する。

    Args:
        ohlc_data: OHLCVデータ（High、Low、Close含む）
        baseline_type: ベースラインタイプ（"sma" or "ema"）
        baseline_period: ベースライン期間
        recovery_direction: 回復方向（"above" or "below"）
        recovery_price: 回復価格（"high"、"low"、"close"）

    Returns:
        pd.Series[bool]: 回復条件成立時にTrue

    Examples:
        >>> # 高値がSMA上抜け（エグジット）
        >>> exit = mean_reversion_exit_signal(
        ...     ohlc_data, "sma", baseline_period=25, recovery_direction="above", recovery_price="high"
        ... )
    """
    logger.debug(
        f"平均回帰エグジットシグナル: ベースライン={baseline_type}({baseline_period}), 回復={recovery_direction}"
    )

    close = ohlc_data["Close"]

    # ベースライン計算（VectorBT使用）
    if baseline_type == "sma":
        baseline = vbt.MA.run(close, baseline_period).ma
    elif baseline_type == "ema":
        baseline = vbt.MA.run(close, baseline_period, ewm=True).ma
    else:
        raise ValueError(f"未対応のベースラインタイプ: {baseline_type} (sma/emaのみ)")

    # 回復価格選択
    if recovery_price == "high":
        price = ohlc_data["High"]
    elif recovery_price == "low":
        price = ohlc_data["Low"]
    else:
        price = close

    # 回復シグナル生成
    return price_recovery_signal(price, baseline, recovery_direction)


def mean_reversion_combined_signal(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
    deviation_threshold: float,
    deviation_direction: str,
    recovery_price: str,
    recovery_direction: str,
) -> pd.Series:
    """
    平均回帰統合シグナル（エントリー・エグジット条件統合）

    乖離条件（deviation）と回復条件（recovery）を個別に制御可能。
    エントリー/エグジットの区別はYAMLパラメータ設定で制御。

    **制御ロジック**:
    - deviation_threshold=0.0: 乖離シグナル無効化（常にFalse）
    - recovery_price="none": 回復シグナル無効化（常にFalse）
    - 両方有効: OR結合（どちらかが成立すればTrue）

    **使い分け**:
    - エントリー用: deviation_thresholdを設定、recovery_price="none"
    - エグジット用: deviation_threshold=0.0、recovery_priceを設定

    Args:
        ohlc_data: OHLCVデータ（High、Low、Close含む）
        baseline_type: ベースラインタイプ（"sma" or "ema"）
        baseline_period: ベースライン期間
        deviation_threshold: 乖離率閾値（0.0で無効化、例: 0.2 = 20%乖離）
        deviation_direction: 乖離方向（"below" or "above"）
        recovery_price: 回復価格（"high"/"low"/"close"/"none"、"none"で無効化）
        recovery_direction: 回復方向（"above" or "below"）

    Returns:
        pd.Series[bool]: 乖離条件 OR 回復条件が成立時にTrue

    Examples:
        >>> # エントリー用（乖離のみ）: SMAより20%以上安い
        >>> entry = mean_reversion_combined_signal(
        ...     ohlc_data, "sma", 25, deviation_threshold=0.2,
        ...     deviation_direction="below", recovery_price="none",
        ...     recovery_direction="above"
        ... )
        >>>
        >>> # エグジット用（回復のみ）: 高値がSMA上抜け
        >>> exit = mean_reversion_combined_signal(
        ...     ohlc_data, "sma", 25, deviation_threshold=0.0,
        ...     deviation_direction="above", recovery_price="high",
        ...     recovery_direction="above"
        ... )
    """
    close = ohlc_data["Close"]

    # 空データチェック
    if close.empty:
        return pd.Series([], dtype=bool)

    # ベースライン計算（VectorBT使用）
    if baseline_type == "sma":
        baseline = vbt.MA.run(close, baseline_period).ma
    elif baseline_type == "ema":
        baseline = vbt.MA.run(close, baseline_period, ewm=True).ma
    else:
        raise ValueError(f"未対応のベースラインタイプ: {baseline_type} (sma/emaのみ)")

    # 乖離シグナル（deviation_threshold=0.0で無効化）
    if deviation_threshold > 0.0:
        deviation_signal_result = deviation_signal(
            close, baseline, deviation_threshold, deviation_direction
        )
    else:
        deviation_signal_result = pd.Series(False, index=close.index)

    # 回復シグナル（recovery_price="none"で無効化）
    if recovery_price != "none":
        if recovery_price == "high":
            price = ohlc_data["High"]
        elif recovery_price == "low":
            price = ohlc_data["Low"]
        else:
            price = close

        recovery_signal_result = price_recovery_signal(
            price, baseline, recovery_direction
        )
    else:
        recovery_signal_result = pd.Series(False, index=close.index)

    # 両シグナルのOR結合（どちらかが成立すればTrue）
    combined = deviation_signal_result | recovery_signal_result

    logger.info(
        f"平均回帰統合シグナル: 乖離={deviation_signal_result.sum()}, "
        f"回復={recovery_signal_result.sum()}, 統合={combined.sum()}"
    )

    return combined
