"""
クロスオーバーシグナル実装

2本の指標線（SMA、RSI、MACD等）のクロスオーバーを検出する汎用シグナル関数
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def crossover_signal(
    fast_line: pd.Series,
    slow_line: pd.Series,
    direction: str = "golden",
) -> pd.Series:
    """
    クロスオーバーシグナル（汎用）

    2本の指標線のクロスオーバーを検出する。
    既存シグナル（volume_surge_signal等）と同じく、条件判定のみを行う。

    Args:
        fast_line: 高速線（短期SMA、短期RSI、MACD等）
        slow_line: 低速線（長期SMA、長期RSI、Signal線等）
        direction: クロス方向
            - "golden": ゴールデンクロス（fast > slow かつ 前日 fast <= slow）
            - "dead": デッドクロス（fast < slow かつ 前日 fast >= slow）

    Returns:
        pd.Series[bool]: クロスオーバー発生時にTrue

    Examples:
        >>> # SMAゴールデンクロス
        >>> sma_short = data["Close"].rolling(10).mean()
        >>> sma_long = data["Close"].rolling(30).mean()
        >>> entry_signal = crossover_signal(sma_short, sma_long, direction="golden")
        >>>
        >>> # RSIデッドクロス
        >>> exit_signal = crossover_signal(rsi_short, rsi_long, direction="dead")
    """
    logger.debug(
        f"クロスオーバーシグナル: 処理開始 (方向={direction}, データ長={len(fast_line)})"
    )

    if direction == "golden":
        # ゴールデンクロス: fast > slow かつ 前日 fast <= slow
        signal = (fast_line > slow_line) & (fast_line.shift(1) <= slow_line.shift(1))
    elif direction == "dead":
        # デッドクロス: fast < slow かつ 前日 fast >= slow
        signal = (fast_line < slow_line) & (fast_line.shift(1) >= slow_line.shift(1))
    else:
        raise ValueError(f"不正なdirection: {direction} (golden/deadのみ)")

    result = signal.fillna(False)

    logger.debug(
        f"クロスオーバーシグナル: 処理完了 (方向={direction}, True: {result.sum()}/{len(result)})"
    )
    return result


def indicator_crossover_signal(
    close: pd.Series,
    indicator_type: str,
    fast_period: int,
    slow_period: int,
    direction: str = "golden",
    signal_period: int = 9,
    lookback_days: int = 1,
) -> pd.Series:
    """
    インジケーター計算 + クロスオーバーシグナル統合関数

    VectorBTを使用してインジケーター（SMA/EMA/RSI/MACD）を計算し、
    クロスオーバーシグナルを生成する。

    Args:
        close: 終値データ
        indicator_type: インジケータータイプ
            - "sma": 単純移動平均
            - "ema": 指数移動平均
            - "rsi": RSI
            - "macd": MACD
        fast_period: 短期期間
        slow_period: 長期期間
        direction: クロス方向（"golden" or "dead"）
        signal_period: MACDシグナル期間（indicator_type="macd"時のみ使用）
        lookback_days: クロス検出期間（1=その日のみ、>1=直近X日以内）

    Returns:
        pd.Series[bool]: クロスオーバー発生時にTrue

    Examples:
        >>> # SMAゴールデンクロス（エントリー）
        >>> entry = indicator_crossover_signal(
        ...     close, "sma", fast_period=10, slow_period=30, direction="golden"
        ... )
        >>>
        >>> # RSIデッドクロス（エグジット）
        >>> exit = indicator_crossover_signal(
        ...     close, "rsi", fast_period=7, slow_period=14, direction="dead"
        ... )
        >>>
        >>> # 直近10日以内MACDゴールデンクロス
        >>> recent_cross = indicator_crossover_signal(
        ...     close, "macd", fast_period=12, slow_period=26,
        ...     direction="golden", lookback_days=10
        ... )
    """
    logger.debug(
        f"インジケーター計算 + クロスオーバーシグナル: タイプ={indicator_type}, "
        f"短期={fast_period}, 長期={slow_period}, ルックバック={lookback_days}日"
    )

    # インジケーター計算（VectorBT使用）
    if indicator_type == "sma":
        fast = vbt.MA.run(close, fast_period).ma
        slow = vbt.MA.run(close, slow_period).ma
    elif indicator_type == "ema":
        fast = vbt.MA.run(close, fast_period, ewm=True).ma
        slow = vbt.MA.run(close, slow_period, ewm=True).ma
    elif indicator_type == "rsi":
        fast = vbt.RSI.run(close, fast_period).rsi
        slow = vbt.RSI.run(close, slow_period).rsi
    elif indicator_type == "macd":
        macd = vbt.MACD.run(
            close,
            fast_window=fast_period,
            slow_window=slow_period,
            signal_window=signal_period,
        )
        fast = macd.macd
        slow = macd.signal
    else:
        raise ValueError(
            f"未対応のインジケータータイプ: {indicator_type} (sma/ema/rsi/macdのみ)"
        )

    # クロスオーバーシグナル生成
    signal = crossover_signal(fast, slow, direction)

    # lookback_days > 1の場合、直近X日以内クロス検出
    if lookback_days > 1:
        logger.debug(f"直近{lookback_days}日以内クロス検出を適用")
        signal = (signal.astype(int).rolling(lookback_days).max() >= 1).fillna(False)

    return signal
