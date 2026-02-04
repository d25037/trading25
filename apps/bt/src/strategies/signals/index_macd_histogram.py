"""
INDEXヒストグラムシグナル

INDEX（TOPIX等）のMACDヒストグラム（MACD線 - Signal線）の符号に基づくシグナル生成機能を提供します。
市場のモメンタム（強弱）に基づくエントリー・エグジット制御に使用します。

用途:
- エントリーフィルター: 市場が強い時（histogram > 0）のみエントリー
- エグジットトリガー: 市場が弱くなった時（histogram < 0）にエグジット
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def index_macd_histogram_signal(
    index_data: pd.DataFrame,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    direction: str = "positive",
) -> pd.Series:
    """
    INDEXヒストグラムシグナル

    INDEXデータ（TOPIX等）のMACDヒストグラム（MACD線 - Signal線）の符号でシグナルを生成します。
    市場のモメンタム（強弱）を判定し、エントリー・エグジット制御に使用します。

    Args:
        index_data: INDEXのOHLCデータ（Close カラム必須）
        fast_period: MACD高速EMA期間
        slow_period: MACD低速EMA期間
        signal_period: MACDシグナル期間
        direction: 判定方向
            - "positive": histogram > 0 の日にTrue（市場が強い）
            - "negative": histogram < 0 の日にTrue（市場が弱い）

    Returns:
        pd.Series[bool]: シグナル結果

    Raises:
        ValueError: index_data が空またはNone、Close カラムが存在しない場合

    Examples:
        >>> # エントリーフィルター: TOPIX MACDヒストグラムが正の時のみエントリー
        >>> entry_signal = index_macd_histogram_signal(
        ...     topix_data,
        ...     fast_period=12,
        ...     slow_period=26,
        ...     signal_period=9,
        ...     direction="positive"
        ... )

        >>> # エグジットトリガー: TOPIX MACDヒストグラムが負になったらエグジット
        >>> exit_signal = index_macd_histogram_signal(
        ...     topix_data,
        ...     fast_period=12,
        ...     slow_period=26,
        ...     signal_period=9,
        ...     direction="negative"
        ... )
    """
    # 入力検証
    if index_data is None or index_data.empty:
        raise ValueError("index_data が空またはNoneです")

    if "Close" not in index_data.columns:
        raise ValueError("index_data に 'Close' カラムが必要です")

    logger.debug(
        f"INDEXヒストグラムシグナル: MACD期間=({fast_period}/{slow_period}/{signal_period}), "
        f"方向={direction}"
    )

    # Close価格を取得
    close: pd.Series = index_data["Close"]

    # VectorBTでMACD計算
    macd_indicator = vbt.MACD.run(
        close,
        fast_window=fast_period,
        slow_window=slow_period,
        signal_window=signal_period,
    )

    # ヒストグラム = MACD線 - Signal線
    histogram: pd.Series = macd_indicator.macd - macd_indicator.signal

    # 方向に応じたシグナル生成
    if direction == "positive":
        # ヒストグラムが正の日にTrue（市場が強い）
        signal = histogram > 0
    elif direction == "negative":
        # ヒストグラムが負の日にTrue（市場が弱い）
        signal = histogram < 0
    else:
        raise ValueError(
            f"direction は 'positive' または 'negative' である必要があります: {direction}"
        )

    # NaN値をFalseに置換（初期期間のヒストグラムはNaNになる）
    signal = signal.fillna(False)

    logger.debug(
        f"INDEXヒストグラムシグナル生成完了: True: {signal.sum()}/{len(signal)}"
    )

    return signal


def index_macd_histogram_multi_signal(
    index_data: pd.DataFrame,
    stock_count: int,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    direction: str = "positive",
) -> pd.DataFrame:
    """
    複数銘柄向けINDEXヒストグラムシグナル（全銘柄に同一シグナル適用）

    INDEXデータのMACDヒストグラムシグナルを全銘柄に同一適用します。
    市場モメンタムフィルターとして機能します。

    Args:
        index_data: INDEXのOHLCデータ
        stock_count: 銘柄数
        fast_period: MACD高速EMA期間
        slow_period: MACD低速EMA期間
        signal_period: MACDシグナル期間
        direction: 判定方向（positive/negative）

    Returns:
        pd.DataFrame: 各銘柄に対するシグナル（全銘柄同一）

    Examples:
        >>> # 10銘柄のポートフォリオで同一シグナルを適用
        >>> multi_signal = index_macd_histogram_multi_signal(
        ...     topix_data,
        ...     stock_count=10,
        ...     fast_period=12,
        ...     slow_period=26,
        ...     signal_period=9,
        ...     direction="positive"
        ... )
    """
    # 基本シグナルを生成
    base_signal = index_macd_histogram_signal(
        index_data=index_data,
        fast_period=fast_period,
        slow_period=slow_period,
        signal_period=signal_period,
        direction=direction,
    )

    # 全銘柄に同一シグナルを適用
    signal_dict = {f"stock_{i}": base_signal for i in range(stock_count)}

    logger.debug(
        f"INDEXヒストグラムマルチシグナル生成完了: {stock_count}銘柄に同一シグナル適用"
    )

    return pd.DataFrame(signal_dict)
