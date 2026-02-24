"""
ブレイクアウトシグナル実装（統合版）

期間ブレイクアウト・移動平均線ブレイクを統一的に処理
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def period_breakout_signal(
    price: pd.Series,
    period: int = 20,
    direction: str = "high",
    condition: str = "break",
    lookback_days: int = 1,
) -> pd.Series:
    """
    期間ブレイクアウトシグナル（高値・安値統合版）

    lookback_days期間の最高値/最安値とperiod期間の最高値/最安値を比較してブレイクを検出

    Args:
        price: 価格データ（High or Low）
        period: 比較対象期間（N日最高値/最安値）
        direction:
            - "high": 最高値の比較（lookback_days期間max vs period期間max）
            - "low": 最安値の比較（lookback_days期間min vs period期間min）
        condition:
            - "break": ブレイク検出
              - high: lookback_days >= period
              - low: lookback_days <= period
            - "maintained": 維持検出（breakの逆）
              - high: lookback_days < period
              - low: lookback_days > period
        lookback_days: イベント検出期間（1=今日の価格、N=直近N日の最高値/最安値）

    Returns:
        pd.Series[bool]: 条件を満たす日にTrue

    Examples:
        >>> # 今日の高値が200日最高値をブレイク
        >>> period_breakout_signal(high, period=200, direction="high", lookback_days=1)
        >>>
        >>> # 直近10日の最高値が200日最高値をブレイク（range_break_v5）
        >>> period_breakout_signal(high, period=200, direction="high", lookback_days=10)
        >>>
        >>> # 直近10日間で200日最高値未更新（range_break_v4 exit）
        >>> period_breakout_signal(high, period=200, direction="high",
        ...                        condition="maintained", lookback_days=10)
        >>>
        >>> # 今日の安値が60日最安値をブレイク
        >>> period_breakout_signal(low, period=60, direction="low", lookback_days=1)
        >>>
        >>> # 直近20日の安値が120日最安値を維持（サポート維持）
        >>> period_breakout_signal(low, period=120, direction="low",
        ...                        condition="maintained", lookback_days=20)
    """
    logger.debug(
        f"期間ブレイクアウト: 比較期間={period}日, lookback={lookback_days}日, "
        f"方向={direction}, 条件={condition}"
    )

    if direction == "high":
        # 最高値の比較
        lookback_value = price.rolling(lookback_days).max()
        period_value = price.rolling(period).max()

        if condition == "break":
            signal = lookback_value >= period_value
        else:  # maintained
            signal = lookback_value < period_value

    else:  # direction == "low"
        # 最安値の比較
        lookback_value = price.rolling(lookback_days).min()
        period_value = price.rolling(period).min()

        if condition == "break":
            signal = lookback_value <= period_value
        else:  # maintained
            signal = lookback_value > period_value

    signal = signal.fillna(False)

    logger.debug(f"期間ブレイクアウト: 完了 (True: {signal.sum()}/{len(signal)})")
    return signal


def ma_breakout_signal(
    price: pd.Series,
    period: int = 200,
    ma_type: str = "sma",
    direction: str = "above",
    lookback_days: int = 1,
) -> pd.Series:
    """
    移動平均線ブレイクアウトシグナル（クロス検出版）

    価格がMAを「上抜ける/下抜ける」イベントを検出（状態検出ではない）

    Args:
        price: 価格データ（通常Close）
        period: 移動平均期間
        ma_type: 移動平均タイプ（sma/ema）
        direction:
            - "above": 上抜けクロス（前日MA以下 → 今日MA上）
            - "below": 下抜けクロス（前日MA以上 → 今日MA下）
        lookback_days: 直近N日以内のクロスイベント検出

    Returns:
        pd.Series[bool]: クロス発生日にTrue

    Examples:
        >>> # 200日SMA上抜けクロス
        >>> ma_breakout_signal(close, period=200, ma_type="sma", direction="above")
        >>>
        >>> # 直近3日以内に50日EMA下抜けクロス
        >>> ma_breakout_signal(
        ...     close, period=50, ma_type="ema",
        ...     direction="below", lookback_days=3
        ... )
    """
    logger.debug(
        f"MA線ブレイクアウト（クロス検出）: タイプ={ma_type}, 期間={period}日, "
        f"方向={direction}, lookback={lookback_days}日"
    )

    if ma_type == "sma":
        ma = vbt.MA.run(price, period).ma
    else:  # ema
        ma = vbt.MA.run(price, period, ewm=True).ma

    # クロス検出ロジック（状態検出 → イベント検出に変更）
    if direction == "above":
        # 上抜けクロス: 前日MA以下 → 今日MA上
        crossover = (price > ma) & (price.shift(1) <= ma.shift(1))
    else:  # below
        # 下抜けクロス: 前日MA以上 → 今日MA下
        crossover = (price < ma) & (price.shift(1) >= ma.shift(1))

    signal = crossover.fillna(False)

    # lookback_days処理: 直近N日以内にクロスが発生したか検出
    if lookback_days > 1:
        logger.debug(f"直近{lookback_days}日以内クロスイベント検出を適用")
        signal = (signal.astype(int).rolling(lookback_days).max() >= 1).fillna(False)

    logger.debug(f"MA線ブレイクアウト: 完了 (クロス: {signal.sum()}/{len(signal)})")
    return signal


def atr_support_break_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    lookback_period: int = 20,
    atr_multiplier: float = 2.0,
    direction: str = "break",
    price_column: str = "close",
) -> pd.Series:
    """
    ATRサポートラインブレイクシグナル（統合版）

    サポートライン = 期間最高値 - ATR * 倍率
    - direction="break": 価格がサポートライン下回り（ショートエントリー/損切り）
    - direction="recovery": 価格がサポートライン上回り（ショートエグジット/反発）

    Args:
        high: 高値データ
        low: 安値データ
        close: 終値データ
        lookback_period: サポートライン・ATR両方の計算期間（統一期間）
        atr_multiplier: ATR倍率
        direction: ブレイク方向
            - "break": サポート割れ（価格 < サポートライン）
            - "recovery": サポート回復（価格 > サポートライン）
        price_column: 判定価格カラム（"close"/"low"）

    Returns:
        pd.Series[bool]: 条件を満たす日にTrue

    Examples:
        >>> # ショートエントリー: 終値がサポートライン下回り
        >>> atr_support_break_signal(
        ...     high, low, close,
        ...     lookback_period=20, atr_multiplier=2.0,
        ...     direction="break", price_column="close"
        ... )
        >>>
        >>> # ロング損切り: 安値がサポートライン下回り
        >>> atr_support_break_signal(
        ...     high, low, close,
        ...     lookback_period=20, atr_multiplier=3.0,
        ...     direction="break", price_column="low"
        ... )
        >>>
        >>> # ショートエグジット: 終値がサポートライン上回り
        >>> atr_support_break_signal(
        ...     high, low, close,
        ...     direction="recovery", price_column="close"
        ... )
    """
    logger.debug(
        f"ATRサポートブレイク: lookback={lookback_period}日（ATR・最高値統一）, "
        f"ATR倍率={atr_multiplier}, 方向={direction}, 価格={price_column}"
    )

    # サポートライン計算（共通関数使用）
    from src.domains.strategy.indicators import compute_atr_support_line

    support_line = compute_atr_support_line(
        high, low, close, lookback_period, atr_multiplier
    )

    # 判定価格選択
    price = close if price_column == "close" else low

    # 方向に応じたシグナル生成
    if direction == "break":
        # サポート割れ: 価格がサポートライン下回る
        signal = price < support_line
    else:  # recovery
        # サポート回復: 価格がサポートライン上回る
        signal = price > support_line

    signal = signal.fillna(False)

    logger.debug(f"ATRサポートブレイク: 完了 (True: {signal.sum()}/{len(signal)})")
    return signal


def retracement_signal(
    high: pd.Series,
    close: pd.Series,
    low: pd.Series | None = None,
    lookback_period: int = 20,
    retracement_level: float = 0.382,
    direction: str = "break",
    price_column: str = "close",
) -> pd.Series:
    """
    フィボナッチリトレースメントシグナル（下落率ベース）

    N日最高値からの下落率で押し目・戻りを判定（ATRを使わないシンプル版）
    リトレースメント価格 = 最高値 × (1 - retracement_level)

    Args:
        high: 高値データ
        close: 終値データ
        low: 安値データ（price_column="low"の場合必須）
        lookback_period: 最高値計算期間（デフォルト: 20日）
        retracement_level: 下落率（0.0-1.0）
            - 0.236: 23.6%リトレースメント（浅い押し目）
            - 0.382: 38.2%リトレースメント（標準）
            - 0.500: 50%リトレースメント（半値押し）
            - 0.618: 61.8%リトレースメント（深い押し目）
            - 0.786: 78.6%リトレースメント（非常に深い押し目）
        direction: ブレイク方向
            - "break": リトレースメントレベル下抜け（押し目完了）
            - "recovery": リトレースメントレベル上抜け（反発）
        price_column: 判定価格カラム（"close"/"low"）

    Returns:
        pd.Series[bool]: 条件を満たす日にTrue

    Examples:
        >>> # エントリー: 61.8%押し目で買い
        >>> retracement_signal(
        ...     high, close,
        ...     lookback_period=20,
        ...     retracement_level=0.618,
        ...     direction="break",
        ...     price_column="close"
        ... )
        >>>
        >>> # エグジット: 38.2%戻りで利確
        >>> retracement_signal(
        ...     high, close,
        ...     lookback_period=20,
        ...     retracement_level=0.382,
        ...     direction="recovery",
        ...     price_column="close"
        ... )
        >>>
        >>> # 安値判定: 50%半値押しを安値が割り込んだか
        >>> retracement_signal(
        ...     high, close, low,
        ...     lookback_period=50,
        ...     retracement_level=0.5,
        ...     direction="break",
        ...     price_column="low"
        ... )
    """
    logger.debug(
        f"リトレースメントシグナル: lookback={lookback_period}日, "
        f"レベル={retracement_level:.1%}, 方向={direction}, 価格={price_column}"
    )

    # 期間最高値計算
    highest = high.rolling(window=lookback_period).max()

    # リトレースメント価格 = 最高値 × (1 - 下落率)
    retracement_price = highest * (1.0 - retracement_level)

    # 判定価格選択
    if price_column == "close":
        price = close
    elif price_column == "low":
        if low is None:
            raise ValueError("price_column='low'の場合、lowデータが必須です")
        price = low
    else:
        raise ValueError(f"不正なprice_column: {price_column} (close/lowのみ)")

    # 方向に応じたシグナル生成
    if direction == "break":
        # リトレースメントレベル下抜け: 価格がリトレースメント価格を下回る
        signal = price < retracement_price
    elif direction == "recovery":
        # リトレースメントレベル上抜け: 価格がリトレースメント価格を上回る
        signal = price > retracement_price
    else:
        raise ValueError(f"不正なdirection: {direction} (break/recoveryのみ)")

    signal = signal.fillna(False)

    logger.debug(
        f"リトレースメントシグナル: 完了 (True: {signal.sum()}/{len(signal)}, "
        f"レベル={retracement_level:.1%})"
    )
    return signal


# ===== レガシー関数（段階的削除予定） =====


def threshold_breakout_signal(
    ohlc_data: pd.DataFrame,
    threshold_type: str,
    period: int,
    direction: str = "upward",
    price_column: str = "high",
) -> pd.Series:
    """
    【レガシー】閾値ブレイクアウトシグナル

    統合版のperiod_breakout_signal/ma_breakout_signalに移行予定

    Args:
        ohlc_data: OHLCVデータ
        threshold_type: 閾値タイプ（rolling_max/rolling_min/sma/ema）
        period: 期間
        direction: ブレイク方向（upward/downward）
        price_column: 価格カラム（high/low/close）

    Returns:
        pd.Series[bool]: ブレイクアウト発生時にTrue
    """
    logger.warning(
        "threshold_breakout_signal はレガシー関数です。"
        "period_breakout_signal または ma_breakout_signal に移行してください。"
    )

    # 価格データ取得
    if price_column == "high":
        price = ohlc_data["High"]
    elif price_column == "low":
        price = ohlc_data["Low"]
    else:
        price = ohlc_data["Close"]

    # 統合版関数への変換
    if threshold_type in ["rolling_max", "rolling_min"]:
        # period_breakout_signalに変換
        dir_map = {"upward": "high", "downward": "low"}
        return period_breakout_signal(
            price,
            period=period,
            direction=dir_map.get(direction, "high"),
            condition="break",
            lookback_days=1,
        )
    elif threshold_type in ["sma", "ema"]:
        # ma_breakout_signalに変換
        dir_map = {"upward": "above", "downward": "below"}
        return ma_breakout_signal(
            price,
            period=period,
            ma_type=threshold_type,
            direction=dir_map.get(direction, "above"),
            lookback_days=1,
        )
    else:
        raise ValueError(f"未対応の閾値タイプ: {threshold_type}")
