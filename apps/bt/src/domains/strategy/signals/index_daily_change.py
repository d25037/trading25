"""
指数前日比シグナル

指数（TOPIX等）の前日比に基づくシグナル生成機能を提供します。
短期スイングトレードで翌日の利益確定売りを避ける、または超短期の逆張り状態を目指す目的で使用します。

用途:
- エントリーフィルター: 市場が大きく上昇していない日にエントリー（前日比 ≤ +X%）
- エグジットトリガー: 市場が大きく上昇した日にエグジット（前日比 > +X%）
"""

import pandas as pd
from typing import cast


def index_daily_change_signal(
    index_data: pd.DataFrame,
    max_daily_change_pct: float = 1.0,
    direction: str = "below",
) -> pd.Series:
    """
    指数の前日比シグナル

    Args:
        index_data: 指数のOHLCデータ（Close カラム必須）
        max_daily_change_pct: 前日比閾値（%単位、例: 1.0 = +1.0%）
        direction: 判定方向
            - "below": 前日比 ≤ +X% の日にTrue（エントリーフィルター用）
            - "above": 前日比 > +X% の日にTrue（エグジットトリガー用）

    Returns:
        pd.Series[bool]: シグナル結果

    Examples:
        >>> # エントリーフィルター: 市場が+1.0%以下の日のみエントリー
        >>> entry_signal = index_daily_change_signal(
        ...     topix_data,
        ...     max_daily_change_pct=1.0,
        ...     direction="below"
        ... )

        >>> # エグジットトリガー: 市場が+1.0%超の日にエグジット
        >>> exit_signal = index_daily_change_signal(
        ...     topix_data,
        ...     max_daily_change_pct=1.0,
        ...     direction="above"
        ... )
    """
    # 入力検証
    if index_data is None or index_data.empty:
        raise ValueError("index_data が空またはNoneです")

    if "Close" not in index_data.columns:
        raise ValueError("index_data に 'Close' カラムが必要です")

    # Close価格を取得
    close: pd.Series = index_data["Close"]

    # 前日比（パーセント変化率）を計算
    # pct_change() は (今日 - 昨日) / 昨日 の計算
    daily_change_pct: pd.Series = close.pct_change() * 100.0  # %単位に変換

    # 浮動小数点誤差対策: 小数点以下4桁に丸める（0.0001%の精度）
    # NaN値がある場合はfloat型に明示的に変換
    if daily_change_pct.dtype == object:
        daily_change_pct = pd.to_numeric(daily_change_pct, errors="coerce")
    daily_change_pct = daily_change_pct.round(4)

    # 閾値（%）での判定
    threshold_pct = max_daily_change_pct

    # 方向に応じたシグナル生成
    if direction == "below":
        # 前日比が閾値以下の日にTrue（エントリーフィルター用）
        signal = daily_change_pct <= threshold_pct
    elif direction == "above":
        # 前日比が閾値を超える日にTrue（エグジットトリガー用）
        signal = daily_change_pct > threshold_pct
    else:
        raise ValueError(
            f"direction は 'below' または 'above' である必要があります: {direction}"
        )

    # NaN値をFalseに置換（初日の前日比はNaNになる）
    signal = cast(pd.Series, signal.fillna(False))

    return signal


def index_daily_change_multi_signal(
    index_data: pd.DataFrame,
    stock_count: int,
    max_daily_change_pct: float = 1.0,
    direction: str = "below",
) -> pd.DataFrame:
    """
    複数銘柄向け指数前日比シグナル（全銘柄に同一シグナル適用）

    Args:
        index_data: 指数のOHLCデータ
        stock_count: 銘柄数
        max_daily_change_pct: 前日比閾値（%単位）
        direction: 判定方向（"below" or "above"）

    Returns:
        pd.DataFrame: 各銘柄に対するシグナル（全銘柄同一）

    Examples:
        >>> # 10銘柄のポートフォリオで同一シグナルを適用
        >>> multi_signal = index_daily_change_multi_signal(
        ...     topix_data,
        ...     stock_count=10,
        ...     max_daily_change_pct=1.0,
        ...     direction="below"
        ... )
    """
    # 基本シグナルを生成
    base_signal = index_daily_change_signal(index_data, max_daily_change_pct, direction)

    # 全銘柄に同一シグナルを適用
    signal_dict = {f"stock_{i}": base_signal for i in range(stock_count)}

    return pd.DataFrame(signal_dict)


def calculate_index_statistics(
    index_data: pd.DataFrame,
    window: int = 20,
) -> pd.DataFrame:
    """
    指数の統計情報を計算（デバッグ・分析用）

    Args:
        index_data: 指数のOHLCデータ
        window: 移動平均期間

    Returns:
        pd.DataFrame: 統計情報（前日比、移動平均、標準偏差等）

    Examples:
        >>> stats = calculate_index_statistics(topix_data, window=20)
        >>> print(stats[["daily_change_pct", "ma_daily_change", "std_daily_change"]])
    """
    close = index_data["Close"]

    # 前日比（%）
    daily_change_pct = close.pct_change() * 100.0

    # 移動平均
    ma_daily_change = daily_change_pct.rolling(window=window).mean()

    # 標準偏差
    std_daily_change = daily_change_pct.rolling(window=window).std()

    # 統計情報DataFrame作成
    stats = pd.DataFrame(
        {
            "close": close,
            "daily_change_pct": daily_change_pct,
            "ma_daily_change": ma_daily_change,
            "std_daily_change": std_daily_change,
        }
    )

    return stats


def index_volatility_adjusted_signal(
    index_data: pd.DataFrame,
    base_threshold: float = 1.0,
    volatility_window: int = 20,  # 将来の拡張用パラメータ
    direction: str = "below",
) -> pd.Series:
    """
    ボラティリティ調整済み指数前日比シグナル（将来拡張用）

    Args:
        index_data: 指数のOHLCデータ
        base_threshold: 基準閾値（%単位）
        volatility_window: ボラティリティ計算期間（将来の拡張用）
        direction: 判定方向（"below" or "above"）

    Returns:
        pd.Series[bool]: シグナル結果

    Note:
        現在は基本実装と同じですが、将来的にボラティリティ調整を追加する予定
        volatility_windowは将来の拡張時に使用します
    """
    # 現在は基本実装と同じ
    # 将来的には標準偏差に基づく動的閾値調整を実装（volatility_windowを使用）
    _ = volatility_window  # 将来の拡張用に明示的に保持
    return index_daily_change_signal(index_data, base_threshold, direction)
