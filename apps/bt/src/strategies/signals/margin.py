"""
信用残高シグナル実装

VectorBTベースの信用残高関連シグナル関数を提供
"""

import pandas as pd

from src.models.signals import MarginSignalParams


def margin_balance_percentile_signal(
    margin_balance: pd.Series,
    params: MarginSignalParams | None = None,
    lookback_period: int | None = None,
    percentile_threshold: float | None = None,
) -> pd.Series:
    """
    買い残高パーセンタイルシグナル（一定期間内の買い残高が下位Xパーセンタイルにあるかを判定）

    Args:
        margin_balance: 買い残高データ（FinancialMetricsSchemaで検証済み）
        params: MarginSignalParamsインスタンス（優先）
        lookback_period: 参照期間（paramsが未指定の場合のフォールバック、デフォルト150日）
        percentile_threshold: パーセンタイル闾値（paramsが未指定の場合のフォールバック、デフォルト 0.2 = 20%）

    Returns:
        pd.Series: 買い残高が指定期間で下位パーセンタイルにある場合にTrue

    Raises:
        ValueError: パラメータが無効な場合
    """
    # パラメータ解決（MarginSignalParamsを優先）
    if params is not None:
        if not params.enabled:
            # シグナルが無効の場合は全てFalseを返す
            return pd.Series(False, index=margin_balance.index, dtype=bool)
        final_lookback = params.lookback_period
        final_threshold = params.percentile_threshold
    else:
        # フォールバック値使用
        final_lookback = lookback_period if lookback_period is not None else 150
        final_threshold = (
            percentile_threshold if percentile_threshold is not None else 0.2
        )

        # フォールバック値の基本バリデーション
        if final_lookback <= 0 or final_lookback > 500:
            raise ValueError(
                f"lookback_period must be in range (0, 500], got {final_lookback}"
            )
        if final_threshold <= 0 or final_threshold >= 1:
            raise ValueError(
                f"percentile_threshold must be in range (0, 1), got {final_threshold}"
            )

    # ローリングパーセンタイル計算
    rolling_percentile = margin_balance.rolling(
        window=final_lookback, min_periods=final_lookback
    ).quantile(final_threshold)

    # パーセンタイル条件：現在値 <= ローリングパーセンタイル
    percentile_condition = margin_balance <= rolling_percentile

    return percentile_condition.fillna(False)
