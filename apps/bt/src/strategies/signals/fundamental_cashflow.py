"""
財務指標シグナル — キャッシュフロー系・時価総額系

営業CF・営業CF/純利益・簡易FCF・CFO/FCF利回り・CFO/FCFマージン・時価総額に基づくシグナルを提供
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from src.utils.financial import calc_market_cap

from .fundamental_helpers import (
    _calc_consecutive_threshold_signal,
    _calc_growth_signal,
    _calc_threshold_signal,
)


def _calc_margin_percent(
    numerator: pd.Series[float],
    sales: pd.Series[float],
) -> pd.Series[float]:
    """売上高が正の期間のみを分母にしてマージン(%)を計算する。"""
    return (numerator / sales.where(sales > 0, np.nan)) * 100


def operating_cash_flow_threshold(
    operating_cash_flow: pd.Series[float],
    threshold: float = 0.0,
    condition: Literal["above", "below"] = "above",
    consecutive_periods: int = 1,
) -> pd.Series[bool]:
    """
    営業キャッシュフロー閾値シグナル

    営業キャッシュフロー（CFO）が指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        threshold: 営業CF閾値（デフォルト0.0）
        condition: 条件（above=閾値より大きい、below=閾値より小さい）
        consecutive_periods: 連続期間数（直近N回分の決算発表で条件を満たす必要がある）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 営業CFがNaNの場合はFalseを返す
        - consecutive_periods > 1 の場合、直近N回分の決算発表で条件を満たす必要がある
        - 推奨period_type: "2Q" または "all"（営業CFは中間決算でも発表）
    """
    if consecutive_periods > 1:
        return _calc_consecutive_threshold_signal(
            operating_cash_flow, threshold, condition, consecutive_periods
        )
    return _calc_threshold_signal(
        operating_cash_flow, threshold, condition, require_positive=False
    )


def cfo_to_net_profit_ratio_threshold(
    operating_cash_flow: pd.Series[float],
    net_profit: pd.Series[float],
    threshold: float = 1.0,
    condition: Literal["above", "below"] = "above",
    consecutive_periods: int = 1,
) -> pd.Series[bool]:
    """
    営業CF/純利益 比率シグナル

    営業CF/純利益 比率が指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        net_profit: 純利益データ（日次インデックスに補完済み想定）
        threshold: 比率閾値（デフォルト1.0）
        condition: 条件（above=閾値以上、below=閾値未満）
        consecutive_periods: 連続期間数（直近N回分の決算発表で条件を満たす必要がある）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 純利益が0の場合は比率を無効値（NaN）として扱う
        - 純利益が負の場合も計算対象（比率は負値になり得る）
        - consecutive_periods > 1 の場合、直近N回分の決算発表で条件を満たす必要がある
        - 推奨period_type: "FY"（純利益・営業CFの整合を優先）
    """
    ratio = operating_cash_flow / net_profit.where(net_profit != 0, np.nan)

    if consecutive_periods > 1:
        return _calc_consecutive_release_threshold_signal(
            ratio,
            threshold,
            condition,
            consecutive_periods,
            operating_cash_flow,
            net_profit,
        )
    return _calc_threshold_signal(ratio, threshold, condition, require_positive=False)


def simple_fcf_threshold(
    operating_cash_flow: pd.Series[float],
    investing_cash_flow: pd.Series[float],
    threshold: float = 0.0,
    condition: Literal["above", "below"] = "above",
    consecutive_periods: int = 1,
) -> pd.Series[bool]:
    """
    簡易FCF（CFO + CFI）閾値シグナル

    簡易フリーキャッシュフロー（営業CF + 投資CF）が指定した条件で
    閾値と比較してTrueを返すシグナル

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        investing_cash_flow: 投資キャッシュフローデータ（日次インデックスに補完済み想定）
        threshold: 簡易FCF閾値（デフォルト0.0）
        condition: 条件（above=閾値以上、below=閾値以下）
        consecutive_periods: 連続期間数（直近N回分の決算発表で条件を満たす必要がある）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 営業CFまたは投資CFがNaNの場合はFalseを返す
        - 簡易FCF = 営業CF + 投資CF（通常、投資CFは負の値）
        - consecutive_periods > 1 の場合、直近N回分の決算発表で条件を満たす必要がある
        - 推奨period_type: "FY"（CFデータはFYで一貫性あり）
    """
    fcf = operating_cash_flow + investing_cash_flow
    if consecutive_periods > 1:
        return _calc_consecutive_threshold_signal(
            fcf, threshold, condition, consecutive_periods
        )
    return _calc_threshold_signal(fcf, threshold, condition, require_positive=False)


def cfo_yield_threshold(
    close: pd.Series[float],
    operating_cash_flow: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    CFO利回り（営業キャッシュフロー/時価総額）シグナル

    CFO利回り = (CFO / 時価総額) × 100 [%]
    時価総額 = 終値 × 株式数

    Args:
        close: 終値データ（日次）
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        shares_outstanding: 発行済み株式数（日次インデックスに補完済み想定）
        treasury_shares: 自己株式数（日次インデックスに補完済み想定）
        threshold: CFO利回り閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）
        use_floating_shares: 株式数の計算方法
            - True (デフォルト): 流通株式 = 発行済み - 自己株式
            - False: 発行済み株式全体

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 株式数が0以下またはNaNの場合はFalseを返す
        - 時価総額が0以下またはNaNの場合はFalseを返す
        - CFOが負の場合も計算対象（負の利回りとなる）
        - 推奨period_type: "FY"（CFOデータはFYで一貫性あり）
    """
    market_cap = calc_market_cap(
        close, shares_outstanding, treasury_shares, use_floating_shares
    )
    cfo_yield = (operating_cash_flow / market_cap.where(market_cap > 0, np.nan)) * 100

    return _calc_threshold_signal(cfo_yield, threshold, condition, require_positive=False)


def cfo_margin_threshold(
    operating_cash_flow: pd.Series[float],
    sales: pd.Series[float],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    CFOマージン（営業キャッシュフロー/売上高）シグナル

    CFOマージン = (CFO / 売上高) × 100 [%]

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        sales: 売上高データ（日次インデックスに補完済み想定）
        threshold: CFOマージン閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 売上高が0以下またはNaNの場合はFalseを返す
        - CFOが負の場合も計算対象（負のマージンとなる）
        - 推奨period_type: "FY"（CFデータはFYで一貫性あり）
    """
    cfo_margin = _calc_margin_percent(operating_cash_flow, sales)
    return _calc_threshold_signal(cfo_margin, threshold, condition, require_positive=False)


def simple_fcf_yield_threshold(
    close: pd.Series[float],
    operating_cash_flow: pd.Series[float],
    investing_cash_flow: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    簡易FCF利回り（(CFO+CFI)/時価総額）シグナル

    simple FCF利回り = ((CFO + CFI) / 時価総額) × 100 [%]
    時価総額 = 終値 × 株式数

    Args:
        close: 終値データ（日次）
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        investing_cash_flow: 投資キャッシュフローデータ（日次インデックスに補完済み想定）
        shares_outstanding: 発行済み株式数（日次インデックスに補完済み想定）
        treasury_shares: 自己株式数（日次インデックスに補完済み想定）
        threshold: 簡易FCF利回り閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）
        use_floating_shares: 株式数の計算方法
            - True (デフォルト): 流通株式 = 発行済み - 自己株式
            - False: 発行済み株式全体

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 株式数が0以下またはNaNの場合はFalseを返す
        - 時価総額が0以下またはNaNの場合はFalseを返す
        - 簡易FCF = 営業CF + 投資CF（通常、投資CFは負の値）
        - 簡易FCFが負の場合も計算対象（負の利回りとなる）
        - 推奨period_type: "FY"（CFデータはFYで一貫性あり）
    """
    market_cap = calc_market_cap(
        close, shares_outstanding, treasury_shares, use_floating_shares
    )
    fcf = operating_cash_flow + investing_cash_flow
    fcf_yield = (fcf / market_cap.where(market_cap > 0, np.nan)) * 100

    return _calc_threshold_signal(fcf_yield, threshold, condition, require_positive=False)


def simple_fcf_margin_threshold(
    operating_cash_flow: pd.Series[float],
    investing_cash_flow: pd.Series[float],
    sales: pd.Series[float],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    簡易FCFマージン（(CFO+CFI)/売上高）シグナル

    簡易FCFマージン = ((CFO + CFI) / 売上高) × 100 [%]

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        investing_cash_flow: 投資キャッシュフローデータ（日次インデックスに補完済み想定）
        sales: 売上高データ（日次インデックスに補完済み想定）
        threshold: 簡易FCFマージン閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 売上高が0以下またはNaNの場合はFalseを返す
        - 簡易FCF = 営業CF + 投資CF（通常、投資CFは負の値）
        - 簡易FCFが負の場合も計算対象（負のマージンとなる）
        - 推奨period_type: "FY"（CFデータはFYで一貫性あり）
    """
    fcf = operating_cash_flow + investing_cash_flow
    fcf_margin = _calc_margin_percent(fcf, sales)
    return _calc_threshold_signal(fcf_margin, threshold, condition, require_positive=False)


def _freeze_metric_by_release_dates(
    metric: pd.Series[float],
    *release_sources: pd.Series,
) -> pd.Series[float]:
    """開示更新タイミングのみ値を採用し、日次へffillした系列を返す。"""
    if metric.empty:
        return metric

    release_mask = _build_release_mask(metric.index, *release_sources)
    return metric.where(release_mask).ffill()


def _calc_consecutive_release_threshold_signal(
    metric: pd.Series[float],
    threshold: float,
    condition: Literal["above", "below"],
    consecutive_periods: int,
    *release_sources: pd.Series,
) -> pd.Series[bool]:
    """
    開示更新タイミングを基準に連続閾値判定を行う。

    metric 値自体が同値継続でも、source が更新されていれば新しい決算発表として扱う。
    """
    if metric.empty:
        return metric.astype(bool)

    release_mask = _build_release_mask(metric.index, *release_sources)
    release_dates = release_mask[release_mask].index
    if len(release_dates) < consecutive_periods:
        return pd.Series(False, index=metric.index)

    release_values = metric.loc[release_dates]
    meets_threshold = (
        release_values >= threshold if condition == "above" else release_values < threshold
    )
    valid_release = meets_threshold & release_values.notna()
    consecutive_met = (
        valid_release
        .rolling(window=consecutive_periods, min_periods=consecutive_periods)
        .sum()
        .eq(consecutive_periods)
    )

    daily_result = pd.Series(np.nan, index=metric.index)
    daily_result.loc[consecutive_met.index] = consecutive_met.astype(float)
    return daily_result.ffill().fillna(0.0).astype(bool) & metric.notna()


def _build_release_mask(
    index: pd.Index,
    *release_sources: pd.Series,
) -> pd.Series[bool]:
    """開示更新タイミングを表す bool マスクを構築する。"""
    release_mask = pd.Series(False, index=index)
    for source in release_sources:
        aligned_source = source.reindex(index)
        # NaN継続は「更新なし」とみなす。NaN -> 値 の遷移は更新として扱う。
        source_release = aligned_source.notna() & aligned_source.ne(aligned_source.shift(1))
        release_mask |= source_release.fillna(False)

    if len(release_mask) > 0:
        release_mask.iloc[0] = True
    return release_mask


def is_growing_cfo_yield(
    close: pd.Series[float],
    operating_cash_flow: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    growth_threshold: float = 0.1,
    periods: int = 1,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    CFO利回り成長率シグナル

    CFO利回り = (CFO / 時価総額) × 100 [%]
    開示更新日の利回りのみを採用して日次へffillし、決算期間ベースの成長率を判定する。
    """
    market_cap = calc_market_cap(
        close, shares_outstanding, treasury_shares, use_floating_shares
    )
    cfo_yield = (operating_cash_flow / market_cap.where(market_cap > 0, np.nan)) * 100
    release_sources: list[pd.Series] = [operating_cash_flow, shares_outstanding]
    if use_floating_shares:
        release_sources.append(treasury_shares)
    cfo_yield_release = _freeze_metric_by_release_dates(
        cfo_yield,
        *release_sources,
    )
    return _calc_growth_signal(cfo_yield_release, periods, growth_threshold, condition)


def is_growing_simple_fcf_yield(
    close: pd.Series[float],
    operating_cash_flow: pd.Series[float],
    investing_cash_flow: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    growth_threshold: float = 0.1,
    periods: int = 1,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    簡易FCF利回り成長率シグナル

    簡易FCF利回り = ((CFO + CFI) / 時価総額) × 100 [%]
    開示更新日の利回りのみを採用して日次へffillし、決算期間ベースの成長率を判定する。
    """
    market_cap = calc_market_cap(
        close, shares_outstanding, treasury_shares, use_floating_shares
    )
    fcf = operating_cash_flow + investing_cash_flow
    fcf_yield = (fcf / market_cap.where(market_cap > 0, np.nan)) * 100
    release_sources: list[pd.Series] = [
        operating_cash_flow,
        investing_cash_flow,
        shares_outstanding,
    ]
    if use_floating_shares:
        release_sources.append(treasury_shares)
    fcf_yield_release = _freeze_metric_by_release_dates(
        fcf_yield,
        *release_sources,
    )
    return _calc_growth_signal(fcf_yield_release, periods, growth_threshold, condition)


def market_cap_threshold(
    close: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    threshold: float = 100.0,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    時価総額閾値シグナル

    時価総額が指定した条件で閾値（億円単位）と比較してTrueを返すシグナル

    Args:
        close: 終値データ（日次）
        shares_outstanding: 発行済み株式数（日次インデックスに補完済み想定）
        treasury_shares: 自己株式数（日次インデックスに補完済み想定）
        threshold: 時価総額閾値（億円単位、100.0 = 100億円）
        condition: 条件（above=閾値以上、below=閾値未満）
        use_floating_shares: 株式数の計算方法
            - True (デフォルト): 流通株式 = 発行済み - 自己株式
            - False: 発行済み株式全体

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 株式数が0以下またはNaNの場合はFalseを返す
        - 閾値は億円単位（内部で1億=1e8円に変換）
        - 推奨用途: 大型株フィルター（entry: above）、小型株除外（exit: below）
    """
    market_cap = calc_market_cap(close, shares_outstanding, treasury_shares, use_floating_shares)
    # 円→億円に変換
    market_cap_oku = market_cap / 1e8

    return _calc_threshold_signal(market_cap_oku, threshold, condition, require_positive=True)
