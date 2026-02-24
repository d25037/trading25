"""
セクターシグナル関数

33業種セクターインデックスOHLCデータを活用したシグナル:
- A. Sector Strength Ranking — 複合スコアで上位Nセクターのみエントリー許可
- B. Sector Rotation Phase — RRG的4象限分類（Entry + Exit）
- D. Sector Volatility Regime — 低ボラ環境フィルタ（Entry + Exit）
"""

import numpy as np
import pandas as pd
from loguru import logger


def sector_strength_ranking_signal(
    sector_data: dict[str, pd.DataFrame],
    stock_sector_name: str,
    benchmark_close: pd.Series,
    momentum_period: int = 20,
    sharpe_period: int = 60,
    top_n: int = 10,
    momentum_weight: float = 0.4,
    sharpe_weight: float = 0.4,
    relative_weight: float = 0.2,
    selection_mode: str = "top",
) -> "pd.Series[bool]":
    """
    セクター強度ランキングシグナル

    全セクターの日次複合スコア（モメンタム + シャープレシオ + TOPIX対比RS）を計算し、
    銘柄が属するセクターが上位または下位N位以内であればTrueを返す。

    Args:
        sector_data: 全セクターインデックスOHLCデータ {sector_name: DataFrame}
        stock_sector_name: 当該銘柄のセクター名
        benchmark_close: ベンチマーク（TOPIX等）終値 pd.Series[float]
        momentum_period: モメンタム計算期間（日数）
        sharpe_period: シャープレシオ計算期間（日数）
        top_n: 選択するセクター数（上位/下位N）
        momentum_weight: モメンタムスコア重み
        sharpe_weight: シャープレシオスコア重み
        relative_weight: 相対強度スコア重み
        selection_mode: 選択モード（"top"=上位N、"bottom"=下位N）

    Returns:
        pd.Series[bool]: 当該銘柄のセクターが選択範囲に入っていればTrue
    """
    if selection_mode not in ("top", "bottom"):
        raise ValueError(
            f"Invalid selection_mode: {selection_mode}. Must be 'top' or 'bottom'"
        )

    logger.debug(
        f"セクター強度ランキング: sector={stock_sector_name}, "
        f"momentum_period={momentum_period}, sharpe_period={sharpe_period}, "
        f"top_n={top_n}, selection_mode={selection_mode}"
    )

    if not sector_data:
        logger.warning("セクターデータが空です")
        return pd.Series(False, index=benchmark_close.index, dtype=bool)

    if stock_sector_name not in sector_data:
        logger.warning(f"セクター '{stock_sector_name}' がデータに含まれていません")
        return pd.Series(False, index=benchmark_close.index, dtype=bool)

    # 全セクターの終値を取得
    sector_closes: dict[str, "pd.Series[float]"] = {}
    for name, df in sector_data.items():
        if "Close" in df.columns and not df["Close"].isna().all():
            sector_closes[name] = df["Close"].astype(float)

    if not sector_closes:
        logger.warning("有効なセクター終値データがありません")
        return pd.Series(False, index=benchmark_close.index, dtype=bool)

    # 共通インデックスを構築
    reference_index = benchmark_close.index

    # 各セクターの日次複合スコアを計算
    sector_scores: dict[str, "pd.Series[float]"] = {}

    benchmark_aligned: pd.Series[float] = benchmark_close.reindex(reference_index).astype(float)

    for name, close in sector_closes.items():
        # インデックスを揃える
        close_aligned: pd.Series[float] = close.reindex(reference_index)

        # 日次リターン
        daily_return: pd.Series[float] = close_aligned.pct_change()

        # 1. モメンタムスコア: momentum_period日間リターン
        momentum: pd.Series[float] = close_aligned / close_aligned.shift(momentum_period) - 1.0

        # 2. シャープレシオスコア: sharpe_period日間のSharpe
        rolling_mean: pd.Series[float] = daily_return.rolling(
            window=sharpe_period, min_periods=max(1, sharpe_period // 2)
        ).mean()
        rolling_std: pd.Series[float] = daily_return.rolling(
            window=sharpe_period, min_periods=max(1, sharpe_period // 2)
        ).std()
        # ゼロ除算回避
        sharpe: pd.Series[float] = rolling_mean / rolling_std.replace(0, np.nan) * np.sqrt(252)

        # 3. 相対強度スコア: セクター終値 / ベンチマーク終値の変化率
        rs_ratio: pd.Series[float] = close_aligned / benchmark_aligned.replace(0, np.nan)
        relative_strength: pd.Series[float] = rs_ratio / rs_ratio.shift(momentum_period) - 1.0

        # 複合スコア（NaN保持: データ不足期間はランキング対象外にする）
        # 全構成要素がNaNの場合のみNaN、それ以外は有効値を使用
        composite: pd.Series[float] = (
            momentum_weight * momentum.fillna(0.0)
            + sharpe_weight * sharpe.fillna(0.0)
            + relative_weight * relative_strength.fillna(0.0)
        )
        # ウォームアップ期間マスク: 全構成要素がNaNの場合はスコアもNaN
        all_nan_mask: pd.Series[bool] = momentum.isna() & sharpe.isna() & relative_strength.isna()
        composite = composite.where(~all_nan_mask, other=np.nan)
        sector_scores[name] = composite

    # 日次ランキングを構築
    score_df = pd.DataFrame(sector_scores, index=reference_index)

    # ランキング: 各日で全セクターのスコアを降順でランク付け
    # rank(ascending=False) → スコアが高いほどランクが小さい（1位が最高）
    # na_option="keep" → NaN値はランキングから除外（NaNのまま）
    rank_df = score_df.rank(axis=1, ascending=False, method="min", na_option="keep")

    # 当該銘柄のセクターが選択範囲内かチェック
    if stock_sector_name not in rank_df.columns:
        logger.warning(f"セクター '{stock_sector_name}' のランキングデータがありません")
        return pd.Series(False, index=reference_index, dtype=bool)

    if selection_mode == "top":
        # 上位N位以内: ランクがtop_n以下
        result = (rank_df[stock_sector_name] <= top_n).fillna(False)
    else:
        # 下位N位以内: ランクが (有効セクター数 - top_n + 1) 以上
        # 日ごとに有効セクター数が異なるため動的に計算
        total_sectors: pd.Series[int] = rank_df.notna().sum(axis=1)
        bottom_threshold: pd.Series[int] = (total_sectors - top_n + 1).clip(lower=1)
        # top_n >= total_sectorsの場合、thresholdは1となり全セクターが選択される
        result = (rank_df[stock_sector_name] >= bottom_threshold).fillna(False)

    logger.debug(
        f"セクター強度ランキング完了: True={result.sum()}/{len(result)} "
        f"(sector={stock_sector_name}, mode={selection_mode})"
    )
    return result


def sector_rotation_phase_signal(
    sector_close: "pd.Series[float]",
    benchmark_close: "pd.Series[float]",
    rs_period: int = 20,
    direction: str = "leading",
) -> "pd.Series[bool]":
    """
    RRG的セクターローテーション位相シグナル

    RS_Ratio（相対強度比）とRS_Momentum（モメンタム）の2軸で
    セクターの位相を判定する。

    - direction="leading": RS > MA AND Momentum > 0（先行局面 → Entry用）
    - direction="weakening": RS > MA AND Momentum < 0（衰退局面 → Exit用）

    Args:
        sector_close: セクターインデックス終値 pd.Series[float]
        benchmark_close: ベンチマーク終値 pd.Series[float]
        rs_period: 相対強度移動平均期間（日数）
        direction: 判定方向 "leading" or "weakening"

    Returns:
        pd.Series[bool]: 条件を満たす日にTrue
    """
    logger.debug(
        f"セクターローテーション位相: rs_period={rs_period}, direction={direction}"
    )

    if direction not in ("leading", "weakening"):
        raise ValueError(f"Invalid direction: {direction}. Must be 'leading' or 'weakening'")

    # インデックスを揃える
    common_index = sector_close.index.intersection(benchmark_close.index)
    if len(common_index) == 0:
        logger.warning("セクターとベンチマークの共通日付がありません")
        return pd.Series(False, index=sector_close.index, dtype=bool)

    sector_aligned: pd.Series[float] = sector_close.reindex(common_index).astype(float)
    benchmark_aligned: pd.Series[float] = benchmark_close.reindex(common_index).astype(float)

    # RS_Ratio = sector / benchmark（正規化）
    rs_ratio: pd.Series[float] = sector_aligned / benchmark_aligned.replace(0, np.nan)

    # RS_MA = SMA(RS_Ratio, rs_period)
    rs_ma: pd.Series[float] = rs_ratio.rolling(
        window=rs_period, min_periods=max(1, rs_period // 2)
    ).mean()

    # RS_Momentum = RS_Ratio / RS_MA - 1
    rs_momentum: pd.Series[float] = rs_ratio / rs_ma.replace(0, np.nan) - 1.0

    # 位相判定
    rs_above_ma: pd.Series[bool] = rs_ratio > rs_ma

    if direction == "leading":
        # 先行局面: RS > MA AND Momentum > 0
        signal = rs_above_ma & (rs_momentum > 0)
    else:  # direction == "weakening"
        # 衰退局面: RS > MA AND Momentum < 0
        signal = rs_above_ma & (rs_momentum < 0)

    # 元のインデックスに合わせて返却
    result = signal.reindex(sector_close.index).fillna(False)
    logger.debug(
        f"セクターローテーション位相完了: True={result.sum()}/{len(result)} "
        f"(direction={direction})"
    )
    return result


def sector_volatility_regime_signal(
    sector_close: "pd.Series[float]",
    vol_period: int = 20,
    vol_ma_period: int = 60,
    direction: str = "low_vol",
    spike_multiplier: float = 1.5,
) -> "pd.Series[bool]":
    """
    セクターボラティリティレジームシグナル

    セクターの年率ボラティリティと長期平均を比較し、
    環境を判定する。

    - direction="low_vol": current_vol < vol_ma（低ボラ環境 → Entry用）
    - direction="high_vol": current_vol > vol_ma * spike_multiplier（高ボラ環境 → Exit用）

    Args:
        sector_close: セクターインデックス終値 pd.Series[float]
        vol_period: ボラティリティ計算期間（日数）
        vol_ma_period: ボラティリティ移動平均期間（日数）
        direction: 判定方向 "low_vol" or "high_vol"
        spike_multiplier: 高ボラ判定倍率（direction="high_vol"時に使用）

    Returns:
        pd.Series[bool]: 条件を満たす日にTrue
    """
    logger.debug(
        f"セクターボラティリティレジーム: vol_period={vol_period}, "
        f"vol_ma_period={vol_ma_period}, direction={direction}, "
        f"spike_multiplier={spike_multiplier}"
    )

    if direction not in ("low_vol", "high_vol"):
        raise ValueError(f"Invalid direction: {direction}. Must be 'low_vol' or 'high_vol'")

    sector_float: pd.Series[float] = sector_close.astype(float)

    # 日次リターン
    daily_return: pd.Series[float] = sector_float.pct_change()

    # 現在のボラティリティ（年率換算）
    current_vol: pd.Series[float] = daily_return.rolling(
        window=vol_period, min_periods=max(1, vol_period // 2)
    ).std() * np.sqrt(252)

    # ボラティリティの長期移動平均
    vol_ma: pd.Series[float] = current_vol.rolling(
        window=vol_ma_period, min_periods=max(1, vol_ma_period // 2)
    ).mean()

    # レジーム判定
    if direction == "low_vol":
        # 低ボラ環境: current_vol < vol_ma
        signal = current_vol < vol_ma
    else:  # direction == "high_vol"
        # 高ボラ環境: current_vol > vol_ma * spike_multiplier
        signal = current_vol > (vol_ma * spike_multiplier)

    result = signal.fillna(False)
    logger.debug(
        f"セクターボラティリティレジーム完了: True={result.sum()}/{len(result)} "
        f"(direction={direction})"
    )
    return result
