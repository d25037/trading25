"""Universe-relative price/SMA divergence bucket signal."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from src.shared.models.signals import normalize_bool_series

_PRICE_BUCKETS_REQUIRING_VOLUME_SPLIT = {"q1", "q10", "q456"}


def _assign_ntile(
    ranks: pd.Series,
    counts: pd.Series,
    *,
    tiles: int,
) -> pd.Series:
    rank_array = ranks.to_numpy(dtype=int)
    count_array = counts.to_numpy(dtype=int)

    base_size = count_array // tiles
    remainder = count_array % tiles
    threshold = remainder * (base_size + 1)

    result = np.empty_like(rank_array)
    first_mask = rank_array <= threshold

    result[first_mask] = np.ceil(
        rank_array[first_mask] / (base_size[first_mask] + 1)
    ).astype(int)

    if (~first_mask).any():
        denominator = np.maximum(base_size[~first_mask], 1)
        result[~first_mask] = (
            remainder[~first_mask]
            + np.ceil(
                (rank_array[~first_mask] - threshold[~first_mask]) / denominator
            ).astype(int)
        )

    return pd.Series(result, index=ranks.index, dtype="Int64")


def _extract_daily_frame(payload: object) -> pd.DataFrame | None:
    if not isinstance(payload, Mapping):
        return None
    daily = payload.get("daily")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        return None
    if not {"Close", "Volume"}.issubset(daily.columns):
        return None
    return daily


def build_universe_rank_bucket_feature_panel(
    *,
    universe_multi_data: Mapping[str, Mapping[str, object]],
    universe_member_codes: Sequence[str] | None,
    price_sma_period: int,
    volume_short_period: int,
    volume_long_period: int,
) -> pd.DataFrame:
    candidate_codes = (
        list(dict.fromkeys(str(code) for code in universe_member_codes))
        if universe_member_codes
        else list(universe_multi_data.keys())
    )

    frames: list[pd.DataFrame] = []
    for code in candidate_codes:
        daily = _extract_daily_frame(universe_multi_data.get(code))
        if daily is None:
            continue
        frame = daily.loc[:, ["Close", "Volume"]].copy()
        frame["date"] = pd.DatetimeIndex(frame.index)
        frame["stock_code"] = str(code)
        frames.append(frame.reset_index(drop=True))

    if not frames:
        return pd.DataFrame()

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.sort_values(["stock_code", "date"]).reset_index(drop=True)

    grouped = panel.groupby("stock_code", sort=False)
    price_sma = grouped["Close"].rolling(
        window=price_sma_period,
        min_periods=price_sma_period,
    ).mean().reset_index(level=0, drop=True)
    volume_short = grouped["Volume"].rolling(
        window=volume_short_period,
        min_periods=volume_short_period,
    ).mean().reset_index(level=0, drop=True)
    volume_long = grouped["Volume"].rolling(
        window=volume_long_period,
        min_periods=volume_long_period,
    ).mean().reset_index(level=0, drop=True)

    features = panel.loc[:, ["date", "stock_code"]].copy()
    features["price_ratio"] = panel["Close"] / price_sma.replace(0.0, np.nan) - 1.0
    features["volume_ratio"] = volume_short / volume_long.replace(0.0, np.nan)
    features = features.dropna(subset=["price_ratio", "volume_ratio"])
    if features.empty:
        return features

    features = features.sort_values(
        ["date", "price_ratio", "stock_code"],
        ascending=[True, False, True],
    )
    features["price_rank"] = features.groupby("date").cumcount() + 1
    features["price_count"] = features.groupby("date")["stock_code"].transform("size")
    features["price_decile"] = _assign_ntile(
        features["price_rank"],
        features["price_count"],
        tiles=10,
    )
    features["price_bucket"] = np.select(
        [
            features["price_decile"].eq(1),
            features["price_decile"].eq(10),
            features["price_decile"].isin([4, 5, 6]),
        ],
        ["q1", "q10", "q456"],
        default="other",
    )

    split_mask = features["price_bucket"].isin(_PRICE_BUCKETS_REQUIRING_VOLUME_SPLIT)
    features["volume_bucket"] = pd.Series(index=features.index, dtype="object")
    if split_mask.any():
        split_frame = features.loc[
            split_mask, ["date", "price_bucket", "stock_code", "volume_ratio"]
        ].copy()
        split_frame = split_frame.sort_values(
            ["date", "price_bucket", "volume_ratio", "stock_code"],
            ascending=[True, True, False, True],
        )
        split_frame["volume_rank"] = (
            split_frame.groupby(["date", "price_bucket"]).cumcount() + 1
        )
        split_frame["volume_count"] = split_frame.groupby(
            ["date", "price_bucket"]
        )["stock_code"].transform("size")
        split_frame["volume_half_rank"] = _assign_ntile(
            split_frame["volume_rank"],
            split_frame["volume_count"],
            tiles=2,
        )
        features.loc[split_frame.index, "volume_bucket"] = np.where(
            split_frame["volume_half_rank"].eq(1),
            "high",
            "low",
        )

    return features


def universe_rank_bucket_signal(
    *,
    stock_code: str,
    target_index: pd.Index,
    universe_multi_data: Mapping[str, Mapping[str, object]],
    universe_member_codes: Sequence[str] | None = None,
    feature_panel: pd.DataFrame | None = None,
    price_sma_period: int = 50,
    volume_short_period: int = 20,
    volume_long_period: int = 80,
    price_bucket: str = "q1",
    volume_bucket: str = "any",
    min_constituents: int = 10,
) -> pd.Series:
    """Return True when the target stock sits in the requested price/SMA universe bucket."""

    if min_constituents < 2:
        raise ValueError("min_constituents must be >= 2")
    if price_bucket not in {"q1", "q10", "q456", "other"}:
        raise ValueError(f"unsupported price_bucket: {price_bucket}")
    if volume_bucket not in {"any", "high", "low"}:
        raise ValueError(f"unsupported volume_bucket: {volume_bucket}")

    reference_index = pd.Index(target_index)
    result = pd.Series(False, index=reference_index, dtype=bool)
    if reference_index.empty:
        return result

    if feature_panel is None:
        feature_panel = build_universe_rank_bucket_feature_panel(
            universe_multi_data=universe_multi_data,
            universe_member_codes=universe_member_codes,
            price_sma_period=price_sma_period,
            volume_short_period=volume_short_period,
            volume_long_period=volume_long_period,
        )
    if feature_panel.empty:
        logger.debug("Universe rank bucket signal skipped: no valid universe feature rows")
        return result

    feature_panel = feature_panel.loc[
        feature_panel["price_count"] >= min_constituents
    ].copy()
    if feature_panel.empty:
        logger.debug(
            "Universe rank bucket signal skipped: universe never reached min_constituents={}",
            min_constituents,
        )
        return result

    target_rows = feature_panel.loc[feature_panel["stock_code"] == str(stock_code)].copy()
    if target_rows.empty:
        logger.debug(
            "Universe rank bucket signal skipped: target '{}' not found in universe rows",
            stock_code,
        )
        return result

    target_rows = target_rows.set_index("date").sort_index()
    matched = target_rows["price_bucket"].eq(price_bucket)
    if volume_bucket != "any":
        matched &= target_rows["volume_bucket"].eq(volume_bucket)

    result = normalize_bool_series(matched.reindex(reference_index))
    logger.debug(
        "Universe rank bucket signal generated: stock={} price_bucket={} volume_bucket={} "
        "true_count={}/{}",
        stock_code,
        price_bucket,
        volume_bucket,
        int(result.sum()),
        len(result),
    )
    return result
