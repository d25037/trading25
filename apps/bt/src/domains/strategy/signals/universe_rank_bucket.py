"""Universe-relative price/SMA divergence bucket signal."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from src.shared.models.signals import normalize_bool_series


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
    if "Close" not in daily.columns:
        return None
    return daily


def build_universe_rank_bucket_feature_panel(
    *,
    universe_multi_data: Mapping[str, Mapping[str, object]],
    universe_member_codes: Sequence[str] | None,
    price_sma_period: int,
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
        frame = daily.loc[:, ["Close"]].copy()
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

    features = panel.loc[:, ["date", "stock_code"]].copy()
    features["price_ratio"] = panel["Close"] / price_sma.replace(0.0, pd.NA) - 1.0
    features = features.dropna(subset=["price_ratio"])
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
    return features


def universe_rank_bucket_signal(
    *,
    stock_code: str,
    target_index: pd.Index,
    universe_multi_data: Mapping[str, Mapping[str, object]],
    universe_member_codes: Sequence[str] | None = None,
    feature_panel: pd.DataFrame | None = None,
    price_sma_period: int = 50,
    price_bucket: str = "q1",
    min_constituents: int = 10,
) -> pd.Series:
    """Return True when the target stock sits in the requested price/SMA universe bucket."""

    if min_constituents < 2:
        raise ValueError("min_constituents must be >= 2")
    if price_bucket not in {"q1", "q10", "q456", "other"}:
        raise ValueError(f"unsupported price_bucket: {price_bucket}")

    reference_index = pd.Index(target_index)
    result = pd.Series(False, index=reference_index, dtype=bool)
    if reference_index.empty:
        return result

    if feature_panel is None:
        feature_panel = build_universe_rank_bucket_feature_panel(
            universe_multi_data=universe_multi_data,
            universe_member_codes=universe_member_codes,
            price_sma_period=price_sma_period,
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

    result = normalize_bool_series(matched.reindex(reference_index))
    logger.debug(
        "Universe rank bucket signal generated: stock={} price_bucket={} true_count={}/{}",
        stock_code,
        price_bucket,
        int(result.sum()),
        len(result),
    )
    return result
