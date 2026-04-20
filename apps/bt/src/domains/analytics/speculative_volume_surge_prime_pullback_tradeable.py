"""
Tradeable prime-only speculative pullback entry research.

This study converts the ex-post prime pullback profile into entry rules that
are closer to an executable daily workflow:

- Watch prime-market speculative surge episodes.
- After the initial peak, enter when the first pullback close reaches
  `0-10%` or `10-20%` above the pre-surge base close.
- Enter on the next session open.
- Exit at the earlier of:
  - the first session whose high strictly reclaims above the initial peak price
  - the close after a 20-session holding window

It also checks whether these tradeable entry cohorts align with the ex-post
deepest-pullback family and whether pullback speed matters.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.speculative_volume_surge_follow_on import (
    DEFAULT_ADV_WINDOW,
    DEFAULT_COOLDOWN_SESSIONS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_PRICE_JUMP_THRESHOLD,
    DEFAULT_VOLUME_RATIO_THRESHOLD,
    DEFAULT_VOLUME_WINDOW,
    SpeculativeVolumeSurgeFollowOnResult,
    run_speculative_volume_surge_follow_on_research,
)
from src.domains.analytics.speculative_volume_surge_prime_pullback_profile import (
    DEFAULT_INITIAL_PEAK_WINDOW,
    DEFAULT_PULLBACK_SEARCH_WINDOW,
    PRIME_MARKET_NAME,
    _build_prime_pullback_profile_df,
    _bucket_pullback_position,
    _coerce_float,
    _empty_df,
    _open_analysis_connection,
    _profile_columns,
    _query_episode_price_paths,
)

SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_TRADEABLE_EXPERIMENT_ID = (
    "market-behavior/speculative-volume-surge-prime-pullback-tradeable"
)
DEFAULT_ENTRY_BUCKETS: tuple[str, ...] = ("0-10%", "10-20%")
ENTRY_BUCKET_ORDER: tuple[str, ...] = ("0-10%", "10-20%")
DEEPEST_BUCKET_ORDER: tuple[str, ...] = ("<0%", "0-10%", "10-20%", "20-35%", "35%+", "missing")
DEFAULT_HOLDING_PERIOD_SESSIONS = 20
DEFAULT_SAMPLE_SIZE = 8
SPEED_BUCKET_ORDER: tuple[str, ...] = ("1-2d", "3-5d", "6-10d", "11-20d", "missing")
PRIME_EPISODE_COLUMNS: tuple[str, ...] = (
    "episode_id",
    "code",
    "company_name",
    "event_date",
    "base_close",
    "market_name",
    "scale_category",
    "adv20_bucket",
    "price_bucket",
)
ENTRY_TRADE_COLUMNS: tuple[str, ...] = (
    "episode_id",
    "code",
    "company_name",
    "event_date",
    "adv20_bucket",
    "price_bucket",
    "base_close",
    "initial_peak_offset",
    "initial_peak_price",
    "deepest_pullback_bucket",
    "deepest_pullback_bucket_order",
    "deepest_pullback_position_pct",
    "entry_bucket",
    "entry_bucket_order",
    "signal_offset",
    "signal_date",
    "signal_close",
    "signal_position_pct",
    "days_from_peak_to_signal",
    "speed_bucket",
    "speed_bucket_order",
    "entry_offset",
    "entry_date",
    "entry_price",
    "hold_end_offset",
    "hold_end_date",
    "hold_close_price",
    "hold_close_return_pct",
    "max_upside_pct",
    "max_downside_pct",
    "asymmetry_pct",
    "reclaim_hit",
    "exit_reason",
    "exit_offset",
    "exit_date",
    "exit_price",
    "trade_return_pct",
    "positive_hold_close_return",
    "positive_trade_return",
)
ENTRY_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "entry_bucket",
    "entry_bucket_order",
    "observation_count",
    "median_signal_position_pct",
    "median_deepest_pullback_position_pct",
    "median_days_from_peak_to_signal",
    "median_hold_close_return_pct",
    "median_trade_return_pct",
    "median_asymmetry_pct",
    "positive_hold_close_return_rate",
    "positive_trade_return_rate",
    "reclaim_hit_rate",
)
ENTRY_SPEED_SUMMARY_COLUMNS: tuple[str, ...] = (
    "entry_bucket",
    "entry_bucket_order",
    "speed_bucket",
    "speed_bucket_order",
    "observation_count",
    "median_trade_return_pct",
    "median_hold_close_return_pct",
    "positive_trade_return_rate",
    "reclaim_hit_rate",
)
DEEPEST_ALIGNMENT_COLUMNS: tuple[str, ...] = (
    "entry_bucket",
    "entry_bucket_order",
    "deepest_pullback_bucket",
    "deepest_pullback_bucket_order",
    "observation_count",
    "share_within_entry_bucket",
    "median_trade_return_pct",
    "median_hold_close_return_pct",
    "reclaim_hit_rate",
)
TOP_EXAMPLES_COLUMNS: tuple[str, ...] = (
    "example_group",
    "episode_id",
    "code",
    "company_name",
    "event_date",
    "adv20_bucket",
    "entry_bucket",
    "deepest_pullback_bucket",
    "signal_date",
    "entry_date",
    "days_from_peak_to_signal",
    "speed_bucket",
    "entry_price",
    "initial_peak_price",
    "hold_close_return_pct",
    "trade_return_pct",
    "asymmetry_pct",
    "reclaim_hit",
    "exit_reason",
    "exit_date",
)
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "prime_episode_df",
    "prime_pullback_profile_df",
    "trade_entry_df",
    "entry_bucket_summary_df",
    "entry_speed_summary_df",
    "deepest_alignment_df",
    "top_examples_df",
)
_EPSILON = 1e-9


@dataclass(frozen=True)
class SpeculativeVolumeSurgePrimePullbackTradeableResult:
    db_path: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_name: str
    lookback_years: int
    price_jump_threshold: float
    volume_ratio_threshold: float
    volume_window: int
    adv_window: int
    cooldown_sessions: int
    initial_peak_window: int
    pullback_search_window: int
    entry_buckets: tuple[str, ...]
    holding_period_sessions: int
    sample_size: int
    total_prime_episode_count: int
    total_deepest_profile_count: int
    total_trade_entry_count: int
    prime_episode_df: pd.DataFrame
    prime_pullback_profile_df: pd.DataFrame
    trade_entry_df: pd.DataFrame
    entry_bucket_summary_df: pd.DataFrame
    entry_speed_summary_df: pd.DataFrame
    deepest_alignment_df: pd.DataFrame
    top_examples_df: pd.DataFrame


def _normalize_entry_buckets(
    value: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_ENTRY_BUCKETS
    normalized = tuple(
        bucket
        for bucket in ENTRY_BUCKET_ORDER
        if bucket in {str(item) for item in value}
    )
    if not normalized:
        raise ValueError("entry_buckets must contain at least one supported bucket")
    return normalized


def _bucket_speed(days_from_peak_to_signal: int | None) -> str:
    if days_from_peak_to_signal is None or days_from_peak_to_signal <= 0:
        return "missing"
    if days_from_peak_to_signal <= 2:
        return "1-2d"
    if days_from_peak_to_signal <= 5:
        return "3-5d"
    if days_from_peak_to_signal <= 10:
        return "6-10d"
    return "11-20d"


def _median(series: pd.Series) -> float | None:
    scoped = pd.to_numeric(series, errors="coerce").dropna()
    if scoped.empty:
        return None
    return float(scoped.median())


def _bool_rate(series: pd.Series) -> float | None:
    scoped = series.dropna()
    if scoped.empty:
        return None
    return float(scoped.astype(bool).mean())


def _build_trade_entry_df(
    prime_episode_df: pd.DataFrame,
    prime_pullback_profile_df: pd.DataFrame,
    path_df: pd.DataFrame,
    *,
    pullback_search_window: int,
    entry_buckets: Sequence[str],
    holding_period_sessions: int,
) -> pd.DataFrame:
    empty_trade_df = _empty_df(ENTRY_TRADE_COLUMNS)
    if prime_episode_df.empty or prime_pullback_profile_df.empty or path_df.empty:
        return empty_trade_df

    profile_lookup = prime_pullback_profile_df.set_index("episode_id")
    path_groups: dict[Any, pd.DataFrame] = {
        episode_id: group.sort_values(by="session_offset", kind="stable").reset_index(drop=True)
        for episode_id, group in path_df.groupby("episode_id", sort=False, dropna=False)
    }
    rows: list[dict[str, Any]] = []

    for episode in prime_episode_df.itertuples(index=False):
        if episode.episode_id not in profile_lookup.index:
            continue
        episode_path = path_groups.get(episode.episode_id)
        if episode_path is None or episode_path.empty:
            continue

        profile_row = profile_lookup.loc[episode.episode_id]
        if isinstance(profile_row, pd.DataFrame):
            profile_row = profile_row.iloc[0]

        base_close = _coerce_float(episode.base_close)
        initial_peak_offset = int(cast(Any, profile_row["initial_peak_offset"]))
        initial_peak_price = float(cast(Any, profile_row["initial_peak_price"]))
        deepest_bucket = str(profile_row["deepest_pullback_bucket"])
        deepest_bucket_order = (
            DEEPEST_BUCKET_ORDER.index(deepest_bucket)
            if deepest_bucket in DEEPEST_BUCKET_ORDER
            else len(DEEPEST_BUCKET_ORDER)
        )
        deepest_position_pct = float(cast(Any, profile_row["deepest_pullback_position_pct"]))

        search_rows = episode_path.loc[
            (episode_path["session_offset"] > initial_peak_offset)
            & (episode_path["session_offset"] <= pullback_search_window)
        ].copy()
        if search_rows.empty or base_close is None or base_close <= 0:
            continue

        reclaim_rows = search_rows.loc[
            pd.to_numeric(search_rows["high"], errors="coerce") > initial_peak_price + _EPSILON
        ]
        if not reclaim_rows.empty:
            first_reclaim_offset = int(cast(Any, reclaim_rows.iloc[0]["session_offset"]))
            search_rows = search_rows.loc[search_rows["session_offset"] < first_reclaim_offset].copy()
        search_rows = search_rows.loc[
            pd.to_numeric(search_rows["close"], errors="coerce") < initial_peak_price - _EPSILON
        ].copy()
        if search_rows.empty:
            continue

        search_rows["position_pct"] = (
            pd.to_numeric(search_rows["close"], errors="coerce") / base_close - 1.0
        )
        search_rows["bucket"] = search_rows["position_pct"].map(_bucket_pullback_position)
        max_available_offset = int(cast(Any, episode_path["session_offset"].max()))

        for entry_bucket in entry_buckets:
            bucket_rows = search_rows.loc[search_rows["bucket"] == entry_bucket].copy()
            if bucket_rows.empty:
                continue

            signal_row = bucket_rows.iloc[0]
            signal_offset = int(cast(Any, signal_row["session_offset"]))
            entry_offset = signal_offset + 1
            hold_end_offset = entry_offset + holding_period_sessions - 1
            if max_available_offset < hold_end_offset:
                continue

            entry_rows = episode_path.loc[episode_path["session_offset"] == entry_offset].copy()
            if entry_rows.empty:
                continue
            entry_row = entry_rows.iloc[0]
            entry_price = float(cast(Any, entry_row["open"]))
            if entry_price <= 0:
                continue

            hold_window = episode_path.loc[
                (episode_path["session_offset"] >= entry_offset)
                & (episode_path["session_offset"] <= hold_end_offset)
            ].copy()
            if hold_window.empty:
                continue
            hold_close_row = hold_window.iloc[-1]
            hold_close_price = float(cast(Any, hold_close_row["close"]))

            reclaim_hits = hold_window.loc[
                pd.to_numeric(hold_window["high"], errors="coerce") > initial_peak_price + _EPSILON
            ]
            if reclaim_hits.empty:
                exit_row = hold_close_row
                exit_price = hold_close_price
                exit_reason = "hold_close"
                reclaim_hit = False
            else:
                exit_row = reclaim_hits.iloc[0]
                exit_price = initial_peak_price
                exit_reason = "peak_reclaim"
                reclaim_hit = True

            max_upside_pct = float(pd.to_numeric(hold_window["high"], errors="coerce").max()) / entry_price - 1.0
            max_downside_pct = 1.0 - float(pd.to_numeric(hold_window["low"], errors="coerce").min()) / entry_price
            hold_close_return_pct = hold_close_price / entry_price - 1.0
            trade_return_pct = exit_price / entry_price - 1.0
            days_from_peak_to_signal = signal_offset - initial_peak_offset
            speed_bucket = _bucket_speed(days_from_peak_to_signal)

            rows.append(
                {
                    "episode_id": episode.episode_id,
                    "code": episode.code,
                    "company_name": episode.company_name,
                    "event_date": episode.event_date,
                    "adv20_bucket": episode.adv20_bucket,
                    "price_bucket": episode.price_bucket,
                    "base_close": base_close,
                    "initial_peak_offset": initial_peak_offset,
                    "initial_peak_price": initial_peak_price,
                    "deepest_pullback_bucket": deepest_bucket,
                    "deepest_pullback_bucket_order": deepest_bucket_order,
                    "deepest_pullback_position_pct": deepest_position_pct,
                    "entry_bucket": entry_bucket,
                    "entry_bucket_order": ENTRY_BUCKET_ORDER.index(entry_bucket),
                    "signal_offset": signal_offset,
                    "signal_date": signal_row["date"],
                    "signal_close": float(cast(Any, signal_row["close"])),
                    "signal_position_pct": float(cast(Any, signal_row["position_pct"])),
                    "days_from_peak_to_signal": days_from_peak_to_signal,
                    "speed_bucket": speed_bucket,
                    "speed_bucket_order": SPEED_BUCKET_ORDER.index(speed_bucket)
                    if speed_bucket in SPEED_BUCKET_ORDER
                    else len(SPEED_BUCKET_ORDER),
                    "entry_offset": entry_offset,
                    "entry_date": entry_row["date"],
                    "entry_price": entry_price,
                    "hold_end_offset": hold_end_offset,
                    "hold_end_date": hold_close_row["date"],
                    "hold_close_price": hold_close_price,
                    "hold_close_return_pct": hold_close_return_pct,
                    "max_upside_pct": max_upside_pct,
                    "max_downside_pct": max_downside_pct,
                    "asymmetry_pct": max_upside_pct - max_downside_pct,
                    "reclaim_hit": reclaim_hit,
                    "exit_reason": exit_reason,
                    "exit_offset": int(cast(Any, exit_row["session_offset"])),
                    "exit_date": exit_row["date"],
                    "exit_price": exit_price,
                    "trade_return_pct": trade_return_pct,
                    "positive_hold_close_return": hold_close_return_pct > 0,
                    "positive_trade_return": trade_return_pct > 0,
                }
            )

    if not rows:
        return empty_trade_df

    return pd.DataFrame(rows).reindex(columns=ENTRY_TRADE_COLUMNS).sort_values(
        by=["entry_bucket_order", "event_date", "code", "signal_offset"],
        kind="stable",
    ).reset_index(drop=True)


def _build_entry_bucket_summary_df(trade_entry_df: pd.DataFrame) -> pd.DataFrame:
    if trade_entry_df.empty:
        return _empty_df(ENTRY_BUCKET_SUMMARY_COLUMNS)
    rows: list[dict[str, Any]] = []
    for entry_bucket, bucket_df in trade_entry_df.groupby("entry_bucket", sort=False, dropna=False):
        bucket_name = "missing" if pd.isna(entry_bucket) else str(entry_bucket)
        rows.append(
            {
                "entry_bucket": bucket_name,
                "entry_bucket_order": ENTRY_BUCKET_ORDER.index(bucket_name)
                if bucket_name in ENTRY_BUCKET_ORDER
                else len(ENTRY_BUCKET_ORDER),
                "observation_count": int(len(bucket_df)),
                "median_signal_position_pct": _median(bucket_df["signal_position_pct"]),
                "median_deepest_pullback_position_pct": _median(
                    bucket_df["deepest_pullback_position_pct"]
                ),
                "median_days_from_peak_to_signal": _median(
                    bucket_df["days_from_peak_to_signal"]
                ),
                "median_hold_close_return_pct": _median(bucket_df["hold_close_return_pct"]),
                "median_trade_return_pct": _median(bucket_df["trade_return_pct"]),
                "median_asymmetry_pct": _median(bucket_df["asymmetry_pct"]),
                "positive_hold_close_return_rate": _bool_rate(
                    bucket_df["positive_hold_close_return"]
                ),
                "positive_trade_return_rate": _bool_rate(bucket_df["positive_trade_return"]),
                "reclaim_hit_rate": _bool_rate(bucket_df["reclaim_hit"]),
            }
        )
    return pd.DataFrame(rows).sort_values(
        by=["entry_bucket_order"],
        kind="stable",
    ).reset_index(drop=True)


def _build_entry_speed_summary_df(trade_entry_df: pd.DataFrame) -> pd.DataFrame:
    if trade_entry_df.empty:
        return _empty_df(ENTRY_SPEED_SUMMARY_COLUMNS)
    rows: list[dict[str, Any]] = []
    for (entry_bucket, speed_bucket), group_df in trade_entry_df.groupby(
        ["entry_bucket", "speed_bucket"],
        sort=False,
        dropna=False,
    ):
        entry_name = "missing" if pd.isna(entry_bucket) else str(entry_bucket)
        speed_name = "missing" if pd.isna(speed_bucket) else str(speed_bucket)
        rows.append(
            {
                "entry_bucket": entry_name,
                "entry_bucket_order": ENTRY_BUCKET_ORDER.index(entry_name)
                if entry_name in ENTRY_BUCKET_ORDER
                else len(ENTRY_BUCKET_ORDER),
                "speed_bucket": speed_name,
                "speed_bucket_order": SPEED_BUCKET_ORDER.index(speed_name)
                if speed_name in SPEED_BUCKET_ORDER
                else len(SPEED_BUCKET_ORDER),
                "observation_count": int(len(group_df)),
                "median_trade_return_pct": _median(group_df["trade_return_pct"]),
                "median_hold_close_return_pct": _median(group_df["hold_close_return_pct"]),
                "positive_trade_return_rate": _bool_rate(group_df["positive_trade_return"]),
                "reclaim_hit_rate": _bool_rate(group_df["reclaim_hit"]),
            }
        )
    return pd.DataFrame(rows).sort_values(
        by=["entry_bucket_order", "speed_bucket_order"],
        kind="stable",
    ).reset_index(drop=True)


def _build_deepest_alignment_df(trade_entry_df: pd.DataFrame) -> pd.DataFrame:
    if trade_entry_df.empty:
        return _empty_df(DEEPEST_ALIGNMENT_COLUMNS)
    rows: list[dict[str, Any]] = []
    for entry_bucket, bucket_df in trade_entry_df.groupby("entry_bucket", sort=False, dropna=False):
        entry_name = "missing" if pd.isna(entry_bucket) else str(entry_bucket)
        total_count = float(len(bucket_df))
        for deepest_bucket, group_df in bucket_df.groupby(
            "deepest_pullback_bucket",
            sort=False,
            dropna=False,
        ):
            deepest_name = "missing" if pd.isna(deepest_bucket) else str(deepest_bucket)
            rows.append(
                {
                    "entry_bucket": entry_name,
                    "entry_bucket_order": ENTRY_BUCKET_ORDER.index(entry_name)
                    if entry_name in ENTRY_BUCKET_ORDER
                    else len(ENTRY_BUCKET_ORDER),
                    "deepest_pullback_bucket": deepest_name,
                    "deepest_pullback_bucket_order": DEEPEST_BUCKET_ORDER.index(deepest_name)
                    if deepest_name in DEEPEST_BUCKET_ORDER
                    else len(DEEPEST_BUCKET_ORDER),
                    "observation_count": int(len(group_df)),
                    "share_within_entry_bucket": len(group_df) / total_count if total_count > 0 else None,
                    "median_trade_return_pct": _median(group_df["trade_return_pct"]),
                    "median_hold_close_return_pct": _median(group_df["hold_close_return_pct"]),
                    "reclaim_hit_rate": _bool_rate(group_df["reclaim_hit"]),
                }
            )
    return pd.DataFrame(rows).sort_values(
        by=["entry_bucket_order", "deepest_pullback_bucket_order"],
        kind="stable",
    ).reset_index(drop=True)


def _build_top_examples_df(
    trade_entry_df: pd.DataFrame,
    *,
    sample_size: int,
) -> pd.DataFrame:
    if trade_entry_df.empty:
        return _empty_df(TOP_EXAMPLES_COLUMNS)
    scoped = trade_entry_df.sort_values(by=["trade_return_pct", "event_date", "code"], kind="stable")
    negative_df = scoped.head(sample_size).copy()
    negative_df["example_group"] = "negative_trade"
    positive_df = scoped.tail(sample_size).copy().sort_values(
        by=["trade_return_pct", "event_date", "code"],
        ascending=[False, True, True],
        kind="stable",
    )
    positive_df["example_group"] = "positive_trade"
    combined = pd.concat([positive_df, negative_df], ignore_index=True)
    return combined.reindex(columns=TOP_EXAMPLES_COLUMNS)


def _build_research_bundle_summary_markdown(
    result: SpeculativeVolumeSurgePrimePullbackTradeableResult,
) -> str:
    lines = [
        "# Speculative Volume-Surge Prime Pullback Tradeable",
        "",
        f"- Analysis range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Market: `{result.market_name}`",
        f"- Entry buckets: `{','.join(result.entry_buckets)}`",
        f"- Pullback search window: `{result.pullback_search_window}` sessions",
        f"- Holding period: `{result.holding_period_sessions}` sessions",
        f"- Prime episodes: `{result.total_prime_episode_count}`",
        f"- Deepest profiles: `{result.total_deepest_profile_count}`",
        f"- Tradeable entries: `{result.total_trade_entry_count}`",
        "",
        "## Entry Bucket Read",
    ]
    if result.entry_bucket_summary_df.empty:
        lines.append("- No trade entries were produced.")
    else:
        for row in result.entry_bucket_summary_df.itertuples(index=False):
            lines.append(
                f"- `{row.entry_bucket}`: entries `{int(cast(Any, row.observation_count))}`, "
                f"trade return `{float(cast(Any, row.median_trade_return_pct)) * 100:.1f}%`, "
                f"hold-close `{float(cast(Any, row.median_hold_close_return_pct)) * 100:.1f}%`, "
                f"asymmetry `{float(cast(Any, row.median_asymmetry_pct)) * 100:.1f}%`, "
                f"reclaim exit `{float(cast(Any, row.reclaim_hit_rate)) * 100:.1f}%`"
            )
    lines.extend(["", "## Assumption"])
    lines.append(
        f"- The signal is observed on the first close that enters the bucket, entry is placed on the next session open, and exit is the earlier of the first strict reclaim above the initial peak (filled at the initial-peak price) or the close after {result.holding_period_sessions} sessions."
    )
    return "\n".join(lines) + "\n"


def _build_published_summary(
    result: SpeculativeVolumeSurgePrimePullbackTradeableResult,
) -> dict[str, Any]:
    return {
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "marketName": result.market_name,
        "primeEpisodes": result.total_prime_episode_count,
        "deepestProfiles": result.total_deepest_profile_count,
        "tradeEntries": result.total_trade_entry_count,
        "entryBuckets": list(result.entry_buckets),
        "topRows": result.entry_bucket_summary_df.to_dict(orient="records"),
    }


def run_speculative_volume_surge_prime_pullback_tradeable_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    price_jump_threshold: float = DEFAULT_PRICE_JUMP_THRESHOLD,
    volume_ratio_threshold: float = DEFAULT_VOLUME_RATIO_THRESHOLD,
    volume_window: int = DEFAULT_VOLUME_WINDOW,
    adv_window: int = DEFAULT_ADV_WINDOW,
    cooldown_sessions: int = DEFAULT_COOLDOWN_SESSIONS,
    initial_peak_window: int = DEFAULT_INITIAL_PEAK_WINDOW,
    pullback_search_window: int = DEFAULT_PULLBACK_SEARCH_WINDOW,
    entry_buckets: tuple[str, ...] | list[str] | None = None,
    holding_period_sessions: int = DEFAULT_HOLDING_PERIOD_SESSIONS,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> SpeculativeVolumeSurgePrimePullbackTradeableResult:
    if lookback_years <= 0:
        raise ValueError("lookback_years must be positive")
    if price_jump_threshold <= 0:
        raise ValueError("price_jump_threshold must be positive")
    if volume_ratio_threshold <= 0:
        raise ValueError("volume_ratio_threshold must be positive")
    if volume_window <= 0:
        raise ValueError("volume_window must be positive")
    if adv_window <= 0:
        raise ValueError("adv_window must be positive")
    if cooldown_sessions < 0:
        raise ValueError("cooldown_sessions must be non-negative")
    if initial_peak_window <= 0:
        raise ValueError("initial_peak_window must be positive")
    if pullback_search_window <= initial_peak_window:
        raise ValueError("pullback_search_window must be greater than initial_peak_window")
    if holding_period_sessions <= 0:
        raise ValueError("holding_period_sessions must be positive")
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    normalized_entry_buckets = _normalize_entry_buckets(entry_buckets)

    base_result: SpeculativeVolumeSurgeFollowOnResult = (
        run_speculative_volume_surge_follow_on_research(
            db_path,
            start_date=start_date,
            end_date=end_date,
            lookback_years=lookback_years,
            price_jump_threshold=price_jump_threshold,
            volume_ratio_threshold=volume_ratio_threshold,
            volume_window=volume_window,
            adv_window=adv_window,
            cooldown_sessions=cooldown_sessions,
        )
    )
    prime_episode_df = base_result.event_ledger_df.loc[
        base_result.event_ledger_df["market_name"] == PRIME_MARKET_NAME,
        list(PRIME_EPISODE_COLUMNS),
    ].copy()

    if prime_episode_df.empty:
        empty_profile_df = _empty_df(_profile_columns((holding_period_sessions,)))
        empty_trade_df = _empty_df(ENTRY_TRADE_COLUMNS)
        return SpeculativeVolumeSurgePrimePullbackTradeableResult(
            db_path=db_path,
            analysis_start_date=base_result.analysis_start_date,
            analysis_end_date=base_result.analysis_end_date,
            market_name=PRIME_MARKET_NAME,
            lookback_years=lookback_years,
            price_jump_threshold=price_jump_threshold,
            volume_ratio_threshold=volume_ratio_threshold,
            volume_window=volume_window,
            adv_window=adv_window,
            cooldown_sessions=cooldown_sessions,
            initial_peak_window=initial_peak_window,
            pullback_search_window=pullback_search_window,
            entry_buckets=normalized_entry_buckets,
            holding_period_sessions=holding_period_sessions,
            sample_size=sample_size,
            total_prime_episode_count=0,
            total_deepest_profile_count=0,
            total_trade_entry_count=0,
            prime_episode_df=prime_episode_df,
            prime_pullback_profile_df=empty_profile_df,
            trade_entry_df=empty_trade_df,
            entry_bucket_summary_df=_empty_df(ENTRY_BUCKET_SUMMARY_COLUMNS),
            entry_speed_summary_df=_empty_df(ENTRY_SPEED_SUMMARY_COLUMNS),
            deepest_alignment_df=_empty_df(DEEPEST_ALIGNMENT_COLUMNS),
            top_examples_df=_empty_df(TOP_EXAMPLES_COLUMNS),
        )

    max_session_offset = pullback_search_window + holding_period_sessions + 1
    with _open_analysis_connection(db_path) as ctx:
        path_df = _query_episode_price_paths(
            ctx.connection,
            prime_episode_df,
            max_session_offset=max_session_offset,
        )
        prime_pullback_profile_df = _build_prime_pullback_profile_df(
            prime_episode_df,
            path_df,
            initial_peak_window=initial_peak_window,
            pullback_search_window=pullback_search_window,
            future_horizons=(holding_period_sessions,),
        )
        trade_entry_df = _build_trade_entry_df(
            prime_episode_df,
            prime_pullback_profile_df,
            path_df,
            pullback_search_window=pullback_search_window,
            entry_buckets=normalized_entry_buckets,
            holding_period_sessions=holding_period_sessions,
        )

    entry_bucket_summary_df = _build_entry_bucket_summary_df(trade_entry_df)
    entry_speed_summary_df = _build_entry_speed_summary_df(trade_entry_df)
    deepest_alignment_df = _build_deepest_alignment_df(trade_entry_df)
    top_examples_df = _build_top_examples_df(
        trade_entry_df,
        sample_size=sample_size,
    )

    return SpeculativeVolumeSurgePrimePullbackTradeableResult(
        db_path=db_path,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        market_name=PRIME_MARKET_NAME,
        lookback_years=lookback_years,
        price_jump_threshold=price_jump_threshold,
        volume_ratio_threshold=volume_ratio_threshold,
        volume_window=volume_window,
        adv_window=adv_window,
        cooldown_sessions=cooldown_sessions,
        initial_peak_window=initial_peak_window,
        pullback_search_window=pullback_search_window,
        entry_buckets=normalized_entry_buckets,
        holding_period_sessions=holding_period_sessions,
        sample_size=sample_size,
        total_prime_episode_count=int(len(prime_episode_df)),
        total_deepest_profile_count=int(len(prime_pullback_profile_df)),
        total_trade_entry_count=int(len(trade_entry_df)),
        prime_episode_df=prime_episode_df,
        prime_pullback_profile_df=prime_pullback_profile_df,
        trade_entry_df=trade_entry_df,
        entry_bucket_summary_df=entry_bucket_summary_df,
        entry_speed_summary_df=entry_speed_summary_df,
        deepest_alignment_df=deepest_alignment_df,
        top_examples_df=top_examples_df,
    )


def write_speculative_volume_surge_prime_pullback_tradeable_research_bundle(
    result: SpeculativeVolumeSurgePrimePullbackTradeableResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_TRADEABLE_EXPERIMENT_ID,
        module=__name__,
        function="run_speculative_volume_surge_prime_pullback_tradeable_research",
        params={
            "lookback_years": result.lookback_years,
            "price_jump_threshold": result.price_jump_threshold,
            "volume_ratio_threshold": result.volume_ratio_threshold,
            "volume_window": result.volume_window,
            "adv_window": result.adv_window,
            "cooldown_sessions": result.cooldown_sessions,
            "initial_peak_window": result.initial_peak_window,
            "pullback_search_window": result.pullback_search_window,
            "entry_buckets": result.entry_buckets,
            "holding_period_sessions": result.holding_period_sessions,
            "sample_size": result.sample_size,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_speculative_volume_surge_prime_pullback_tradeable_research_bundle(
    bundle_path: str | Path,
) -> SpeculativeVolumeSurgePrimePullbackTradeableResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=SpeculativeVolumeSurgePrimePullbackTradeableResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_speculative_volume_surge_prime_pullback_tradeable_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_TRADEABLE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_speculative_volume_surge_prime_pullback_tradeable_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_TRADEABLE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
