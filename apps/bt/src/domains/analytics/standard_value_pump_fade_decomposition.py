"""Standard value-ranking pump/fade decomposition research."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)

STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/standard-value-pump-fade-decomposition"
)
DEFAULT_FORWARD_HORIZONS: tuple[int, ...] = (20, 60)
DEFAULT_TOP_RANKS: tuple[int, ...] = (25, 50, 100)
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "candidate_event_df",
    "pattern_summary_df",
    "risk_score_summary_df",
    "flag_summary_df",
    "current_snapshot_df",
)


@dataclass(frozen=True)
class StandardValuePumpFadeDecompositionResult:
    db_path: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    snapshot_count: int
    candidate_count: int
    forward_horizons: tuple[int, ...]
    top_ranks: tuple[int, ...]
    score_profile: str
    pump_fade_policy: str
    candidate_event_df: pd.DataFrame
    pattern_summary_df: pd.DataFrame
    risk_score_summary_df: pd.DataFrame
    flag_summary_df: pd.DataFrame
    current_snapshot_df: pd.DataFrame


def run_standard_value_pump_fade_decomposition_from_frames(
    *,
    db_path: str,
    ranking_snapshot_df: pd.DataFrame,
    price_history_df: pd.DataFrame,
    forward_horizons: Sequence[int] = DEFAULT_FORWARD_HORIZONS,
    top_ranks: Sequence[int] = DEFAULT_TOP_RANKS,
    score_profile: str = "standard_breakout_120d20",
) -> StandardValuePumpFadeDecompositionResult:
    """Attach pump/fade diagnostics and future outcomes to ranking snapshots."""

    normalized_horizons = _normalize_positive_ints(forward_horizons, "forward_horizons")
    normalized_top_ranks = _normalize_positive_ints(top_ranks, "top_ranks")
    snapshots = _normalize_ranking_snapshot_frame(ranking_snapshot_df)
    prices = _normalize_price_history_frame(price_history_df)

    if snapshots.empty:
        candidate_event_df = _empty_candidate_event_df(normalized_horizons)
    else:
        candidate_event_df = _build_candidate_event_df(
            snapshots,
            prices,
            forward_horizons=normalized_horizons,
        )

    pattern_summary_df = _build_group_summary_df(
        candidate_event_df,
        group_columns=("top_rank_bucket", "pattern_bucket"),
        forward_horizons=normalized_horizons,
    )
    risk_score_summary_df = _build_group_summary_df(
        candidate_event_df,
        group_columns=("top_rank_bucket", "speculative_risk_bucket"),
        forward_horizons=normalized_horizons,
    )
    flag_summary_df = _build_flag_summary_df(candidate_event_df, normalized_horizons)
    current_snapshot_df = _build_current_snapshot_df(candidate_event_df)

    analysis_start_date = (
        str(candidate_event_df["snapshot_date"].min()) if not candidate_event_df.empty else None
    )
    analysis_end_date = (
        str(candidate_event_df["snapshot_date"].max()) if not candidate_event_df.empty else None
    )

    return StandardValuePumpFadeDecompositionResult(
        db_path=str(db_path),
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        snapshot_count=int(candidate_event_df["snapshot_date"].nunique())
        if not candidate_event_df.empty
        else 0,
        candidate_count=int(len(candidate_event_df)),
        forward_horizons=normalized_horizons,
        top_ranks=normalized_top_ranks,
        score_profile=score_profile,
        pump_fade_policy=(
            "pump_fade_like risk score = microcap(<5bn JPY) + ADV60<30mn "
            "+ volatility60d>=50% + drawdown_from_2y_high<=-40% + "
            "large prior monthly candle faded >=30%."
        ),
        candidate_event_df=candidate_event_df,
        pattern_summary_df=pattern_summary_df,
        risk_score_summary_df=risk_score_summary_df,
        flag_summary_df=flag_summary_df,
        current_snapshot_df=current_snapshot_df,
    )


def _normalize_positive_ints(values: Sequence[int], label: str) -> tuple[int, ...]:
    normalized: list[int] = []
    for raw_value in values:
        value = int(raw_value)
        if value < 1:
            raise ValueError(f"{label} values must be positive")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError(f"{label} must contain at least one value")
    return tuple(sorted(normalized))


def _normalize_ranking_snapshot_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"snapshot_date", "rank", "code", "score"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"ranking_snapshot_df missing required columns: {sorted(missing)}")
    result = frame.copy()
    result["snapshot_date"] = pd.to_datetime(result["snapshot_date"]).dt.strftime("%Y-%m-%d")
    result["code"] = result["code"].astype(str)
    result["rank"] = pd.to_numeric(result["rank"], errors="coerce").astype("Int64")
    return result.sort_values(["snapshot_date", "rank", "code"]).reset_index(drop=True)


def _normalize_price_history_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"code", "date", "open", "high", "low", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price_history_df missing required columns: {sorted(missing)}")
    result = frame.copy()
    result["code"] = result["code"].astype(str)
    result["date"] = pd.to_datetime(result["date"])
    for column in ("open", "high", "low", "close", "volume"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result.sort_values(["code", "date"]).reset_index(drop=True)


def _build_candidate_event_df(
    snapshots: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    forward_horizons: tuple[int, ...],
) -> pd.DataFrame:
    price_by_code = {
        str(code): group.sort_values("date").reset_index(drop=True)
        for code, group in prices.groupby("code", sort=False)
    }
    records: list[dict[str, Any]] = []
    for row in snapshots.to_dict(orient="records"):
        code = str(row["code"])
        snapshot_date = pd.Timestamp(row["snapshot_date"])
        history = price_by_code.get(code)
        if history is None or history.empty:
            continue
        feature_payload = _build_price_feature_payload(history, snapshot_date)
        outcome_payload = _build_forward_outcome_payload(
            history,
            snapshot_date,
            forward_horizons=forward_horizons,
        )
        combined = {**row, **feature_payload, **outcome_payload}
        combined["top_rank_bucket"] = _bucket_rank(row.get("rank"))
        combined["speculative_risk_score"] = _speculative_risk_score(combined)
        combined["speculative_risk_bucket"] = _bucket_speculative_risk(
            combined["speculative_risk_score"]
        )
        combined["pattern_bucket"] = _bucket_pattern(combined)
        records.append(combined)
    return pd.DataFrame.from_records(records)


def _build_price_feature_payload(history: pd.DataFrame, snapshot_date: pd.Timestamp) -> dict[str, Any]:
    past = history[history["date"] <= snapshot_date].copy()
    if past.empty:
        return _empty_price_feature_payload()
    latest = past.iloc[-1]
    two_year_start = snapshot_date - pd.DateOffset(years=2)
    two_year = past[past["date"] >= two_year_start].copy()
    if two_year.empty:
        two_year = past.copy()
    latest_close = _finite_float(latest.get("close"))
    daily_returns = past["close"].pct_change()
    volatility_60d_pct = (
        float(daily_returns.tail(60).std(ddof=1) * np.sqrt(252.0) * 100.0)
        if daily_returns.tail(60).count() >= 60
        else None
    )
    close_252d = _finite_float(past["close"].iloc[-253]) if len(past) >= 253 else None
    return_252d_pct = (
        (latest_close / close_252d - 1.0) * 100.0
        if latest_close is not None and close_252d is not None and close_252d > 0
        else None
    )
    low_close_252d = (
        _finite_float(past["close"].tail(252).min()) if len(past) >= 252 else None
    )
    rebound_from_252d_low_pct = (
        (latest_close / low_close_252d - 1.0) * 100.0
        if latest_close is not None and low_close_252d is not None and low_close_252d > 0
        else None
    )
    high_2y = _finite_float(two_year["high"].max())
    high_idx = two_year["high"].idxmax() if high_2y is not None else None
    high_date = (
        _format_date(cast(pd.Timestamp, two_year.loc[high_idx, "date"]))
        if high_idx is not None
        else None
    )
    drawdown_from_2y_high_pct = (
        (latest_close / high_2y - 1.0) * 100.0
        if latest_close is not None and high_2y is not None and high_2y > 0
        else None
    )
    monthly_payload = _build_monthly_candle_payload(two_year, latest_close)
    deep_drawdown_after_large_month = bool(
        monthly_payload.get("large_month_candle")
        and drawdown_from_2y_high_pct is not None
        and drawdown_from_2y_high_pct <= -40.0
    )
    return {
        "price_feature_date": _format_date(cast(pd.Timestamp, latest["date"])),
        "latest_close": latest_close,
        "return_252d_pct": return_252d_pct,
        "rebound_from_252d_low_pct": rebound_from_252d_low_pct,
        "volatility_60d_pct": volatility_60d_pct,
        "two_year_high": high_2y,
        "two_year_high_date": high_date,
        "drawdown_from_2y_high_pct": drawdown_from_2y_high_pct,
        **monthly_payload,
        "deep_drawdown_after_large_month": deep_drawdown_after_large_month,
    }


def _empty_price_feature_payload() -> dict[str, Any]:
    return {
        "price_feature_date": None,
        "latest_close": None,
        "return_252d_pct": None,
        "rebound_from_252d_low_pct": None,
        "volatility_60d_pct": None,
        "two_year_high": None,
        "two_year_high_date": None,
        "drawdown_from_2y_high_pct": None,
        "max_month": None,
        "max_month_body_pct": None,
        "max_month_range_pct": None,
        "max_month_high": None,
        "max_month_close": None,
        "current_vs_max_month_close_pct": None,
        "current_vs_max_month_high_pct": None,
        "large_month_candle": False,
        "faded_after_large_month": False,
        "faded_after_large_month_high": False,
        "deep_drawdown_after_large_month": False,
    }


def _build_monthly_candle_payload(two_year: pd.DataFrame, latest_close: float | None) -> dict[str, Any]:
    if two_year.empty:
        return {
            "max_month": None,
            "max_month_body_pct": None,
            "max_month_range_pct": None,
            "max_month_high": None,
            "max_month_close": None,
            "current_vs_max_month_close_pct": None,
            "current_vs_max_month_high_pct": None,
            "large_month_candle": False,
            "faded_after_large_month": False,
            "faded_after_large_month_high": False,
            "deep_drawdown_after_large_month": False,
        }
    monthly = (
        two_year.set_index("date")
        .resample("MS")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .dropna(subset=["open", "high", "low", "close"])
    )
    if monthly.empty:
        return {
            "max_month": None,
            "max_month_body_pct": None,
            "max_month_range_pct": None,
            "max_month_high": None,
            "max_month_close": None,
            "current_vs_max_month_close_pct": None,
            "current_vs_max_month_high_pct": None,
            "large_month_candle": False,
            "faded_after_large_month": False,
            "faded_after_large_month_high": False,
            "deep_drawdown_after_large_month": False,
        }
    monthly["body_return_pct"] = (monthly["close"] / monthly["open"] - 1.0) * 100.0
    monthly["range_pct"] = (monthly["high"] / monthly["low"] - 1.0) * 100.0
    candidate_mask = (monthly["body_return_pct"] >= 50.0) | (monthly["range_pct"] >= 100.0)
    candidate_months = monthly[candidate_mask].copy()
    if candidate_months.empty:
        max_body_idx = monthly["body_return_pct"].idxmax()
    else:
        max_body_idx = candidate_months["high"].idxmax()
    row = monthly.loc[max_body_idx]
    max_body = _finite_float(row["body_return_pct"])
    max_range = _finite_float(row["range_pct"])
    max_month_high = _finite_float(row["high"])
    max_month_close = _finite_float(row["close"])
    current_vs_max_month_close_pct = (
        (latest_close / max_month_close - 1.0) * 100.0
        if latest_close is not None and max_month_close is not None and max_month_close > 0
        else None
    )
    current_vs_max_month_high_pct = (
        (latest_close / max_month_high - 1.0) * 100.0
        if latest_close is not None and max_month_high is not None and max_month_high > 0
        else None
    )
    large_month_candle = bool(
        (max_body is not None and max_body >= 50.0)
        or (max_range is not None and max_range >= 100.0)
    )
    faded_after_large_month = bool(
        large_month_candle
        and current_vs_max_month_close_pct is not None
        and current_vs_max_month_close_pct <= -30.0
    )
    faded_after_large_month_high = bool(
        large_month_candle
        and current_vs_max_month_high_pct is not None
        and current_vs_max_month_high_pct <= -50.0
    )
    return {
        "max_month": str(pd.Timestamp(max_body_idx).date())[:7],
        "max_month_body_pct": max_body,
        "max_month_range_pct": max_range,
        "max_month_high": max_month_high,
        "max_month_close": max_month_close,
        "current_vs_max_month_close_pct": current_vs_max_month_close_pct,
        "current_vs_max_month_high_pct": current_vs_max_month_high_pct,
        "large_month_candle": large_month_candle,
        "faded_after_large_month": faded_after_large_month,
        "faded_after_large_month_high": faded_after_large_month_high,
    }


def _build_forward_outcome_payload(
    history: pd.DataFrame,
    snapshot_date: pd.Timestamp,
    *,
    forward_horizons: tuple[int, ...],
) -> dict[str, Any]:
    future = history[history["date"] > snapshot_date].sort_values("date").reset_index(drop=True)
    payload: dict[str, Any] = {}
    for horizon in forward_horizons:
        prefix = f"fwd_{horizon}d"
        if len(future) < horizon:
            payload.update(
                {
                    f"{prefix}_entry_date": None,
                    f"{prefix}_exit_date": None,
                    f"{prefix}_return_pct": None,
                    f"{prefix}_max_upside_pct": None,
                    f"{prefix}_max_downside_pct": None,
                    f"{prefix}_severe_loss": None,
                    f"{prefix}_upside_20pct": None,
                }
            )
            continue
        window = future.iloc[:horizon].copy()
        entry_open = _finite_float(window.iloc[0]["open"])
        exit_close = _finite_float(window.iloc[-1]["close"])
        max_high = _finite_float(window["high"].max())
        min_low = _finite_float(window["low"].min())
        if entry_open is None or entry_open <= 0:
            return_pct = None
            max_upside_pct = None
            max_downside_pct = None
        else:
            return_pct = (
                (exit_close / entry_open - 1.0) * 100.0
                if exit_close is not None
                else None
            )
            max_upside_pct = (
                (max_high / entry_open - 1.0) * 100.0 if max_high is not None else None
            )
            max_downside_pct = (
                (min_low / entry_open - 1.0) * 100.0 if min_low is not None else None
            )
        payload.update(
            {
                f"{prefix}_entry_date": _format_date(cast(pd.Timestamp, window.iloc[0]["date"])),
                f"{prefix}_exit_date": _format_date(cast(pd.Timestamp, window.iloc[-1]["date"])),
                f"{prefix}_return_pct": return_pct,
                f"{prefix}_max_upside_pct": max_upside_pct,
                f"{prefix}_max_downside_pct": max_downside_pct,
                f"{prefix}_severe_loss": (
                    return_pct is not None and return_pct <= -10.0
                ),
                f"{prefix}_upside_20pct": (
                    max_upside_pct is not None and max_upside_pct >= 20.0
                ),
            }
        )
    return payload


def _speculative_risk_score(row: dict[str, Any]) -> int:
    return int(
        bool(_lt(row.get("market_cap_bil_jpy"), 5.0))
        + bool(_lt(row.get("avg_trading_value_60d_mil_jpy"), 30.0))
        + bool(_ge(row.get("volatility_60d_pct"), 50.0))
        + bool(_le(row.get("drawdown_from_2y_high_pct"), -40.0))
        + bool(row.get("faded_after_large_month_high"))
    )


def _bucket_speculative_risk(score: object) -> str:
    value = _finite_int(score) or 0
    if value >= 3:
        return "pump_fade_like_score_ge3"
    if value == 2:
        return "watch_score_2"
    return "ordinary_value_score_0_1"


def _bucket_pattern(row: dict[str, Any]) -> str:
    if bool(row.get("deep_drawdown_after_large_month")) and bool(
        row.get("faded_after_large_month_high")
    ):
        return "deep_high_fade_after_large_month"
    if bool(row.get("faded_after_large_month_high")):
        return "high_fade_after_large_month"
    if bool(row.get("faded_after_large_month")):
        return "close_fade_after_large_month"
    if bool(row.get("deep_drawdown_after_large_month")):
        return "deep_drawdown_after_large_month"
    if _le(row.get("drawdown_from_2y_high_pct"), -40.0):
        return "deep_2y_drawdown"
    if _ge(row.get("rebound_from_252d_low_pct"), 50.0) and _ge(row.get("return_252d_pct"), 30.0):
        return "active_rebound"
    return "plain_value"


def _bucket_rank(rank: object) -> str:
    value = _finite_int(rank) or 999999
    if value <= 25:
        return "top_25"
    if value <= 50:
        return "top_50"
    return "top_100"


def _build_group_summary_df(
    event_df: pd.DataFrame,
    *,
    group_columns: tuple[str, ...],
    forward_horizons: tuple[int, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if event_df.empty:
        return pd.DataFrame()
    for horizon in forward_horizons:
        return_column = f"fwd_{horizon}d_return_pct"
        valid = event_df[pd.to_numeric(event_df[return_column], errors="coerce").notna()].copy()
        if valid.empty:
            continue
        for keys, group in valid.groupby(list(group_columns), dropna=False, sort=True):
            key_tuple = keys if isinstance(keys, tuple) else (keys,)
            row = {column: key_tuple[idx] for idx, column in enumerate(group_columns)}
            row.update(_summarize_outcome(group, horizon))
            rows.append(row)
    return pd.DataFrame.from_records(rows)


def _build_flag_summary_df(
    event_df: pd.DataFrame,
    forward_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if event_df.empty:
        return pd.DataFrame()
    flag_specs = (
        ("microcap_lt5bn", "market_cap_bil_jpy", "<", 5.0),
        ("adv60_lt30mn", "avg_trading_value_60d_mil_jpy", "<", 30.0),
        ("volatility60d_ge50", "volatility_60d_pct", ">=", 50.0),
        ("drawdown_2y_ge40", "drawdown_from_2y_high_pct", "<=", -40.0),
        ("large_month_candle", "large_month_candle", "bool", 1.0),
        ("faded_after_large_month_close", "faded_after_large_month", "bool", 1.0),
        ("faded_after_large_month_high", "faded_after_large_month_high", "bool", 1.0),
        ("deep_drawdown_after_large_month", "deep_drawdown_after_large_month", "bool", 1.0),
    )
    rows: list[dict[str, Any]] = []
    for flag_name, column, op, threshold in flag_specs:
        if op == "<":
            flag = pd.to_numeric(event_df[column], errors="coerce") < threshold
        elif op == "<=":
            flag = pd.to_numeric(event_df[column], errors="coerce") <= threshold
        elif op == ">=":
            flag = pd.to_numeric(event_df[column], errors="coerce") >= threshold
        else:
            flag = event_df[column].fillna(False).astype(bool)
        for flag_value, group in event_df.assign(_flag=flag).groupby("_flag", sort=False):
            for horizon in forward_horizons:
                return_column = f"fwd_{horizon}d_return_pct"
                valid = group[pd.to_numeric(group[return_column], errors="coerce").notna()]
                if valid.empty:
                    continue
                row = {"flag": flag_name, "flag_value": bool(flag_value)}
                row.update(_summarize_outcome(valid, horizon))
                rows.append(row)
    return pd.DataFrame.from_records(rows)


def _summarize_outcome(group: pd.DataFrame, horizon: int) -> dict[str, Any]:
    returns = pd.to_numeric(group[f"fwd_{horizon}d_return_pct"], errors="coerce").dropna()
    max_upside = pd.to_numeric(group[f"fwd_{horizon}d_max_upside_pct"], errors="coerce")
    max_downside = pd.to_numeric(group[f"fwd_{horizon}d_max_downside_pct"], errors="coerce")
    severe = group[f"fwd_{horizon}d_severe_loss"].astype("boolean").fillna(False).astype(bool)
    upside_20 = group[f"fwd_{horizon}d_upside_20pct"].astype("boolean").fillna(False).astype(bool)
    return {
        "horizon_days": horizon,
        "event_count": int(len(group)),
        "mean_return_pct": _series_mean(returns),
        "median_return_pct": _series_median(returns),
        "p10_return_pct": _series_quantile(returns, 0.10),
        "severe_loss_rate_pct": float(severe.mean() * 100.0) if len(severe) else np.nan,
        "upside_20pct_rate_pct": float(upside_20.mean() * 100.0) if len(upside_20) else np.nan,
        "mean_max_upside_pct": _series_mean(max_upside),
        "mean_max_downside_pct": _series_mean(max_downside),
    }


def _build_current_snapshot_df(event_df: pd.DataFrame) -> pd.DataFrame:
    if event_df.empty:
        return pd.DataFrame()
    latest_date = str(event_df["snapshot_date"].max())
    columns = [
        "snapshot_date",
        "rank",
        "code",
        "company_name",
        "score",
        "score_before_boost",
        "breakout_boost",
        "pbr",
        "forward_per",
        "market_cap_bil_jpy",
        "avg_trading_value_60d_mil_jpy",
        "volatility_60d_pct",
        "return_252d_pct",
        "rebound_from_252d_low_pct",
        "drawdown_from_2y_high_pct",
        "max_month",
        "max_month_body_pct",
        "max_month_range_pct",
        "max_month_high",
        "max_month_close",
        "current_vs_max_month_close_pct",
        "current_vs_max_month_high_pct",
        "large_month_candle",
        "faded_after_large_month",
        "faded_after_large_month_high",
        "deep_drawdown_after_large_month",
        "speculative_risk_score",
        "speculative_risk_bucket",
        "pattern_bucket",
    ]
    available_columns = [column for column in columns if column in event_df.columns]
    return (
        event_df[event_df["snapshot_date"].astype(str) == latest_date]
        .sort_values(["rank", "code"])
        .loc[:, available_columns]
        .head(50)
        .reset_index(drop=True)
    )


def _empty_candidate_event_df(forward_horizons: Iterable[int]) -> pd.DataFrame:
    columns = [
        "snapshot_date",
        "rank",
        "code",
        "company_name",
        "score",
        "pattern_bucket",
        "speculative_risk_bucket",
    ]
    for horizon in forward_horizons:
        columns.extend(
            [
                f"fwd_{horizon}d_return_pct",
                f"fwd_{horizon}d_max_upside_pct",
                f"fwd_{horizon}d_max_downside_pct",
                f"fwd_{horizon}d_severe_loss",
                f"fwd_{horizon}d_upside_20pct",
            ]
        )
    return pd.DataFrame(columns=columns)


def _series_mean(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else np.nan


def _series_median(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.median()) if not values.empty else np.nan


def _series_quantile(series: pd.Series, quantile: float) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.quantile(quantile)) if not values.empty else np.nan


def _finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return number


def _finite_int(value: Any) -> int | None:
    number = _finite_float(value)
    return int(number) if number is not None else None


def _lt(value: object, threshold: float) -> bool:
    number = _finite_float(value)
    return number is not None and number < threshold


def _le(value: object, threshold: float) -> bool:
    number = _finite_float(value)
    return number is not None and number <= threshold


def _ge(value: object, threshold: float) -> bool:
    number = _finite_float(value)
    return number is not None and number >= threshold


def _format_date(value: pd.Timestamp) -> str:
    return value.strftime("%Y-%m-%d")


def _format_pct(value: object) -> str:
    number = _finite_float(value)
    return "-" if number is None else f"{number:.2f}%"


def _format_int(value: object) -> str:
    number = _finite_int(value)
    return "-" if number is None else str(number)


def _format_score(value: object) -> str:
    number = _finite_float(value)
    return "-" if number is None else f"{number:.3f}"


def _build_summary_markdown(result: StandardValuePumpFadeDecompositionResult) -> str:
    lines = [
        "# Standard Value Pump/Fade Decomposition",
        "",
        f"- Analysis window: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Snapshot count: `{result.snapshot_count}`",
        f"- Candidate rows: `{result.candidate_count}`",
        f"- Score profile: `{result.score_profile}`",
        f"- Pump/fade policy: {result.pump_fade_policy}",
        "",
        "## Risk Score Summary",
        "",
    ]
    focus = result.risk_score_summary_df
    if focus.empty:
        lines.append("- No completed forward-return rows.")
    else:
        lines.extend(
            [
                "| Top bucket | Risk bucket | Horizon | Events | Mean | Median | P10 | Severe | Upside20 |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        display = focus.sort_values(
            ["horizon_days", "top_rank_bucket", "speculative_risk_bucket"]
        ).head(36)
        for row in display.to_dict(orient="records"):
            lines.append(
                "| "
                f"`{row.get('top_rank_bucket')}` | "
                f"`{row.get('speculative_risk_bucket')}` | "
                f"{_format_int(row.get('horizon_days'))} | "
                f"{_format_int(row.get('event_count'))} | "
                f"{_format_pct(row.get('mean_return_pct'))} | "
                f"{_format_pct(row.get('median_return_pct'))} | "
                f"{_format_pct(row.get('p10_return_pct'))} | "
                f"{_format_pct(row.get('severe_loss_rate_pct'))} | "
                f"{_format_pct(row.get('upside_20pct_rate_pct'))} |"
            )
    lines.extend(
        [
            "",
            "## Pattern Summary",
            "",
        ]
    )
    if result.pattern_summary_df.empty:
        lines.append("- No pattern rows.")
    else:
        lines.extend(
            [
                "| Top bucket | Pattern | Horizon | Events | Mean | Median | P10 | Severe | Upside20 |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        display = result.pattern_summary_df.sort_values(
            ["horizon_days", "top_rank_bucket", "pattern_bucket"]
        ).head(48)
        for row in display.to_dict(orient="records"):
            lines.append(
                "| "
                f"`{row.get('top_rank_bucket')}` | "
                f"`{row.get('pattern_bucket')}` | "
                f"{_format_int(row.get('horizon_days'))} | "
                f"{_format_int(row.get('event_count'))} | "
                f"{_format_pct(row.get('mean_return_pct'))} | "
                f"{_format_pct(row.get('median_return_pct'))} | "
                f"{_format_pct(row.get('p10_return_pct'))} | "
                f"{_format_pct(row.get('severe_loss_rate_pct'))} | "
                f"{_format_pct(row.get('upside_20pct_rate_pct'))} |"
            )
    lines.extend(
        [
            "",
            "## Current Snapshot Examples",
            "",
        ]
    )
    if result.current_snapshot_df.empty:
        lines.append("- No current snapshot rows.")
    else:
        lines.extend(
            [
                "| Rank | Code | Name | Score | MCap bn | ADV60 mn | Vol60 | 2y DD | Max month | Fade close | Fade high | Risk | Pattern |",
                "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |",
            ]
        )
        for row in result.current_snapshot_df.head(25).to_dict(orient="records"):
            lines.append(
                "| "
                f"{_format_int(row.get('rank'))} | "
                f"`{row.get('code')}` | "
                f"{row.get('company_name')} | "
                f"{_format_score(row.get('score'))} | "
                f"{_format_number(row.get('market_cap_bil_jpy'))} | "
                f"{_format_number(row.get('avg_trading_value_60d_mil_jpy'))} | "
                f"{_format_pct(row.get('volatility_60d_pct'))} | "
                f"{_format_pct(row.get('drawdown_from_2y_high_pct'))} | "
                f"`{row.get('max_month')}` | "
                f"{_format_pct(row.get('current_vs_max_month_close_pct'))} | "
                f"{_format_pct(row.get('current_vs_max_month_high_pct'))} | "
                f"{_format_int(row.get('speculative_risk_score'))} | "
                f"`{row.get('pattern_bucket')}` |"
            )
    lines.append("")
    return "\n".join(lines)


def _format_number(value: object) -> str:
    number = _finite_float(value)
    return "-" if number is None else f"{number:.1f}"


def write_standard_value_pump_fade_decomposition_bundle(
    result: StandardValuePumpFadeDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID,
        module="src.domains.analytics.standard_value_pump_fade_decomposition",
        function="run_standard_value_pump_fade_decomposition_from_frames",
        params={
            "forward_horizons": list(result.forward_horizons),
            "top_ranks": list(result.top_ranks),
            "score_profile": result.score_profile,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_standard_value_pump_fade_decomposition_bundle(
    bundle_path: str | Path,
) -> StandardValuePumpFadeDecompositionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=StandardValuePumpFadeDecompositionResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_standard_value_pump_fade_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_standard_value_pump_fade_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__ = [
    "DEFAULT_FORWARD_HORIZONS",
    "DEFAULT_TOP_RANKS",
    "STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID",
    "StandardValuePumpFadeDecompositionResult",
    "get_standard_value_pump_fade_decomposition_bundle_path_for_run_id",
    "get_standard_value_pump_fade_decomposition_latest_bundle_path",
    "load_standard_value_pump_fade_decomposition_bundle",
    "run_standard_value_pump_fade_decomposition_from_frames",
    "write_standard_value_pump_fade_decomposition_bundle",
]
