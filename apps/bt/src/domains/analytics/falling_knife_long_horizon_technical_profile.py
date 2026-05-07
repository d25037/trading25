"""Long-horizon technical profile for falling-knife events."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.falling_knife_reversal_study import (
    get_falling_knife_reversal_study_latest_bundle_path,
    load_falling_knife_reversal_study_bundle,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    load_research_bundle_info,
    resolve_required_bundle_path,
    write_dataclass_research_bundle,
)

FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID = (
    "market-behavior/falling-knife-long-horizon-technical-profile"
)
DEFAULT_BUCKET_COUNT = 5
DEFAULT_HORIZON_DAYS = 20
DEFAULT_SEVERE_LOSS_THRESHOLD = -0.10
DEFAULT_REBOUND_THRESHOLD = 0.0
DEFAULT_SMA_WINDOW = 250
DEFAULT_LONG_WINDOW = 252

_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_event_df",
    "technical_feature_summary_df",
    "technical_bucket_summary_df",
    "technical_rule_summary_df",
    "interaction_summary_df",
    "feature_rank_df",
)
_PREFER_4DIGIT_ORDER_SQL = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"


@dataclass(frozen=True)
class LongHorizonTechnicalFeatureSpec:
    key: str
    family: str
    label: str
    preferred_high: bool | None


@dataclass(frozen=True)
class FallingKnifeLongHorizonTechnicalProfileResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizon_days: int
    severe_loss_threshold: float
    rebound_threshold: float
    bucket_count: int
    baseline_count: int
    technical_feature_count: int
    feature_policy: str
    enriched_event_df: pd.DataFrame
    technical_feature_summary_df: pd.DataFrame
    technical_bucket_summary_df: pd.DataFrame
    technical_rule_summary_df: pd.DataFrame
    interaction_summary_df: pd.DataFrame
    feature_rank_df: pd.DataFrame


LONG_HORIZON_TECHNICAL_FEATURES: tuple[LongHorizonTechnicalFeatureSpec, ...] = (
    LongHorizonTechnicalFeatureSpec("return_252d_pct", "momentum", "Return 252d", None),
    LongHorizonTechnicalFeatureSpec(
        "rebound_from_252d_low_pct",
        "reversal",
        "Rebound from 252d low",
        None,
    ),
    LongHorizonTechnicalFeatureSpec(
        "drawdown_from_252d_high_pct",
        "reversal",
        "Drawdown from 252d high",
        False,
    ),
    LongHorizonTechnicalFeatureSpec(
        "range_position_252d",
        "reversal",
        "252d range position",
        None,
    ),
    LongHorizonTechnicalFeatureSpec("price_to_sma250", "trend", "Price / SMA250", None),
    LongHorizonTechnicalFeatureSpec(
        "sma250_slope_20d_pct",
        "trend",
        "SMA250 slope 20d",
        True,
    ),
)
_FEATURE_BY_KEY = {feature.key: feature for feature in LONG_HORIZON_TECHNICAL_FEATURES}


def run_falling_knife_long_horizon_technical_profile(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    severe_loss_threshold: float = DEFAULT_SEVERE_LOSS_THRESHOLD,
    rebound_threshold: float = DEFAULT_REBOUND_THRESHOLD,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
) -> FallingKnifeLongHorizonTechnicalProfileResult:
    if horizon_days < 1:
        raise ValueError("horizon_days must be positive")
    if bucket_count < 2:
        raise ValueError("bucket_count must be >= 2")
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_falling_knife_reversal_study_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Falling-knife reversal study bundle was not found. "
            "Run run_falling_knife_reversal_study.py first or pass --input-bundle."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    input_result = load_falling_knife_reversal_study_bundle(resolved_input)
    event_df = _prepare_event_df(
        input_result.event_df,
        horizon_days=horizon_days,
        severe_loss_threshold=severe_loss_threshold,
        rebound_threshold=rebound_threshold,
    )
    source_mode, source_detail, price_df = _load_price_df(
        input_result.db_path,
        codes=tuple(event_df["code"].astype(str).unique()) if not event_df.empty else (),
    )
    enriched_event_df = _build_enriched_event_df(event_df, price_df=price_df)
    technical_feature_summary_df = _build_technical_feature_summary_df(enriched_event_df)
    technical_bucket_summary_df = _build_technical_bucket_summary_df(
        enriched_event_df,
        bucket_count=bucket_count,
    )
    technical_rule_summary_df = _build_technical_rule_summary_df(
        enriched_event_df,
        severe_loss_threshold=severe_loss_threshold,
    )
    interaction_summary_df = _build_interaction_summary_df(
        enriched_event_df,
        severe_loss_threshold=severe_loss_threshold,
    )
    feature_rank_df = _build_feature_rank_df(technical_bucket_summary_df)
    technical_feature_count = (
        int(pd.to_numeric(enriched_event_df["range_position_252d"], errors="coerce").notna().sum())
        if not enriched_event_df.empty
        else 0
    )
    return FallingKnifeLongHorizonTechnicalProfileResult(
        db_path=input_result.db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_result.analysis_start_date,
        analysis_end_date=input_result.analysis_end_date,
        horizon_days=int(horizon_days),
        severe_loss_threshold=float(severe_loss_threshold),
        rebound_threshold=float(rebound_threshold),
        bucket_count=int(bucket_count),
        baseline_count=int(len(event_df)),
        technical_feature_count=technical_feature_count,
        feature_policy=(
            "long-horizon technical features use each event's signal_date close only; "
            "post-signal price rows are excluded from feature construction; "
            "rows with fewer than 250 prior closes are kept as history_class diagnostics"
        ),
        enriched_event_df=enriched_event_df,
        technical_feature_summary_df=technical_feature_summary_df,
        technical_bucket_summary_df=technical_bucket_summary_df,
        technical_rule_summary_df=technical_rule_summary_df,
        interaction_summary_df=interaction_summary_df,
        feature_rank_df=feature_rank_df,
    )


def _prepare_event_df(
    event_df: pd.DataFrame,
    *,
    horizon_days: int,
    severe_loss_threshold: float,
    rebound_threshold: float,
) -> pd.DataFrame:
    columns = [
        "event_id",
        "signal_date",
        "code",
        "market_name",
        "risk_adjusted_bucket",
        "condition_count",
        "catch_return",
        "non_rebound",
        "severe_loss",
    ]
    return_column = f"catch_return_{horizon_days}d"
    if event_df.empty:
        return _empty_df(columns)
    if return_column not in event_df.columns:
        raise ValueError(f"input event_df does not contain {return_column}")
    result = event_df.copy().reset_index(drop=True)
    result["event_id"] = [f"event_{idx:08d}" for idx in range(len(result))]
    result["catch_return"] = pd.to_numeric(result[return_column], errors="coerce")
    result = result[result["catch_return"].notna()].copy()
    result["non_rebound"] = result["catch_return"] <= rebound_threshold
    result["severe_loss"] = result["catch_return"] <= severe_loss_threshold
    for column in ("market_name", "risk_adjusted_bucket", "condition_count"):
        if column not in result.columns:
            result[column] = np.nan
    return result[columns].reset_index(drop=True)


def _load_price_df(db_path: str, *, codes: Sequence[str]) -> tuple[SourceMode, str, pd.DataFrame]:
    columns = ["date", "code", "open", "high", "low", "close", "volume"]
    if not codes:
        return "live", "empty input", _empty_df(columns)
    normalized_codes = tuple(dict.fromkeys(str(code) for code in codes if str(code).strip()))
    placeholders = ", ".join("?" for _ in normalized_codes)
    normalized_code_sql = normalize_code_sql("code")
    sql = f"""
        SELECT
            date,
            normalized_code AS code,
            open,
            high,
            low,
            close,
            volume
        FROM (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY {_PREFER_4DIGIT_ORDER_SQL}, code
                ) AS row_priority
            FROM stock_data
            WHERE {normalized_code_sql} IN ({placeholders})
        )
        WHERE row_priority = 1
            AND close IS NOT NULL
        ORDER BY code, date
    """
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="falling-knife-long-tech-",
    ) as ctx:
        frame = ctx.connection.execute(sql, list(normalized_codes)).fetchdf()
        return ctx.source_mode, ctx.source_detail, frame


def _build_enriched_event_df(event_df: pd.DataFrame, *, price_df: pd.DataFrame) -> pd.DataFrame:
    if event_df.empty:
        return _ensure_enriched_columns(event_df.copy())
    feature_frames = {
        str(code): _build_symbol_feature_frame(frame)
        for code, frame in price_df.groupby("code", sort=False)
    }
    first_price_dates = {
        str(code): _first_date(frame) for code, frame in price_df.groupby("code", sort=False)
    }
    price_dates_by_code = {
        str(code): pd.DatetimeIndex(
            pd.to_datetime(frame["date"], errors="coerce").dropna().sort_values()
        )
        for code, frame in price_df.groupby("code", sort=False)
    }
    rows: list[dict[str, Any]] = []
    for event in event_df.to_dict(orient="records"):
        payload = {str(key): value for key, value in event.items()}
        code = str(payload.get("code", ""))
        signal_date = _coerce_timestamp(payload.get("signal_date"))
        feature_payload = _lookup_signal_feature_row(
            feature_frames.get(code),
            signal_date=signal_date,
        )
        prior_close_rows = _prior_close_count(
            price_dates_by_code.get(code),
            signal_date=signal_date,
        )
        payload.update(feature_payload)
        payload["first_price_date"] = first_price_dates.get(code)
        payload["prior_close_rows"] = prior_close_rows
        payload["history_class"] = _classify_history(prior_close_rows=prior_close_rows)
        rows.append(payload)
    return _ensure_enriched_columns(pd.DataFrame(rows))


def _build_symbol_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "close",
        *(feature.key for feature in LONG_HORIZON_TECHNICAL_FEATURES),
    ]
    if frame.empty:
        return _empty_df(columns)
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna(subset=["date", "close"]).sort_values("date", kind="stable")
    result = result.drop_duplicates("date", keep="last").reset_index(drop=True)
    close = result["close"].astype(float)
    sma250 = close.rolling(DEFAULT_SMA_WINDOW, min_periods=DEFAULT_SMA_WINDOW).mean()
    result["price_to_sma250"] = close / sma250
    result["sma250_slope_20d_pct"] = (sma250 / sma250.shift(20) - 1.0) * 100.0
    result["return_252d_pct"] = (close / close.shift(DEFAULT_LONG_WINDOW) - 1.0) * 100.0
    high_252 = close.rolling(DEFAULT_LONG_WINDOW, min_periods=DEFAULT_LONG_WINDOW).max()
    low_252 = close.rolling(DEFAULT_LONG_WINDOW, min_periods=DEFAULT_LONG_WINDOW).min()
    range_252 = high_252 - low_252
    result["drawdown_from_252d_high_pct"] = (close / high_252 - 1.0) * 100.0
    result["rebound_from_252d_low_pct"] = (close / low_252 - 1.0) * 100.0
    result["range_position_252d"] = (close - low_252) / range_252.replace(0, np.nan)
    return result[columns]


def _lookup_signal_feature_row(
    feature_df: pd.DataFrame | None,
    *,
    signal_date: pd.Timestamp | None,
) -> dict[str, Any]:
    feature_keys = tuple(feature.key for feature in LONG_HORIZON_TECHNICAL_FEATURES)
    if feature_df is None or feature_df.empty or signal_date is None:
        return {key: np.nan for key in feature_keys} | {
            "feature_date": None,
            "feature_lag_days": np.nan,
            "close": np.nan,
        }
    dates = pd.DatetimeIndex(pd.to_datetime(feature_df["date"], errors="coerce"))
    position = dates.searchsorted(signal_date, side="right") - 1
    if position < 0:
        return {key: np.nan for key in feature_keys} | {
            "feature_date": None,
            "feature_lag_days": np.nan,
            "close": np.nan,
        }
    row = feature_df.iloc[int(position)]
    feature_date = cast(pd.Timestamp, row["date"])
    payload: dict[str, Any] = {
        "feature_date": feature_date.strftime("%Y-%m-%d"),
        "feature_lag_days": float((signal_date - feature_date).days),
        "close": _float_or_nan(row.get("close")),
    }
    for key in feature_keys:
        payload[key] = _float_or_nan(row.get(key))
    return payload


def _prior_close_count(
    dates: pd.DatetimeIndex | None,
    *,
    signal_date: pd.Timestamp | None,
) -> int:
    if dates is None or signal_date is None:
        return 0
    return int(dates.searchsorted(signal_date, side="left"))


def _build_technical_feature_summary_df(enriched_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "feature_key",
        "feature_family",
        "feature_label",
        "event_count",
        "coverage_pct",
        "feature_min",
        "feature_median",
        "feature_max",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    total = len(enriched_event_df)
    records: list[dict[str, Any]] = []
    for spec in LONG_HORIZON_TECHNICAL_FEATURES:
        values = pd.to_numeric(enriched_event_df[spec.key], errors="coerce").dropna()
        records.append(
            {
                "feature_key": spec.key,
                "feature_family": spec.family,
                "feature_label": spec.label,
                "event_count": int(len(values)),
                "coverage_pct": float(len(values) / total * 100.0) if total else math.nan,
                "feature_min": float(values.min()) if not values.empty else math.nan,
                "feature_median": float(values.median()) if not values.empty else math.nan,
                "feature_max": float(values.max()) if not values.empty else math.nan,
            }
        )
    return pd.DataFrame(records, columns=columns)


def _build_technical_bucket_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "feature_key",
        "feature_family",
        "feature_label",
        "market_name",
        "bucket_rank",
        "bucket_count",
        "event_count",
        "coverage_pct",
        "feature_min",
        "feature_median",
        "feature_max",
        "mean_return_pct",
        "median_return_pct",
        "non_rebound_rate_pct",
        "severe_loss_rate_pct",
        "p10_return_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    total_by_market = enriched_event_df.groupby("market_name", dropna=False).size().to_dict()
    for spec in LONG_HORIZON_TECHNICAL_FEATURES:
        frame = enriched_event_df[pd.to_numeric(enriched_event_df[spec.key], errors="coerce").notna()].copy()
        if frame.empty:
            continue
        frame["bucket_rank"] = _bucket_rank_by_market(frame, spec.key, bucket_count=bucket_count)
        for keys, group in frame.groupby(["market_name", "bucket_rank"], dropna=False, sort=True):
            market_name, bucket_rank = keys
            values = pd.to_numeric(group[spec.key], errors="coerce").dropna()
            total = int(total_by_market.get(market_name, len(group)))
            row = {
                "feature_key": spec.key,
                "feature_family": spec.family,
                "feature_label": spec.label,
                "market_name": market_name,
                "bucket_rank": int(cast(int, bucket_rank)),
                "bucket_count": int(bucket_count),
                "event_count": int(len(group)),
                "coverage_pct": float(len(group) / total * 100.0) if total else math.nan,
                "feature_min": float(values.min()) if not values.empty else math.nan,
                "feature_median": float(values.median()) if not values.empty else math.nan,
                "feature_max": float(values.max()) if not values.empty else math.nan,
            }
            row.update(_return_stats(group))
            records.append(row)
    return pd.DataFrame(records, columns=columns)


def _build_technical_rule_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    columns = [
        "rule_name",
        "rule_label",
        "baseline_count",
        "kept_count",
        "removed_count",
        "kept_fraction_pct",
        "baseline_mean_pct",
        "kept_mean_pct",
        "removed_mean_pct",
        "baseline_median_pct",
        "kept_median_pct",
        "baseline_severe_loss_rate_pct",
        "kept_severe_loss_rate_pct",
        "removed_severe_loss_rate_pct",
        "severe_loss_rate_reduction_pct",
        "mean_return_cost_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    baseline_stats = _return_stats(enriched_event_df)
    records: list[dict[str, Any]] = []
    for rule_name, rule_label, keep_mask in _candidate_keep_masks(enriched_event_df):
        kept = enriched_event_df[keep_mask].copy()
        removed = enriched_event_df[~keep_mask].copy()
        kept_stats = _return_stats(kept)
        removed_stats = _return_stats(removed)
        records.append(
            {
                "rule_name": rule_name,
                "rule_label": rule_label,
                "baseline_count": int(len(enriched_event_df)),
                "kept_count": int(len(kept)),
                "removed_count": int(len(removed)),
                "kept_fraction_pct": float(len(kept) / len(enriched_event_df) * 100.0),
                "baseline_mean_pct": baseline_stats["mean_return_pct"],
                "kept_mean_pct": kept_stats["mean_return_pct"],
                "removed_mean_pct": removed_stats["mean_return_pct"],
                "baseline_median_pct": baseline_stats["median_return_pct"],
                "kept_median_pct": kept_stats["median_return_pct"],
                "baseline_severe_loss_rate_pct": baseline_stats["severe_loss_rate_pct"],
                "kept_severe_loss_rate_pct": kept_stats["severe_loss_rate_pct"],
                "removed_severe_loss_rate_pct": removed_stats["severe_loss_rate_pct"],
                "severe_loss_rate_reduction_pct": _subtract_or_nan(
                    baseline_stats["severe_loss_rate_pct"],
                    kept_stats["severe_loss_rate_pct"],
                ),
                "mean_return_cost_pct": _subtract_or_nan(
                    baseline_stats["mean_return_pct"],
                    kept_stats["mean_return_pct"],
                ),
            }
        )
    result = pd.DataFrame(records, columns=columns)
    return result.sort_values(
        ["severe_loss_rate_reduction_pct", "mean_return_cost_pct"],
        ascending=[False, True],
        na_position="last",
    ).reset_index(drop=True)


def _candidate_keep_masks(enriched_event_df: pd.DataFrame) -> list[tuple[str, str, pd.Series]]:
    market = enriched_event_df["market_name"].astype(str)
    range_pos = pd.to_numeric(enriched_event_df["range_position_252d"], errors="coerce")
    rebound = pd.to_numeric(enriched_event_df["rebound_from_252d_low_pct"], errors="coerce")
    ret252 = pd.to_numeric(enriched_event_df["return_252d_pct"], errors="coerce")
    drawdown = pd.to_numeric(enriched_event_df["drawdown_from_252d_high_pct"], errors="coerce")
    return [
        ("keep_non_growth", "Keep non-Growth markets", market != "グロース"),
        ("keep_standard", "Keep Standard market", market == "スタンダード"),
        (
            "keep_rebounded_from_low",
            "Keep rebound_from_252d_low_pct >= median",
            rebound >= _finite_median(rebound),
        ),
        (
            "keep_low_to_mid_range_position",
            "Keep range_position_252d <= median",
            range_pos <= _finite_median(range_pos),
        ),
        (
            "keep_positive_252d_return",
            "Keep return_252d_pct > 0",
            ret252 > 0.0,
        ),
        (
            "exclude_deep_252d_drawdown",
            "Exclude drawdown_from_252d_high_pct <= -50%",
            drawdown > -50.0,
        ),
        (
            "keep_standard_and_rebounded_from_low",
            "Keep Standard with rebound_from_252d_low_pct >= median",
            (market == "スタンダード") & (rebound >= _finite_median(rebound)),
        ),
    ]


def _build_interaction_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    columns = [
        "interaction_name",
        "interaction_value",
        "event_count",
        "mean_return_pct",
        "median_return_pct",
        "non_rebound_rate_pct",
        "severe_loss_rate_pct",
        "p10_return_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    result = enriched_event_df.copy()
    result["range_position_side"] = _median_side(
        result["range_position_252d"],
        low_label="range_low",
        high_label="range_high",
        missing_label="range_missing",
    )
    result["rebound_from_low_side"] = _median_side(
        result["rebound_from_252d_low_pct"],
        low_label="rebound_low",
        high_label="rebound_high",
        missing_label="rebound_missing",
    )
    interaction_specs = (
        ("market_x_history", ["market_name", "history_class"]),
        ("market_x_range_position", ["market_name", "range_position_side"]),
        ("market_x_rebound_from_low", ["market_name", "rebound_from_low_side"]),
    )
    records: list[dict[str, Any]] = []
    for interaction_name, group_columns in interaction_specs:
        for keys, group in result.groupby(group_columns, dropna=False, sort=True):
            key_values = keys if isinstance(keys, tuple) else (keys,)
            row = {
                "interaction_name": interaction_name,
                "interaction_value": "__".join(str(value) for value in key_values),
                "event_count": int(len(group)),
            }
            row.update(_return_stats(group, severe_loss_threshold=severe_loss_threshold))
            records.append(row)
    return pd.DataFrame(records, columns=columns)


def _build_feature_rank_df(technical_bucket_summary_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "feature_key",
        "feature_family",
        "feature_label",
        "market_name",
        "low_bucket_mean_return_pct",
        "high_bucket_mean_return_pct",
        "high_minus_low_mean_return_pct",
        "low_bucket_severe_loss_rate_pct",
        "high_bucket_severe_loss_rate_pct",
        "high_minus_low_severe_loss_rate_pct",
        "best_bucket_rank",
        "best_bucket_mean_return_pct",
        "worst_bucket_rank",
        "worst_bucket_mean_return_pct",
        "best_minus_worst_mean_return_pct",
        "rank_score",
        "direction_hint",
    ]
    if technical_bucket_summary_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in technical_bucket_summary_df.groupby(["feature_key", "market_name"], sort=False):
        feature_key, market_name = keys
        spec = _FEATURE_BY_KEY[str(feature_key)]
        bucket_count = int(pd.to_numeric(group["bucket_count"], errors="coerce").max())
        low = group[group["bucket_rank"].astype(int) == 1]
        high = group[group["bucket_rank"].astype(int) == bucket_count]
        best = group.sort_values("mean_return_pct", ascending=False, na_position="last").head(1)
        worst = group.sort_values("mean_return_pct", ascending=True, na_position="last").head(1)
        high_minus_low = _row_value(high, "mean_return_pct") - _row_value(low, "mean_return_pct")
        high_minus_low_severe = _row_value(high, "severe_loss_rate_pct") - _row_value(low, "severe_loss_rate_pct")
        best_minus_worst = _row_value(best, "mean_return_pct") - _row_value(worst, "mean_return_pct")
        rank_score = abs(best_minus_worst) + abs(high_minus_low) + abs(high_minus_low_severe)
        records.append(
            {
                "feature_key": spec.key,
                "feature_family": spec.family,
                "feature_label": spec.label,
                "market_name": market_name,
                "low_bucket_mean_return_pct": _row_value(low, "mean_return_pct"),
                "high_bucket_mean_return_pct": _row_value(high, "mean_return_pct"),
                "high_minus_low_mean_return_pct": high_minus_low,
                "low_bucket_severe_loss_rate_pct": _row_value(low, "severe_loss_rate_pct"),
                "high_bucket_severe_loss_rate_pct": _row_value(high, "severe_loss_rate_pct"),
                "high_minus_low_severe_loss_rate_pct": high_minus_low_severe,
                "best_bucket_rank": _row_value(best, "bucket_rank"),
                "best_bucket_mean_return_pct": _row_value(best, "mean_return_pct"),
                "worst_bucket_rank": _row_value(worst, "bucket_rank"),
                "worst_bucket_mean_return_pct": _row_value(worst, "mean_return_pct"),
                "best_minus_worst_mean_return_pct": best_minus_worst,
                "rank_score": rank_score,
                "direction_hint": _direction_hint(high_minus_low, spec),
            }
        )
    return pd.DataFrame(records, columns=columns).sort_values(
        ["market_name", "rank_score"],
        ascending=[True, False],
        na_position="last",
    ).reset_index(drop=True)


def _return_stats(
    frame: pd.DataFrame,
    *,
    severe_loss_threshold: float = DEFAULT_SEVERE_LOSS_THRESHOLD,
) -> dict[str, float]:
    returns = pd.to_numeric(frame.get("catch_return", pd.Series(dtype="float64")), errors="coerce").dropna()
    if returns.empty:
        return {
            "mean_return_pct": math.nan,
            "median_return_pct": math.nan,
            "non_rebound_rate_pct": math.nan,
            "severe_loss_rate_pct": math.nan,
            "p10_return_pct": math.nan,
        }
    return {
        "mean_return_pct": float(returns.mean() * 100.0),
        "median_return_pct": float(returns.median() * 100.0),
        "non_rebound_rate_pct": _label_rate_pct(frame, "non_rebound", returns <= DEFAULT_REBOUND_THRESHOLD),
        "severe_loss_rate_pct": _label_rate_pct(frame, "severe_loss", returns <= severe_loss_threshold),
        "p10_return_pct": float(returns.quantile(0.10) * 100.0),
    }


def _label_rate_pct(frame: pd.DataFrame, column: str, fallback: pd.Series) -> float:
    if column in frame.columns:
        labels = frame.loc[fallback.index, column].dropna()
        if not labels.empty:
            return float(labels.astype(bool).mean() * 100.0)
    return float(fallback.mean() * 100.0)


def _median_side(
    values: pd.Series,
    *,
    low_label: str,
    high_label: str,
    missing_label: str,
) -> pd.Series:
    numeric_values = pd.to_numeric(values, errors="coerce")
    median = _finite_median(numeric_values)
    result = pd.Series(missing_label, index=values.index, dtype="object")
    if not math.isfinite(median):
        return result
    result.loc[numeric_values.notna() & (numeric_values < median)] = low_label
    result.loc[numeric_values.notna() & (numeric_values >= median)] = high_label
    return result


def _bucket_rank_by_market(frame: pd.DataFrame, column: str, *, bucket_count: int) -> pd.Series:
    ranks = pd.Series(np.nan, index=frame.index, dtype="float64")
    values = pd.to_numeric(frame[column], errors="coerce")
    for _, group in frame.groupby("market_name", dropna=False, sort=False):
        valid = values.loc[group.index].dropna().sort_values(kind="stable")
        count = len(valid)
        if count == 0:
            continue
        resolved_bucket_count = min(bucket_count, count)
        bucket = (np.floor(np.arange(count, dtype=float) * resolved_bucket_count / count) + 1).astype(int)
        ranks.loc[valid.index] = bucket
    return ranks


def _ensure_enriched_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for feature in LONG_HORIZON_TECHNICAL_FEATURES:
        if feature.key not in result.columns:
            result[feature.key] = np.nan
    for column in ("feature_date", "feature_lag_days", "close", "first_price_date", "prior_close_rows", "history_class"):
        if column not in result.columns:
            result[column] = np.nan
    return result


def _classify_history(*, prior_close_rows: int) -> str:
    if prior_close_rows >= 250:
        return "has_250_prior_closes"
    return "short_history_lt250_prior_closes"


def _first_date(frame: pd.DataFrame) -> str | None:
    dates = frame["date"].astype(str).sort_values()
    return str(dates.iloc[0]) if not dates.empty else None


def _coerce_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(ts):
        return None
    return cast(pd.Timestamp, ts.normalize())


def _float_or_nan(value: Any) -> float:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return float("nan")
    return number if math.isfinite(number) else float("nan")


def _finite_median(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.median()) if not clean.empty else float("nan")


def _subtract_or_nan(left: float | None, right: float | None) -> float:
    if left is None or right is None:
        return float("nan")
    if not math.isfinite(left) or not math.isfinite(right):
        return float("nan")
    return float(left - right)


def _row_value(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return float("nan")
    return _float_or_nan(frame.iloc[0][column])


def _direction_hint(high_minus_low: float, spec: LongHorizonTechnicalFeatureSpec) -> str:
    if not math.isfinite(high_minus_low) or abs(high_minus_low) < 1e-9:
        return "flat"
    direction = "high_better" if high_minus_low > 0 else "low_better"
    if spec.preferred_high is True:
        return f"{direction}_expected_high"
    if spec.preferred_high is False:
        return f"{direction}_expected_low"
    return direction


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _fmt(value: object, digits: int = 2) -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _build_summary_markdown(result: FallingKnifeLongHorizonTechnicalProfileResult) -> str:
    lines = [
        "# Falling Knife Long-Horizon Technical Profile",
        "",
        "## Snapshot",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Horizon: `{result.horizon_days}` sessions",
        f"- Baseline events: `{result.baseline_count}`",
        f"- Rows with 252d range features: `{result.technical_feature_count}`",
        f"- Feature policy: {result.feature_policy}.",
        "",
        "## Top Feature Rank Rows",
        "",
    ]
    if result.feature_rank_df.empty:
        lines.append("- No feature rank rows were produced.")
    else:
        for row in result.feature_rank_df.head(16).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_name']}` / `{row['feature_key']}`: "
                f"best-worst mean `{_fmt(row['best_minus_worst_mean_return_pct'])}pp`, "
                f"high-low mean `{_fmt(row['high_minus_low_mean_return_pct'])}pp`, "
                f"high-low severe `{_fmt(row['high_minus_low_severe_loss_rate_pct'])}pp`, "
                f"direction `{row['direction_hint']}`"
            )
    lines.extend(["", "## Top Rule Rows", ""])
    if result.technical_rule_summary_df.empty:
        lines.append("- No technical rule rows were produced.")
    else:
        for row in result.technical_rule_summary_df.head(12).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['rule_name']}`: kept `{int(cast(int, row['kept_count']))}`, "
                f"kept severe `{_fmt(row['kept_severe_loss_rate_pct'])}%`, "
                f"severe reduction `{_fmt(row['severe_loss_rate_reduction_pct'])}pt`, "
                f"mean cost `{_fmt(row['mean_return_cost_pct'])}pt`"
            )
    lines.extend(
        [
            "",
            "## Tables",
            "",
            "- `enriched_event_df`",
            "- `technical_feature_summary_df`",
            "- `technical_bucket_summary_df`",
            "- `technical_rule_summary_df`",
            "- `interaction_summary_df`",
            "- `feature_rank_df`",
        ]
    )
    return "\n".join(lines)


def write_falling_knife_long_horizon_technical_profile_bundle(
    result: FallingKnifeLongHorizonTechnicalProfileResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID,
        module=__name__,
        function="run_falling_knife_long_horizon_technical_profile",
        params={
            "input_bundle_path": result.input_bundle_path,
            "horizon_days": result.horizon_days,
            "severe_loss_threshold": result.severe_loss_threshold,
            "rebound_threshold": result.rebound_threshold,
            "bucket_count": result.bucket_count,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_falling_knife_long_horizon_technical_profile_bundle(
    bundle_path: str | Path,
) -> FallingKnifeLongHorizonTechnicalProfileResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FallingKnifeLongHorizonTechnicalProfileResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_falling_knife_long_horizon_technical_profile_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_falling_knife_long_horizon_technical_profile_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "FALLING_KNIFE_LONG_HORIZON_TECHNICAL_PROFILE_EXPERIMENT_ID",
    "FallingKnifeLongHorizonTechnicalProfileResult",
    "LONG_HORIZON_TECHNICAL_FEATURES",
    "get_falling_knife_long_horizon_technical_profile_bundle_path_for_run_id",
    "get_falling_knife_long_horizon_technical_profile_latest_bundle_path",
    "load_falling_knife_long_horizon_technical_profile_bundle",
    "run_falling_knife_long_horizon_technical_profile",
    "write_falling_knife_long_horizon_technical_profile_bundle",
)
