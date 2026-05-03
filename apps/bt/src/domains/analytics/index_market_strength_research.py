"""Index market-strength state research over sector indices."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)

INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/index-market-strength-research"
)
DEFAULT_LOOKBACK_WINDOWS: tuple[int, ...] = (20, 60, 120, 250)
DEFAULT_HORIZON_SESSIONS = 20
DEFAULT_DISCOVERY_END_DATE = "2019-12-31"
DEFAULT_VALIDATION_END_DATE = "2023-12-31"

_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "index_price_feature_df",
    "index_state_summary_df",
    "breadth_state_df",
    "breadth_state_summary_df",
    "feature_rank_df",
)


@dataclass(frozen=True)
class IndexMarketStrengthResearchResult:
    db_path: str
    source_mode: str
    source_detail: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_windows: tuple[int, ...]
    horizon_sessions: int
    target_category_prefix: str
    index_count: int
    observation_count: int
    feature_policy: str
    index_price_feature_df: pd.DataFrame
    index_state_summary_df: pd.DataFrame
    breadth_state_df: pd.DataFrame
    breadth_state_summary_df: pd.DataFrame
    feature_rank_df: pd.DataFrame


def run_index_market_strength_research(
    db_path: str | Path,
    *,
    lookback_windows: Sequence[int] = DEFAULT_LOOKBACK_WINDOWS,
    horizon_sessions: int = DEFAULT_HORIZON_SESSIONS,
    target_category_prefix: str = "sector33",
    discovery_end_date: str = DEFAULT_DISCOVERY_END_DATE,
    validation_end_date: str = DEFAULT_VALIDATION_END_DATE,
    min_summary_observations: int = 20,
) -> IndexMarketStrengthResearchResult:
    windows = _normalize_lookback_windows(lookback_windows)
    horizon = int(horizon_sessions)
    if horizon < 1:
        raise ValueError("horizon_sessions must be >= 1")
    if min_summary_observations < 1:
        raise ValueError("min_summary_observations must be >= 1")

    resolved_db_path = str(Path(db_path).expanduser())
    with open_readonly_analysis_connection(
        resolved_db_path,
        snapshot_prefix="index-market-strength-",
    ) as ctx:
        index_price_df = _query_index_price_rows(
            ctx.connection,
            target_category_prefix=target_category_prefix,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    if index_price_df.empty:
        raise RuntimeError(
            "No indices_data rows were found for "
            f"category prefix: {target_category_prefix!r}"
        )

    feature_df = _build_index_price_feature_df(
        index_price_df,
        lookback_windows=windows,
        horizon_sessions=horizon,
        discovery_end_date=discovery_end_date,
        validation_end_date=validation_end_date,
    )
    state_summary_df = _build_index_state_summary_df(
        feature_df,
        lookback_windows=windows,
        horizon_sessions=horizon,
        min_observations=min_summary_observations,
    )
    breadth_state_df = _build_breadth_state_df(
        feature_df,
        lookback_windows=windows,
        horizon_sessions=horizon,
    )
    breadth_summary_df = _build_breadth_state_summary_df(
        breadth_state_df,
        horizon_sessions=horizon,
        min_observations=max(5, min_summary_observations // 2),
    )
    feature_rank_df = _build_feature_rank_df(
        state_summary_df,
        breadth_summary_df,
    )

    valid_dates = pd.to_datetime(feature_df["date"], errors="coerce").dropna()
    return IndexMarketStrengthResearchResult(
        db_path=resolved_db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        analysis_start_date=valid_dates.min().strftime("%Y-%m-%d")
        if not valid_dates.empty
        else None,
        analysis_end_date=valid_dates.max().strftime("%Y-%m-%d")
        if not valid_dates.empty
        else None,
        lookback_windows=windows,
        horizon_sessions=horizon,
        target_category_prefix=target_category_prefix,
        index_count=int(feature_df["code"].nunique()),
        observation_count=int(len(feature_df)),
        feature_policy=(
            "Features use only each index's close/high/low history available at date t. "
            f"The objective is close-to-close return after {horizon} trading sessions. "
            "Bucket thresholds are fixed ex ante; breadth is equal-weight across the "
            "available target indices on each date."
        ),
        index_price_feature_df=feature_df,
        index_state_summary_df=state_summary_df,
        breadth_state_df=breadth_state_df,
        breadth_state_summary_df=breadth_summary_df,
        feature_rank_df=feature_rank_df,
    )


def _normalize_lookback_windows(lookback_windows: Sequence[int]) -> tuple[int, ...]:
    normalized = tuple(sorted({int(window) for window in lookback_windows}))
    if not normalized:
        raise ValueError("lookback_windows must not be empty")
    if any(window < 2 for window in normalized):
        raise ValueError("lookback_windows must be >= 2")
    return normalized


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _query_index_price_rows(
    conn: Any,
    *,
    target_category_prefix: str,
) -> pd.DataFrame:
    has_master = _table_exists(conn, "index_master")
    if has_master:
        category = target_category_prefix.lower().rstrip("*")
        category_operator = "LIKE" if target_category_prefix.endswith("*") else "="
        category_value = f"{category}%" if category_operator == "LIKE" else category
        sql = """
            SELECT
                d.code,
                d.date,
                d.open,
                d.high,
                d.low,
                d.close,
                COALESCE(NULLIF(d.sector_name, ''), m.name, d.code) AS index_name,
                COALESCE(m.category, '') AS category
            FROM indices_data d
            LEFT JOIN index_master m ON d.code = m.code
            WHERE lower(COALESCE(m.category, '')) {category_operator} ?
              AND d.open IS NOT NULL
              AND d.high IS NOT NULL
              AND d.low IS NOT NULL
              AND d.close IS NOT NULL
              AND d.close > 0
            ORDER BY d.code, d.date
        """.format(category_operator=category_operator)
        rows = conn.execute(sql, [category_value]).fetchdf()
    else:
        rows = conn.execute(
            """
            SELECT
                code,
                date,
                open,
                high,
                low,
                close,
                COALESCE(NULLIF(sector_name, ''), code) AS index_name,
                '' AS category
            FROM indices_data
            WHERE close IS NOT NULL
              AND upper(code) <> 'TOPIX'
            ORDER BY code, date
            """
        ).fetchdf()
    if rows.empty:
        return pd.DataFrame(
            columns=["code", "date", "open", "high", "low", "close", "index_name", "category"]
        )
    rows = rows.copy()
    rows["code"] = rows["code"].astype(str)
    rows["date"] = pd.to_datetime(rows["date"], errors="coerce")
    for col in ("open", "high", "low", "close"):
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    rows = rows.dropna(subset=["code", "date", "close"]).sort_values(["code", "date"])
    return rows.reset_index(drop=True)


def _build_index_price_feature_df(
    index_price_df: pd.DataFrame,
    *,
    lookback_windows: tuple[int, ...],
    horizon_sessions: int,
    discovery_end_date: str,
    validation_end_date: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    horizon_col = f"forward_{horizon_sessions}d_return_pct"
    for _, group in index_price_df.groupby("code", sort=True):
        g = group.sort_values("date").copy()
        g[f"forward_{horizon_sessions}d_close"] = g["close"].shift(-horizon_sessions)
        g[horizon_col] = (
            g[f"forward_{horizon_sessions}d_close"] / g["close"] - 1.0
        ) * 100.0
        for window in lookback_windows:
            rolling_low = g["low"].rolling(window, min_periods=window).min()
            rolling_high = g["high"].rolling(window, min_periods=window).max()
            close_lag = g["close"].shift(window)
            range_width = rolling_high - rolling_low
            g[f"low_{window}d"] = rolling_low
            g[f"high_{window}d"] = rolling_high
            g[f"return_{window}d_pct"] = (g["close"] / close_lag - 1.0) * 100.0
            g[f"rebound_from_low_{window}d_pct"] = (
                g["close"] / rolling_low - 1.0
            ) * 100.0
            g[f"price_position_{window}d"] = np.where(
                range_width > 0,
                (g["close"] - rolling_low) / range_width,
                np.nan,
            )
            g[f"return_{window}d_bucket"] = g[f"return_{window}d_pct"].map(
                _bucket_return_pct
            )
            g[f"rebound_from_low_{window}d_bucket"] = g[
                f"rebound_from_low_{window}d_pct"
            ].map(_bucket_rebound_pct)
            g[f"price_position_{window}d_bucket"] = g[f"price_position_{window}d"].map(
                _bucket_price_position
            )
        frames.append(g)
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if result.empty:
        return result
    result["sample_period"] = result["date"].map(
        lambda value: _sample_period(
            value,
            discovery_end_date=discovery_end_date,
            validation_end_date=validation_end_date,
        )
    )
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    return result


def _bucket_return_pct(value: object) -> str | None:
    number = _coerce_float(value)
    if number is None:
        return None
    if number <= -10.0:
        return "loss_le_-10pct"
    if number <= 0.0:
        return "negative_to_0pct"
    if number <= 5.0:
        return "gain_0_to_5pct"
    if number <= 15.0:
        return "gain_5_to_15pct"
    return "gain_gt_15pct"


def _bucket_rebound_pct(value: object) -> str | None:
    number = _coerce_float(value)
    if number is None:
        return None
    if number < 1.0:
        return "no_rebound_lt_1pct"
    if number < 5.0:
        return "rebound_1_to_5pct"
    if number < 10.0:
        return "rebound_5_to_10pct"
    if number < 20.0:
        return "rebound_10_to_20pct"
    return "rebound_ge_20pct"


def _bucket_price_position(value: object) -> str | None:
    number = _coerce_float(value)
    if number is None:
        return None
    if number < 0.2:
        return "range_low_0_20"
    if number < 0.5:
        return "range_lower_mid_20_50"
    if number < 0.8:
        return "range_upper_mid_50_80"
    return "range_high_80_100"


def _coerce_float(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return number


def _sample_period(
    value: pd.Timestamp,
    *,
    discovery_end_date: str,
    validation_end_date: str,
) -> str:
    if value <= pd.Timestamp(discovery_end_date):
        return "discovery"
    if value <= pd.Timestamp(validation_end_date):
        return "validation"
    return "holdout"


def _build_index_state_summary_df(
    feature_df: pd.DataFrame,
    *,
    lookback_windows: tuple[int, ...],
    horizon_sessions: int,
    min_observations: int,
) -> pd.DataFrame:
    horizon_col = f"forward_{horizon_sessions}d_return_pct"
    rows: list[dict[str, object]] = []
    feature_specs = (
        ("return", "return_{window}d_bucket", "return_{window}d_pct"),
        (
            "rebound_from_low",
            "rebound_from_low_{window}d_bucket",
            "rebound_from_low_{window}d_pct",
        ),
        ("price_position", "price_position_{window}d_bucket", "price_position_{window}d"),
    )
    for window in lookback_windows:
        for family, bucket_template, value_template in feature_specs:
            bucket_col = bucket_template.format(window=window)
            value_col = value_template.format(window=window)
            for sample_period, period_df in _iter_period_frames(feature_df):
                usable = period_df.dropna(subset=[bucket_col, horizon_col])
                if usable.empty:
                    continue
                grouped = usable.groupby(bucket_col, dropna=True, sort=True)
                for bucket, group in grouped:
                    if len(group) < min_observations:
                        continue
                    rows.append(
                        _summary_row(
                            group,
                            return_col=horizon_col,
                            sample_period=sample_period,
                            lookback=window,
                            state_family="index_feature",
                            feature_family=family,
                            bucket=str(bucket),
                            feature_mean=float(
                                pd.to_numeric(group[value_col], errors="coerce").mean()
                            ),
                        )
                    )
    return pd.DataFrame(rows)


def _iter_period_frames(df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    frames = [("all", df)]
    if "sample_period" not in df.columns:
        return frames
    for period in ("discovery", "validation", "holdout"):
        period_df = df[df["sample_period"].astype(str) == period]
        if not period_df.empty:
            frames.append((period, period_df))
    return frames


def _summary_row(
    group: pd.DataFrame,
    *,
    return_col: str,
    sample_period: str,
    lookback: int,
    state_family: str,
    feature_family: str,
    bucket: str,
    feature_mean: float | None = None,
) -> dict[str, object]:
    returns = pd.to_numeric(group[return_col], errors="coerce").dropna()
    return {
        "sample_period": sample_period,
        "lookback": lookback,
        "state_family": state_family,
        "feature_family": feature_family,
        "bucket": bucket,
        "observation_count": int(len(returns)),
        "index_count": int(group["code"].nunique()) if "code" in group.columns else np.nan,
        "date_count": int(group["date"].nunique()) if "date" in group.columns else np.nan,
        "feature_mean": feature_mean,
        "mean_forward_return_pct": _safe_stat(returns, "mean"),
        "median_forward_return_pct": _safe_stat(returns, "median"),
        "hit_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else np.nan,
        "p05_forward_return_pct": _safe_quantile(returns, 0.05),
        "p10_forward_return_pct": _safe_quantile(returns, 0.10),
        "p90_forward_return_pct": _safe_quantile(returns, 0.90),
    }


def _safe_stat(series: pd.Series, method: str) -> float:
    if series.empty:
        return float("nan")
    if method == "mean":
        return float(series.mean())
    if method == "median":
        return float(series.median())
    raise ValueError(f"Unsupported stat method: {method}")


def _safe_quantile(series: pd.Series, q: float) -> float:
    if series.empty:
        return float("nan")
    return float(series.quantile(q))


def _build_breadth_state_df(
    feature_df: pd.DataFrame,
    *,
    lookback_windows: tuple[int, ...],
    horizon_sessions: int,
) -> pd.DataFrame:
    horizon_col = f"forward_{horizon_sessions}d_return_pct"
    rows: list[pd.DataFrame] = []
    for window in lookback_windows:
        return_col = f"return_{window}d_pct"
        rebound_col = f"rebound_from_low_{window}d_pct"
        position_col = f"price_position_{window}d"
        usable = feature_df.dropna(subset=[return_col, rebound_col, position_col, horizon_col]).copy()
        if usable.empty:
            continue
        usable["is_strong_index"] = (
            (pd.to_numeric(usable[return_col], errors="coerce") > 0.0)
            & (pd.to_numeric(usable[position_col], errors="coerce") >= 0.8)
        )
        usable["is_weak_index"] = (
            (pd.to_numeric(usable[return_col], errors="coerce") < 0.0)
            & (pd.to_numeric(usable[position_col], errors="coerce") <= 0.2)
        )
        usable["is_rebound_index"] = (
            (pd.to_numeric(usable[rebound_col], errors="coerce") >= 5.0)
            & (pd.to_numeric(usable[position_col], errors="coerce") >= 0.2)
        )
        usable["is_overheat_index"] = (
            (pd.to_numeric(usable[return_col], errors="coerce") >= 10.0)
            & (pd.to_numeric(usable[position_col], errors="coerce") >= 0.9)
        )
        grouped = usable.groupby("date", sort=True)
        frame = grouped.agg(
            index_count=("code", "nunique"),
            strong_index_count=("is_strong_index", "sum"),
            weak_index_count=("is_weak_index", "sum"),
            rebound_index_count=("is_rebound_index", "sum"),
            overheat_index_count=("is_overheat_index", "sum"),
            market_forward_return_pct=(horizon_col, "mean"),
            sample_period=("sample_period", "first"),
        ).reset_index()
        frame["lookback"] = window
        for col in (
            "strong_index_count",
            "weak_index_count",
            "rebound_index_count",
            "overheat_index_count",
        ):
            ratio_col = col.replace("_index_count", "_breadth_ratio")
            frame[ratio_col] = frame[col] / frame["index_count"]
        rows.append(frame)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _build_breadth_state_summary_df(
    breadth_state_df: pd.DataFrame,
    *,
    horizon_sessions: int,
    min_observations: int,
) -> pd.DataFrame:
    del horizon_sessions
    if breadth_state_df.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    ratio_specs = (
        ("strong_breadth", "strong_breadth_ratio"),
        ("weak_breadth", "weak_breadth_ratio"),
        ("rebound_breadth", "rebound_breadth_ratio"),
        ("overheat_breadth", "overheat_breadth_ratio"),
    )
    frame = breadth_state_df.copy()
    for family, ratio_col in ratio_specs:
        frame[f"{ratio_col}_bucket"] = frame[ratio_col].map(_bucket_breadth_ratio)
    for lookback, lookback_df in frame.groupby("lookback", sort=True):
        lookback_int = int(cast(Any, lookback))
        for sample_period, period_df in _iter_period_frames(lookback_df):
            for family, ratio_col in ratio_specs:
                bucket_col = f"{ratio_col}_bucket"
                for bucket, group in period_df.groupby(bucket_col, dropna=True, sort=True):
                    if len(group) < min_observations:
                        continue
                    rows.append(
                        _summary_row(
                            group.rename(
                                columns={
                                    "market_forward_return_pct": "_market_forward_return_pct"
                                }
                            ),
                            return_col="_market_forward_return_pct",
                            sample_period=sample_period,
                            lookback=lookback_int,
                            state_family="market_breadth",
                            feature_family=family,
                            bucket=str(bucket),
                            feature_mean=float(
                                pd.to_numeric(group[ratio_col], errors="coerce").mean()
                            ),
                        )
                    )
    return pd.DataFrame(rows)


def _bucket_breadth_ratio(value: object) -> str | None:
    number = _coerce_float(value)
    if number is None:
        return None
    if number < 0.3:
        return "breadth_low_lt_30pct"
    if number < 0.6:
        return "breadth_mid_30_60pct"
    return "breadth_high_ge_60pct"


def _build_feature_rank_df(
    index_state_summary_df: pd.DataFrame,
    breadth_state_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    frames = []
    if not index_state_summary_df.empty:
        frames.append(index_state_summary_df)
    if not breadth_state_summary_df.empty:
        frames.append(breadth_state_summary_df)
    if not frames:
        return pd.DataFrame()
    all_summary = pd.concat(frames, ignore_index=True)
    all_rows = all_summary[all_summary["sample_period"].astype(str) == "all"].copy()
    if all_rows.empty:
        return pd.DataFrame()
    all_rows["strength_score"] = (
        pd.to_numeric(all_rows["mean_forward_return_pct"], errors="coerce")
        + pd.to_numeric(all_rows["p10_forward_return_pct"], errors="coerce") * 0.35
        + (pd.to_numeric(all_rows["hit_rate_pct"], errors="coerce") - 50.0) * 0.05
    )
    all_rows["defensive_score"] = (
        pd.to_numeric(all_rows["p10_forward_return_pct"], errors="coerce")
        + pd.to_numeric(all_rows["mean_forward_return_pct"], errors="coerce") * 0.25
    )
    strength = all_rows.sort_values("strength_score", ascending=False).head(30).copy()
    strength["rank_type"] = "strong_market"
    strength["rank_score"] = strength["strength_score"]
    defensive = all_rows.sort_values("defensive_score", ascending=False).head(30).copy()
    defensive["rank_type"] = "bad_tail_resistant"
    defensive["rank_score"] = defensive["defensive_score"]
    result = pd.concat([strength, defensive], ignore_index=True)
    cols = [
        "rank_type",
        "rank_score",
        "state_family",
        "feature_family",
        "lookback",
        "bucket",
        "observation_count",
        "index_count",
        "date_count",
        "feature_mean",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "hit_rate_pct",
        "p10_forward_return_pct",
        "p05_forward_return_pct",
        "p90_forward_return_pct",
    ]
    return result[cols].reset_index(drop=True)


def _build_summary_markdown(result: IndexMarketStrengthResearchResult) -> str:
    lines = [
        "# Index Market Strength Research",
        "",
        f"- DB: `{result.db_path}`",
        f"- Source: `{result.source_mode}` ({result.source_detail})",
        f"- Analysis range: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Target index category: `{result.target_category_prefix}`",
        f"- Index count: `{result.index_count}`",
        f"- Horizon: `{result.horizon_sessions}` sessions",
        f"- Lookbacks: `{', '.join(str(v) for v in result.lookback_windows)}`",
        "",
        "## Policy",
        "",
        result.feature_policy,
        "",
        "## Top Strong-Market States",
        "",
        _format_rank_rows(result.feature_rank_df, rank_type="strong_market"),
        "",
        "## Top Bad-Tail-Resistant States",
        "",
        _format_rank_rows(result.feature_rank_df, rank_type="bad_tail_resistant"),
        "",
        "## Output Tables",
        "",
    ]
    for table_name in _RESULT_TABLE_NAMES:
        table = getattr(result, table_name)
        lines.append(f"- `{table_name}`: `{len(table)}` rows")
    return "\n".join(lines) + "\n"


def _format_rank_rows(feature_rank_df: pd.DataFrame, *, rank_type: str) -> str:
    if feature_rank_df.empty:
        return "No ranked rows."
    rows = feature_rank_df[
        feature_rank_df["rank_type"].astype(str) == rank_type
    ].head(10)
    if rows.empty:
        return "No ranked rows."
    lines = [
        "| State | Lookback | Bucket | Obs | Mean | Hit | P10 | Score |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row['state_family']}/{row['feature_family']}` | "
            f"`{int(row['lookback'])}` | "
            f"`{row['bucket']}` | "
            f"`{int(row['observation_count'])}` | "
            f"`{_fmt(row['mean_forward_return_pct'])}%` | "
            f"`{_fmt(row['hit_rate_pct'])}%` | "
            f"`{_fmt(row['p10_forward_return_pct'])}%` | "
            f"`{_fmt(row['rank_score'])}` |"
        )
    return "\n".join(lines)


def _fmt(value: object, digits: int = 2) -> str:
    number = _coerce_float(value)
    if number is None:
        return "nan"
    return f"{number:.{digits}f}"


def write_index_market_strength_research_bundle(
    result: IndexMarketStrengthResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_index_market_strength_research",
        params={
            "db_path": result.db_path,
            "lookback_windows": list(result.lookback_windows),
            "horizon_sessions": result.horizon_sessions,
            "target_category_prefix": result.target_category_prefix,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_index_market_strength_research_bundle(
    bundle_path: str | Path,
) -> IndexMarketStrengthResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=IndexMarketStrengthResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_index_market_strength_research_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_index_market_strength_research_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "DEFAULT_HORIZON_SESSIONS",
    "DEFAULT_LOOKBACK_WINDOWS",
    "INDEX_MARKET_STRENGTH_RESEARCH_EXPERIMENT_ID",
    "IndexMarketStrengthResearchResult",
    "get_index_market_strength_research_bundle_path_for_run_id",
    "get_index_market_strength_research_latest_bundle_path",
    "load_index_market_strength_research_bundle",
    "run_index_market_strength_research",
    "write_index_market_strength_research_bundle",
)
