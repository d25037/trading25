"""Regime decomposition for free-float liquidity gap observations."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics.free_float_liquidity_gap import (
    get_free_float_liquidity_gap_latest_bundle_path,
    load_free_float_liquidity_gap_bundle,
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

FREE_FLOAT_LIQUIDITY_REGIME_DECOMPOSITION_EXPERIMENT_ID = (
    "market-behavior/free-float-liquidity-regime-decomposition"
)
DEFAULT_RECENT_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_HIGH_RESIDUAL_Z = 1.0
DEFAULT_LOW_RESIDUAL_Z = -1.0
DEFAULT_RECOVERY_CHANGE_THRESHOLD = 0.25
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_observation_df",
    "regime_forward_return_df",
    "market_regime_diagnostics_df",
    "latest_prime_regime_df",
)
_MARKET_ORDER: dict[str, int] = {
    "all": 0,
    "prime": 1,
    "standard": 2,
    "growth": 3,
    "unknown": 99,
}
_REGIME_ORDER: dict[str, int] = {
    "rerating_participation": 0,
    "distribution_stress": 1,
    "liquidity_recovery": 2,
    "stale_liquidity": 3,
    "neutral": 4,
}


@dataclass(frozen=True)
class FreeFloatLiquidityRegimeDecompositionResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    recent_return_windows: tuple[int, ...]
    high_residual_z: float
    low_residual_z: float
    recovery_change_threshold: float
    feature_policy: str
    enriched_observation_df: pd.DataFrame
    regime_forward_return_df: pd.DataFrame
    market_regime_diagnostics_df: pd.DataFrame
    latest_prime_regime_df: pd.DataFrame


def run_free_float_liquidity_regime_decomposition(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    db_path: str | Path | None = None,
    recent_return_windows: Iterable[int] = DEFAULT_RECENT_RETURN_WINDOWS,
    high_residual_z: float = DEFAULT_HIGH_RESIDUAL_Z,
    low_residual_z: float = DEFAULT_LOW_RESIDUAL_Z,
    recovery_change_threshold: float = DEFAULT_RECOVERY_CHANGE_THRESHOLD,
) -> FreeFloatLiquidityRegimeDecompositionResult:
    resolved_windows = tuple(sorted({int(window) for window in recent_return_windows}))
    _validate_params(
        recent_return_windows=resolved_windows,
        high_residual_z=high_residual_z,
        low_residual_z=low_residual_z,
        recovery_change_threshold=recovery_change_threshold,
    )
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_free_float_liquidity_gap_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Free-float liquidity gap bundle was not found. "
            "Run run_free_float_liquidity_gap.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    gap_result = load_free_float_liquidity_gap_bundle(resolved_input)
    resolved_db_path = Path(db_path or gap_result.db_path).expanduser().resolve()
    if not resolved_db_path.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {resolved_db_path}")

    base_df = gap_result.observation_df.copy()
    price_feature_df, source_mode, source_detail = _load_recent_return_features(
        resolved_db_path,
        base_df,
        recent_return_windows=resolved_windows,
    )
    enriched = _build_enriched_observation_df(
        base_df,
        price_feature_df,
        recent_return_windows=resolved_windows,
        high_residual_z=high_residual_z,
        low_residual_z=low_residual_z,
        recovery_change_threshold=recovery_change_threshold,
    )
    regime_forward_return_df = _build_regime_forward_return_df(enriched)
    market_regime_diagnostics_df = _build_market_regime_diagnostics_df(enriched)
    latest_prime_regime_df = _build_latest_prime_regime_df(enriched)

    return FreeFloatLiquidityRegimeDecompositionResult(
        db_path=str(resolved_db_path),
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=gap_result.analysis_start_date,
        analysis_end_date=gap_result.analysis_end_date,
        recent_return_windows=resolved_windows,
        high_residual_z=float(high_residual_z),
        low_residual_z=float(low_residual_z),
        recovery_change_threshold=float(recovery_change_threshold),
        feature_policy=(
            "Recent return features use adjusted close history through the observation date; "
            "regime labels are diagnostics over Phase 1 liquidity residuals, not entry signals; "
            "rerating_participation requires high residual with positive 20d and 60d recent returns; "
            "distribution_stress requires high residual with negative 20d or 60d recent return"
        ),
        enriched_observation_df=enriched,
        regime_forward_return_df=regime_forward_return_df,
        market_regime_diagnostics_df=market_regime_diagnostics_df,
        latest_prime_regime_df=latest_prime_regime_df,
    )


def write_free_float_liquidity_regime_decomposition_bundle(
    result: FreeFloatLiquidityRegimeDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FREE_FLOAT_LIQUIDITY_REGIME_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_free_float_liquidity_regime_decomposition",
        params={
            "input_bundle_path": result.input_bundle_path,
            "recent_return_windows": list(result.recent_return_windows),
            "high_residual_z": result.high_residual_z,
            "low_residual_z": result.low_residual_z,
            "recovery_change_threshold": result.recovery_change_threshold,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_free_float_liquidity_regime_decomposition_bundle(
    bundle_path: str | Path,
) -> FreeFloatLiquidityRegimeDecompositionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FreeFloatLiquidityRegimeDecompositionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_free_float_liquidity_regime_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FREE_FLOAT_LIQUIDITY_REGIME_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_free_float_liquidity_regime_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FREE_FLOAT_LIQUIDITY_REGIME_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def build_summary_markdown(result: FreeFloatLiquidityRegimeDecompositionResult) -> str:
    diagnostics = _top_rows_for_markdown(
        result.market_regime_diagnostics_df,
        sort_columns=["market_scope", "adv_window", "liquidity_regime"],
        ascending=[True, True, True],
        limit=30,
    )
    forward = _top_rows_for_markdown(
        result.regime_forward_return_df,
        sort_columns=["market_scope", "adv_window", "horizon", "liquidity_regime"],
        ascending=[True, True, True, True],
        limit=48,
    )
    latest_prime = _top_rows_for_markdown(
        result.latest_prime_regime_df,
        sort_columns=["adv_window", "liquidity_regime", "liquidity_residual_z"],
        ascending=[True, True, False],
        limit=30,
    )
    return "\n".join(
        [
            "# Free-Float Liquidity Regime Decomposition",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Input bundle: `{result.input_bundle_path}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Recent return windows: `{list(result.recent_return_windows)}`",
            f"- High residual z: `{result.high_residual_z}`",
            f"- Low residual z: `{result.low_residual_z}`",
            "",
            "## Market Regime Diagnostics",
            "",
            diagnostics,
            "",
            "## Regime Forward Returns",
            "",
            forward,
            "",
            "## Latest Prime Regimes",
            "",
            latest_prime,
            "",
        ]
    )


def _validate_params(
    *,
    recent_return_windows: Sequence[int],
    high_residual_z: float,
    low_residual_z: float,
    recovery_change_threshold: float,
) -> None:
    if not recent_return_windows or any(
        window <= 0 for window in recent_return_windows
    ):
        raise ValueError("recent_return_windows must contain positive integers")
    if high_residual_z <= 0:
        raise ValueError("high_residual_z must be positive")
    if low_residual_z >= 0:
        raise ValueError("low_residual_z must be negative")
    if recovery_change_threshold <= 0:
        raise ValueError("recovery_change_threshold must be positive")


def _load_recent_return_features(
    db_path: Path,
    observation_df: pd.DataFrame,
    *,
    recent_return_windows: Sequence[int],
) -> tuple[pd.DataFrame, SourceMode, str]:
    if observation_df.empty:
        return (
            _empty_recent_return_df(recent_return_windows),
            "live",
            "empty observation input",
        )
    codes = tuple(
        sorted(
            str(code) for code in observation_df["code"].dropna().astype(str).unique()
        )
    )
    if not codes:
        return (
            _empty_recent_return_df(recent_return_windows),
            "live",
            "empty code input",
        )
    min_date = str(observation_df["date"].min())
    max_date = str(observation_df["date"].max())
    max_window = max(recent_return_windows)
    query_start = _offset_calendar_date(min_date, days=-(max_window * 3 + 30))
    with open_readonly_analysis_connection(
        str(db_path),
        snapshot_prefix="free-float-liquidity-regime-",
    ) as ctx:
        feature_df = _query_price_feature_rows(
            ctx.connection,
            codes=codes,
            start_date=query_start,
            end_date=max_date,
            recent_return_windows=recent_return_windows,
        )
        return feature_df, ctx.source_mode, ctx.source_detail


def _query_price_feature_rows(
    conn: Any,
    *,
    codes: Sequence[str],
    start_date: str,
    end_date: str,
    recent_return_windows: Sequence[int],
) -> pd.DataFrame:
    normalized_code = normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    lag_columns = ",\n".join(
        [
            f"LAG(close, {window}) OVER (PARTITION BY code ORDER BY date) AS close_{window}d_ago"
            for window in recent_return_windows
        ]
    )
    df = conn.execute(
        f"""
        WITH price_base AS (
            SELECT *
            FROM (
                SELECT
                    {normalized_code} AS code,
                    date,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}, date
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM stock_data
                WHERE {normalized_code} IN ({_placeholder_sql(len(codes))})
                  AND date >= ?
                  AND date <= ?
                  AND close > 0
            )
            WHERE rn = 1
        )
        SELECT
            code,
            date,
            close,
            {lag_columns}
        FROM price_base
        ORDER BY code, date
        """,
        [*codes, start_date, end_date],
    ).fetchdf()
    if df.empty:
        return _empty_recent_return_df(recent_return_windows)
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    for window in recent_return_windows:
        prior = pd.to_numeric(df[f"close_{window}d_ago"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        df[f"recent_return_{window}d_pct"] = (close / prior - 1.0) * 100.0
    columns = [
        "code",
        "date",
        "close",
        *[f"recent_return_{window}d_pct" for window in recent_return_windows],
    ]
    return df[columns]


def _build_enriched_observation_df(
    observation_df: pd.DataFrame,
    price_feature_df: pd.DataFrame,
    *,
    recent_return_windows: Sequence[int],
    high_residual_z: float,
    low_residual_z: float,
    recovery_change_threshold: float,
) -> pd.DataFrame:
    if observation_df.empty:
        return _empty_enriched_observation_df(observation_df, recent_return_windows)
    merged = observation_df.merge(
        price_feature_df, on=["code", "date"], how="left", suffixes=("", "_price")
    )
    merged["liquidity_regime"] = [
        _classify_liquidity_regime(
            {str(key): value for key, value in row.items()},
            recent_return_windows=recent_return_windows,
            high_residual_z=high_residual_z,
            low_residual_z=low_residual_z,
            recovery_change_threshold=recovery_change_threshold,
        )
        for row in merged.to_dict(orient="records")
    ]
    return merged.sort_values(
        ["market_scope", "adv_window", "date", "code"], kind="stable"
    ).reset_index(drop=True)


def _classify_liquidity_regime(
    row: dict[str, Any],
    *,
    recent_return_windows: Sequence[int],
    high_residual_z: float,
    low_residual_z: float,
    recovery_change_threshold: float,
) -> str:
    residual_z = _to_float(row.get("liquidity_residual_z"))
    residual_change = _to_float(row.get("liquidity_residual_change"))
    recent_returns = [
        _to_float(row.get(f"recent_return_{window}d_pct"))
        for window in recent_return_windows
    ]
    valid_returns = [
        value for value in recent_returns if value is not None and np.isfinite(value)
    ]
    if residual_z is None:
        return "neutral"
    if residual_z >= high_residual_z:
        if len(valid_returns) != len(recent_return_windows):
            return "neutral"
        if valid_returns and all(value >= 0 for value in valid_returns):
            return "rerating_participation"
        if valid_returns and any(value < 0 for value in valid_returns):
            return "distribution_stress"
        return "neutral"
    if residual_z <= low_residual_z:
        if residual_change is not None and residual_change >= recovery_change_threshold:
            return "liquidity_recovery"
        return "stale_liquidity"
    return "neutral"


def _build_regime_forward_return_df(enriched_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "adv_window",
        "horizon",
        "liquidity_regime",
        "observation_count",
        "code_count",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "win_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_liquidity_residual_z",
        "median_liquidity_residual_change",
        "median_adv_mil_jpy",
        "median_free_float_market_cap_bil_jpy",
    ]
    if enriched_df.empty:
        return pd.DataFrame(columns=columns)
    scoped = _expand_market_scope(enriched_df)
    horizons = _infer_forward_horizons(scoped)
    records: list[dict[str, Any]] = []
    for keys, group in scoped.groupby(
        ["market_scope", "adv_window", "liquidity_regime"], sort=False
    ):
        market_scope, adv_window, regime = keys
        for horizon in horizons:
            returns = pd.to_numeric(
                group[f"forward_return_{horizon}d_pct"], errors="coerce"
            )
            excess = pd.to_numeric(
                group[f"forward_excess_return_{horizon}d_pct"], errors="coerce"
            )
            records.append(
                {
                    "market_scope": str(market_scope),
                    "adv_window": int(adv_window),
                    "horizon": int(horizon),
                    "liquidity_regime": str(regime),
                    "observation_count": int(returns.notna().sum()),
                    "code_count": int(group["code"].nunique()),
                    "mean_forward_return_pct": _mean(returns),
                    "median_forward_return_pct": _median(returns),
                    "mean_forward_excess_return_pct": _mean(excess),
                    "median_forward_excess_return_pct": _median(excess),
                    "win_rate_pct": _win_rate(returns),
                    "median_recent_return_20d_pct": _median(
                        group.get("recent_return_20d_pct")
                    ),
                    "median_recent_return_60d_pct": _median(
                        group.get("recent_return_60d_pct")
                    ),
                    "median_liquidity_residual_z": _median(
                        group["liquidity_residual_z"]
                    ),
                    "median_liquidity_residual_change": _median(
                        group["liquidity_residual_change"]
                    ),
                    "median_adv_mil_jpy": _median(group["adv_mil_jpy"]),
                    "median_free_float_market_cap_bil_jpy": _median(
                        group["free_float_market_cap_bil_jpy"]
                    ),
                }
            )
    return _sort_summary_frame(
        pd.DataFrame.from_records(records, columns=columns),
        ["adv_window", "horizon", "liquidity_regime"],
    )


def _build_market_regime_diagnostics_df(enriched_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "adv_window",
        "liquidity_regime",
        "observation_count",
        "code_count",
        "start_date",
        "end_date",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_liquidity_residual_z",
        "median_liquidity_residual_change",
        "median_adv_mil_jpy",
        "median_free_float_market_cap_bil_jpy",
    ]
    if enriched_df.empty:
        return pd.DataFrame(columns=columns)
    scoped = _expand_market_scope(enriched_df)
    records: list[dict[str, Any]] = []
    for keys, group in scoped.groupby(
        ["market_scope", "adv_window", "liquidity_regime"], sort=False
    ):
        market_scope, adv_window, regime = keys
        records.append(
            {
                "market_scope": str(market_scope),
                "adv_window": int(adv_window),
                "liquidity_regime": str(regime),
                "observation_count": int(len(group)),
                "code_count": int(group["code"].nunique()),
                "start_date": _str_or_none(group["date"].min()),
                "end_date": _str_or_none(group["date"].max()),
                "median_recent_return_20d_pct": _median(
                    group.get("recent_return_20d_pct")
                ),
                "median_recent_return_60d_pct": _median(
                    group.get("recent_return_60d_pct")
                ),
                "median_liquidity_residual_z": _median(group["liquidity_residual_z"]),
                "median_liquidity_residual_change": _median(
                    group["liquidity_residual_change"]
                ),
                "median_adv_mil_jpy": _median(group["adv_mil_jpy"]),
                "median_free_float_market_cap_bil_jpy": _median(
                    group["free_float_market_cap_bil_jpy"]
                ),
            }
        )
    return _sort_summary_frame(
        pd.DataFrame.from_records(records, columns=columns),
        ["adv_window", "liquidity_regime"],
    )


def _build_latest_prime_regime_df(enriched_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "code",
        "company_name",
        "sector_33_name",
        "adv_window",
        "liquidity_regime",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "liquidity_residual_z",
        "liquidity_residual_change",
        "liquidity_implied_ffcap_gap_pct",
        "adv_mil_jpy",
        "free_float_market_cap_bil_jpy",
    ]
    if enriched_df.empty:
        return pd.DataFrame(columns=columns)
    prime = enriched_df[enriched_df["market_scope"].astype(str) == "prime"].copy()
    if prime.empty:
        return pd.DataFrame(columns=columns)
    latest_date = str(prime["date"].max())
    latest = prime[
        (prime["date"].astype(str) == latest_date)
        & (prime["liquidity_regime"].astype(str) != "neutral")
    ]
    return (
        latest[columns]
        .sort_values(
            ["adv_window", "liquidity_regime", "liquidity_residual_z"],
            ascending=[True, True, False],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _expand_market_scope(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    actual = df.copy()
    all_scope = df.copy()
    all_scope["market_scope"] = "all"
    return pd.concat([all_scope, actual], ignore_index=True)


def _infer_forward_horizons(df: pd.DataFrame) -> tuple[int, ...]:
    horizons: list[int] = []
    prefix = "forward_return_"
    suffix = "d_pct"
    for column in df.columns:
        name = str(column)
        if name.startswith(prefix) and name.endswith(suffix):
            raw = name[len(prefix) : -len(suffix)]
            if raw.isdigit():
                horizons.append(int(raw))
    return tuple(sorted(set(horizons)))


def _empty_recent_return_df(recent_return_windows: Sequence[int]) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "code",
            "date",
            "close",
            *[f"recent_return_{window}d_pct" for window in recent_return_windows],
        ]
    )


def _empty_enriched_observation_df(
    observation_df: pd.DataFrame,
    recent_return_windows: Sequence[int],
) -> pd.DataFrame:
    result = observation_df.copy()
    for window in recent_return_windows:
        result[f"recent_return_{window}d_pct"] = pd.Series(dtype="float64")
    result["liquidity_regime"] = pd.Series(dtype="object")
    return result


def _sort_summary_frame(df: pd.DataFrame, extra_columns: Sequence[str]) -> pd.DataFrame:
    if df.empty or "market_scope" not in df:
        return df
    ordered = df.copy()
    ordered["_market_order"] = (
        ordered["market_scope"].map(_MARKET_ORDER).fillna(50).astype(int)
    )
    ordered["_regime_order"] = (
        ordered["liquidity_regime"].map(_REGIME_ORDER).fillna(50).astype(int)
    )
    sort_columns = [
        "_market_order",
        *[column for column in extra_columns if column != "liquidity_regime"],
    ]
    if "liquidity_regime" in extra_columns:
        sort_columns.append("_regime_order")
    return (
        ordered.sort_values(sort_columns, kind="stable")
        .drop(columns=["_market_order", "_regime_order"])
        .reset_index(drop=True)
    )


def _top_rows_for_markdown(
    df: pd.DataFrame,
    *,
    sort_columns: Sequence[str],
    ascending: Sequence[bool],
    limit: int,
) -> str:
    if df.empty:
        return "_No rows._"
    frame = df.copy()
    existing_sort = [column for column in sort_columns if column in frame.columns]
    if existing_sort:
        frame = frame.sort_values(
            existing_sort,
            ascending=list(ascending)[: len(existing_sort)],
            kind="stable",
        )
    return _frame_to_markdown(frame.head(limit))


def _frame_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    columns = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in df.itertuples(index=False, name=None):
        values = [_format_markdown_cell(value) for value in row]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _format_markdown_cell(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float):
        return str(_round_float(value, 6))
    return str(value).replace("|", "\\|")


def _placeholder_sql(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _offset_calendar_date(date: str | None, *, days: int) -> str:
    if not date:
        return "1900-01-01"
    return (pd.Timestamp(date) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def _to_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(values: Iterable[Any] | None) -> float:
    if values is None:
        return np.nan
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return _round_float(float(series.mean())) if not series.empty else np.nan


def _median(values: Iterable[Any] | None) -> float:
    if values is None:
        return np.nan
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return _round_float(float(series.median())) if not series.empty else np.nan


def _win_rate(values: Iterable[Any] | None) -> float:
    if values is None:
        return np.nan
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    return (
        _round_float(float((series > 0).mean() * 100.0)) if not series.empty else np.nan
    )


def _round_float(value: Any, digits: int = 4) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return np.nan
    if not np.isfinite(numeric):
        return np.nan
    return round(numeric, digits)


def _str_or_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)
