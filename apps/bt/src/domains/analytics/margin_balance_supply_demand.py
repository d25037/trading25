"""PIT-safe phase-1 research for local margin balance supply/demand signals."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, cast

import duckdb
import numpy as np
import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

MARGIN_BALANCE_SUPPLY_DEMAND_EXPERIMENT_ID = (
    "market-behavior/margin-balance-supply-demand"
)
DEFAULT_HORIZONS: tuple[int, ...] = (1, 5, 10, 20)
DEFAULT_ADV_WINDOW = 20
DEFAULT_EFFECTIVE_LAG_SESSIONS = 3
DEFAULT_BUCKET_COUNT = 5
DEFAULT_MIN_DAILY_OBSERVATIONS = 20
DEFAULT_DISCOVERY_END_DATE = "2021-12-31"
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_PERCENTILE_WINDOW = 52
DEFAULT_PRIOR_RETURN_WINDOWS: tuple[int, ...] = (5, 20, 60)

FEATURE_COLUMNS: tuple[str, ...] = (
    "long_to_adv20",
    "short_to_adv20",
    "net_to_adv20",
    "delta_long_to_adv20",
    "delta_short_to_adv20",
    "delta_net_to_adv20",
    "long_weekly_change_pct",
    "short_weekly_change_pct",
    "long_short_ratio",
    "long_percentile_52w",
    "net_percentile_52w",
)


@dataclass(frozen=True)
class MarginBalanceSupplyDemandResult:
    db_path: str
    source_mode: str
    source_detail: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    adv_window: int
    effective_lag_sessions: int
    bucket_count: int
    min_daily_observations: int
    discovery_end_date: str
    severe_loss_threshold_pct: float
    observation_df: pd.DataFrame
    bucket_return_summary_df: pd.DataFrame
    pruning_summary_df: pd.DataFrame
    market_summary_df: pd.DataFrame
    price_margin_interaction_summary_df: pd.DataFrame
    coverage_summary_df: pd.DataFrame


def run_margin_balance_supply_demand_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    adv_window: int = DEFAULT_ADV_WINDOW,
    effective_lag_sessions: int = DEFAULT_EFFECTIVE_LAG_SESSIONS,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    min_daily_observations: int = DEFAULT_MIN_DAILY_OBSERVATIONS,
    discovery_end_date: str = DEFAULT_DISCOVERY_END_DATE,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    percentile_window: int = DEFAULT_PERCENTILE_WINDOW,
    prior_return_windows: Iterable[int] = DEFAULT_PRIOR_RETURN_WINDOWS,
) -> MarginBalanceSupplyDemandResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_prior_return_windows = tuple(
        sorted({int(window) for window in prior_return_windows})
    )
    _validate_params(
        horizons=resolved_horizons,
        prior_return_windows=resolved_prior_return_windows,
        adv_window=adv_window,
        effective_lag_sessions=effective_lag_sessions,
        bucket_count=bucket_count,
        min_daily_observations=min_daily_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        percentile_window=percentile_window,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="margin-balance-supply-demand-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        observation_df = _query_observation_frame(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            horizons=resolved_horizons,
            prior_return_windows=resolved_prior_return_windows,
            adv_window=adv_window,
            effective_lag_sessions=effective_lag_sessions,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    observation_df = _add_margin_features(
        observation_df,
        adv_window=adv_window,
        percentile_window=percentile_window,
    )
    observation_df = _add_feature_buckets(
        observation_df,
        feature_columns=FEATURE_COLUMNS,
        bucket_count=bucket_count,
        min_daily_observations=min_daily_observations,
    )
    bucket_return_summary_df = _build_bucket_return_summary_df(
        observation_df,
        feature_columns=FEATURE_COLUMNS,
        horizons=resolved_horizons,
        bucket_count=bucket_count,
    )
    pruning_summary_df = _build_pruning_summary_df(
        observation_df,
        feature_columns=FEATURE_COLUMNS,
        horizons=resolved_horizons,
        bucket_count=bucket_count,
        discovery_end_date=discovery_end_date,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    market_summary_df = _build_market_summary_df(
        observation_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    price_margin_interaction_summary_df = _build_price_margin_interaction_summary_df(
        observation_df,
        horizons=resolved_horizons,
        prior_return_windows=resolved_prior_return_windows,
        discovery_end_date=discovery_end_date,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    coverage_summary_df = _build_coverage_summary_df(
        observation_df,
        horizons=resolved_horizons,
        prior_return_windows=resolved_prior_return_windows,
        start_date=start_date,
        end_date=end_date,
        effective_lag_sessions=effective_lag_sessions,
        adv_window=adv_window,
        percentile_window=percentile_window,
    )

    analysis_start_date = _str_or_none(observation_df["effective_date"].min())
    analysis_end_date = _str_or_none(observation_df["effective_date"].max())
    return MarginBalanceSupplyDemandResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        horizons=resolved_horizons,
        adv_window=adv_window,
        effective_lag_sessions=effective_lag_sessions,
        bucket_count=bucket_count,
        min_daily_observations=min_daily_observations,
        discovery_end_date=discovery_end_date,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_df=observation_df,
        bucket_return_summary_df=bucket_return_summary_df,
        pruning_summary_df=pruning_summary_df,
        market_summary_df=market_summary_df,
        price_margin_interaction_summary_df=price_margin_interaction_summary_df,
        coverage_summary_df=coverage_summary_df,
    )


def write_margin_balance_supply_demand_bundle(
    result: MarginBalanceSupplyDemandResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=MARGIN_BALANCE_SUPPLY_DEMAND_EXPERIMENT_ID,
        module=__name__,
        function="run_margin_balance_supply_demand_research",
        params={
            "horizons": list(result.horizons),
            "adv_window": result.adv_window,
            "effective_lag_sessions": result.effective_lag_sessions,
            "bucket_count": result.bucket_count,
            "min_daily_observations": result.min_daily_observations,
            "discovery_end_date": result.discovery_end_date,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "prior_return_windows": [
                int(column.removeprefix("prior_return_").removesuffix("d"))
                for column in result.observation_df.columns
                if column.startswith("prior_return_") and column.endswith("d")
            ],
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "observation_count": int(len(result.observation_df)),
            "code_count": int(result.observation_df["code"].nunique())
            if "code" in result.observation_df
            else 0,
        },
        result_tables={
            "coverage_summary_df": result.coverage_summary_df,
            "bucket_return_summary_df": result.bucket_return_summary_df,
            "pruning_summary_df": result.pruning_summary_df,
            "price_margin_interaction_summary_df": (
                result.price_margin_interaction_summary_df
            ),
            "market_summary_df": result.market_summary_df,
            "observation_df": result.observation_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: MarginBalanceSupplyDemandResult) -> str:
    best_pruning = _top_rows_for_markdown(
        result.pruning_summary_df,
        sort_columns=["period", "horizon", "retained_mean_return_delta_pct"],
        ascending=[True, True, False],
        limit=12,
    )
    coverage = _top_rows_for_markdown(result.coverage_summary_df, limit=12)
    bucket = _top_rows_for_markdown(
        result.bucket_return_summary_df,
        sort_columns=["horizon", "feature", "bucket"],
        ascending=[True, True, True],
        limit=24,
    )
    interaction = _top_rows_for_markdown(
        result.price_margin_interaction_summary_df,
        sort_columns=[
            "period",
            "prior_return_window",
            "horizon",
            "delta_vs_price_segment_pct",
        ],
        ascending=[True, True, True, True],
        limit=24,
    )
    return "\n".join(
        [
            "# Margin Balance Supply/Demand Phase 1",
            "",
            "This bundle tests only the local `margin_data` table: aggregate margin "
            "long/short balances from J-Quants `/markets/margin-interest`. It does "
            "not include institutional short-selling position reports.",
            "",
            "## PIT Policy",
            "",
            f"- Margin rows become tradable after `{result.effective_lag_sessions}` "
            "trading sessions from the recorded margin balance date.",
            "- Entry return accounting starts at the effective-date open.",
            "- ADV features use prior sessions only.",
            "",
            "## Coverage",
            "",
            coverage,
            "",
            "## Top Pruning Diagnostics",
            "",
            best_pruning,
            "",
            "## Price Decline x Margin Long Change",
            "",
            interaction,
            "",
            "## Bucket Return Sample",
            "",
            bucket,
            "",
        ]
    )


def _validate_params(
    *,
    horizons: tuple[int, ...],
    prior_return_windows: tuple[int, ...],
    adv_window: int,
    effective_lag_sessions: int,
    bucket_count: int,
    min_daily_observations: int,
    severe_loss_threshold_pct: float,
    percentile_window: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if not prior_return_windows or any(window <= 1 for window in prior_return_windows):
        raise ValueError("prior_return_windows must be greater than 1")
    if adv_window <= 0:
        raise ValueError("adv_window must be positive")
    if effective_lag_sessions < 1:
        raise ValueError("effective_lag_sessions must be at least 1")
    if bucket_count < 2:
        raise ValueError("bucket_count must be at least 2")
    if min_daily_observations < bucket_count:
        raise ValueError("min_daily_observations must be >= bucket_count")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if percentile_window < 2:
        raise ValueError("percentile_window must be at least 2")


def _assert_required_tables(conn: duckdb.DuckDBPyConnection) -> None:
    table_names = {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    missing = sorted({"stock_data", "margin_data", "stock_master_daily"} - table_names)
    if missing:
        raise RuntimeError(f"market.duckdb missing required table(s): {', '.join(missing)}")


def _query_observation_frame(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_date: str | None,
    end_date: str | None,
    horizons: tuple[int, ...],
    prior_return_windows: tuple[int, ...],
    adv_window: int,
    effective_lag_sessions: int,
) -> pd.DataFrame:
    normalized_stock_code_sql = normalize_code_sql("code")
    return_exprs: list[str] = []
    future_joins: list[str] = []
    for horizon in horizons:
        alias = f"future_{horizon}"
        offset = horizon - 1
        future_joins.append(
            f"""
            LEFT JOIN stock_features {alias}
              ON {alias}.code = entry.code
             AND {alias}.code_rn = entry.code_rn + {offset}
            """
        )
        return_exprs.append(
            f"""
            CASE
              WHEN entry.open > 0 AND {alias}.close IS NOT NULL
              THEN ({alias}.close / entry.open) - 1.0
              ELSE NULL
            END AS return_open_to_close_{horizon}d
            """
        )

    prior_return_exprs: list[str] = []
    prior_joins: list[str] = [
        """
        LEFT JOIN stock_features prior_1
          ON prior_1.code = entry.code
         AND prior_1.code_rn = entry.code_rn - 1
        """
    ]
    for window in prior_return_windows:
        alias = f"prior_{window}"
        prior_joins.append(
            f"""
            LEFT JOIN stock_features {alias}
              ON {alias}.code = entry.code
             AND {alias}.code_rn = entry.code_rn - {window}
            """
        )
        prior_return_exprs.append(
            f"""
            CASE
              WHEN prior_1.close > 0 AND {alias}.close > 0
              THEN (prior_1.close / {alias}.close) - 1.0
              ELSE NULL
            END AS prior_return_{window}d
            """
        )

    date_filters: list[str] = []
    params: list[Any] = [effective_lag_sessions]
    if start_date is not None:
        date_filters.append("effective.date >= ?")
        params.append(start_date)
    if end_date is not None:
        date_filters.append("effective.date <= ?")
        params.append(end_date)
    date_filter_sql = "WHERE " + " AND ".join(date_filters) if date_filters else ""
    sql = f"""
        WITH trading_calendar AS (
            SELECT
                date,
                ROW_NUMBER() OVER (ORDER BY date) AS trading_rn
            FROM (
                SELECT DISTINCT date
                FROM stock_data
                WHERE date IS NOT NULL
            )
        ),
        margin_rows AS (
            SELECT
                {normalize_code_sql("code")} AS code,
                date AS margin_date,
                CAST(long_margin_volume AS DOUBLE) AS long_margin_volume,
                CAST(short_margin_volume AS DOUBLE) AS short_margin_volume,
                LAG(CAST(long_margin_volume AS DOUBLE)) OVER (
                    PARTITION BY {normalize_code_sql("code")} ORDER BY date
                ) AS prev_long_margin_volume,
                LAG(CAST(short_margin_volume AS DOUBLE)) OVER (
                    PARTITION BY {normalize_code_sql("code")} ORDER BY date
                ) AS prev_short_margin_volume
            FROM margin_data
            WHERE code IS NOT NULL
              AND date IS NOT NULL
        ),
        margin_with_effective_rn AS (
            SELECT
                m.*,
                (
                    SELECT MAX(c.trading_rn)
                    FROM trading_calendar c
                    WHERE c.date <= m.margin_date
                ) + ? AS effective_trading_rn
            FROM margin_rows m
        ),
        stock_rows_raw AS (
            SELECT
                {normalized_stock_code_sql} AS code,
                date,
                CAST(open AS DOUBLE) AS open,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_stock_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE code IS NOT NULL
              AND date IS NOT NULL
              AND open IS NOT NULL
              AND close IS NOT NULL
              AND open > 0
              AND close > 0
        ),
        stock_rows AS (
            SELECT code, date, open, close, volume
            FROM stock_rows_raw
            WHERE row_priority = 1
        ),
        stock_features AS (
            SELECT
                code,
                date,
                open,
                close,
                volume,
                AVG(volume) OVER (
                    PARTITION BY code
                    ORDER BY date
                    ROWS BETWEEN {adv_window} PRECEDING AND 1 PRECEDING
                ) AS adv20,
                MAX(close) OVER (
                    PARTITION BY code
                    ORDER BY date
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS prior_high_close_20,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date) AS code_rn
            FROM stock_rows
        ),
        stock_master_asof AS (
            SELECT
                {normalize_code_sql("code")} AS code,
                date,
                market_code,
                market_name,
                scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalize_code_sql("code")}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_master_daily
        )
        SELECT
            m.code,
            m.margin_date,
            effective.date AS effective_date,
            entry.open AS entry_open,
            entry.close AS entry_close,
            entry.volume AS entry_volume,
            entry.adv20,
            m.long_margin_volume,
            m.short_margin_volume,
            m.prev_long_margin_volume,
            m.prev_short_margin_volume,
            (m.long_margin_volume - m.short_margin_volume) AS net_margin_volume,
            (
                (m.long_margin_volume - m.short_margin_volume)
                - (m.prev_long_margin_volume - m.prev_short_margin_volume)
            ) AS delta_net_margin_volume,
            sl.market_code,
            sl.market_name,
            sl.scale_category,
            CASE
              WHEN prior_1.close > 0 AND entry.prior_high_close_20 > 0
              THEN (prior_1.close / entry.prior_high_close_20) - 1.0
              ELSE NULL
            END AS drawdown_from_20d_high,
            {", ".join(prior_return_exprs)},
            {", ".join(return_exprs)}
        FROM margin_with_effective_rn m
        JOIN trading_calendar effective
          ON effective.trading_rn = m.effective_trading_rn
        JOIN stock_features entry
          ON entry.code = m.code
         AND entry.date = effective.date
        {" ".join(prior_joins)}
        {" ".join(future_joins)}
        LEFT JOIN stock_master_asof sl
          ON sl.code = m.code
         AND sl.date = effective.date
         AND sl.row_priority = 1
        {date_filter_sql}
        ORDER BY effective.date, m.code
    """
    frame = conn.execute(sql, params).fetchdf()
    if frame.empty:
        raise RuntimeError("no margin observations were available for the requested range")
    return frame


def _add_margin_features(
    frame: pd.DataFrame,
    *,
    adv_window: int,
    percentile_window: int,
) -> pd.DataFrame:
    df = frame.copy()
    df["long_to_adv20"] = _safe_divide(df["long_margin_volume"], df["adv20"])
    df["short_to_adv20"] = _safe_divide(df["short_margin_volume"], df["adv20"])
    df["net_to_adv20"] = _safe_divide(df["net_margin_volume"], df["adv20"])
    df["delta_long_margin_volume"] = (
        df["long_margin_volume"] - df["prev_long_margin_volume"]
    )
    df["delta_short_margin_volume"] = (
        df["short_margin_volume"] - df["prev_short_margin_volume"]
    )
    df["delta_long_to_adv20"] = _safe_divide(df["delta_long_margin_volume"], df["adv20"])
    df["delta_short_to_adv20"] = _safe_divide(
        df["delta_short_margin_volume"],
        df["adv20"],
    )
    df["delta_net_to_adv20"] = _safe_divide(df["delta_net_margin_volume"], df["adv20"])
    df["long_weekly_change_pct"] = (
        _safe_divide(df["long_margin_volume"], df["prev_long_margin_volume"]) - 1.0
    ) * 100.0
    df["short_weekly_change_pct"] = (
        _safe_divide(df["short_margin_volume"], df["prev_short_margin_volume"]) - 1.0
    ) * 100.0
    df["long_short_ratio"] = _safe_divide(
        df["long_margin_volume"],
        df["short_margin_volume"].replace(0, np.nan),
    )
    df["long_percentile_52w"] = _rolling_latest_percentile_by_code(
        df,
        value_col="long_margin_volume",
        window=percentile_window,
    )
    df["net_percentile_52w"] = _rolling_latest_percentile_by_code(
        df,
        value_col="net_margin_volume",
        window=percentile_window,
    )
    df["adv_window"] = adv_window
    return df


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, np.nan)
    return numerator.astype(float) / denominator.astype(float)


def _rolling_latest_percentile_by_code(
    frame: pd.DataFrame,
    *,
    value_col: str,
    window: int,
) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    ordered = frame.sort_values(["code", "margin_date"], kind="stable")
    for _code, group in ordered.groupby("code", sort=False):
        values = group[value_col].astype(float).to_numpy()
        percentiles: list[float] = []
        for idx, current in enumerate(values):
            if math.isnan(current):
                percentiles.append(np.nan)
                continue
            start = max(0, idx - window + 1)
            sample = values[start : idx + 1]
            sample = sample[~np.isnan(sample)]
            if len(sample) < min(window, 4):
                percentiles.append(np.nan)
                continue
            percentiles.append(float(np.mean(sample <= current)))
        result.loc[group.index] = pd.Series(percentiles, index=group.index)
    return result


def _add_feature_buckets(
    frame: pd.DataFrame,
    *,
    feature_columns: tuple[str, ...],
    bucket_count: int,
    min_daily_observations: int,
) -> pd.DataFrame:
    df = frame.copy()
    for feature in feature_columns:
        bucket_col = f"{feature}_bucket"
        df[bucket_col] = pd.Series(pd.NA, index=df.index, dtype="Int64")
        valid = df[["effective_date", feature]].dropna()
        counts = valid.groupby("effective_date", observed=True)[feature].transform("count")
        rank_pct = valid.groupby("effective_date", observed=True)[feature].rank(
            method="first",
            pct=True,
        )
        buckets = np.ceil(rank_pct * bucket_count).clip(1, bucket_count).astype("Int64")
        buckets = buckets.where(counts >= min_daily_observations)
        df.loc[valid.index, bucket_col] = buckets
    return df


def _build_bucket_return_summary_df(
    observation_df: pd.DataFrame,
    *,
    feature_columns: tuple[str, ...],
    horizons: tuple[int, ...],
    bucket_count: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for feature in feature_columns:
        bucket_col = f"{feature}_bucket"
        for horizon in horizons:
            return_col = f"return_open_to_close_{horizon}d"
            grouped = observation_df.dropna(subset=[bucket_col, return_col]).groupby(
                bucket_col,
                observed=True,
            )
            for bucket, group in grouped:
                bucket_int = int(cast(Any, bucket))
                returns = group[return_col].astype(float)
                rows.append(
                    {
                        "feature": feature,
                        "bucket": bucket_int,
                        "bucket_count": bucket_count,
                        "bucket_side": _bucket_side(bucket_int, bucket_count),
                        "horizon": horizon,
                        **_return_stats(returns),
                    }
                )
    return pd.DataFrame(rows)


def _build_pruning_summary_df(
    observation_df: pd.DataFrame,
    *,
    feature_columns: tuple[str, ...],
    horizons: tuple[int, ...],
    bucket_count: int,
    discovery_end_date: str,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    periods = [
        ("full", observation_df),
        (
            "discovery",
            observation_df[observation_df["effective_date"].astype(str) <= discovery_end_date],
        ),
        (
            "validation",
            observation_df[observation_df["effective_date"].astype(str) > discovery_end_date],
        ),
    ]
    for feature in feature_columns:
        bucket_col = f"{feature}_bucket"
        for excluded_bucket, direction in ((1, "low"), (bucket_count, "high")):
            candidate = f"exclude_{direction}_{feature}"
            for period_name, period_df in periods:
                candidate_df = period_df.dropna(subset=[bucket_col])
                excluded_mask = candidate_df[bucket_col].astype(int) == excluded_bucket
                retained_df = candidate_df.loc[~excluded_mask]
                excluded_df = candidate_df.loc[excluded_mask]
                for horizon in horizons:
                    return_col = f"return_open_to_close_{horizon}d"
                    baseline_returns = candidate_df[return_col].dropna().astype(float)
                    retained_returns = retained_df[return_col].dropna().astype(float)
                    excluded_returns = excluded_df[return_col].dropna().astype(float)
                    if baseline_returns.empty or retained_returns.empty:
                        continue
                    baseline_stats = _return_stats(
                        baseline_returns,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                    retained_stats = _return_stats(
                        retained_returns,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                    excluded_stats = _return_stats(
                        excluded_returns,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                    rows.append(
                        {
                            "candidate": candidate,
                            "feature": feature,
                            "excluded_bucket": excluded_bucket,
                            "excluded_side": direction,
                            "period": period_name,
                            "horizon": horizon,
                            "baseline_obs": baseline_stats["obs"],
                            "retained_obs": retained_stats["obs"],
                            "excluded_obs": excluded_stats["obs"],
                            "baseline_mean_return_pct": baseline_stats[
                                "mean_return_pct"
                            ],
                            "retained_mean_return_pct": retained_stats[
                                "mean_return_pct"
                            ],
                            "excluded_mean_return_pct": excluded_stats[
                                "mean_return_pct"
                            ],
                            "retained_mean_return_delta_pct": retained_stats[
                                "mean_return_pct"
                            ]
                            - baseline_stats["mean_return_pct"],
                            "baseline_hit_rate_pct": baseline_stats["hit_rate_pct"],
                            "retained_hit_rate_pct": retained_stats["hit_rate_pct"],
                            "excluded_hit_rate_pct": excluded_stats["hit_rate_pct"],
                            "baseline_severe_loss_rate_pct": baseline_stats[
                                "severe_loss_rate_pct"
                            ],
                            "retained_severe_loss_rate_pct": retained_stats[
                                "severe_loss_rate_pct"
                            ],
                            "excluded_severe_loss_rate_pct": excluded_stats[
                                "severe_loss_rate_pct"
                            ],
                            "retained_severe_loss_delta_pct": retained_stats[
                                "severe_loss_rate_pct"
                            ]
                            - baseline_stats["severe_loss_rate_pct"],
                        }
                    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "candidate",
                "feature",
                "excluded_bucket",
                "excluded_side",
                "period",
                "horizon",
            ]
        )
    return pd.DataFrame(rows).sort_values(
        ["period", "horizon", "retained_mean_return_delta_pct"],
        ascending=[True, True, False],
        kind="stable",
    )


def _build_market_summary_df(
    observation_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    if "market_code" not in observation_df:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for horizon in horizons:
        return_col = f"return_open_to_close_{horizon}d"
        for market_code, group in observation_df.dropna(subset=[return_col]).groupby(
            "market_code",
            dropna=False,
            observed=True,
        ):
            returns = group[return_col].astype(float)
            rows.append(
                {
                    "market_code": market_code,
                    "market_name": _first_non_null(group.get("market_name")),
                    "horizon": horizon,
                    "code_count": int(group["code"].nunique()),
                    **_return_stats(
                        returns,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    ),
                }
            )
    return pd.DataFrame(rows)


def _build_price_margin_interaction_summary_df(
    observation_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
    prior_return_windows: tuple[int, ...],
    discovery_end_date: str,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    periods = [
        ("full", observation_df),
        (
            "discovery",
            observation_df[observation_df["effective_date"].astype(str) <= discovery_end_date],
        ),
        (
            "validation",
            observation_df[observation_df["effective_date"].astype(str) > discovery_end_date],
        ),
    ]
    for window in prior_return_windows:
        prior_col = f"prior_return_{window}d"
        if prior_col not in observation_df.columns:
            continue
        for period_name, period_df in periods:
            base_df = period_df.dropna(
                subset=[prior_col, "long_weekly_change_pct"]
            ).copy()
            if base_df.empty:
                continue
            base_df["price_segment"] = np.where(
                base_df[prior_col].astype(float) < 0,
                "decline",
                "advance_or_flat",
            )
            base_df["long_change_segment"] = np.where(
                base_df["long_weekly_change_pct"].astype(float) > 0,
                "long_increase",
                "long_decrease_or_flat",
            )
            for horizon in horizons:
                return_col = f"return_open_to_close_{horizon}d"
                horizon_df = base_df.dropna(subset=[return_col])
                if horizon_df.empty:
                    continue
                price_segment_stats = {
                    price_segment: _return_stats(
                        group[return_col].astype(float),
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                    for price_segment, group in horizon_df.groupby(
                        "price_segment",
                        observed=True,
                    )
                }
                for (price_segment, long_segment), group in horizon_df.groupby(
                    ["price_segment", "long_change_segment"],
                    observed=True,
                ):
                    returns = group[return_col].astype(float)
                    stats = _return_stats(
                        returns,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                    price_stats = price_segment_stats[str(price_segment)]
                    rows.append(
                        {
                            "period": period_name,
                            "prior_return_window": window,
                            "horizon": horizon,
                            "price_segment": str(price_segment),
                            "long_change_segment": str(long_segment),
                            "obs": stats["obs"],
                            "code_count": int(group["code"].nunique()),
                            "mean_prior_return_pct": float(
                                group[prior_col].astype(float).mean() * 100.0
                            ),
                            "mean_long_weekly_change_pct": float(
                                group["long_weekly_change_pct"].astype(float).mean()
                            ),
                            "mean_return_pct": stats["mean_return_pct"],
                            "hit_rate_pct": stats["hit_rate_pct"],
                            "severe_loss_rate_pct": stats["severe_loss_rate_pct"],
                            "price_segment_mean_return_pct": price_stats[
                                "mean_return_pct"
                            ],
                            "delta_vs_price_segment_pct": stats["mean_return_pct"]
                            - price_stats["mean_return_pct"],
                            "price_segment_severe_loss_rate_pct": price_stats[
                                "severe_loss_rate_pct"
                            ],
                            "severe_loss_delta_vs_price_segment_pct": stats[
                                "severe_loss_rate_pct"
                            ]
                            - price_stats["severe_loss_rate_pct"],
                        }
                    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "period",
                "prior_return_window",
                "horizon",
                "price_segment",
                "long_change_segment",
            ]
        )
    return pd.DataFrame(rows).sort_values(
        [
            "period",
            "prior_return_window",
            "horizon",
            "price_segment",
            "long_change_segment",
        ],
        kind="stable",
    )


def _build_coverage_summary_df(
    observation_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
    prior_return_windows: tuple[int, ...],
    start_date: str | None,
    end_date: str | None,
    effective_lag_sessions: int,
    adv_window: int,
    percentile_window: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = [
        {
            "metric": "observations",
            "value": int(len(observation_df)),
            "detail": "rows after PIT effective-date mapping and OHLCV join",
        },
        {
            "metric": "codes",
            "value": int(observation_df["code"].nunique()),
            "detail": "distinct normalized stock codes",
        },
        {
            "metric": "start_date",
            "value": _str_or_none(observation_df["effective_date"].min()),
            "detail": start_date or "auto",
        },
        {
            "metric": "end_date",
            "value": _str_or_none(observation_df["effective_date"].max()),
            "detail": end_date or "auto",
        },
        {
            "metric": "effective_lag_sessions",
            "value": effective_lag_sessions,
            "detail": "recorded margin date to first tradable entry date",
        },
        {
            "metric": "adv_window",
            "value": adv_window,
            "detail": "ADV uses prior sessions only",
        },
        {
            "metric": "percentile_window",
            "value": percentile_window,
            "detail": "rolling margin observations per code",
        },
    ]
    for horizon in horizons:
        return_col = f"return_open_to_close_{horizon}d"
        rows.append(
            {
                "metric": f"return_coverage_{horizon}d",
                "value": int(observation_df[return_col].notna().sum()),
                "detail": "observations with enough future close data",
        }
    )
    for window in prior_return_windows:
        prior_col = f"prior_return_{window}d"
        if prior_col in observation_df.columns:
            rows.append(
                {
                    "metric": f"prior_return_coverage_{window}d",
                    "value": int(observation_df[prior_col].notna().sum()),
                    "detail": "observations with PIT-safe prior price return",
                }
            )
    return pd.DataFrame(rows)


def _return_stats(
    returns: pd.Series,
    *,
    severe_loss_threshold_pct: float | None = None,
) -> dict[str, Any]:
    clean = returns.dropna().astype(float)
    if clean.empty:
        return {
            "obs": 0,
            "mean_return_pct": np.nan,
            "median_return_pct": np.nan,
            "hit_rate_pct": np.nan,
            "p10_return_pct": np.nan,
            "p90_return_pct": np.nan,
            "severe_loss_rate_pct": np.nan,
        }
    severe_loss_threshold = (
        severe_loss_threshold_pct / 100.0
        if severe_loss_threshold_pct is not None
        else DEFAULT_SEVERE_LOSS_THRESHOLD_PCT / 100.0
    )
    return {
        "obs": int(len(clean)),
        "mean_return_pct": float(clean.mean() * 100.0),
        "median_return_pct": float(clean.median() * 100.0),
        "hit_rate_pct": float((clean > 0).mean() * 100.0),
        "p10_return_pct": float(clean.quantile(0.10) * 100.0),
        "p90_return_pct": float(clean.quantile(0.90) * 100.0),
        "severe_loss_rate_pct": float((clean <= severe_loss_threshold).mean() * 100.0),
    }


def _bucket_side(bucket: int, bucket_count: int) -> str:
    if bucket == 1:
        return "low"
    if bucket == bucket_count:
        return "high"
    return "middle"


def _first_non_null(series: pd.Series | None) -> Any:
    if series is None:
        return None
    values = series.dropna()
    if values.empty:
        return None
    return values.iloc[0]


def _str_or_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)[:10]


def _top_rows_for_markdown(
    frame: pd.DataFrame,
    *,
    sort_columns: list[str] | None = None,
    ascending: list[bool] | None = None,
    limit: int,
) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame.copy()
    if sort_columns:
        display = display.sort_values(
            sort_columns,
            ascending=ascending or True,
            kind="stable",
        )
    display = display.head(limit)
    return _frame_to_markdown_table(display)


def _frame_to_markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    columns = [str(column) for column in frame.columns]
    rows = [
        [str(_format_markdown_value(value)) for value in row]
        for row in frame.itertuples(index=False, name=None)
    ]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def _format_markdown_value(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)
