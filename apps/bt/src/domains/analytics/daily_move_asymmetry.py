"""Daily move asymmetry research for TOPIX and Prime stocks."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

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
    write_dataclass_research_bundle,
)

DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID = "market-behavior/daily-move-asymmetry"
DEFAULT_ROLLING_VOL_WINDOW = 60
DEFAULT_HORIZONS: tuple[int, ...] = (1, 5, 20, 60)
DEFAULT_MIN_OBSERVATIONS = 100
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -5.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
PRIME_MARKET_CODES: tuple[str, ...] = ("0111", "0101", "prime")

_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "topix_event_summary_df",
    "prime_stock_event_summary_df",
    "paired_asymmetry_df",
    "sign_persistence_df",
    "streak_hazard_df",
    "prime_breadth_df",
    "breadth_conditioned_asymmetry_df",
    "beta_diagnostics_df",
    "observation_sample_df",
)

_EVENT_BUCKET_ORDER: tuple[str, ...] = (
    "extreme_down_le_-2sigma",
    "large_down_-2_to_-1sigma",
    "medium_down_-1_to_-0.5sigma",
    "small_down_-0.5_to_0sigma",
    "flat",
    "small_up_0_to_0.5sigma",
    "medium_up_0.5_to_1sigma",
    "large_up_1_to_2sigma",
    "extreme_up_ge_2sigma",
)

_MAGNITUDE_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("small", "small_down_-0.5_to_0sigma", "small_up_0_to_0.5sigma"),
    ("medium", "medium_down_-1_to_-0.5sigma", "medium_up_0.5_to_1sigma"),
    ("large", "large_down_-2_to_-1sigma", "large_up_1_to_2sigma"),
    ("extreme", "extreme_down_le_-2sigma", "extreme_up_ge_2sigma"),
)


@dataclass(frozen=True)
class DailyMoveAsymmetryResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    rolling_vol_window: int
    horizons: tuple[int, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    topix_observation_count: int
    prime_stock_observation_count: int
    prime_code_count: int
    feature_policy: str
    topix_event_summary_df: pd.DataFrame
    prime_stock_event_summary_df: pd.DataFrame
    paired_asymmetry_df: pd.DataFrame
    sign_persistence_df: pd.DataFrame
    streak_hazard_df: pd.DataFrame
    prime_breadth_df: pd.DataFrame
    breadth_conditioned_asymmetry_df: pd.DataFrame
    beta_diagnostics_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_daily_move_asymmetry_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    rolling_vol_window: int = DEFAULT_ROLLING_VOL_WINDOW,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> DailyMoveAsymmetryResearchResult:
    resolved_horizons = _normalize_horizons(horizons)
    if rolling_vol_window < 5:
        raise ValueError("rolling_vol_window must be >= 5")
    if min_observations < 1:
        raise ValueError("min_observations must be >= 1")
    if observation_sample_limit < 1:
        raise ValueError("observation_sample_limit must be >= 1")

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="daily-move-asymmetry-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        topix_price_df = _query_topix_price_rows(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
        )
        market_source = "stock_master_daily_exact_date"
        prime_price_df = _query_prime_stock_price_rows(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            market_source=market_source,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    topix_feature_df = _build_price_feature_df(
        topix_price_df,
        group_col=None,
        horizons=resolved_horizons,
        rolling_vol_window=rolling_vol_window,
    )
    prime_feature_df = _build_price_feature_df(
        prime_price_df,
        group_col="code",
        horizons=resolved_horizons,
        rolling_vol_window=rolling_vol_window,
    )
    prime_feature_df = _add_topix_forward_controls(
        prime_feature_df,
        topix_feature_df=topix_feature_df,
        horizons=resolved_horizons,
    )
    beta_diagnostics_df = _build_beta_diagnostics_df(
        prime_feature_df,
        topix_feature_df=topix_feature_df,
    )
    prime_feature_df = prime_feature_df.merge(
        beta_diagnostics_df[["code", "beta"]],
        on="code",
        how="left",
    )
    for horizon in resolved_horizons:
        prime_feature_df[f"beta_adjusted_forward_{horizon}d_return_pct"] = (
            prime_feature_df[f"forward_{horizon}d_return_pct"]
            - prime_feature_df["beta"] * prime_feature_df[f"topix_forward_{horizon}d_return_pct"]
        )

    topix_event_summary_df = _build_event_summary_df(
        topix_feature_df,
        scope="topix",
        return_metrics={"raw": "forward_{horizon}d_return_pct"},
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    prime_stock_event_summary_df = _build_event_summary_df(
        prime_feature_df,
        scope="prime_stock",
        return_metrics={
            "raw": "forward_{horizon}d_return_pct",
            "topix_excess": "topix_excess_forward_{horizon}d_return_pct",
            "beta_adjusted": "beta_adjusted_forward_{horizon}d_return_pct",
        },
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    paired_asymmetry_df = _build_paired_asymmetry_df(
        pd.concat([topix_event_summary_df, prime_stock_event_summary_df], ignore_index=True)
    )
    sign_persistence_df = pd.concat(
        [
            _build_sign_persistence_df(topix_feature_df, scope="topix"),
            _build_sign_persistence_df(prime_feature_df, scope="prime_stock"),
        ],
        ignore_index=True,
    )
    streak_hazard_df = pd.concat(
        [
            _build_streak_hazard_df(topix_feature_df, scope="topix", group_col=None),
            _build_streak_hazard_df(prime_feature_df, scope="prime_stock", group_col="code"),
        ],
        ignore_index=True,
    )
    prime_breadth_df = _build_prime_breadth_df(
        prime_feature_df,
        topix_feature_df=topix_feature_df,
    )
    breadth_conditioned_asymmetry_df = _build_breadth_conditioned_asymmetry_df(
        prime_feature_df,
        prime_breadth_df=prime_breadth_df,
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    observation_sample_df = _build_observation_sample_df(
        prime_feature_df,
        limit=observation_sample_limit,
        horizons=resolved_horizons,
    )

    valid_dates = pd.concat(
        [
            topix_feature_df["date"],
            prime_feature_df["date"],
        ],
        ignore_index=True,
    )
    valid_dates = pd.to_datetime(valid_dates, errors="coerce").dropna()
    return DailyMoveAsymmetryResearchResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=valid_dates.min().strftime("%Y-%m-%d")
        if not valid_dates.empty
        else None,
        analysis_end_date=valid_dates.max().strftime("%Y-%m-%d")
        if not valid_dates.empty
        else None,
        rolling_vol_window=rolling_vol_window,
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        topix_observation_count=int(topix_feature_df["event_bucket"].notna().sum()),
        prime_stock_observation_count=int(prime_feature_df["event_bucket"].notna().sum()),
        prime_code_count=int(prime_feature_df["code"].nunique()),
        feature_policy=(
            "Event buckets are based on same-day close-to-close return divided by "
            f"rolling {rolling_vol_window}-session volatility. Forward outcomes use only "
            "future close-to-close returns for measurement, not for event bucket construction. "
            "Prime membership uses same-date stock_master_daily without latest-master fallback. "
            "Stock outcomes are reported as "
            "raw return, TOPIX excess return, and static beta-adjusted residual."
        ),
        topix_event_summary_df=topix_event_summary_df,
        prime_stock_event_summary_df=prime_stock_event_summary_df,
        paired_asymmetry_df=paired_asymmetry_df,
        sign_persistence_df=sign_persistence_df,
        streak_hazard_df=streak_hazard_df,
        prime_breadth_df=prime_breadth_df,
        breadth_conditioned_asymmetry_df=breadth_conditioned_asymmetry_df,
        beta_diagnostics_df=beta_diagnostics_df,
        observation_sample_df=observation_sample_df,
    )


def _normalize_horizons(horizons: Iterable[int]) -> tuple[int, ...]:
    normalized = tuple(sorted({int(horizon) for horizon in horizons}))
    if not normalized:
        raise ValueError("horizons must not be empty")
    if any(horizon < 1 for horizon in normalized):
        raise ValueError("horizons must be >= 1")
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


def _assert_required_tables(conn: Any) -> None:
    missing = [
        table_name
        for table_name in ("topix_data", "stock_data")
        if not _table_exists(conn, table_name)
    ]
    if missing:
        raise RuntimeError(f"Required market tables are missing: {', '.join(missing)}")
    if not _table_exists(conn, "stock_master_daily"):
        raise RuntimeError("market.duckdb requires stock_master_daily for PIT Prime scope")


def _date_conditions(alias: str, start_date: str | None, end_date: str | None) -> tuple[str, list[str]]:
    conditions: list[str] = []
    params: list[str] = []
    if start_date:
        conditions.append(f"{alias}.date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append(f"{alias}.date <= ?")
        params.append(end_date)
    if not conditions:
        return "", []
    return " AND " + " AND ".join(conditions), params


def _query_topix_price_rows(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    date_sql, params = _date_conditions("t", start_date, end_date)
    df = conn.execute(
        f"""
        SELECT
            CAST(t.date AS DATE) AS date,
            CAST(t.close AS DOUBLE) AS close
        FROM topix_data t
        WHERE t.close IS NOT NULL
          AND t.close > 0
          {date_sql}
        ORDER BY t.date
        """,
        params,
    ).fetchdf()
    return _normalize_price_frame(df, include_code=False)


def _query_prime_stock_price_rows(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    market_source: str,
) -> pd.DataFrame:
    date_sql, params = _date_conditions("s", start_date, end_date)
    normalized_stock_code = normalize_code_sql("s.code")
    market_codes_sql = ",".join("?" for _ in PRIME_MARKET_CODES)
    if market_source != "stock_master_daily_exact_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
    normalized_master_code = normalize_code_sql("m.code")
    sql = f"""
        WITH stock_rows AS (
            SELECT
                {normalized_stock_code} AS code,
                CAST(s.date AS DATE) AS date,
                CAST(s.close AS DOUBLE) AS close,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_stock_code}, s.date
                    ORDER BY CASE WHEN length(s.code) = 4 THEN 0 ELSE 1 END, s.code
                ) AS row_priority
            FROM stock_data s
            WHERE s.close IS NOT NULL
              AND s.close > 0
              {date_sql}
        ),
        master_rows AS (
            SELECT
                {normalized_master_code} AS code,
                CAST(m.date AS DATE) AS date,
                m.company_name,
                lower(coalesce(m.market_code, '')) AS market_code,
                coalesce(m.market_name, '') AS market_name,
                coalesce(m.sector_33_name, '') AS sector_33_name,
                coalesce(m.scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_master_code}, m.date
                    ORDER BY CASE WHEN length(m.code) = 4 THEN 0 ELSE 1 END, m.code
                ) AS row_priority
            FROM stock_master_daily m
            WHERE lower(coalesce(m.market_code, '')) IN ({market_codes_sql})
        )
        SELECT
            s.code,
            s.date,
            s.close,
            coalesce(m.company_name, s.code) AS company_name,
            m.market_code,
            m.market_name,
            m.sector_33_name,
            m.scale_category
        FROM stock_rows s
        JOIN master_rows m
          ON s.code = m.code
         AND s.date = m.date
         AND m.row_priority = 1
        WHERE s.row_priority = 1
        ORDER BY s.code, s.date
    """
    query_params = [*params, *PRIME_MARKET_CODES]
    df = conn.execute(sql, query_params).fetchdf()
    return _normalize_price_frame(df, include_code=True)


def _normalize_price_frame(df: pd.DataFrame, *, include_code: bool) -> pd.DataFrame:
    base_cols = ["date", "close"]
    extra_cols = [
        "code",
        "company_name",
        "market_code",
        "market_name",
        "sector_33_name",
        "scale_category",
    ]
    expected_cols = [*base_cols, *(extra_cols if include_code else [])]
    if df.empty:
        return pd.DataFrame(columns=expected_cols)
    normalized = df.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
    if include_code:
        normalized["code"] = normalized["code"].astype(str)
        for col in extra_cols:
            if col not in normalized.columns:
                normalized[col] = ""
            normalized[col] = normalized[col].fillna("").astype(str)
        normalized = normalized.dropna(subset=["code", "date", "close"])
        return normalized[expected_cols].sort_values(["code", "date"]).reset_index(drop=True)
    normalized = normalized.dropna(subset=["date", "close"])
    return normalized[expected_cols].sort_values("date").reset_index(drop=True)


def _build_price_feature_df(
    price_df: pd.DataFrame,
    *,
    group_col: str | None,
    horizons: Sequence[int],
    rolling_vol_window: int,
) -> pd.DataFrame:
    if price_df.empty:
        return price_df.copy()
    df = price_df.copy().sort_values([group_col, "date"] if group_col else ["date"])
    if group_col:
        grouped = df.groupby(group_col, sort=False, observed=True)
        df["return_1d_pct"] = grouped["close"].pct_change() * 100.0
        df["next_1d_return_pct"] = grouped["return_1d_pct"].shift(-1)
        df["rolling_vol_pct"] = grouped["return_1d_pct"].transform(
            lambda series: series.rolling(rolling_vol_window, min_periods=rolling_vol_window).std()
        )
        for horizon in horizons:
            df[f"forward_{horizon}d_return_pct"] = (
                grouped["close"].shift(-horizon) / df["close"] - 1.0
            ) * 100.0
    else:
        df["return_1d_pct"] = df["close"].pct_change() * 100.0
        df["next_1d_return_pct"] = df["return_1d_pct"].shift(-1)
        df["rolling_vol_pct"] = df["return_1d_pct"].rolling(
            rolling_vol_window,
            min_periods=rolling_vol_window,
        ).std()
        for horizon in horizons:
            df[f"forward_{horizon}d_return_pct"] = (
                df["close"].shift(-horizon) / df["close"] - 1.0
            ) * 100.0
    df["z_return_1d"] = df["return_1d_pct"] / df["rolling_vol_pct"]
    df["event_bucket"] = _bucketize_z_return(df["z_return_1d"])
    df["event_side"] = np.select(
        [df["return_1d_pct"] > 0, df["return_1d_pct"] < 0],
        ["up", "down"],
        default="flat",
    )
    return df.reset_index(drop=True)


def _bucketize_z_return(z_return: pd.Series) -> pd.Series:
    values = pd.to_numeric(z_return, errors="coerce")
    bucket = pd.Series(pd.NA, index=values.index, dtype="object")
    bucket[values <= -2.0] = "extreme_down_le_-2sigma"
    bucket[(values > -2.0) & (values <= -1.0)] = "large_down_-2_to_-1sigma"
    bucket[(values > -1.0) & (values <= -0.5)] = "medium_down_-1_to_-0.5sigma"
    bucket[(values > -0.5) & (values < 0.0)] = "small_down_-0.5_to_0sigma"
    bucket[values == 0.0] = "flat"
    bucket[(values > 0.0) & (values < 0.5)] = "small_up_0_to_0.5sigma"
    bucket[(values >= 0.5) & (values < 1.0)] = "medium_up_0.5_to_1sigma"
    bucket[(values >= 1.0) & (values < 2.0)] = "large_up_1_to_2sigma"
    bucket[values >= 2.0] = "extreme_up_ge_2sigma"
    return bucket


def _add_topix_forward_controls(
    stock_df: pd.DataFrame,
    *,
    topix_feature_df: pd.DataFrame,
    horizons: Sequence[int],
) -> pd.DataFrame:
    if stock_df.empty:
        return stock_df.copy()
    columns = ["date", "return_1d_pct", *[f"forward_{h}d_return_pct" for h in horizons]]
    topix = topix_feature_df[columns].copy()
    topix = topix.rename(
        columns={
            "return_1d_pct": "topix_return_1d_pct",
            **{
                f"forward_{h}d_return_pct": f"topix_forward_{h}d_return_pct"
                for h in horizons
            },
        }
    )
    merged = stock_df.merge(topix, on="date", how="left")
    for horizon in horizons:
        merged[f"topix_excess_forward_{horizon}d_return_pct"] = (
            merged[f"forward_{horizon}d_return_pct"]
            - merged[f"topix_forward_{horizon}d_return_pct"]
        )
    return merged


def _build_beta_diagnostics_df(
    stock_df: pd.DataFrame,
    *,
    topix_feature_df: pd.DataFrame,
) -> pd.DataFrame:
    if stock_df.empty:
        return pd.DataFrame(
            columns=["code", "beta", "corr_topix", "sample_count", "annualized_volatility_pct"]
        )
    topix_returns = topix_feature_df[["date", "return_1d_pct"]].rename(
        columns={"return_1d_pct": "topix_return_1d_pct_for_beta"}
    )
    merged = stock_df[["code", "date", "return_1d_pct"]].merge(
        topix_returns,
        on="date",
        how="left",
    )
    rows: list[dict[str, object]] = []
    for code, group in merged.groupby("code", sort=False):
        paired = group[["return_1d_pct", "topix_return_1d_pct_for_beta"]].dropna()
        if len(paired) < 20:
            beta = np.nan
            corr = np.nan
            vol = np.nan
        else:
            stock_values = paired["return_1d_pct"].astype("float64").to_numpy()
            topix_values = paired["topix_return_1d_pct_for_beta"].astype("float64").to_numpy()
            topix_var = float(np.var(topix_values, ddof=1))
            cov = float(np.cov(stock_values, topix_values, ddof=1)[0, 1])
            beta = cov / topix_var if topix_var > 0 else np.nan
            corr = float(np.corrcoef(stock_values, topix_values)[0, 1])
            vol = float(np.std(stock_values, ddof=1) * np.sqrt(252.0))
        rows.append(
            {
                "code": str(code),
                "beta": beta,
                "corr_topix": corr,
                "sample_count": int(len(paired)),
                "annualized_volatility_pct": vol,
            }
        )
    return pd.DataFrame(rows)


def _build_event_summary_df(
    feature_df: pd.DataFrame,
    *,
    scope: str,
    return_metrics: dict[str, str],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if feature_df.empty:
        return pd.DataFrame()
    valid_events = feature_df.dropna(subset=["event_bucket", "return_1d_pct"])
    for bucket in _EVENT_BUCKET_ORDER:
        bucket_df = valid_events[valid_events["event_bucket"].astype(str) == bucket]
        if bucket_df.empty:
            continue
        event_side = _event_side_from_bucket(bucket)
        for horizon in horizons:
            for metric, template in return_metrics.items():
                outcome_col = template.format(horizon=horizon)
                if outcome_col not in bucket_df.columns:
                    continue
                outcome = pd.to_numeric(bucket_df[outcome_col], errors="coerce")
                event_return = pd.to_numeric(bucket_df["return_1d_pct"], errors="coerce")
                summary = _summary_stats(
                    outcome,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                if _summary_observation_count(summary) < min_observations:
                    continue
                rows.append(
                    {
                        "scope": scope,
                        "return_metric": metric,
                        "horizon_sessions": int(horizon),
                        "event_bucket": bucket,
                        "event_side": event_side,
                        "event_mean_return_pct": _safe_mean(event_return),
                        "event_median_return_pct": _safe_median(event_return),
                        "event_mean_abs_return_pct": _safe_mean(event_return.abs()),
                        **summary,
                        "continuation_rate_pct": _rate_for_side(outcome, event_side),
                        "rebound_rate_pct": _rate_greater_than_zero(outcome)
                        if event_side == "down"
                        else np.nan,
                        "reversal_rate_pct": _rate_less_than_zero(outcome)
                        if event_side == "up"
                        else np.nan,
                    }
                )
    return _sort_event_summary(pd.DataFrame(rows))


def _summary_stats(
    values: pd.Series,
    *,
    severe_loss_threshold_pct: float,
) -> dict[str, object]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return {
            "observation_count": 0,
            "mean_forward_return_pct": np.nan,
            "median_forward_return_pct": np.nan,
            "hit_rate_pct": np.nan,
            "p05_forward_return_pct": np.nan,
            "p10_forward_return_pct": np.nan,
            "p90_forward_return_pct": np.nan,
            "severe_loss_rate_pct": np.nan,
        }
    return {
        "observation_count": int(len(clean)),
        "mean_forward_return_pct": float(clean.mean()),
        "median_forward_return_pct": float(clean.median()),
        "hit_rate_pct": _rate_greater_than_zero(clean),
        "p05_forward_return_pct": float(clean.quantile(0.05)),
        "p10_forward_return_pct": float(clean.quantile(0.10)),
        "p90_forward_return_pct": float(clean.quantile(0.90)),
        "severe_loss_rate_pct": float((clean <= severe_loss_threshold_pct).mean() * 100.0),
    }


def _summary_observation_count(summary: dict[str, object]) -> int:
    value = summary.get("observation_count", 0)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, np.integer):
        return int(value)
    return 0


def _sort_event_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    order_map = {bucket: index for index, bucket in enumerate(_EVENT_BUCKET_ORDER)}
    sorted_df = df.copy()
    sorted_df["_bucket_order"] = sorted_df["event_bucket"].map(order_map).fillna(999)
    sorted_df = sorted_df.sort_values(
        ["scope", "return_metric", "horizon_sessions", "_bucket_order"]
    )
    return sorted_df.drop(columns=["_bucket_order"]).reset_index(drop=True)


def _build_paired_asymmetry_df(event_summary_df: pd.DataFrame) -> pd.DataFrame:
    if event_summary_df.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    key_cols = ["scope", "return_metric", "horizon_sessions"]
    for keys, group in event_summary_df.groupby(key_cols, dropna=False, sort=False):
        scope, return_metric, horizon = keys
        by_bucket = {
            str(row["event_bucket"]): row
            for row in group.to_dict(orient="records")
        }
        for magnitude, down_bucket, up_bucket in _MAGNITUDE_PAIRS:
            down = by_bucket.get(down_bucket)
            up = by_bucket.get(up_bucket)
            if down is None or up is None:
                continue
            rows.append(
                {
                    "scope": scope,
                    "return_metric": return_metric,
                    "horizon_sessions": int(horizon),
                    "magnitude_bucket": magnitude,
                    "down_bucket": down_bucket,
                    "up_bucket": up_bucket,
                    "down_observation_count": int(down["observation_count"]),
                    "up_observation_count": int(up["observation_count"]),
                    "down_event_mean_abs_return_pct": down["event_mean_abs_return_pct"],
                    "up_event_mean_abs_return_pct": up["event_mean_abs_return_pct"],
                    "down_forward_mean_pct": down["mean_forward_return_pct"],
                    "up_forward_mean_pct": up["mean_forward_return_pct"],
                    "down_forward_median_pct": down["median_forward_return_pct"],
                    "up_forward_median_pct": up["median_forward_return_pct"],
                    "down_p10_forward_return_pct": down["p10_forward_return_pct"],
                    "up_p10_forward_return_pct": up["p10_forward_return_pct"],
                    "down_severe_loss_rate_pct": down["severe_loss_rate_pct"],
                    "up_severe_loss_rate_pct": up["severe_loss_rate_pct"],
                    "rebound_rate_after_down_pct": down["rebound_rate_pct"],
                    "reversal_rate_after_up_pct": up["reversal_rate_pct"],
                    "down_continuation_rate_pct": down["continuation_rate_pct"],
                    "up_continuation_rate_pct": up["continuation_rate_pct"],
                    "mean_gap_down_minus_up_pct": _diff(
                        down["mean_forward_return_pct"],
                        up["mean_forward_return_pct"],
                    ),
                    "median_gap_down_minus_up_pct": _diff(
                        down["median_forward_return_pct"],
                        up["median_forward_return_pct"],
                    ),
                    "continuation_gap_down_minus_up_pct": _diff(
                        down["continuation_rate_pct"],
                        up["continuation_rate_pct"],
                    ),
                }
            )
    return pd.DataFrame(rows)


def _build_sign_persistence_df(feature_df: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    if feature_df.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    valid = feature_df.dropna(subset=["return_1d_pct", "next_1d_return_pct"]).copy()
    valid["current_sign"] = np.select(
        [valid["return_1d_pct"] > 0, valid["return_1d_pct"] < 0],
        ["up", "down"],
        default="flat",
    )
    for sign in ("up", "down"):
        side_df = valid[valid["current_sign"] == sign]
        next_ret = pd.to_numeric(side_df["next_1d_return_pct"], errors="coerce").dropna()
        if next_ret.empty:
            continue
        same_rate = _rate_greater_than_zero(next_ret) if sign == "up" else _rate_less_than_zero(next_ret)
        opposite_rate = _rate_less_than_zero(next_ret) if sign == "up" else _rate_greater_than_zero(next_ret)
        rows.append(
            {
                "scope": scope,
                "current_sign": sign,
                "observation_count": int(len(next_ret)),
                "next_same_sign_rate_pct": same_rate,
                "next_opposite_sign_rate_pct": opposite_rate,
                "next_mean_return_pct": float(next_ret.mean()),
                "next_median_return_pct": float(next_ret.median()),
                "next_p10_return_pct": float(next_ret.quantile(0.10)),
            }
        )
    return pd.DataFrame(rows)


def _build_streak_hazard_df(
    feature_df: pd.DataFrame,
    *,
    scope: str,
    group_col: str | None,
) -> pd.DataFrame:
    if feature_df.empty:
        return pd.DataFrame()
    with_streaks = _add_streak_lengths(feature_df, group_col=group_col)
    valid = with_streaks.dropna(subset=["next_1d_return_pct"]).copy()
    valid = valid[valid["streak_sign"].isin(["up", "down"])]
    valid["streak_bucket"] = valid["streak_length"].map(
        lambda value: "5+" if int(value) >= 5 else str(int(value))
    )
    rows: list[dict[str, object]] = []
    for keys, group in valid.groupby(["streak_sign", "streak_bucket"], sort=False):
        streak_sign, streak_bucket = keys
        next_ret = pd.to_numeric(group["next_1d_return_pct"], errors="coerce").dropna()
        if next_ret.empty:
            continue
        continuation_rate = (
            _rate_greater_than_zero(next_ret)
            if streak_sign == "up"
            else _rate_less_than_zero(next_ret)
        )
        reversal_rate = (
            _rate_less_than_zero(next_ret)
            if streak_sign == "up"
            else _rate_greater_than_zero(next_ret)
        )
        rows.append(
            {
                "scope": scope,
                "streak_sign": streak_sign,
                "streak_bucket": streak_bucket,
                "observation_count": int(len(next_ret)),
                "continuation_rate_pct": continuation_rate,
                "reversal_rate_pct": reversal_rate,
                "next_mean_return_pct": float(next_ret.mean()),
                "next_median_return_pct": float(next_ret.median()),
                "next_p10_return_pct": float(next_ret.quantile(0.10)),
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    order = {"1": 1, "2": 2, "3": 3, "4": 4, "5+": 5}
    result["_order"] = result["streak_bucket"].map(order)
    return result.sort_values(["scope", "streak_sign", "_order"]).drop(columns=["_order"]).reset_index(drop=True)


def _add_streak_lengths(feature_df: pd.DataFrame, *, group_col: str | None) -> pd.DataFrame:
    df = feature_df.copy().sort_values([group_col, "date"] if group_col else ["date"])
    df["streak_sign"] = np.select(
        [df["return_1d_pct"] > 0, df["return_1d_pct"] < 0],
        ["up", "down"],
        default="flat",
    )
    if group_col:
        frames = [_compute_group_streaks(group) for _, group in df.groupby(group_col, sort=False)]
        return pd.concat(frames, ignore_index=True) if frames else df
    return _compute_group_streaks(df)


def _compute_group_streaks(group: pd.DataFrame) -> pd.DataFrame:
    result = group.copy()
    lengths: list[int] = []
    previous = ""
    length = 0
    for sign in result["streak_sign"].astype(str).tolist():
        if sign not in {"up", "down"}:
            previous = sign
            length = 0
            lengths.append(0)
            continue
        if sign == previous:
            length += 1
        else:
            previous = sign
            length = 1
        lengths.append(length)
    result["streak_length"] = lengths
    return result


def _build_prime_breadth_df(
    stock_df: pd.DataFrame,
    *,
    topix_feature_df: pd.DataFrame,
) -> pd.DataFrame:
    if stock_df.empty:
        return pd.DataFrame()
    grouped = stock_df.dropna(subset=["return_1d_pct"]).groupby("date", sort=True)
    breadth = grouped.agg(
        stock_count=("code", "nunique"),
        up_ratio_pct=("return_1d_pct", lambda series: float((series > 0).mean() * 100.0)),
        down_ratio_pct=("return_1d_pct", lambda series: float((series < 0).mean() * 100.0)),
        extreme_up_ratio_pct=("z_return_1d", lambda series: float((series >= 2.0).mean() * 100.0)),
        extreme_down_ratio_pct=("z_return_1d", lambda series: float((series <= -2.0).mean() * 100.0)),
        equal_weight_return_pct=("return_1d_pct", "mean"),
        median_stock_return_pct=("return_1d_pct", "median"),
    ).reset_index()
    topix = topix_feature_df[["date", "return_1d_pct"]].rename(
        columns={"return_1d_pct": "topix_return_1d_pct"}
    )
    breadth = breadth.merge(topix, on="date", how="left")
    breadth["topix_sign"] = np.select(
        [breadth["topix_return_1d_pct"] > 0, breadth["topix_return_1d_pct"] < 0],
        ["topix_up", "topix_down"],
        default="topix_flat",
    )
    breadth["up_breadth_bucket"] = pd.cut(
        breadth["up_ratio_pct"],
        bins=[-np.inf, 40.0, 60.0, np.inf],
        labels=["weak_up_breadth_lt_40pct", "mixed_40_to_60pct", "broad_up_ge_60pct"],
    ).astype(str)
    breadth["extreme_down_bucket"] = pd.cut(
        breadth["extreme_down_ratio_pct"],
        bins=[-np.inf, 5.0, 15.0, np.inf],
        labels=[
            "low_extreme_down_lt_5pct",
            "medium_extreme_down_5_to_15pct",
            "high_extreme_down_ge_15pct",
        ],
    ).astype(str)
    return breadth.sort_values("date").reset_index(drop=True)


def _build_breadth_conditioned_asymmetry_df(
    stock_df: pd.DataFrame,
    *,
    prime_breadth_df: pd.DataFrame,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    if stock_df.empty or prime_breadth_df.empty:
        return pd.DataFrame()
    controls = prime_breadth_df[
        ["date", "topix_sign", "up_breadth_bucket", "extreme_down_bucket"]
    ]
    merged = stock_df.merge(controls, on="date", how="left")
    event_buckets = [
        "extreme_down_le_-2sigma",
        "large_down_-2_to_-1sigma",
        "large_up_1_to_2sigma",
        "extreme_up_ge_2sigma",
    ]
    rows: list[dict[str, object]] = []
    for keys, group in merged[merged["event_bucket"].isin(event_buckets)].groupby(
        ["topix_sign", "up_breadth_bucket", "extreme_down_bucket", "event_bucket"],
        dropna=False,
        sort=False,
    ):
        topix_sign, up_breadth_bucket, extreme_down_bucket, event_bucket = keys
        for horizon in horizons:
            for metric, col in (
                ("raw", f"forward_{horizon}d_return_pct"),
                ("topix_excess", f"topix_excess_forward_{horizon}d_return_pct"),
            ):
                summary = _summary_stats(
                    group[col],
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                if _summary_observation_count(summary) < min_observations:
                    continue
                rows.append(
                    {
                        "topix_sign": topix_sign,
                        "up_breadth_bucket": up_breadth_bucket,
                        "extreme_down_bucket": extreme_down_bucket,
                        "event_bucket": event_bucket,
                        "event_side": _event_side_from_bucket(str(event_bucket)),
                        "return_metric": metric,
                        "horizon_sessions": int(horizon),
                        **summary,
                    }
                )
    return pd.DataFrame(rows)


def _build_observation_sample_df(
    stock_df: pd.DataFrame,
    *,
    limit: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    if stock_df.empty:
        return pd.DataFrame()
    cols = [
        "date",
        "code",
        "company_name",
        "return_1d_pct",
        "rolling_vol_pct",
        "z_return_1d",
        "event_bucket",
        "topix_return_1d_pct",
        *[f"forward_{h}d_return_pct" for h in horizons],
        *[f"topix_excess_forward_{h}d_return_pct" for h in horizons],
    ]
    available = [col for col in cols if col in stock_df.columns]
    return stock_df[available].dropna(subset=["event_bucket"]).head(limit).reset_index(drop=True)


def _event_side_from_bucket(bucket: str) -> str:
    if "_down" in bucket:
        return "down"
    if "_up" in bucket:
        return "up"
    return "flat"


def _rate_for_side(values: pd.Series, side: str) -> float:
    if side == "up":
        return _rate_greater_than_zero(values)
    if side == "down":
        return _rate_less_than_zero(values)
    return np.nan


def _rate_greater_than_zero(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return np.nan
    return float((clean > 0).mean() * 100.0)


def _rate_less_than_zero(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return np.nan
    return float((clean < 0).mean() * 100.0)


def _safe_mean(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.mean()) if not clean.empty else np.nan


def _safe_median(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.median()) if not clean.empty else np.nan


def _diff(left: object, right: object) -> float:
    left_value = pd.to_numeric(pd.Series([left]), errors="coerce").iloc[0]
    right_value = pd.to_numeric(pd.Series([right]), errors="coerce").iloc[0]
    if pd.isna(left_value) or pd.isna(right_value):
        return np.nan
    return float(left_value - right_value)


def _build_summary_markdown(result: DailyMoveAsymmetryResearchResult) -> str:
    lines = [
        "# Daily Move Asymmetry",
        "",
        f"- DB: `{result.db_path}`",
        f"- Source: `{result.source_mode}` ({result.source_detail})",
        f"- Market source: `{result.market_source}`",
        f"- Analysis range: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Rolling volatility window: `{result.rolling_vol_window}` sessions",
        f"- Horizons: `{', '.join(str(value) for value in result.horizons)}` sessions",
        f"- TOPIX observations: `{result.topix_observation_count}`",
        f"- Prime stock observations: `{result.prime_stock_observation_count}`",
        f"- Prime codes: `{result.prime_code_count}`",
        "",
        "## Policy",
        "",
        result.feature_policy,
        "",
        "## TOPIX Paired Asymmetry",
        "",
        _format_pair_rows(result.paired_asymmetry_df, scope="topix", return_metric="raw"),
        "",
        "## Prime Stock Paired Asymmetry",
        "",
        _format_pair_rows(result.paired_asymmetry_df, scope="prime_stock", return_metric="topix_excess"),
        "",
        "## Sign Persistence",
        "",
        _format_sign_rows(result.sign_persistence_df),
        "",
        "## Output Tables",
        "",
    ]
    for table_name in _RESULT_TABLE_NAMES:
        table = getattr(result, table_name)
        lines.append(f"- `{table_name}`: `{len(table)}` rows")
    return "\n".join(lines) + "\n"


def _format_pair_rows(
    pair_df: pd.DataFrame,
    *,
    scope: str,
    return_metric: str,
) -> str:
    if pair_df.empty:
        return "No paired rows."
    rows = pair_df[
        (pair_df["scope"].astype(str) == scope)
        & (pair_df["return_metric"].astype(str) == return_metric)
    ].head(16)
    if rows.empty:
        return "No paired rows."
    lines = [
        "| Horizon | Magnitude | Down mean | Up mean | Down median | Up median | Down continuation | Up continuation |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{int(row['horizon_sessions'])}` | "
            f"`{row['magnitude_bucket']}` | "
            f"`{_fmt(row['down_forward_mean_pct'])}%` | "
            f"`{_fmt(row['up_forward_mean_pct'])}%` | "
            f"`{_fmt(row['down_forward_median_pct'])}%` | "
            f"`{_fmt(row['up_forward_median_pct'])}%` | "
            f"`{_fmt(row['down_continuation_rate_pct'])}%` | "
            f"`{_fmt(row['up_continuation_rate_pct'])}%` |"
        )
    return "\n".join(lines)


def _format_sign_rows(sign_df: pd.DataFrame) -> str:
    if sign_df.empty:
        return "No sign rows."
    lines = [
        "| Scope | Sign | Obs | Same sign next day | Opposite next day | Next mean |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in sign_df.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row['scope']}` | "
            f"`{row['current_sign']}` | "
            f"`{int(row['observation_count'])}` | "
            f"`{_fmt(row['next_same_sign_rate_pct'])}%` | "
            f"`{_fmt(row['next_opposite_sign_rate_pct'])}%` | "
            f"`{_fmt(row['next_mean_return_pct'])}%` |"
        )
    return "\n".join(lines)


def _fmt(value: object, digits: int = 2) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "nan"
    return f"{float(number):.{digits}f}"


def write_daily_move_asymmetry_bundle(
    result: DailyMoveAsymmetryResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID,
        module=__name__,
        function="run_daily_move_asymmetry_research",
        params={
            "db_path": result.db_path,
            "rolling_vol_window": result.rolling_vol_window,
            "horizons": list(result.horizons),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_daily_move_asymmetry_bundle(
    bundle_path: str | Path,
) -> DailyMoveAsymmetryResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=DailyMoveAsymmetryResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_daily_move_asymmetry_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_daily_move_asymmetry_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID",
    "DEFAULT_HORIZONS",
    "DEFAULT_ROLLING_VOL_WINDOW",
    "DailyMoveAsymmetryResearchResult",
    "get_daily_move_asymmetry_bundle_path_for_run_id",
    "get_daily_move_asymmetry_latest_bundle_path",
    "load_daily_move_asymmetry_bundle",
    "run_daily_move_asymmetry_research",
    "write_daily_move_asymmetry_bundle",
)
