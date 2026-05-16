"""Pre-disclosure valuation proxy research for EPS 1.2x positive events."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast

import numpy as np
import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
    OVERHEAT_STATE,
    _float_or_nan,
    _sort_summary_df,
    _str_or_none,
    _table_exists,
    _top_rows_for_markdown,
)
from src.domains.analytics.fundamental_ranking import adjust_per_share_value
from src.domains.analytics.post_earnings_next_day_entry import (
    DEFAULT_LIQUIDITY_WINDOW,
    DEFAULT_PRE_WINDOWS,
    POST_EARNINGS_NEXT_DAY_ENTRY_EXPERIMENT_ID,
    run_post_earnings_next_day_entry_research,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle
from src.shared.models.types import normalize_period_type
from src.shared.utils.share_adjustment import resolve_latest_quarterly_baseline_shares
from src.shared.utils.statement_document import (
    is_actual_fy_financial_statement,
    is_earn_forecast_revision_document,
)

PRE_EARNINGS_EPS120_PROXY_EXPERIMENT_ID = "market-behavior/pre-earnings-eps120-proxy"
DEFAULT_MIN_EVENTS = 100


@dataclass(frozen=True)
class PreEarningsEps120ProxyResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    min_events: int
    event_feature_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    feature_bucket_df: pd.DataFrame
    threshold_grid_df: pd.DataFrame
    combo_grid_df: pd.DataFrame
    annual_valuation_regime_df: pd.DataFrame
    current_cross_section_df: pd.DataFrame


def run_pre_earnings_eps120_proxy_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    min_events: int = DEFAULT_MIN_EVENTS,
) -> PreEarningsEps120ProxyResult:
    if min_events <= 0:
        raise ValueError("min_events must be positive")
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    base_result = run_post_earnings_next_day_entry_research(
        db_path_obj,
        start_date=start_date,
        end_date=end_date,
        pre_windows=DEFAULT_PRE_WINDOWS,
        horizons=(1,),
        liquidity_window=DEFAULT_LIQUIDITY_WINDOW,
    )
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="pre-earnings-eps120-proxy-",
    ) as ctx:
        statement_df = _query_statement_rows(ctx.connection)
        adjusted_statement_metric_df = _query_adjusted_statement_metric_rows(ctx.connection)
        daily_valuation_df = _query_daily_valuation_rows(
            ctx.connection,
            base_result.event_feature_df,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    event_feature_df = _enrich_events_with_pre_valuation(
        base_result.event_feature_df,
        statement_df,
        adjusted_statement_metric_df=adjusted_statement_metric_df,
        daily_valuation_df=daily_valuation_df,
    )
    scoped_df = _expand_market_scope(event_feature_df)
    coverage_diagnostics_df = _build_coverage_diagnostics_df(scoped_df)
    feature_bucket_df = _build_feature_bucket_df(scoped_df, min_events=min_events)
    threshold_grid_df = _build_threshold_grid_df(scoped_df, min_events=min_events)
    combo_grid_df = _build_combo_grid_df(scoped_df, min_events=min_events)
    annual_valuation_regime_df = _build_annual_valuation_regime_df(event_feature_df)
    current_cross_section_df = _build_current_cross_section_df(str(db_path_obj))

    return PreEarningsEps120ProxyResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=base_result.market_source,
        analysis_start_date=_str_or_none(event_feature_df["disclosed_date"].min())
        if "disclosed_date" in event_feature_df
        else None,
        analysis_end_date=_str_or_none(event_feature_df["disclosed_date"].max())
        if "disclosed_date" in event_feature_df
        else None,
        min_events=min_events,
        event_feature_df=event_feature_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
        feature_bucket_df=feature_bucket_df,
        threshold_grid_df=threshold_grid_df,
        combo_grid_df=combo_grid_df,
        annual_valuation_regime_df=annual_valuation_regime_df,
        current_cross_section_df=current_cross_section_df,
    )


def write_pre_earnings_eps120_proxy_bundle(
    result: PreEarningsEps120ProxyResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=PRE_EARNINGS_EPS120_PROXY_EXPERIMENT_ID,
        module=__name__,
        function="run_pre_earnings_eps120_proxy_research",
        params={"min_events": result.min_events},
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "event_count": int(len(result.event_feature_df)),
            "upstream_experiment_id": POST_EARNINGS_NEXT_DAY_ENTRY_EXPERIMENT_ID,
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "feature_bucket_df": result.feature_bucket_df,
            "threshold_grid_df": result.threshold_grid_df,
            "combo_grid_df": result.combo_grid_df,
            "annual_valuation_regime_df": result.annual_valuation_regime_df,
            "current_cross_section_df": result.current_cross_section_df,
            "event_feature_df": result.event_feature_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: PreEarningsEps120ProxyResult) -> str:
    coverage = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=20)
    buckets = _top_rows_for_markdown(
        result.feature_bucket_df,
        sort_columns=["market_scope", "is_fy", "feature", "bucket_order"],
        limit=50,
    )
    thresholds = _top_rows_for_markdown(
        result.threshold_grid_df,
        sort_columns=["market_scope", "is_fy", "lift_vs_base"],
        limit=50,
    )
    combos = _top_rows_for_markdown(
        result.combo_grid_df,
        sort_columns=["market_scope", "is_fy", "lift_vs_base"],
        limit=50,
    )
    annual = _top_rows_for_markdown(
        result.annual_valuation_regime_df,
        sort_columns=[
            "market",
            "is_fy",
            "liquidity_regime",
            "condition_scope",
            "event_year",
            "bucket_order",
        ],
        limit=80,
    )
    current = _top_rows_for_markdown(
        result.current_cross_section_df,
        sort_columns=[
            "snapshot_date",
            "collection_scope",
            "liquidity_regime",
            "bucket_order",
        ],
        limit=80,
    )
    return "\n".join(
        [
            "# Pre-Earnings EPS 1.2x Proxy",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Min events: `{result.min_events}`",
            "",
            "## Coverage Diagnostics",
            "",
            coverage,
            "",
            "## Feature Buckets",
            "",
            buckets,
            "",
            "## Threshold Grid",
            "",
            thresholds,
            "",
            "## Combo Grid",
            "",
            combos,
            "",
            "## Annual Valuation Regime Trend",
            "",
            annual,
            "",
            "## Current Daily Ranking Cross-Section",
            "",
            current,
            "",
        ]
    )


def _query_statement_rows(conn: Any) -> pd.DataFrame:
    if not _table_exists(conn, "statements"):
        raise RuntimeError("market.duckdb is missing required table: statements")
    normalized_code = normalize_code_sql("code")
    df = conn.execute(
        f"""
        SELECT *
        FROM (
            SELECT
                {normalized_code} AS code,
                disclosed_date,
                type_of_document,
                type_of_current_period,
                CAST(earnings_per_share AS DOUBLE) AS earnings_per_share,
                CAST(next_year_forecast_earnings_per_share AS DOUBLE)
                    AS next_year_forecast_earnings_per_share,
                CAST(forecast_eps AS DOUBLE) AS forecast_eps,
                CAST(bps AS DOUBLE) AS bps,
                CAST(shares_outstanding AS DOUBLE) AS shares_outstanding,
                CAST(treasury_shares AS DOUBLE) AS treasury_shares,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code}, disclosed_date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END,
                             type_of_document NULLS LAST
                ) AS rn
            FROM statements
        )
        WHERE rn = 1
        ORDER BY code, disclosed_date
        """
    ).fetchdf()
    if df.empty:
        return df
    df["code"] = df["code"].astype(str)
    df["disclosed_date"] = df["disclosed_date"].astype(str)
    return df


def _query_adjusted_statement_metric_rows(conn: Any) -> pd.DataFrame:
    if not _table_exists(conn, "statement_metrics_adjusted"):
        return pd.DataFrame(
            columns=[
                "code",
                "disclosed_date",
                "adjusted_eps",
                "adjusted_bps",
                "adjusted_forecast_eps",
                "basis_version",
            ]
        )
    normalized_code = normalize_code_sql("code")
    df = conn.execute(
        f"""
        SELECT *
        FROM (
            SELECT
                {normalized_code} AS code,
                disclosed_date,
                CAST(adjusted_eps AS DOUBLE) AS adjusted_eps,
                CAST(adjusted_bps AS DOUBLE) AS adjusted_bps,
                CAST(adjusted_forecast_eps AS DOUBLE) AS adjusted_forecast_eps,
                basis_version,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code}, disclosed_date
                    ORDER BY price_basis_date DESC NULLS LAST,
                             basis_version DESC,
                             CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                ) AS rn
            FROM statement_metrics_adjusted
        )
        WHERE rn = 1
        ORDER BY code, disclosed_date
        """
    ).fetchdf()
    if df.empty:
        return df
    df["code"] = df["code"].astype(str)
    df["disclosed_date"] = df["disclosed_date"].astype(str)
    return df


def _query_daily_valuation_rows(conn: Any, event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "code",
        "pre_event_date",
        "valuation_actual_eps",
        "valuation_forward_eps",
        "valuation_bps",
        "valuation_shares_outstanding",
        "valuation_fy_disclosed_date",
        "valuation_forward_eps_disclosed_date",
        "valuation_forward_eps_period_type",
        "valuation_forward_eps_source",
        "per",
        "forward_per",
        "pbr",
        "market_cap_bil_jpy",
        "valuation_source",
    ]
    if (
        event_df.empty
        or not _table_exists(conn, "daily_valuation")
        or "pre_event_date" not in event_df.columns
    ):
        return pd.DataFrame(columns=columns)
    pairs = (
        event_df.loc[:, ["code", "pre_event_date"]]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    if pairs.empty:
        return pd.DataFrame(columns=columns)
    conn.register("_pre_earnings_event_pairs", pairs)
    try:
        valuation_code = normalize_code_sql("dv.code")
        df = conn.execute(
            f"""
            WITH valuation_canonical AS (
                SELECT *
                FROM (
                    SELECT
                        {valuation_code} AS code,
                        dv.date,
                        dv.close,
                        dv.eps,
                        dv.bps,
                        dv.forward_eps,
                        dv.per,
                        dv.forward_per,
                        dv.pbr,
                        dv.market_cap,
                        dv.statement_disclosed_date,
                        dv.forward_eps_disclosed_date,
                        dv.forward_eps_source,
                        ROW_NUMBER() OVER (
                            PARTITION BY {valuation_code}, dv.date
                            ORDER BY dv.price_basis_date DESC NULLS LAST,
                                     dv.basis_version DESC,
                                     CASE WHEN length(dv.code) = 4 THEN 0 ELSE 1 END
                        ) AS rn
                    FROM daily_valuation dv
                    JOIN _pre_earnings_event_pairs p
                      ON p.code = {valuation_code}
                     AND p.pre_event_date = dv.date
                )
                WHERE rn = 1
            )
            SELECT
                p.code,
                p.pre_event_date,
                v.eps AS valuation_actual_eps,
                v.forward_eps AS valuation_forward_eps,
                v.bps AS valuation_bps,
                CASE
                    WHEN v.market_cap IS NOT NULL AND v.close > 0
                    THEN v.market_cap / v.close
                    ELSE NULL
                END AS valuation_shares_outstanding,
                v.statement_disclosed_date AS valuation_fy_disclosed_date,
                v.forward_eps_disclosed_date AS valuation_forward_eps_disclosed_date,
                'FY' AS valuation_forward_eps_period_type,
                v.forward_eps_source AS valuation_forward_eps_source,
                v.per,
                v.forward_per,
                v.pbr,
                v.market_cap / 1e9 AS market_cap_bil_jpy,
                'daily_valuation' AS valuation_source
            FROM _pre_earnings_event_pairs p
            JOIN valuation_canonical v
              ON v.code = p.code
             AND v.date = p.pre_event_date
            ORDER BY p.code, p.pre_event_date
            """
        ).fetchdf()
    finally:
        conn.unregister("_pre_earnings_event_pairs")
    if df.empty:
        return pd.DataFrame(columns=columns)
    df["code"] = df["code"].astype(str)
    df["pre_event_date"] = df["pre_event_date"].astype(str)
    return df.loc[:, columns]


def _enrich_events_with_pre_valuation(
    event_df: pd.DataFrame,
    statement_df: pd.DataFrame,
    *,
    adjusted_statement_metric_df: pd.DataFrame | None = None,
    daily_valuation_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if event_df.empty:
        return event_df.copy()
    adjusted_statement_metric_df = (
        adjusted_statement_metric_df
        if adjusted_statement_metric_df is not None
        else pd.DataFrame()
    )
    daily_valuation_df = (
        daily_valuation_df
        if daily_valuation_df is not None
        else pd.DataFrame()
    )
    statement_with_adjusted = statement_df.copy()
    if not adjusted_statement_metric_df.empty:
        statement_with_adjusted = statement_with_adjusted.merge(
            adjusted_statement_metric_df,
            on=["code", "disclosed_date"],
            how="left",
        )
    for column in ("adjusted_eps", "adjusted_bps", "adjusted_forecast_eps"):
        if column not in statement_with_adjusted.columns:
            statement_with_adjusted[column] = np.nan
    statement_with_adjusted["sot_actual_eps"] = statement_with_adjusted[
        "adjusted_eps"
    ].combine_first(statement_with_adjusted["earnings_per_share"])
    statement_with_adjusted["sot_next_forecast_eps"] = statement_with_adjusted[
        "adjusted_forecast_eps"
    ].combine_first(
        statement_with_adjusted["next_year_forecast_earnings_per_share"].combine_first(
            statement_with_adjusted["forecast_eps"]
        )
    )
    current_statement = statement_with_adjusted[
        [
            "code",
            "disclosed_date",
            "sot_actual_eps",
            "sot_next_forecast_eps",
        ]
    ].rename(
        columns={
            "sot_actual_eps": "actual_eps",
            "sot_next_forecast_eps": "next_forecast_eps",
        }
    )
    enriched = event_df.merge(current_statement, on=["code", "disclosed_date"], how="left")

    statement_lookup = _build_statement_lookup(statement_with_adjusted)
    daily_valuation_lookup = _build_daily_valuation_lookup(daily_valuation_df)

    valuation_records: list[dict[str, float | str | None]] = []
    for row in enriched.itertuples(index=False):
        code = str(row.code)
        pre_event_date = str(row.pre_event_date)
        daily_valuation = daily_valuation_lookup.get((code, pre_event_date))
        if daily_valuation is not None:
            valuation_record = dict(daily_valuation)
            valuation_record.pop("code", None)
            valuation_record.pop("pre_event_date", None)
            valuation_records.append(cast(dict[str, float | str | None], valuation_record))
            continue
        statements_as_of = _lookup_statements_as_of(statement_lookup, code, pre_event_date)
        latest_fy = _lookup_latest_actual_fy_statement(statements_as_of)
        baseline_shares = _resolve_baseline_shares(statements_as_of)
        close = _float_or_nan(row.pre_event_close)
        valuation_actual_eps = _adjust_statement_per_share_metric(
            latest_fy,
            "earnings_per_share",
            baseline_shares,
        )
        valuation_bps = _adjust_statement_per_share_metric(
            latest_fy,
            "bps",
            baseline_shares,
        )
        valuation_forward_eps, forward_eps_date, forward_eps_period, forward_eps_source = (
            _resolve_forward_eps_for_valuation(
                statements_as_of,
                latest_fy=latest_fy,
                baseline_shares=baseline_shares,
            )
        )
        valuation_shares = baseline_shares if baseline_shares is not None else np.nan
        valuation_records.append(
            {
                "valuation_actual_eps": _nan_if_none(valuation_actual_eps),
                "valuation_forward_eps": _nan_if_none(valuation_forward_eps),
                "valuation_bps": _nan_if_none(valuation_bps),
                "valuation_shares_outstanding": valuation_shares,
                "valuation_fy_disclosed_date": latest_fy.get("disclosed_date") if latest_fy else None,
                "valuation_forward_eps_disclosed_date": forward_eps_date,
                "valuation_forward_eps_period_type": forward_eps_period,
                "valuation_forward_eps_source": forward_eps_source,
                "per": _ratio(close, _nan_if_none(valuation_actual_eps)),
                "forward_per": _ratio(close, _nan_if_none(valuation_forward_eps)),
                "pbr": _ratio(close, _nan_if_none(valuation_bps)),
                "market_cap_bil_jpy": (
                    close * valuation_shares / 1e9
                    if math.isfinite(close) and math.isfinite(valuation_shares) and valuation_shares > 0
                    else np.nan
                ),
                "valuation_source": "statement_fallback",
            }
        )
    valuation_df = pd.DataFrame.from_records(valuation_records)
    enriched = pd.concat([enriched.reset_index(drop=True), valuation_df], axis=1)
    enriched["eps120_target_eligible"] = (
        (enriched["is_fy"] == True)  # noqa: E712
        & (pd.to_numeric(enriched["actual_eps"], errors="coerce") > 0)
        & pd.to_numeric(enriched["next_forecast_eps"], errors="coerce").notna()
    )
    enriched["eps120_positive_target"] = (
        enriched["eps120_target_eligible"]
        & (enriched["event_strength"].astype(str) == "positive")
        & (
            pd.to_numeric(enriched["next_forecast_eps"], errors="coerce")
            >= pd.to_numeric(enriched["actual_eps"], errors="coerce") * 1.2
        )
    )
    enriched["next_forecast_to_actual_eps_ratio"] = (
        pd.to_numeric(enriched["next_forecast_eps"], errors="coerce")
        / pd.to_numeric(enriched["actual_eps"], errors="coerce").where(
            pd.to_numeric(enriched["actual_eps"], errors="coerce") > 0,
            np.nan,
        )
    )
    for feature in (
        "per",
        "forward_per",
        "pbr",
        "market_cap_bil_jpy",
        "liquidity_residual_z",
    ):
        enriched[f"{feature}_bucket"] = enriched[feature].map(_bucket_for_feature(feature))
    enriched["overheat_state_bucket"] = enriched["overheat_state"].fillna("missing").astype(str)
    return enriched


def _build_statement_lookup(
    statement_df: pd.DataFrame,
) -> dict[str, tuple[np.ndarray, list[dict[str, Any]]]]:
    lookup: dict[str, tuple[np.ndarray, list[dict[str, Any]]]] = {}
    if statement_df.empty:
        return lookup
    for code, frame in statement_df.sort_values(["code", "disclosed_date"], kind="stable").groupby(
        "code", sort=False
    ):
        rows = cast(list[dict[str, Any]], frame.to_dict("records"))
        dates = frame["disclosed_date"].astype(str).to_numpy()
        lookup[str(code)] = (dates, rows)
    return lookup


def _build_daily_valuation_lookup(
    daily_valuation_df: pd.DataFrame,
) -> dict[tuple[str, str], dict[str, Any]]:
    if daily_valuation_df.empty:
        return {}
    return {
        (str(row["code"]), str(row["pre_event_date"])): cast(dict[str, Any], row)
        for row in daily_valuation_df.to_dict("records")
    }


def _lookup_statements_as_of(
    lookup: dict[str, tuple[np.ndarray, list[dict[str, Any]]]],
    code: str,
    as_of_date: str,
) -> list[dict[str, Any]]:
    payload = lookup.get(code)
    if payload is None:
        return []
    dates, rows = payload
    idx = int(np.searchsorted(dates, as_of_date, side="right"))
    if idx <= 0:
        return []
    return rows[:idx]


def _lookup_latest_actual_fy_statement(
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for row in reversed(rows):
        if is_actual_fy_financial_statement(
            row.get("type_of_current_period"),
            row.get("type_of_document"),
            allow_unknown_document=True,
        ):
            return row
    return None


def _resolve_baseline_shares(rows: list[dict[str, Any]]) -> float | None:
    return resolve_latest_quarterly_baseline_shares(
        (
            row.get("type_of_current_period"),
            row.get("disclosed_date"),
            _nullable_float(row.get("shares_outstanding")),
        )
        for row in rows
    )


def _adjust_statement_per_share_metric(
    row: dict[str, Any] | None,
    metric_column: str,
    baseline_shares: float | None,
) -> float | None:
    if row is None:
        return None
    adjusted_column = {
        "earnings_per_share": "adjusted_eps",
        "bps": "adjusted_bps",
        "forecast_eps": "adjusted_forecast_eps",
        "next_year_forecast_earnings_per_share": "adjusted_forecast_eps",
    }.get(metric_column)
    if adjusted_column:
        adjusted = _nullable_float(row.get(adjusted_column))
        if adjusted is not None:
            return adjusted
    return adjust_per_share_value(
        _nullable_float(row.get(metric_column)),
        _nullable_float(row.get("shares_outstanding")),
        baseline_shares,
    )


def _resolve_forward_eps_for_valuation(
    rows: list[dict[str, Any]],
    *,
    latest_fy: dict[str, Any] | None,
    baseline_shares: float | None,
) -> tuple[float | None, str | None, str | None, str | None]:
    if latest_fy is None:
        return None, None, None, None

    fy_disclosed_date = str(latest_fy.get("disclosed_date") or "")
    for row in reversed(rows):
        disclosed_date = str(row.get("disclosed_date") or "")
        if disclosed_date <= fy_disclosed_date:
            break
        period_type = normalize_period_type(row.get("type_of_current_period"))
        if period_type not in {"1Q", "2Q", "3Q"} and not is_earn_forecast_revision_document(
            row.get("type_of_document")
        ):
            continue
        adjusted_forward_eps = _nullable_float(row.get("adjusted_forecast_eps"))
        if adjusted_forward_eps is not None:
            return adjusted_forward_eps, disclosed_date, period_type, "revised"
        raw_forward_eps = _first_positive_float(
            row.get("forecast_eps"),
            row.get("next_year_forecast_earnings_per_share"),
        )
        adjusted = adjust_per_share_value(
            raw_forward_eps,
            _nullable_float(row.get("shares_outstanding")),
            baseline_shares,
        )
        if adjusted is not None:
            return adjusted, disclosed_date, period_type, "revised"

    adjusted_fy = _nullable_float(latest_fy.get("adjusted_forecast_eps"))
    if adjusted_fy is not None:
        return adjusted_fy, fy_disclosed_date, "FY", "fy"
    raw_fy_forward_eps = _first_positive_float(
        latest_fy.get("next_year_forecast_earnings_per_share"),
        latest_fy.get("forecast_eps"),
    )
    adjusted_fy = adjust_per_share_value(
        raw_fy_forward_eps,
        _nullable_float(latest_fy.get("shares_outstanding")),
        baseline_shares,
    )
    return (
        adjusted_fy,
        fy_disclosed_date if adjusted_fy is not None else None,
        "FY" if adjusted_fy is not None else None,
        "fy" if adjusted_fy is not None else None,
    )


def _first_positive_float(*values: object) -> float | None:
    for value in values:
        numeric = _nullable_float(value)
        if numeric is not None and numeric > 0:
            return numeric
    return None


def _nullable_float(value: object) -> float | None:
    try:
        number = float(cast(Any, value))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _nan_if_none(value: float | None) -> float:
    return value if value is not None else np.nan


def _build_coverage_diagnostics_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (market_scope, is_fy), frame in scoped_df.groupby(["market_scope", "is_fy"], sort=False):
        rows.append(
            {
                "market_scope": market_scope,
                "is_fy": bool(is_fy),
                "event_count": int(len(frame)),
                "eligible_count": int((frame["eps120_target_eligible"] == True).sum()),  # noqa: E712
                "target_count": int((frame["eps120_positive_target"] == True).sum()),  # noqa: E712
                "target_rate_pct": _rate_pct(frame["eps120_positive_target"]),
                "eligible_target_rate_pct": _rate_pct(
                    frame.loc[frame["eps120_target_eligible"] == True, "eps120_positive_target"]  # noqa: E712
                ),
                "per_coverage_pct": _nonnull_pct(frame["per"]),
                "forward_per_coverage_pct": _nonnull_pct(frame["forward_per"]),
                "pbr_coverage_pct": _nonnull_pct(frame["pbr"]),
                "market_cap_coverage_pct": _nonnull_pct(frame["market_cap_bil_jpy"]),
                "daily_valuation_source_pct": float(
                    (frame["valuation_source"].astype(str) == "daily_valuation").mean()
                    * 100.0
                ),
                "overheat_count": int(
                    (frame["overheat_state"].astype(str) == OVERHEAT_STATE).sum()
                ),
                "liquidity_residual_z_coverage_pct": _nonnull_pct(
                    frame["liquidity_residual_z"]
                ),
            }
        )
    return _sort_summary_df(
        pd.DataFrame(rows),
        columns=[
            "market_scope",
            "is_fy",
            "event_count",
            "eligible_count",
            "target_count",
            "target_rate_pct",
            "eligible_target_rate_pct",
            "per_coverage_pct",
            "forward_per_coverage_pct",
            "pbr_coverage_pct",
            "market_cap_coverage_pct",
            "daily_valuation_source_pct",
            "overheat_count",
            "liquidity_residual_z_coverage_pct",
        ],
    )


def _build_feature_bucket_df(scoped_df: pd.DataFrame, *, min_events: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    features = (
        "per",
        "forward_per",
        "pbr",
        "market_cap_bil_jpy",
        "liquidity_residual_z",
        "overheat_state",
    )
    for (market_scope, is_fy), group in scoped_df.groupby(["market_scope", "is_fy"], sort=False):
        base_rate = _rate_pct(group["eps120_positive_target"])
        for feature in features:
            bucket_column = f"{feature}_bucket"
            for bucket, frame in group.groupby(bucket_column, sort=False, dropna=False):
                if len(frame) < min_events:
                    continue
                rows.append(
                    {
                        "market_scope": market_scope,
                        "is_fy": bool(is_fy),
                        "feature": feature,
                        "bucket": str(bucket),
                        "bucket_order": _bucket_order(feature, str(bucket)),
                        **_target_summary(frame, base_rate),
                    }
                )
    return _sort_summary_df(
        pd.DataFrame(rows),
        columns=[
            "market_scope",
            "is_fy",
            "feature",
            "bucket",
            "bucket_order",
            *_target_summary_columns(),
        ],
    )


def _build_threshold_grid_df(scoped_df: pd.DataFrame, *, min_events: int) -> pd.DataFrame:
    specs: list[tuple[str, Callable[[pd.DataFrame], pd.Series]]] = [
        ("per_gt0_le10", lambda df: (df["per"] > 0) & (df["per"] <= 10)),
        ("per_gt0_le15", lambda df: (df["per"] > 0) & (df["per"] <= 15)),
        ("per_gt0_le20", lambda df: (df["per"] > 0) & (df["per"] <= 20)),
        ("per_ge20", lambda df: df["per"] >= 20),
        ("per_ge30", lambda df: df["per"] >= 30),
        ("forward_per_gt0_le10", lambda df: (df["forward_per"] > 0) & (df["forward_per"] <= 10)),
        ("forward_per_gt0_le15", lambda df: (df["forward_per"] > 0) & (df["forward_per"] <= 15)),
        ("forward_per_gt0_le20", lambda df: (df["forward_per"] > 0) & (df["forward_per"] <= 20)),
        ("forward_per_ge20", lambda df: df["forward_per"] >= 20),
        ("forward_per_ge30", lambda df: df["forward_per"] >= 30),
        ("pbr_gt0_le0.8", lambda df: (df["pbr"] > 0) & (df["pbr"] <= 0.8)),
        ("pbr_gt0_le1.0", lambda df: (df["pbr"] > 0) & (df["pbr"] <= 1.0)),
        ("pbr_gt0_le1.5", lambda df: (df["pbr"] > 0) & (df["pbr"] <= 1.5)),
        ("pbr_ge2.0", lambda df: df["pbr"] >= 2.0),
        ("market_cap_bil_le50", lambda df: (df["market_cap_bil_jpy"] > 0) & (df["market_cap_bil_jpy"] <= 50)),
        ("market_cap_bil_le100", lambda df: (df["market_cap_bil_jpy"] > 0) & (df["market_cap_bil_jpy"] <= 100)),
        ("market_cap_bil_le300", lambda df: (df["market_cap_bil_jpy"] > 0) & (df["market_cap_bil_jpy"] <= 300)),
        ("market_cap_bil_ge1000", lambda df: df["market_cap_bil_jpy"] >= 1000),
        ("liquidity_residual_z_ge1", lambda df: df["liquidity_residual_z"] >= 1.0),
        ("liquidity_residual_z_le_minus1", lambda df: df["liquidity_residual_z"] <= -1.0),
        ("overheat_20d_ge30", lambda df: df["overheat_state"].astype(str).eq(OVERHEAT_STATE)),
    ]
    return _build_condition_grid(scoped_df, specs, min_events=min_events, grid_name="threshold")


def _build_combo_grid_df(scoped_df: pd.DataFrame, *, min_events: int) -> pd.DataFrame:
    specs: list[tuple[str, Callable[[pd.DataFrame], pd.Series]]] = [
        (
            "low_forward_per15_and_low_pbr1.5",
            lambda df: (df["forward_per"] > 0) & (df["forward_per"] <= 15) & (df["pbr"] > 0) & (df["pbr"] <= 1.5),
        ),
        (
            "low_forward_per15_and_mcap_le300",
            lambda df: (df["forward_per"] > 0) & (df["forward_per"] <= 15) & (df["market_cap_bil_jpy"] > 0) & (df["market_cap_bil_jpy"] <= 300),
        ),
        (
            "low_pbr1.0_and_mcap_le300",
            lambda df: (df["pbr"] > 0) & (df["pbr"] <= 1.0) & (df["market_cap_bil_jpy"] > 0) & (df["market_cap_bil_jpy"] <= 300),
        ),
        (
            "low_forward_per15_low_pbr1.5_mcap_le300",
            lambda df: (df["forward_per"] > 0) & (df["forward_per"] <= 15) & (df["pbr"] > 0) & (df["pbr"] <= 1.5) & (df["market_cap_bil_jpy"] > 0) & (df["market_cap_bil_jpy"] <= 300),
        ),
        (
            "low_forward_per15_and_high_liquidity_z",
            lambda df: (df["forward_per"] > 0)
            & (df["forward_per"] <= 15)
            & (df["liquidity_residual_z"] >= 1.0),
        ),
        (
            "low_forward_per15_and_stale_liquidity",
            lambda df: (df["forward_per"] > 0)
            & (df["forward_per"] <= 15)
            & (df["liquidity_residual_z"] <= -1.0),
        ),
        (
            "low_forward_per15_and_not_overheat",
            lambda df: (df["forward_per"] > 0)
            & (df["forward_per"] <= 15)
            & df["overheat_state"].astype(str).ne(OVERHEAT_STATE),
        ),
    ]
    return _build_condition_grid(scoped_df, specs, min_events=min_events, grid_name="combo")


def _build_annual_valuation_regime_df(event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market",
        "is_fy",
        "event_year",
        "condition_scope",
        "liquidity_regime",
        "overheat_state",
        "forward_per_bucket",
        "bucket_order",
        *_target_summary_columns(),
        "regime_year_event_count",
        "bucket_share_within_regime_year_pct",
    ]
    if event_df.empty:
        return pd.DataFrame(columns=columns)

    frame = event_df.copy()
    frame["event_year"] = frame["pre_event_date"].astype(str).str.slice(0, 4)
    frame = frame[frame["event_year"].str.match(r"^\d{4}$", na=False)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)

    scope_frames = []
    all_events = frame.copy()
    all_events["condition_scope"] = "all_events"
    scope_frames.append(all_events)
    ex_strong_20d = frame[
        frame["pre_return_20d_bucket"].astype(str).ne("strong_runup")
    ].copy()
    ex_strong_20d["condition_scope"] = "ex_20d_strong_runup"
    scope_frames.append(ex_strong_20d)
    scoped = pd.concat(scope_frames, ignore_index=True)

    denominator = (
        scoped.groupby(
            [
                "market",
                "is_fy",
                "event_year",
                "condition_scope",
                "liquidity_regime",
                "overheat_state",
            ],
            dropna=False,
        )
        .size()
        .rename("regime_year_event_count")
        .reset_index()
    )

    rows: list[dict[str, Any]] = []
    group_columns = [
        "market",
        "is_fy",
        "event_year",
        "condition_scope",
        "liquidity_regime",
        "overheat_state",
        "forward_per_bucket",
    ]
    for keys, group in scoped.groupby(group_columns, sort=False, dropna=False):
        key_values = dict(zip(group_columns, keys, strict=True))
        rows.append(
            {
                **key_values,
                "bucket_order": _bucket_order(
                    "forward_per",
                    str(key_values["forward_per_bucket"]),
                ),
                **_target_summary(group, _rate_pct(group["eps120_positive_target"])),
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)

    result = pd.DataFrame.from_records(rows)
    result = result.merge(
        denominator,
        on=[
            "market",
            "is_fy",
            "event_year",
            "condition_scope",
            "liquidity_regime",
            "overheat_state",
        ],
        how="left",
    )
    result["bucket_share_within_regime_year_pct"] = (
        result["event_count"]
        / result["regime_year_event_count"].where(
            result["regime_year_event_count"] > 0,
            np.nan,
        )
        * 100.0
    )
    return _sort_summary_df(
        result,
        columns=columns,
    )


def _build_current_cross_section_df(db_path: str) -> pd.DataFrame:
    columns = [
        "snapshot_date",
        "market_scope",
        "collection_scope",
        "diagnostic_status",
        "diagnostic_message",
        "liquidity_regime",
        "overheat_state",
        "forward_per_bucket",
        "bucket_order",
        "stock_count",
        "regime_stock_count",
        "bucket_share_within_regime_pct",
        "median_forward_per",
        "median_per",
        "median_pbr",
        "median_market_cap_bil_jpy",
        "median_liquidity_residual_z",
        "median_adv60_to_free_float_pct",
    ]
    try:
        from src.application.services.ranking_service import RankingService
        from src.infrastructure.db.market.market_reader import MarketDbReader

        reader = MarketDbReader(db_path, read_only=True)
        try:
            service = RankingService(reader)
            response = service.get_rankings(
                date=None,
                limit=0,
                markets="prime",
                lookback_days=1,
                include_valuation=True,
                forward_eps_disclosed_within_days=0,
            )
        finally:
            reader.close()
    except Exception as exc:  # pragma: no cover - exercised by small fixture DBs.
        return pd.DataFrame.from_records(
            [
                {
                    "snapshot_date": None,
                    "market_scope": "prime",
                    "collection_scope": "all_daily_ranking_collections",
                    "diagnostic_status": "error",
                    "diagnostic_message": str(exc),
                    "liquidity_regime": "missing",
                    "overheat_state": "missing",
                    "forward_per_bucket": "missing",
                    "bucket_order": _bucket_order("forward_per", "missing"),
                    "stock_count": 0,
                    "regime_stock_count": 0,
                    "bucket_share_within_regime_pct": np.nan,
                    "median_forward_per": np.nan,
                    "median_per": np.nan,
                    "median_pbr": np.nan,
                    "median_market_cap_bil_jpy": np.nan,
                    "median_liquidity_residual_z": np.nan,
                    "median_adv60_to_free_float_pct": np.nan,
                }
            ],
            columns=columns,
        )

    collection_items = {
        "trading_value": response.rankings.tradingValue,
        "gainers": response.rankings.gainers,
        "losers": response.rankings.losers,
        "period_high": response.rankings.periodHigh,
        "period_low": response.rankings.periodLow,
    }
    detail_rows: list[dict[str, Any]] = []
    unique_items: dict[str, Any] = {}
    for collection_scope, items in collection_items.items():
        for item in items:
            unique_items.setdefault(item.code, item)
            detail_rows.append(
                _current_cross_section_item_record(
                    item,
                    snapshot_date=response.date,
                    collection_scope=collection_scope,
                )
            )
    for item in unique_items.values():
        detail_rows.append(
            _current_cross_section_item_record(
                item,
                snapshot_date=response.date,
                collection_scope="all_daily_ranking_collections",
            )
        )
    if not detail_rows:
        return pd.DataFrame(columns=columns)
    detail_df = pd.DataFrame.from_records(detail_rows)
    return _summarize_current_cross_section_df(detail_df, columns=columns)


def _current_cross_section_item_record(
    item: Any,
    *,
    snapshot_date: str,
    collection_scope: str,
) -> dict[str, Any]:
    market_cap = _float_or_nan(getattr(item, "marketCap", None))
    return {
        "snapshot_date": snapshot_date,
        "market_scope": "prime",
        "collection_scope": collection_scope,
        "diagnostic_status": "ok",
        "diagnostic_message": None,
        "code": str(getattr(item, "code", "")),
        "liquidity_regime": getattr(item, "liquidityRegime", None) or "missing",
        "overheat_state": (
            OVERHEAT_STATE
            if OVERHEAT_STATE in (getattr(item, "riskFlags", None) or [])
            else "not_overheat"
        ),
        "forward_per": _float_or_nan(getattr(item, "forwardPer", None)),
        "forward_per_bucket": _bucket_forward_per(getattr(item, "forwardPer", None)),
        "per": _float_or_nan(getattr(item, "per", None)),
        "pbr": _float_or_nan(getattr(item, "pbr", None)),
        "market_cap_bil_jpy": market_cap / 1e9 if math.isfinite(market_cap) else np.nan,
        "liquidity_residual_z": _float_or_nan(getattr(item, "liquidityResidualZ", None)),
        "adv60_to_free_float_pct": _float_or_nan(
            getattr(item, "adv60ToFreeFloatPct", None)
        ),
    }


def _summarize_current_cross_section_df(
    detail_df: pd.DataFrame,
    *,
    columns: list[str],
) -> pd.DataFrame:
    denominator = (
        detail_df.groupby(
            [
                "snapshot_date",
                "market_scope",
                "collection_scope",
                "diagnostic_status",
                "diagnostic_message",
                "liquidity_regime",
                "overheat_state",
            ],
            dropna=False,
        )
        .size()
        .rename("regime_stock_count")
        .reset_index()
    )
    rows: list[dict[str, Any]] = []
    group_columns = [
        "snapshot_date",
        "market_scope",
        "collection_scope",
        "diagnostic_status",
        "diagnostic_message",
        "liquidity_regime",
        "overheat_state",
        "forward_per_bucket",
    ]
    for keys, group in detail_df.groupby(group_columns, sort=False, dropna=False):
        key_values = dict(zip(group_columns, keys, strict=True))
        rows.append(
            {
                **key_values,
                "bucket_order": _bucket_order(
                    "forward_per",
                    str(key_values["forward_per_bucket"]),
                ),
                "stock_count": int(len(group)),
                "median_forward_per": _median(group["forward_per"]),
                "median_per": _median(group["per"]),
                "median_pbr": _median(group["pbr"]),
                "median_market_cap_bil_jpy": _median(group["market_cap_bil_jpy"]),
                "median_liquidity_residual_z": _median(group["liquidity_residual_z"]),
                "median_adv60_to_free_float_pct": _median(
                    group["adv60_to_free_float_pct"]
                ),
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    result = pd.DataFrame.from_records(rows)
    result = result.merge(
        denominator,
        on=[
            "snapshot_date",
            "market_scope",
            "collection_scope",
            "diagnostic_status",
            "diagnostic_message",
            "liquidity_regime",
            "overheat_state",
        ],
        how="left",
    )
    result["bucket_share_within_regime_pct"] = (
        result["stock_count"]
        / result["regime_stock_count"].where(result["regime_stock_count"] > 0, np.nan)
        * 100.0
    )
    return _sort_summary_df(result, columns=columns)


def _build_condition_grid(
    scoped_df: pd.DataFrame,
    specs: list[tuple[str, Callable[[pd.DataFrame], pd.Series]]],
    *,
    min_events: int,
    grid_name: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (market_scope, is_fy), group in scoped_df.groupby(["market_scope", "is_fy"], sort=False):
        base_rate = _rate_pct(group["eps120_positive_target"])
        for condition, predicate in specs:
            mask = predicate(group).fillna(False)
            frame = group.loc[mask].copy()
            if len(frame) < min_events:
                continue
            rows.append(
                {
                    "market_scope": market_scope,
                    "is_fy": bool(is_fy),
                    "grid": grid_name,
                    "condition": condition,
                    **_target_summary(frame, base_rate),
                }
            )
    return _sort_summary_df(
        pd.DataFrame(rows),
        columns=[
            "market_scope",
            "is_fy",
            "grid",
            "condition",
            *_target_summary_columns(),
        ],
    )


def _target_summary(frame: pd.DataFrame, base_rate_pct: float) -> dict[str, Any]:
    target_rate = _rate_pct(frame["eps120_positive_target"])
    return {
        "event_count": int(len(frame)),
        "eligible_count": int((frame["eps120_target_eligible"] == True).sum()),  # noqa: E712
        "target_count": int((frame["eps120_positive_target"] == True).sum()),  # noqa: E712
        "target_rate_pct": target_rate,
        "eligible_target_rate_pct": _rate_pct(
            frame.loc[frame["eps120_target_eligible"] == True, "eps120_positive_target"]  # noqa: E712
        ),
        "base_target_rate_pct": base_rate_pct,
        "lift_vs_base": target_rate / base_rate_pct if base_rate_pct > 0 else np.nan,
        "median_per": _median(frame["per"]),
        "median_forward_per": _median(frame["forward_per"]),
        "median_pbr": _median(frame["pbr"]),
        "median_market_cap_bil_jpy": _median(frame["market_cap_bil_jpy"]),
        "median_liquidity_residual_z": _median(frame["liquidity_residual_z"]),
    }


def _target_summary_columns() -> list[str]:
    return [
        "event_count",
        "eligible_count",
        "target_count",
        "target_rate_pct",
        "eligible_target_rate_pct",
        "base_target_rate_pct",
        "lift_vs_base",
        "median_per",
        "median_forward_per",
        "median_pbr",
        "median_market_cap_bil_jpy",
        "median_liquidity_residual_z",
    ]


def _expand_market_scope(event_df: pd.DataFrame) -> pd.DataFrame:
    if event_df.empty:
        frame = event_df.copy()
        frame["market_scope"] = pd.Series(dtype="object")
        return frame
    all_scope = event_df.copy()
    all_scope["market_scope"] = "all"
    actual = event_df.copy()
    actual["market_scope"] = actual["market"].astype(str)
    return pd.concat([all_scope, actual], ignore_index=True)


def _ratio(price: float, denominator: float) -> float:
    if not math.isfinite(price) or not math.isfinite(denominator) or denominator <= 0:
        return np.nan
    value = price / denominator
    return value if math.isfinite(value) and value > 0 else np.nan


def _rate_pct(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    return float((values == True).mean() * 100.0)  # noqa: E712


def _nonnull_pct(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)
    return float(numeric.notna().mean() * 100.0)


def _median(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(numeric.median()) if not numeric.empty else np.nan


def _bucket_for_feature(feature: str) -> Callable[[object], str]:
    if feature == "per":
        return _bucket_per
    if feature == "forward_per":
        return _bucket_forward_per
    if feature == "pbr":
        return _bucket_pbr
    if feature == "market_cap_bil_jpy":
        return _bucket_market_cap
    if feature == "liquidity_residual_z":
        return _bucket_liquidity_residual_z
    if feature == "overheat_state":
        return lambda value: str(value) if value is not None else "missing"
    raise ValueError(f"Unsupported feature: {feature}")


def _bucket_per(value: object) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric):
        return "missing"
    if numeric <= 0:
        return "non_positive"
    if numeric <= 10:
        return "le10"
    if numeric <= 15:
        return "10-15"
    if numeric <= 20:
        return "15-20"
    if numeric <= 30:
        return "20-30"
    return "gt30"


def _bucket_forward_per(value: object) -> str:
    return _bucket_per(value)


def _bucket_pbr(value: object) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric):
        return "missing"
    if numeric <= 0:
        return "non_positive"
    if numeric <= 0.8:
        return "le0.8"
    if numeric <= 1.0:
        return "0.8-1.0"
    if numeric <= 1.5:
        return "1.0-1.5"
    if numeric <= 2.0:
        return "1.5-2.0"
    return "gt2.0"


def _bucket_market_cap(value: object) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric) or numeric <= 0:
        return "missing"
    if numeric <= 50:
        return "le50"
    if numeric <= 100:
        return "50-100"
    if numeric <= 300:
        return "100-300"
    if numeric <= 1000:
        return "300-1000"
    return "gt1000"


def _bucket_liquidity_residual_z(value: object) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric):
        return "missing"
    if numeric <= -1.0:
        return "low"
    if numeric >= 1.0:
        return "high"
    return "neutral"


def _bucket_order(feature: str, bucket: str) -> int:
    orders = {
        "per": ["non_positive", "le10", "10-15", "15-20", "20-30", "gt30", "missing"],
        "forward_per": ["non_positive", "le10", "10-15", "15-20", "20-30", "gt30", "missing"],
        "pbr": ["non_positive", "le0.8", "0.8-1.0", "1.0-1.5", "1.5-2.0", "gt2.0", "missing"],
        "market_cap_bil_jpy": ["le50", "50-100", "100-300", "300-1000", "gt1000", "missing"],
        "liquidity_residual_z": ["low", "neutral", "high", "missing"],
        "overheat_state": ["not_overheat", "overheat", "missing"],
    }
    return orders.get(feature, []).index(bucket) if bucket in orders.get(feature, []) else 999
