"""Pre-disclosure valuation proxy research for EPS 1.2x positive events."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast

import numpy as np
import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
    _float_or_nan,
    _sort_summary_df,
    _str_or_none,
    _table_exists,
    _top_rows_for_markdown,
)
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
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    event_feature_df = _enrich_events_with_pre_valuation(
        base_result.event_feature_df,
        statement_df,
    )
    scoped_df = _expand_market_scope(event_feature_df)
    coverage_diagnostics_df = _build_coverage_diagnostics_df(scoped_df)
    feature_bucket_df = _build_feature_bucket_df(scoped_df, min_events=min_events)
    threshold_grid_df = _build_threshold_grid_df(scoped_df, min_events=min_events)
    combo_grid_df = _build_combo_grid_df(scoped_df, min_events=min_events)

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


def _enrich_events_with_pre_valuation(
    event_df: pd.DataFrame,
    statement_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_df.empty:
        return event_df.copy()
    current_statement = statement_df[
        [
            "code",
            "disclosed_date",
            "earnings_per_share",
            "next_year_forecast_earnings_per_share",
        ]
    ].rename(
        columns={
            "earnings_per_share": "actual_eps",
            "next_year_forecast_earnings_per_share": "next_forecast_eps",
        }
    )
    enriched = event_df.merge(current_statement, on=["code", "disclosed_date"], how="left")

    fy_rows = statement_df[
        statement_df["type_of_current_period"].astype(str).str.upper().eq("FY")
    ].copy()
    fy_lookup = _build_statement_lookup(fy_rows)
    forward_rows = statement_df.copy()
    forward_rows["forward_eps_candidate"] = forward_rows[
        "next_year_forecast_earnings_per_share"
    ].combine_first(forward_rows["forecast_eps"])
    forward_lookup = _build_statement_lookup(
        forward_rows[pd.to_numeric(forward_rows["forward_eps_candidate"], errors="coerce") > 0]
    )

    valuation_records: list[dict[str, float | str | None]] = []
    for row in enriched.itertuples(index=False):
        code = str(row.code)
        pre_event_date = str(row.pre_event_date)
        latest_fy = _lookup_latest_statement(fy_lookup, code, pre_event_date)
        latest_forward = _lookup_latest_statement(forward_lookup, code, pre_event_date)
        close = _float_or_nan(row.pre_event_close)
        valuation_actual_eps = _float_or_nan(latest_fy.get("earnings_per_share")) if latest_fy else np.nan
        valuation_bps = _float_or_nan(latest_fy.get("bps")) if latest_fy else np.nan
        valuation_shares = _float_or_nan(latest_fy.get("shares_outstanding")) if latest_fy else np.nan
        valuation_forward_eps = (
            _float_or_nan(latest_forward.get("forward_eps_candidate"))
            if latest_forward
            else np.nan
        )
        valuation_records.append(
            {
                "valuation_actual_eps": valuation_actual_eps,
                "valuation_forward_eps": valuation_forward_eps,
                "valuation_bps": valuation_bps,
                "valuation_shares_outstanding": valuation_shares,
                "valuation_fy_disclosed_date": latest_fy.get("disclosed_date") if latest_fy else None,
                "valuation_forward_eps_disclosed_date": (
                    latest_forward.get("disclosed_date") if latest_forward else None
                ),
                "per": _ratio(close, valuation_actual_eps),
                "forward_per": _ratio(close, valuation_forward_eps),
                "pbr": _ratio(close, valuation_bps),
                "market_cap_bil_jpy": (
                    close * valuation_shares / 1e9
                    if math.isfinite(close) and math.isfinite(valuation_shares) and valuation_shares > 0
                    else np.nan
                ),
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
    for feature in ("per", "forward_per", "pbr", "market_cap_bil_jpy"):
        enriched[f"{feature}_bucket"] = enriched[feature].map(_bucket_for_feature(feature))
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


def _lookup_latest_statement(
    lookup: dict[str, tuple[np.ndarray, list[dict[str, Any]]]],
    code: str,
    as_of_date: str,
) -> dict[str, Any] | None:
    payload = lookup.get(code)
    if payload is None:
        return None
    dates, rows = payload
    idx = int(np.searchsorted(dates, as_of_date, side="right")) - 1
    if idx < 0:
        return None
    return rows[idx]


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
        ],
    )


def _build_feature_bucket_df(scoped_df: pd.DataFrame, *, min_events: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    features = ("per", "forward_per", "pbr", "market_cap_bil_jpy")
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
    ]
    return _build_condition_grid(scoped_df, specs, min_events=min_events, grid_name="combo")


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


def _bucket_order(feature: str, bucket: str) -> int:
    orders = {
        "per": ["non_positive", "le10", "10-15", "15-20", "20-30", "gt30", "missing"],
        "forward_per": ["non_positive", "le10", "10-15", "15-20", "20-30", "gt30", "missing"],
        "pbr": ["non_positive", "le0.8", "0.8-1.0", "1.0-1.5", "1.5-2.0", "gt2.0", "missing"],
        "market_cap_bil_jpy": ["le50", "50-100", "100-300", "300-1000", "gt1000", "missing"],
    }
    return orders.get(feature, []).index(bucket) if bucket in orders.get(feature, []) else 999
