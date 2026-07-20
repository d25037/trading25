"""Fundamental ranking query helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from src.application.services.ranking_query_helpers import (
    build_market_filter,
    normalize_equity_code,
    normalized_code_sql,
    prefer_4digit_order_sql,
    positive_ratio,
    stocks_canonical_cte,
)
from src.application.services.ranking_response_items import finite_or_none, str_or_none
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    StatementRow,
    normalize_period_label,
)
from src.infrastructure.db.market.market_reader import MarketDbQueryable, MarketDbReader
from src.shared.utils.pit_guard import filter_records_as_of
from src.shared.utils.share_adjustment import (
    ShareCountSnapshot,
    resolve_latest_quarterly_share_snapshot,
)

FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)


@dataclass(frozen=True)
class ProviderWindow:
    code: str
    coverage_start: str
    coverage_end: str
    provider_as_of: str
    source_fingerprint: str


def resolve_provider_windows(
    reader: MarketDbQueryable,
    codes: Sequence[str],
    effective_market_date: str,
) -> dict[str, ProviderWindow]:
    """Resolve complete provider windows and fail closed on pending current metrics."""
    normalized_codes = sorted({normalize_equity_code(code) for code in codes if code})
    if not normalized_codes:
        return {}
    placeholders = ", ".join("?" for _ in normalized_codes)
    rows = reader.query(
        f"""
        SELECT code, coverage_start, coverage_end, provider_as_of, source_fingerprint
        FROM stock_provider_windows
        WHERE code IN ({placeholders})
        ORDER BY code
        """,
        tuple(normalized_codes),
    )
    by_code: dict[str, Any] = {}
    for row in rows:
        by_code[normalize_equity_code(row["code"])] = row

    pending_rows = reader.query(
        f"""
        SELECT code FROM current_basis_recompute_pending
        WHERE code IN ({placeholders})
        ORDER BY code
        """,
        tuple(normalized_codes),
    )
    if pending_rows:
        raise ValueError(
            "adjusted_metrics_pit current-basis recompute is pending for "
            + ", ".join(str(row["code"]) for row in pending_rows)
        )

    resolved: dict[str, ProviderWindow] = {}
    for code in normalized_codes:
        row = by_code.get(code)
        if row is None:
            raise ValueError(
                f"adjusted_metrics_pit unavailable for {code} on {effective_market_date}: "
                "provider window is missing"
            )
        coverage_start = str(row["coverage_start"])
        coverage_end = str(row["coverage_end"])
        if (
            coverage_start > effective_market_date
            or coverage_end < effective_market_date
            or not str(row["provider_as_of"] or "").strip()
            or not str(row["source_fingerprint"] or "").strip()
        ):
            raise ValueError(
                f"adjusted_metrics_pit unavailable for {code} on {effective_market_date}: "
                "provider window does not fully cover the target date"
            )
        resolved[code] = ProviderWindow(
            code=code,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            provider_as_of=str(row["provider_as_of"]),
            source_fingerprint=str(row["source_fingerprint"]),
        )
    return resolved


def _target_stock_codes(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[str]:
    market_clause, market_params = build_market_filter(market_codes)
    normalized = normalized_code_sql("s.code")
    rows = reader.query(
        f"""
        SELECT DISTINCT {normalized} AS code
        FROM stock_master_daily AS s
        WHERE s.date = ?{market_clause}
        ORDER BY code
        """,
        (date, *market_params),
    )
    return [str(row["code"]) for row in rows]


def provider_price_cte() -> str:
    """Canonical normalized provider-adjusted stock_data relation."""
    price_norm = normalized_code_sql("price.code")
    price_order = prefer_4digit_order_sql("price.code")
    return f"""
        provider_price AS (
            SELECT normalized_code, date, open, high, low, close, volume
            FROM (
                SELECT
                    {price_norm} AS normalized_code,
                    price.date, price.open, price.high, price.low, price.close, price.volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {price_norm}, price.date
                        ORDER BY {price_order}
                    ) AS rn
                FROM stock_data AS price
            )
            WHERE rn = 1
        )
        """


def table_exists(reader: MarketDbReader, table_name: str) -> bool:
    row = reader.query_one(
        """
        SELECT 1 AS exists
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        (table_name,),
    )
    return row is not None


def daily_valuation_column_exists(reader: MarketDbReader, column_name: str) -> bool:
    rows = reader.query("SELECT name FROM pragma_table_info('daily_valuation')")
    return column_name in {str(row["name"]) for row in rows}


def optional_daily_valuation_expr(
    reader: MarketDbReader,
    column_name: str,
    *,
    column_type: str = "DOUBLE",
) -> str:
    if daily_valuation_column_exists(reader, column_name):
        return column_name
    return f"CAST(NULL AS {column_type})"


def resolve_latest_stock_data_date(reader: MarketDbReader) -> str:
    if not table_exists(reader, "stock_data"):
        raise ValueError("No trading data available in database")
    row = reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
    if row is None or row["max_date"] is None:
        raise ValueError("No trading data available in database")
    return str(row["max_date"])


def load_fundamental_stock_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    market_clause, market_params = build_market_filter(market_codes)
    resolve_provider_windows(
        reader, _target_stock_codes(reader, date, market_codes), date
    )
    price_ctes = provider_price_cte()
    stocks_cte = stocks_canonical_cte()
    sql = f"""
        WITH
        {stocks_cte},
        {price_ctes}
        SELECT {FUNDAMENTAL_BASE_COLUMNS}
        FROM stocks_canonical s
        JOIN provider_price sd
            ON sd.normalized_code = s.normalized_code AND sd.date = ?
        WHERE 1 = 1{market_clause}
    """
    return reader.query(sql, (date, date, *market_params))


def load_adjusted_daily_valuation_frame(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> pd.DataFrame:
    if not table_exists(reader, "daily_valuation"):
        raise ValueError("adjusted_metrics_pit daily_valuation is unavailable")
    market_clause, market_params = build_market_filter(market_codes)
    resolve_provider_windows(
        reader,
        _target_stock_codes(reader, date, market_codes),
        date,
    )
    price_ctes = provider_price_cte()
    stocks_cte = stocks_canonical_cte()
    valuation_norm = normalized_code_sql("code")
    valuation_order = prefer_4digit_order_sql("code")
    sales_expr = optional_daily_valuation_expr(reader, "sales")
    forward_sales_expr = optional_daily_valuation_expr(reader, "forward_sales")
    psr_expr = optional_daily_valuation_expr(reader, "psr")
    forward_psr_expr = optional_daily_valuation_expr(reader, "forward_psr")
    forward_sales_disclosed_date_expr = optional_daily_valuation_expr(
        reader,
        "forward_sales_disclosed_date",
        column_type="TEXT",
    )
    forward_sales_source_expr = optional_daily_valuation_expr(
        reader,
        "forward_sales_source",
        column_type="TEXT",
    )
    sql = f"""
        WITH
        {stocks_cte},
        {price_ctes},
        valuation_canonical AS (
            SELECT
                normalized_code,
                date,
                price_basis_date,
                close,
                eps,
                bps,
                forward_eps,
                per,
                forward_per,
                sales,
                forward_sales,
                psr,
                forward_psr,
                p_op,
                forward_p_op,
                pbr,
                market_cap,
                free_float_market_cap,
                statement_disclosed_date,
                forward_eps_disclosed_date,
                forward_eps_source,
                forward_sales_disclosed_date,
                forward_sales_source,
                fundamentals_adjustment_basis_date,
                source_fingerprint
            FROM (
                SELECT
                    {valuation_norm} AS normalized_code,
                    date,
                    price_basis_date,
                    close,
                    eps,
                    bps,
                    forward_eps,
                    per,
                    forward_per,
                    {sales_expr} AS sales,
                    {forward_sales_expr} AS forward_sales,
                    {psr_expr} AS psr,
                    {forward_psr_expr} AS forward_psr,
                    p_op,
                    forward_p_op,
                    pbr,
                    market_cap,
                    free_float_market_cap,
                    statement_disclosed_date,
                    forward_eps_disclosed_date,
                    forward_eps_source,
                    {forward_sales_disclosed_date_expr} AS forward_sales_disclosed_date,
                    {forward_sales_source_expr} AS forward_sales_source,
                    fundamentals_adjustment_basis_date,
                    source_fingerprint,
                    ROW_NUMBER() OVER (
                        PARTITION BY {valuation_norm}, date
                        ORDER BY
                            {valuation_order}
                    ) AS rn
                FROM daily_valuation
                WHERE date = ?
            )
            WHERE rn = 1
        ),
        metric_snapshots AS (
            SELECT
                {normalized_code_sql("metric.code")} AS normalized_code,
                ARG_MAX(metric.adjusted_eps, metric.disclosed_at)
                    FILTER (WHERE metric.adjusted_eps IS NOT NULL) AS actual_eps,
                ARG_MAX(metric.adjusted_bps, metric.disclosed_at)
                    FILTER (WHERE metric.adjusted_bps IS NOT NULL) AS actual_bps,
                ARG_MAX(metric.disclosed_date, metric.disclosed_at)
                    FILTER (WHERE metric.adjusted_eps IS NOT NULL) AS actual_disclosed_date,
                ARG_MAX(metric.adjusted_forecast_eps, metric.disclosed_at)
                    FILTER (WHERE metric.adjusted_forecast_eps IS NOT NULL) AS forecast_eps,
                ARG_MAX(metric.disclosed_date, metric.disclosed_at)
                    FILTER (WHERE metric.adjusted_forecast_eps IS NOT NULL) AS forecast_disclosed_date,
                ARG_MAX(metric.period_type, metric.disclosed_at)
                    FILTER (WHERE metric.adjusted_forecast_eps IS NOT NULL) AS forecast_period_type
            FROM statement_metrics_adjusted AS metric
            JOIN stock_provider_windows AS metric_provider
              ON metric_provider.code = {normalized_code_sql("metric.code")}
             AND metric_provider.coverage_end = metric.fundamentals_adjustment_basis_date
            WHERE metric.disclosed_date <= ?
            GROUP BY normalized_code
        )
        SELECT
            s.code,
            s.company_name,
            s.market_code,
            s.sector_33_name,
            bp.close AS current_price,
            bp.volume,
            COALESCE(v.eps, metric.actual_eps) AS eps,
            COALESCE(v.bps, metric.actual_bps) AS bps,
            COALESCE(v.forward_eps, metric.forecast_eps) AS forward_eps,
            COALESCE(
                v.per,
                bp.close / NULLIF(metric.actual_eps, 0)
            ) AS per,
            COALESCE(
                v.forward_per,
                bp.close / NULLIF(metric.forecast_eps, 0)
            ) AS forward_per,
            v.sales,
            v.forward_sales,
            v.psr,
            v.forward_psr,
            v.p_op,
            v.forward_p_op,
            COALESCE(
                v.pbr,
                bp.close / NULLIF(metric.actual_bps, 0)
            ) AS pbr,
            v.market_cap,
            v.free_float_market_cap,
            CASE
                WHEN v.eps IS NOT NULL THEN v.statement_disclosed_date
                ELSE metric.actual_disclosed_date
            END AS statement_disclosed_date,
            COALESCE(v.forward_eps_disclosed_date, metric.forecast_disclosed_date)
                AS forward_eps_disclosed_date,
            CASE
                WHEN COALESCE(v.forward_eps, metric.forecast_eps) IS NULL THEN NULL
                WHEN v.forward_eps_source = 'revised' THEN 'revised'
                WHEN upper(COALESCE(metric.forecast_period_type, 'FY')) != 'FY'
                    THEN 'revised'
                ELSE 'fy'
            END AS forward_eps_source,
            v.forward_sales_disclosed_date,
            v.forward_sales_source,
            v.price_basis_date,
            v.fundamentals_adjustment_basis_date,
            provider.provider_as_of
        FROM valuation_canonical v
        JOIN stocks_canonical s
            ON s.normalized_code = v.normalized_code
        JOIN provider_price bp
            ON bp.normalized_code = v.normalized_code AND bp.date = v.date
        JOIN stock_provider_windows provider
            ON provider.code = v.normalized_code
        LEFT JOIN metric_snapshots metric
            ON metric.normalized_code = v.normalized_code
        WHERE 1 = 1{market_clause}
    """
    rows = reader.query(sql, (date, date, date, *market_params))
    return pd.DataFrame([dict(row.items()) for row in rows])


def load_adjusted_statement_metric_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    if not table_exists(reader, "statement_metrics_adjusted"):
        raise ValueError("adjusted_metrics_pit statement metrics are unavailable")
    market_clause, market_params = build_market_filter(market_codes)
    resolve_provider_windows(
        reader,
        _target_stock_codes(reader, date, market_codes),
        date,
    )
    price_ctes = provider_price_cte()
    stocks_cte = stocks_canonical_cte()
    metrics_norm = normalized_code_sql("code")
    metrics_order = prefer_4digit_order_sql("code")
    sql = f"""
        WITH
        {stocks_cte},
        {price_ctes},
        metrics_canonical AS (
            SELECT
                normalized_code,
                disclosed_date,
                period_end,
                period_type,
                adjusted_eps,
                adjusted_bps,
                adjusted_forecast_eps,
                fundamentals_adjustment_basis_date
            FROM (
                SELECT
                    {metrics_norm} AS normalized_code,
                    disclosed_date,
                    period_end,
                    period_type,
                    adjusted_eps,
                    adjusted_bps,
                    adjusted_forecast_eps,
                    fundamentals_adjustment_basis_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY {metrics_norm}, disclosed_date, period_end, period_type
                        ORDER BY
                            {metrics_order}
                    ) AS rn
                FROM statement_metrics_adjusted
                WHERE disclosed_date <= ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            m.disclosed_date,
            m.period_end,
            m.period_type,
            m.adjusted_eps,
            m.adjusted_bps,
            m.adjusted_forecast_eps,
            m.fundamentals_adjustment_basis_date
        FROM metrics_canonical m
        JOIN stocks_canonical s
            ON s.normalized_code = m.normalized_code
        JOIN provider_price bp
            ON bp.normalized_code = m.normalized_code AND bp.date = ?
        WHERE 1 = 1{market_clause}
        ORDER BY s.code, m.disclosed_date DESC
    """
    return reader.query(sql, (date, date, date, *market_params))


def adjusted_recent_actual_eps_max_by_code(
    reader: MarketDbReader,
    *,
    target_date: str,
    market_codes: list[str],
    lookback_fy_count: int,
) -> dict[str, float | None]:
    rows = load_adjusted_statement_metric_rows(reader, target_date, market_codes)
    values_by_code: dict[str, list[float]] = {}
    seen_by_code: dict[str, set[str]] = {}
    for row in rows:
        period_type = normalize_period_label(str_or_none(row["period_type"]))
        if period_type != "FY":
            continue
        code = str(row["code"])
        disclosed_date = str(row["disclosed_date"])
        seen = seen_by_code.setdefault(code, set())
        if disclosed_date in seen:
            continue
        eps = finite_or_none(row["adjusted_eps"])
        if eps is None:
            continue
        seen.add(disclosed_date)
        bucket = values_by_code.setdefault(code, [])
        if len(bucket) < lookback_fy_count:
            bucket.append(eps)
    return {
        code: max(values) if len(values) >= lookback_fy_count else None
        for code, values in values_by_code.items()
    }


def build_adjusted_fundamental_ratio_candidates(
    reader: MarketDbReader,
    adjusted_valuation: pd.DataFrame,
    *,
    target_date: str,
    market_codes: list[str],
    forecast_above_recent_fy_actuals: bool,
    forecast_lookback_fy_count: int,
) -> list[FundamentalItem]:
    recent_actual_max_by_code: dict[str, float | None] = {}
    if forecast_above_recent_fy_actuals:
        recent_actual_max_by_code = adjusted_recent_actual_eps_max_by_code(
            reader,
            target_date=target_date,
            market_codes=market_codes,
            lookback_fy_count=forecast_lookback_fy_count,
        )

    candidates: list[FundamentalItem] = []
    for row in adjusted_valuation.to_dict(orient="records"):
        eps = finite_or_none(row.get("eps"))
        forward_eps = finite_or_none(row.get("forward_eps"))
        ratio = positive_ratio(forward_eps, eps)
        if ratio is None:
            continue
        code = str(row["code"])
        if forecast_above_recent_fy_actuals:
            recent_max = recent_actual_max_by_code.get(code)
            if recent_max is None or forward_eps is None or forward_eps <= recent_max:
                continue
        source_raw = str_or_none(row.get("forward_eps_source"))
        source: Literal["revised", "fy"] = "revised" if source_raw == "revised" else "fy"
        candidates.append(
            FundamentalItem(
                code=code,
                company_name=str(row["company_name"]),
                market_code=str(row["market_code"]),
                sector_33_name=str(row["sector_33_name"]),
                current_price=float(row["current_price"]),
                volume=float(row["volume"]),
                eps_value=round(ratio, 4),
                disclosed_date=(
                    str_or_none(row.get("forward_eps_disclosed_date"))
                    or str_or_none(row.get("statement_disclosed_date"))
                    or target_date
                ),
                period_type="FY",
                source=source,
            )
        )
    return candidates


def resolve_baseline_share_snapshot(
    rows: list[StatementRow],
    *,
    as_of_date: str | None = None,
) -> ShareCountSnapshot | None:
    eligible_rows = (
        filter_records_as_of(
            rows,
            as_of_date=as_of_date,
            date_getter=lambda row: row.disclosed_date,
        )
        if as_of_date is not None
        else list(rows)
    )
    snapshots = [
        (row.period_type, row.disclosed_date, row.shares_outstanding)
        for row in eligible_rows
    ]
    return resolve_latest_quarterly_share_snapshot(snapshots)
