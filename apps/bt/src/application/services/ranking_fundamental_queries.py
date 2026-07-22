"""Fundamental ranking query helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Literal

import pandas as pd

from src.application.services.ranking_query_helpers import (
    build_market_filter,
    normalized_code_sql,
    prefer_4digit_order_sql,
    stocks_canonical_cte,
)
from src.application.services.ranking_response_items import finite_or_none, str_or_none
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    StatementRow,
    normalize_period_label,
)
from src.domains.fundamentals import build_eps_metric_snapshot
from src.infrastructure.db.market.market_reader import MarketDbQueryable, MarketDbReader
from src.infrastructure.data_access.fundamentals_pit_reader import (
    resolve_market_session_date,
)
from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.infrastructure.data_access.current_basis_fundamentals import (
    CurrentBasisProviderWindow,
    resolve_current_basis_provider_windows,
)
from src.shared.utils.pit_guard import filter_records_as_of
from src.shared.utils.share_adjustment import (
    ShareCountSnapshot,
    resolve_latest_quarterly_share_snapshot,
)

FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)


def resolve_provider_windows(
    reader: MarketDbQueryable,
    codes: Sequence[str],
    effective_market_date: str,
) -> dict[str, CurrentBasisProviderWindow]:
    """Compatibility wrapper around the shared batch current-basis validator."""
    try:
        target = date.fromisoformat(effective_market_date)
        return resolve_current_basis_provider_windows(reader, codes, target)
    except (FundamentalsPitSnapshotError, ValueError) as exc:
        raise ValueError(str(exc)) from exc


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


def resolve_latest_fundamental_date(reader: MarketDbReader) -> str:
    _knowledge_cutoff, effective_market_date = resolve_market_session_date(reader)
    return effective_market_date.isoformat()


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
        raise ValueError("market_db_sync daily_valuation is unavailable")
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
        )
        SELECT
            s.code,
            s.company_name,
            s.market_code,
            s.sector_33_name,
            v.close AS current_price,
            bp.volume,
            v.eps,
            v.bps,
            v.forward_eps,
            v.per,
            v.forward_per,
            v.sales,
            v.forward_sales,
            v.psr,
            v.forward_psr,
            v.p_op,
            v.forward_p_op,
            v.pbr,
            v.market_cap,
            v.free_float_market_cap,
            v.statement_disclosed_date,
            v.forward_eps_disclosed_date,
            v.forward_eps_source,
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
        JOIN current_basis_fundamentals_state state
            ON state.code = v.normalized_code
           AND state.fundamentals_adjustment_basis_date =
               v.fundamentals_adjustment_basis_date
           AND state.source_fingerprint = v.source_fingerprint
        JOIN stock_provider_windows provider
            ON provider.code = v.normalized_code
           AND provider.coverage_end = state.fundamentals_adjustment_basis_date
        WHERE 1 = 1{market_clause}
    """
    rows = reader.query(sql, (date, date, *market_params))
    return pd.DataFrame([dict(row.items()) for row in rows])


def load_adjusted_statement_metric_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    if not table_exists(reader, "statement_metrics_adjusted"):
        raise ValueError("market_db_sync statement metrics are unavailable")
    market_clause, market_params = build_market_filter(market_codes)
    resolve_provider_windows(
        reader,
        _target_stock_codes(reader, date, market_codes),
        date,
    )
    price_ctes = provider_price_cte()
    stocks_cte = stocks_canonical_cte()
    metrics_norm = normalized_code_sql("metric.code")
    metrics_order = prefer_4digit_order_sql("metric.code")
    source_norm = normalized_code_sql("source.code")
    sql = f"""
        WITH
        {stocks_cte},
        {price_ctes},
        metrics_canonical AS (
            SELECT
                normalized_code,
                statement_id,
                disclosed_date,
                disclosed_at,
                period_end,
                period_type,
                adjusted_eps,
                adjusted_bps,
                adjusted_forecast_eps,
                fundamentals_adjustment_basis_date,
                source_fingerprint
            FROM (
                SELECT
                    {metrics_norm} AS normalized_code,
                    metric.statement_id,
                    metric.disclosed_date,
                    metric.disclosed_at,
                    metric.period_end,
                    metric.period_type,
                    metric.adjusted_eps,
                    metric.adjusted_bps,
                    metric.adjusted_forecast_eps,
                    metric.fundamentals_adjustment_basis_date,
                    metric.source_fingerprint,
                    ROW_NUMBER() OVER (
                        PARTITION BY {metrics_norm}, metric.statement_id
                        ORDER BY {metrics_order}
                    ) AS rn
                FROM statement_metrics_adjusted AS metric
                JOIN current_basis_fundamentals_state AS state
                  ON state.code = {metrics_norm}
                 AND state.fundamentals_adjustment_basis_date =
                     metric.fundamentals_adjustment_basis_date
                 AND state.source_fingerprint = metric.source_fingerprint
                JOIN stock_provider_windows AS provider
                  ON provider.code = state.code
                 AND provider.coverage_end =
                     state.fundamentals_adjustment_basis_date
                JOIN statements AS source
                  ON {source_norm} = {metrics_norm}
                 AND source.statement_id = metric.statement_id
                WHERE metric.disclosed_at <= ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            m.statement_id,
            m.disclosed_date,
            m.disclosed_at,
            m.period_end,
            m.period_type,
            m.adjusted_eps,
            m.adjusted_bps,
            m.adjusted_forecast_eps,
            m.fundamentals_adjustment_basis_date,
            m.source_fingerprint
        FROM metrics_canonical m
        JOIN stocks_canonical s
            ON s.normalized_code = m.normalized_code
        JOIN provider_price bp
            ON bp.normalized_code = m.normalized_code AND bp.date = ?
        WHERE 1 = 1{market_clause}
        ORDER BY s.code, m.disclosed_at DESC, m.statement_id
    """
    return reader.query(
        sql,
        (date, f"{date}T23:59:59.999999+09:00", date, *market_params),
    )


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
        eps_snapshot = build_eps_metric_snapshot(
            actual_eps=eps,
            forecast_eps=forward_eps,
        )
        ratio = eps_snapshot.forecast_to_actual_ratio
        change_rate = eps_snapshot.forecast_eps_change_rate
        if (
            ratio is None
            or change_rate is None
            or eps_snapshot.actual_eps is None
            or eps_snapshot.forecast_eps is None
        ):
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
                actual_eps=eps_snapshot.actual_eps,
                forecast_eps=eps_snapshot.forecast_eps,
                forecast_to_actual_ratio=ratio,
                forecast_eps_change_rate=change_rate,
                disclosed_date=(
                    str_or_none(row.get("forward_eps_disclosed_date"))
                    or str_or_none(row.get("statement_disclosed_date"))
                    or target_date
                ),
                actual_disclosed_date=(
                    str_or_none(row.get("statement_disclosed_date")) or target_date
                ),
                forecast_disclosed_date=(
                    str_or_none(row.get("forward_eps_disclosed_date")) or target_date
                ),
                period_type="FY",
                source=source,
                fundamentals_adjustment_basis_date=str_or_none(
                    row.get("fundamentals_adjustment_basis_date")
                ),
                provider_as_of=str_or_none(row.get("provider_as_of")),
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
