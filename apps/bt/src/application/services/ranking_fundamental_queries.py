"""Fundamental ranking query helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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
from src.domains.fundamentals.adjustment_basis import StockAdjustmentBasis
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


def resolve_ready_adjustment_bases(
    reader: MarketDbQueryable,
    codes: Sequence[str],
    effective_market_date: str,
) -> dict[str, StockAdjustmentBasis]:
    """Resolve exactly one ready, containing, fully covered basis per code."""
    normalized_codes = sorted({normalize_equity_code(code) for code in codes if code})
    if not normalized_codes:
        return {}
    placeholders = ", ".join("?" for _ in normalized_codes)
    rows = reader.query(
        f"""
        SELECT code, basis_id, valid_from, valid_to_exclusive,
               adjustment_through_date, source_fingerprint,
               materialized_through_date, status
        FROM stock_adjustment_bases
        WHERE code IN ({placeholders})
          AND valid_from <= ?
          AND (valid_to_exclusive IS NULL OR ? < valid_to_exclusive)
        ORDER BY code, valid_from
        """,
        (*normalized_codes, effective_market_date, effective_market_date),
    )
    by_code: dict[str, list[Any]] = {}
    for row in rows:
        by_code.setdefault(normalize_equity_code(row["code"]), []).append(row)

    resolved: dict[str, StockAdjustmentBasis] = {}
    for code in normalized_codes:
        candidates = by_code.get(code, [])
        if len(candidates) != 1:
            raise ValueError(
                f"adjusted_metrics_pit unavailable for {code} on {effective_market_date}: "
                f"expected one containing basis, found {len(candidates)}"
            )
        row = candidates[0]
        raw_materialized_through_date = row["materialized_through_date"]
        materialized_through_date = (
            str(raw_materialized_through_date)
            if raw_materialized_through_date is not None
            else ""
        )
        if row["status"] != "ready" or materialized_through_date < effective_market_date:
            raise ValueError(
                f"adjusted_metrics_pit unavailable for {code} on {effective_market_date}: "
                "basis is not ready and fully covered"
            )
        resolved[code] = StockAdjustmentBasis(
            code=code,
            basis_id=str(row["basis_id"]),
            valid_from=str(row["valid_from"]),
            valid_to_exclusive=(
                str(row["valid_to_exclusive"])
                if row["valid_to_exclusive"] is not None
                else None
            ),
            adjustment_through_date=str(row["adjustment_through_date"]),
            source_fingerprint=str(row["source_fingerprint"]),
            materialized_through_date=materialized_through_date,
            status="ready",
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


def _resolved_basis_values_sql(
    bases: Mapping[str, StockAdjustmentBasis],
) -> tuple[str, tuple[str, ...]]:
    if not bases:
        return "VALUES (CAST(NULL AS TEXT), CAST(NULL AS TEXT))", ()
    values = ", ".join("(?, ?)" for _ in bases)
    params = tuple(value for code, basis in bases.items() for value in (code, basis.basis_id))
    return f"VALUES {values}", params


def resolved_basis_price_ctes(
    bases: Mapping[str, StockAdjustmentBasis],
) -> tuple[str, tuple[str, ...]]:
    """Build exact-basis projected raw OHLCV CTEs for analytics queries."""
    basis_values_sql, basis_params = _resolved_basis_values_sql(bases)
    raw_norm = normalized_code_sql("raw.code")
    raw_order = prefer_4digit_order_sql("raw.code")
    return (
        f"""
        resolved_bases(normalized_code, basis_id) AS ({basis_values_sql}),
        normalized_raw AS (
            SELECT normalized_code, date, open, high, low, close, volume
            FROM (
                SELECT
                    {raw_norm} AS normalized_code,
                    raw.date, raw.open, raw.high, raw.low, raw.close, raw.volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {raw_norm}, raw.date
                        ORDER BY {raw_order}
                    ) AS rn
                FROM stock_data_raw AS raw
            )
            WHERE rn = 1
        ),
        basis_price AS (
            SELECT
                raw.normalized_code,
                raw.date,
                raw.open * segment.cumulative_factor AS open,
                raw.high * segment.cumulative_factor AS high,
                raw.low * segment.cumulative_factor AS low,
                raw.close * segment.cumulative_factor AS close,
                ROUND(raw.volume / segment.cumulative_factor) AS volume
            FROM normalized_raw AS raw
            JOIN resolved_bases AS basis USING (normalized_code)
            JOIN stock_adjustment_basis_segments AS segment
              ON segment.code = basis.normalized_code
             AND segment.basis_id = basis.basis_id
             AND raw.date >= segment.source_date_from
             AND (
                 segment.source_date_to_exclusive IS NULL
                 OR raw.date < segment.source_date_to_exclusive
             )
        )
        """,
        basis_params,
    )


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
    if not table_exists(reader, "stock_data_raw"):
        raise ValueError("No trading data available in database")
    row = reader.query_one("SELECT MAX(date) as max_date FROM stock_data_raw")
    if row is None or row["max_date"] is None:
        raise ValueError("No trading data available in database")
    return str(row["max_date"])


def load_fundamental_stock_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    market_clause, market_params = build_market_filter(market_codes)
    bases = resolve_ready_adjustment_bases(
        reader, _target_stock_codes(reader, date, market_codes), date
    )
    price_ctes, basis_params = resolved_basis_price_ctes(bases)
    stocks_cte = stocks_canonical_cte()
    sql = f"""
        WITH
        {stocks_cte},
        {price_ctes}
        SELECT {FUNDAMENTAL_BASE_COLUMNS}
        FROM stocks_canonical s
        JOIN basis_price sd
            ON sd.normalized_code = s.normalized_code AND sd.date = ?
        WHERE 1 = 1{market_clause}
    """
    return reader.query(sql, (date, *basis_params, date, *market_params))


def load_adjusted_daily_valuation_frame(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> pd.DataFrame:
    if not table_exists(reader, "daily_valuation"):
        raise ValueError("adjusted_metrics_pit daily_valuation is unavailable")
    market_clause, market_params = build_market_filter(market_codes)
    bases = resolve_ready_adjustment_bases(
        reader,
        _target_stock_codes(reader, date, market_codes),
        date,
    )
    price_ctes, basis_params = resolved_basis_price_ctes(bases)
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
                basis_version
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
                    basis_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY {valuation_norm}, date
                        ORDER BY
                            price_basis_date DESC NULLS LAST,
                            basis_version DESC,
                            {valuation_order}
                    ) AS rn
                FROM daily_valuation
                JOIN resolved_bases
                  ON resolved_bases.normalized_code = {valuation_norm}
                 AND resolved_bases.basis_id = daily_valuation.basis_version
                WHERE date = ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            s.company_name,
            s.market_code,
            s.sector_33_name,
            bp.close AS current_price,
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
            v.basis_version
        FROM valuation_canonical v
        JOIN stocks_canonical s
            ON s.normalized_code = v.normalized_code
        JOIN basis_price bp
            ON bp.normalized_code = v.normalized_code AND bp.date = v.date
        WHERE 1 = 1{market_clause}
    """
    rows = reader.query(sql, (date, *basis_params, date, *market_params))
    return pd.DataFrame([dict(row.items()) for row in rows])


def load_adjusted_statement_metric_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    if not table_exists(reader, "statement_metrics_adjusted"):
        raise ValueError("adjusted_metrics_pit statement metrics are unavailable")
    market_clause, market_params = build_market_filter(market_codes)
    bases = resolve_ready_adjustment_bases(
        reader,
        _target_stock_codes(reader, date, market_codes),
        date,
    )
    price_ctes, basis_params = resolved_basis_price_ctes(bases)
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
                basis_version
            FROM (
                SELECT
                    {metrics_norm} AS normalized_code,
                    disclosed_date,
                    period_end,
                    period_type,
                    adjusted_eps,
                    adjusted_bps,
                    adjusted_forecast_eps,
                    basis_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY {metrics_norm}, disclosed_date, period_end, period_type
                        ORDER BY
                            price_basis_date DESC NULLS LAST,
                            basis_version DESC,
                            {metrics_order}
                    ) AS rn
                FROM statement_metrics_adjusted
                JOIN resolved_bases
                  ON resolved_bases.normalized_code = {metrics_norm}
                 AND resolved_bases.basis_id = statement_metrics_adjusted.basis_version
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
            m.basis_version
        FROM metrics_canonical m
        JOIN stocks_canonical s
            ON s.normalized_code = m.normalized_code
        JOIN basis_price bp
            ON bp.normalized_code = m.normalized_code AND bp.date = ?
        WHERE 1 = 1{market_clause}
        ORDER BY s.code, m.disclosed_date DESC
    """
    return reader.query(sql, (date, *basis_params, date, date, *market_params))


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
