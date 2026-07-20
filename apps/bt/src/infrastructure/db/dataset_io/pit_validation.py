"""Fail-closed validation for immutable Dataset v4 provider snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal


def _normalized_code(column: str) -> str:
    return (
        f"CASE WHEN {column} IS NULL THEN '' "
        f"WHEN length({column}) IN (5, 6) AND right({column}, 1) = '0' "
        f"THEN left({column}, length({column}) - 1) ELSE {column} END"
    )


def _invalid_iso_date(
    column: str,
    *,
    mode: Literal["required", "nullable", "nonblank"],
) -> str:
    canonical = (
        f"coalesce(strftime(try_strptime({column}, '%Y-%m-%d'), '%Y-%m-%d'), '') "
        f"<> {column}"
    )
    if mode == "nonblank":
        return f"({column} IS NOT NULL AND {column} <> '' AND ({canonical}))"
    if mode == "nullable":
        return f"({column} IS NOT NULL AND ({column} = '' OR {canonical}))"
    return f"({column} IS NULL OR {column} = '' OR {canonical})"


def find_dataset_date_audit_error(
    conn: Any,
    *,
    tables: Mapping[str, str],
) -> str | None:
    """Return the first non-canonical physical business-date error."""

    date_fields: dict[
        str,
        tuple[tuple[str, Literal["required", "nullable", "nonblank"]], ...],
    ] = {
        "stock_data": (("date", "required"),),
        "topix_data": (("date", "required"),),
        "indices_data": (("date", "required"),),
        "margin_data": (("date", "required"),),
        "stock_data_raw": (("date", "required"),),
        "stock_master_daily": (("date", "required"), ("listed_date", "nonblank")),
        "stocks": (("listed_date", "nonblank"),),
        "statements": (
            ("disclosed_date", "required"),
            ("period_start", "required"),
            ("period_end", "required"),
        ),
        "statement_metrics_adjusted": (
            ("disclosed_date", "required"),
            ("period_end", "required"),
            ("fundamentals_adjustment_basis_date", "required"),
        ),
        "daily_valuation": (
            ("date", "required"),
            ("price_basis_date", "required"),
            ("statement_disclosed_date", "nullable"),
            ("forward_eps_disclosed_date", "nullable"),
            ("forward_sales_disclosed_date", "nullable"),
            ("fundamentals_adjustment_basis_date", "nullable"),
        ),
    }
    for logical_name, fields in date_fields.items():
        table = tables.get(logical_name)
        if table is None:
            continue
        for field, mode in fields:
            condition = _invalid_iso_date(field, mode=mode)
            if conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}").fetchone()[0]:
                return f"Dataset {logical_name}.{field} is not canonical ISO YYYY-MM-DD"
    return None


def find_dataset_current_basis_audit_error(
    conn: Any,
    *,
    coverage_start: str,
    coverage_end: str,
    fundamentals_basis_date: str,
    tables: Mapping[str, str],
) -> str | None:
    """Validate provider bounds and current-statement/valuation identity."""

    for logical_name, field in (
        ("stock_data", "date"),
        ("topix_data", "date"),
        ("indices_data", "date"),
        ("margin_data", "date"),
        ("stock_data_raw", "date"),
        ("stock_master_daily", "date"),
        ("daily_valuation", "date"),
    ):
        table = tables.get(logical_name)
        if table is None:
            continue
        if conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {field} < ? OR {field} > ?",
            [coverage_start, coverage_end],
        ).fetchone()[0]:
            return f"Dataset {logical_name} exceeds pinned provider coverage"

    statements = tables.get("statements")
    metrics = tables.get("statement_metrics_adjusted")
    if statements is not None:
        if conn.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {_normalized_code('code')} AS code, statement_id, COUNT(*) AS n
                FROM {statements}
                GROUP BY 1, 2 HAVING COUNT(*) <> 1
            ) duplicates
            """
        ).fetchone()[0]:
            return "Dataset raw statements have duplicate provider identity"
    if metrics is not None:
        if conn.execute(
            f"""
            SELECT COUNT(*) FROM {metrics}
            WHERE fundamentals_adjustment_basis_date <> ?
               OR source_fingerprint IS NULL OR trim(source_fingerprint) = ''
            """,
            [fundamentals_basis_date],
        ).fetchone()[0]:
            return "Dataset adjusted metrics do not match pinned current basis"
        if statements is not None and conn.execute(
            f"""
            SELECT COUNT(*) FROM {metrics} metric
            LEFT JOIN {statements} statement
              ON {_normalized_code('metric.code')} = {_normalized_code('statement.code')}
             AND metric.statement_id = statement.statement_id
            WHERE statement.statement_id IS NULL
               OR metric.disclosed_date <> statement.disclosed_date
               OR metric.disclosed_at <> statement.disclosed_at
               OR metric.period_end <> statement.period_end
            """
        ).fetchone()[0]:
            return "Dataset adjusted metric has no exact raw statement identity"
        if statements is not None and conn.execute(
            f"""
            SELECT COUNT(*) FROM {statements} statement
            LEFT JOIN {metrics} metric
              ON {_normalized_code('metric.code')} = {_normalized_code('statement.code')}
             AND metric.statement_id = statement.statement_id
            WHERE metric.statement_id IS NULL
            """
        ).fetchone()[0]:
            return "Dataset raw statement has no exact adjusted metric identity"

    valuation = tables.get("daily_valuation")
    if valuation is not None:
        if conn.execute(
            f"""
            SELECT COUNT(*) FROM {valuation}
            WHERE price_basis_date <> date
               OR (statement_disclosed_at IS NOT NULL
                   AND statement_disclosed_at > date || 'T23:59:59+09:00')
               OR (fundamentals_adjustment_basis_date IS NOT NULL
                   AND fundamentals_adjustment_basis_date <> ?)
               OR (source_fingerprint IS NOT NULL AND trim(source_fingerprint) = '')
            """,
            [fundamentals_basis_date],
        ).fetchone()[0]:
            return "Dataset daily valuation current-basis provenance is inconsistent"
        if metrics is not None and conn.execute(
            f"""
            SELECT COUNT(*) FROM {valuation} valuation
            LEFT JOIN {metrics} metric
              ON {_normalized_code('valuation.code')} = {_normalized_code('metric.code')}
             AND valuation.statement_id = metric.statement_id
            WHERE (valuation.statement_id IS NULL AND (
                       valuation.statement_disclosed_date IS NOT NULL
                    OR valuation.statement_disclosed_at IS NOT NULL
                    OR valuation.fundamentals_adjustment_basis_date IS NOT NULL
                    OR valuation.source_fingerprint IS NOT NULL))
               OR (valuation.statement_id IS NOT NULL AND (
                       metric.statement_id IS NULL
                    OR valuation.statement_disclosed_date IS DISTINCT FROM metric.disclosed_date
                    OR valuation.statement_disclosed_at IS DISTINCT FROM metric.disclosed_at
                    OR valuation.fundamentals_adjustment_basis_date IS DISTINCT FROM metric.fundamentals_adjustment_basis_date
                    OR valuation.source_fingerprint IS DISTINCT FROM metric.source_fingerprint))
            """
        ).fetchone()[0]:
            return "Dataset daily valuation has no exact adjusted statement provenance"

    raw = tables.get("stock_data_raw")
    prices = tables.get("stock_data")
    if raw is not None and prices is not None and conn.execute(
        f"""
        SELECT COUNT(*) FROM (
            (SELECT code, date, adjusted_open, adjusted_high, adjusted_low,
                    adjusted_close, adjusted_volume FROM {raw}
             EXCEPT ALL
             SELECT code, date, open, high, low, close, volume FROM {prices})
            UNION ALL
            (SELECT code, date, open, high, low, close, volume FROM {prices}
             EXCEPT ALL
             SELECT code, date, adjusted_open, adjusted_high, adjusted_low,
                    adjusted_close, adjusted_volume FROM {raw})
        ) mismatches
        """
    ).fetchone()[0]:
        return "Dataset stock_data differs from provider-adjusted raw values"

    return None


def find_dataset_snapshot_audit_error(
    conn: Any,
    *,
    coverage_start: str,
    coverage_end: str,
    fundamentals_basis_date: str,
    tables: Mapping[str, str],
) -> str | None:
    return find_dataset_date_audit_error(
        conn, tables=tables
    ) or find_dataset_current_basis_audit_error(
        conn,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        fundamentals_basis_date=fundamentals_basis_date,
        tables=tables,
    )
