"""Shared fail-closed validation for Dataset v3 event-time PIT graphs."""

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


def find_dataset_pit_date_audit_error(
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
        "statements": (("disclosed_date", "required"),),
        "stock_adjustment_bases": (
            ("valid_from", "required"),
            ("valid_to_exclusive", "nullable"),
            ("adjustment_through_date", "required"),
            ("materialized_through_date", "required"),
        ),
        "stock_adjustment_basis_segments": (
            ("source_date_from", "required"),
            ("source_date_to_exclusive", "nullable"),
        ),
        "statement_metrics_adjusted": (
            ("disclosed_date", "required"),
            ("period_end", "required"),
            ("price_basis_date", "required"),
        ),
        "daily_valuation": (
            ("date", "required"),
            ("price_basis_date", "required"),
            ("statement_disclosed_date", "nullable"),
            ("forward_eps_disclosed_date", "nullable"),
            ("forward_sales_disclosed_date", "nullable"),
        ),
    }
    for logical_name, fields in date_fields.items():
        table = tables.get(logical_name)
        if table is None:
            continue
        for field, mode in fields:
            condition = _invalid_iso_date(field, mode=mode)
            if conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}").fetchone()[0]:
                return f"Dataset PIT {logical_name}.{field} is not canonical ISO YYYY-MM-DD"
    return None


def find_dataset_pit_graph_audit_error(
    conn: Any,
    *,
    cutoff: str,
    tables: Mapping[str, str],
) -> str | None:
    """Return the first PIT graph identity or boundary error."""

    basis = tables.get("stock_adjustment_bases")
    if basis is not None:
        normalized = _normalized_code("code")
        invalid_basis = conn.execute(
            f"""
            SELECT COUNT(*) FROM {basis}
            WHERE valid_from > ? OR adjustment_through_date > ?
               OR materialized_through_date > ?
               OR adjustment_through_date <> valid_from
               OR materialized_through_date < valid_from
               OR (valid_to_exclusive IS NOT NULL AND valid_to_exclusive <> ''
                   AND (valid_to_exclusive > ? OR valid_to_exclusive <= valid_from))
               OR basis_id <> 'event-pit-v1:' || {normalized} || ':' || valid_from
            """,
            [cutoff, cutoff, cutoff, cutoff],
        ).fetchone()[0]
        if invalid_basis:
            return "Dataset PIT adjustment basis identity or boundary is invalid"

    segments = tables.get("stock_adjustment_basis_segments")
    if segments is not None:
        invalid_segment = conn.execute(
            f"""
            SELECT COUNT(*) FROM {segments}
            WHERE source_date_from > ?
               OR (source_date_to_exclusive IS NOT NULL
                   AND source_date_to_exclusive <> ''
                   AND (source_date_to_exclusive > ?
                        OR source_date_to_exclusive <= source_date_from))
            """,
            [cutoff, cutoff],
        ).fetchone()[0]
        if invalid_segment:
            return "Dataset PIT adjustment segment boundary is invalid"

    metrics = tables.get("statement_metrics_adjusted")
    statements = tables.get("statements")
    if statements is not None:
        duplicate_statement_identity = conn.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {_normalized_code('code')} AS normalized_code,
                       disclosed_date, COUNT(*) AS identity_count
                FROM {statements}
                GROUP BY 1, 2
                HAVING COUNT(*) <> 1
            ) duplicates
            """
        ).fetchone()[0]
        if duplicate_statement_identity:
            return "Dataset PIT raw statements have duplicate normalized identity"
    if metrics is not None and basis is not None:
        metric_basis_error = conn.execute(
            f"""
            SELECT COUNT(*) FROM {metrics} metric
            LEFT JOIN {basis} basis
              ON {_normalized_code('metric.code')} = {_normalized_code('basis.code')}
             AND metric.basis_version = basis.basis_id
            WHERE metric.price_basis_date > ? OR basis.basis_id IS NULL
               OR metric.price_basis_date IS DISTINCT FROM basis.adjustment_through_date
            """,
            [cutoff],
        ).fetchone()[0]
        if metric_basis_error:
            return "Dataset PIT adjusted metric price basis is inconsistent"
        if statements is not None:
            reverse_identity_error = conn.execute(
                f"""
                SELECT COUNT(*) FROM {metrics} metric
                LEFT JOIN {statements} statement
                  ON {_normalized_code('metric.code')} =
                     {_normalized_code('statement.code')}
                 AND metric.disclosed_date = statement.disclosed_date
                 AND metric.period_end = statement.disclosed_date
                 AND metric.period_type = coalesce(statement.type_of_current_period, '')
                WHERE statement.code IS NULL
                """
            ).fetchone()[0]
            if reverse_identity_error:
                return "Dataset PIT adjusted metric has no exact raw statement identity"

    valuation = tables.get("daily_valuation")
    if valuation is not None and basis is not None:
        valuation_basis_error = conn.execute(
            f"""
            SELECT COUNT(*) FROM {valuation} valuation
            LEFT JOIN {basis} basis
              ON {_normalized_code('valuation.code')} = {_normalized_code('basis.code')}
             AND valuation.basis_version = basis.basis_id
            WHERE valuation.price_basis_date > ? OR basis.basis_id IS NULL
               OR valuation.price_basis_date IS DISTINCT FROM basis.adjustment_through_date
            """,
            [cutoff],
        ).fetchone()[0]
        if valuation_basis_error:
            return "Dataset PIT daily valuation price basis is inconsistent"

    return None


def find_dataset_pit_audit_error(
    conn: Any,
    *,
    cutoff: str,
    tables: Mapping[str, str],
) -> str | None:
    """Return the first PIT audit error, or ``None`` for a valid graph."""

    return find_dataset_pit_date_audit_error(
        conn, tables=tables
    ) or find_dataset_pit_graph_audit_error(conn, cutoff=cutoff, tables=tables)
