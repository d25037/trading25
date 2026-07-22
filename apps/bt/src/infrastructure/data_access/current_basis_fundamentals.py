"""Shared batch validation for Market v5 provider/current-basis fundamentals."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.infrastructure.db.market.market_reader import MarketDbQueryable
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.provider_stock_window import validate_provider_plan


def _normalized_code_sql(column: str) -> str:
    return (
        "CASE "
        f"WHEN length({column}) IN (5, 6) AND right({column}, 1) = '0' "
        f"THEN left({column}, length({column}) - 1) ELSE {column} END"
    )


@dataclass(frozen=True)
class CurrentBasisProviderWindow:
    code: str
    coverage_start: str
    coverage_end: str
    provider_plan: str
    provider_as_of: str
    source_fingerprint: str
    fundamentals_adjustment_basis_date: str
    fundamentals_source_fingerprint: str
    statement_count: int
    materialized_at: str


def resolve_current_basis_provider_windows(
    reader: MarketDbQueryable,
    codes: Sequence[str],
    effective_market_date: date,
) -> dict[str, CurrentBasisProviderWindow]:
    """Validate provider lineage/current-basis identities once for a code batch."""
    normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
    if not normalized_codes:
        return {}
    placeholders = ", ".join("?" for _ in normalized_codes)
    provider_code = _normalized_code_sql("provider.code")
    rows = reader.query(
        f"""
        SELECT {provider_code} AS code,
               provider.coverage_start, provider.coverage_end,
               provider.provider_plan, provider.provider_as_of,
               provider.source_fingerprint AS provider_source_fingerprint,
               state.fundamentals_adjustment_basis_date,
               state.source_fingerprint AS fundamentals_source_fingerprint,
               state.statement_count, state.materialized_at
        FROM stock_provider_windows AS provider
        LEFT JOIN current_basis_fundamentals_state AS state
          ON state.code = {provider_code}
        WHERE {provider_code} IN ({placeholders})
        ORDER BY code
        """,
        tuple(normalized_codes),
    )
    by_code = {normalize_stock_code(str(row["code"])): row for row in rows}

    pending_code = _normalized_code_sql("code")
    pending = reader.query(
        f"""
        SELECT DISTINCT {pending_code} AS code
        FROM current_basis_recompute_pending
        WHERE {pending_code} IN ({placeholders})
        ORDER BY code
        """,
        tuple(normalized_codes),
    )
    if pending:
        raise FundamentalsPitSnapshotError(
            "current_adjusted_metrics_required",
            "market_db_sync recovery required: current-basis recompute is pending for "
            + ", ".join(str(row["code"]) for row in pending),
        )

    metric_code = _normalized_code_sql("metric.code")
    source_code = _normalized_code_sql("source.code")
    metric_rows = reader.query(
        f"""
        SELECT {metric_code} AS code,
               COUNT(DISTINCT metric.statement_id) AS metric_count,
               COUNT(*) FILTER (
                   WHERE state.code IS NULL
                      OR metric.fundamentals_adjustment_basis_date
                         IS DISTINCT FROM state.fundamentals_adjustment_basis_date
                      OR metric.source_fingerprint IS DISTINCT FROM state.source_fingerprint
                      OR source.statement_id IS NULL
                      OR metric.disclosed_date IS DISTINCT FROM source.disclosed_date
                      OR metric.disclosed_at IS DISTINCT FROM source.disclosed_at
                      OR metric.period_end IS DISTINCT FROM source.period_end
                      OR upper(COALESCE(metric.period_type, '')) IS DISTINCT FROM
                         upper(COALESCE(source.type_of_current_period, ''))
               ) AS invalid_count
        FROM statement_metrics_adjusted AS metric
        LEFT JOIN current_basis_fundamentals_state AS state
          ON state.code = {metric_code}
        LEFT JOIN statements AS source
          ON {source_code} = {metric_code}
         AND source.statement_id = metric.statement_id
        WHERE {metric_code} IN ({placeholders})
        GROUP BY 1
        """,
        tuple(normalized_codes),
    )
    metric_integrity = {
        normalize_stock_code(str(row["code"])): (
            int(row["metric_count"] or 0),
            int(row["invalid_count"] or 0),
        )
        for row in metric_rows
    }
    raw_rows = reader.query(
        f"""
        SELECT {source_code} AS code,
               COUNT(DISTINCT source.statement_id) AS statement_count
        FROM statements AS source
        WHERE {source_code} IN ({placeholders})
        GROUP BY 1
        """,
        tuple(normalized_codes),
    )
    raw_counts = {
        normalize_stock_code(str(row["code"])): int(row["statement_count"] or 0)
        for row in raw_rows
    }

    resolved: dict[str, CurrentBasisProviderWindow] = {}
    provider_plans: set[str] = set()
    provider_as_of_dates: set[date] = set()
    for code in normalized_codes:
        row: Any = by_code.get(code)
        if row is None:
            raise FundamentalsPitSnapshotError(
                "provider_window_required",
                f"market_db_sync recovery required: provider window is unavailable for {code}",
            )
        try:
            coverage_start = date.fromisoformat(str(row["coverage_start"]))
            coverage_end = date.fromisoformat(str(row["coverage_end"]))
            provider_as_of = date.fromisoformat(str(row["provider_as_of"]))
            provider_plan = validate_provider_plan(row["provider_plan"])
        except (TypeError, ValueError) as exc:
            raise FundamentalsPitSnapshotError(
                "provider_window_required",
                f"market_db_sync recovery required: provider lineage is invalid for {code}",
            ) from exc
        metric_count, invalid_count = metric_integrity.get(code, (0, 0))
        raw_count = raw_counts.get(code, 0)
        state_count = row["statement_count"]
        state_basis = str(row["fundamentals_adjustment_basis_date"] or "")
        state_fingerprint = str(row["fundamentals_source_fingerprint"] or "")
        if (
            coverage_start > effective_market_date
            or coverage_end < effective_market_date
            or coverage_start > coverage_end
            or provider_as_of < coverage_end
            or not str(row["provider_source_fingerprint"] or "").strip()
        ):
            raise FundamentalsPitSnapshotError(
                "provider_window_required",
                f"market_db_sync recovery required: provider window for {code} "
                f"does not cover {effective_market_date}",
            )
        if (
            state_basis != coverage_end.isoformat()
            or not state_fingerprint.strip()
            or state_count is None
            or int(state_count) != metric_count
            or int(state_count) != raw_count
            or invalid_count != 0
            or not str(row["materialized_at"] or "").strip()
        ):
            raise FundamentalsPitSnapshotError(
                "current_adjusted_metrics_required",
                f"market_db_sync recovery required: current-basis fundamentals for {code} "
                "is stale or incomplete",
            )
        resolved[code] = CurrentBasisProviderWindow(
            code=code,
            coverage_start=coverage_start.isoformat(),
            coverage_end=coverage_end.isoformat(),
            provider_plan=provider_plan,
            provider_as_of=str(row["provider_as_of"]),
            source_fingerprint=str(row["provider_source_fingerprint"]),
            fundamentals_adjustment_basis_date=state_basis,
            fundamentals_source_fingerprint=state_fingerprint,
            statement_count=int(state_count),
            materialized_at=str(row["materialized_at"]),
        )
        provider_plans.add(provider_plan)
        provider_as_of_dates.add(provider_as_of)
    if len(provider_plans) != 1 or len(provider_as_of_dates) != 1:
        raise FundamentalsPitSnapshotError(
            "provider_window_required",
            "market_db_sync recovery required: provider windows do not share one "
            "provider plan and as-of frontier",
        )
    return resolved
