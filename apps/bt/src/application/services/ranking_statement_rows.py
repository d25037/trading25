"""Statement row adapters for ranking fundamentals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.application.services.ranking_response_items import row_get, str_or_none
from src.domains.analytics.fundamental_ranking import (
    StatementRow,
    normalize_period_label,
    resolve_fy_cycle_key,
    to_nullable_float,
)


def statement_row_from_mapping(row: Mapping[str, Any]) -> StatementRow:
    disclosed_date = str(row["disclosed_date"])
    return StatementRow(
        code=str(row["code"]),
        disclosed_date=disclosed_date,
        period_type=normalize_period_label(row["type_of_current_period"]),
        earnings_per_share=to_nullable_float(row["earnings_per_share"]),
        forecast_eps=to_nullable_float(row["forecast_eps"]),
        next_year_forecast_earnings_per_share=to_nullable_float(
            row["next_year_forecast_earnings_per_share"]
        ),
        shares_outstanding=to_nullable_float(row["shares_outstanding"]),
        fy_cycle_key=resolve_fy_cycle_key(disclosed_date),
        type_of_document=str_or_none(row_get(row, "type_of_document")),
    )


def statement_rows_from_mappings(
    rows: Sequence[Mapping[str, Any]],
) -> list[StatementRow]:
    return [statement_row_from_mapping(row) for row in rows]


def statement_rows_by_code(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[StatementRow]]:
    grouped: dict[str, list[StatementRow]] = {}
    for row in rows:
        grouped.setdefault(str(row["code"]), []).append(statement_row_from_mapping(row))
    return grouped
