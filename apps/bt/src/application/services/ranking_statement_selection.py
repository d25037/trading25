"""Statement selection helpers for ranking fundamentals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.application.services.ranking_response_items import row_get, str_or_none
from src.domains.analytics.fundamental_ranking import (
    adjust_per_share_value,
    normalize_period_label,
    to_nullable_float,
)
from src.shared.utils.statement_document import is_actual_fy_financial_statement


def _is_actual_fy_row(row: Mapping[str, Any], *, as_of_date: str) -> bool:
    return str(row["disclosed_date"]) <= str(as_of_date) and is_actual_fy_financial_statement(
        normalize_period_label(row["type_of_current_period"]),
        str_or_none(row_get(row, "type_of_document")),
        allow_unknown_document=True,
    )


def latest_value_bps_statement(
    rows: Sequence[Mapping[str, Any]],
    baseline_shares: float | None,
    *,
    as_of_date: str,
) -> Mapping[str, Any] | None:
    eligible = [row for row in rows if _is_actual_fy_row(row, as_of_date=as_of_date)]
    for row in sorted(
        eligible, key=lambda row: str(row["disclosed_date"]), reverse=True
    ):
        bps = adjust_per_share_value(
            to_nullable_float(row["bps"]),
            to_nullable_float(row["shares_outstanding"]),
            baseline_shares,
        )
        if bps is not None and bps > 0:
            return row
    return None


def latest_actual_fy_disclosed_date(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of_date: str,
) -> str | None:
    eligible = [row for row in rows if _is_actual_fy_row(row, as_of_date=as_of_date)]
    if not eligible:
        return None
    return str(max(eligible, key=lambda row: str(row["disclosed_date"]))["disclosed_date"])
