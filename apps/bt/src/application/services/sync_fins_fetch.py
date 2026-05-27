"""Fetch helpers for J-Quants fins summary sync."""

from __future__ import annotations

from typing import Any

from src.application.services.sync_row_converters import extract_list_items as _extract_list_items
from src.infrastructure.db.market.query_helpers import expand_stock_code, normalize_stock_code, stock_code_candidates

_MAX_FINS_SUMMARY_PAGES = 2000

__all__ = ("_fetch_fins_summary_by_code", "_fetch_fins_summary_paginated")


async def _fetch_fins_summary_paginated(
    client: Any,
    params: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    """`/fins/summary` を pagination_key が尽きるまで取得する。"""
    current_params = dict(params)
    all_rows: list[dict[str, Any]] = []
    api_calls = 0

    while True:
        body = await client.get("/fins/summary", params=current_params)
        api_calls += 1

        page_rows = _extract_list_items(body, preferred_keys=("data",))
        all_rows.extend(page_rows)

        pagination_key = body.get("pagination_key")
        if not pagination_key:
            break

        if api_calls >= _MAX_FINS_SUMMARY_PAGES:
            raise RuntimeError("fins/summary pagination exceeded safety limit")

        current_params = {**current_params, "pagination_key": pagination_key}

    return all_rows, api_calls


async def _fetch_fins_summary_by_code(
    client: Any,
    code: str,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch /fins/summary by trying both 5-digit and 4-digit code formats.

    dataset builder は 5桁コードで fetch しているため、
    market sync も 5桁優先で試行し、空結果やエラー時のみ 4桁へフォールバックする。
    """
    normalized_code = normalize_stock_code(code)
    candidates = list(
        dict.fromkeys(
            (
                expand_stock_code(normalized_code),
                *stock_code_candidates(normalized_code),
            )
        )
    )

    total_calls = 0
    last_error: Exception | None = None
    saw_empty_payload = False

    for candidate in candidates:
        try:
            data, page_calls = await _fetch_fins_summary_paginated(
                client,
                {"code": candidate},
            )
            total_calls += page_calls
            if data:
                return data, total_calls
            saw_empty_payload = True
            continue
        except Exception as exc:
            last_error = exc
            continue

    if saw_empty_payload:
        return [], total_calls

    if last_error is None:
        raise RuntimeError(f"fins/summary code fetch failed for {code}")
    raise last_error
