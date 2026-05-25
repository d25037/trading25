"""In-place collection helpers for market ranking items."""

from __future__ import annotations

from datetime import date as calendar_date, datetime, timedelta

from src.entrypoints.http.schemas.ranking import RankingItem


def filter_ranking_collections_by_forward_eps_source_date(
    collections: tuple[list[RankingItem], ...],
    *,
    target_date: str,
    forward_eps_disclosed_within_days: int,
) -> None:
    if forward_eps_disclosed_within_days <= 0:
        return

    try:
        max_date = datetime.fromisoformat(target_date).date()
    except ValueError:
        return
    min_date = max_date - timedelta(days=forward_eps_disclosed_within_days)

    for collection in collections:
        collection[:] = [
            item
            for item in collection
            if is_forward_eps_source_date_in_window(
                item.forwardEpsDisclosedDate,
                min_date=min_date,
                max_date=max_date,
            )
        ]


def is_forward_eps_source_date_in_window(
    disclosed_date: str | None,
    *,
    min_date: calendar_date,
    max_date: calendar_date,
) -> bool:
    if disclosed_date is None:
        return False
    try:
        source_date = datetime.fromisoformat(disclosed_date).date()
    except ValueError:
        return False
    return min_date <= source_date <= max_date


def limit_and_rerank_ranking_collections(
    collections: tuple[list[RankingItem], ...],
    limit: int,
) -> None:
    for collection in collections:
        if limit > 0:
            del collection[limit:]
        for rank, item in enumerate(collection, start=1):
            item.rank = rank
