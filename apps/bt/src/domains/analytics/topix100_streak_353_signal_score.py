"""Runtime TOPIX100 streak 3/53 signal scores for the ranking page.

The score is a transparent stage-1 baseline built from the published
multivariate-priority research bundle. It does not retrain online. Instead it
reads the latest validation lookup table and shrinks specific cells toward
broader subsets.

Targets:
- long score: expected 5-day return
- short score: expected 1-day downside, expressed as a positive short edge
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from src.domains.analytics.research_bundle import load_research_bundle_info
from src.domains.analytics.topix100_streak_353_multivariate_priority import (
    get_topix100_streak_353_multivariate_priority_latest_bundle_path,
    load_topix100_streak_353_multivariate_priority_research_bundle,
)

TOPIX100_STREAK_353_LONG_SCORE_HORIZON_DAYS = 5
TOPIX100_STREAK_353_SHORT_SCORE_HORIZON_DAYS = 1
_LONG_BLEND_PRIOR = 260.0
_SHORT_BLEND_PRIOR = 320.0
_LONG_CHAIN: tuple[tuple[str, str], ...] = (
    ("universe", "universe"),
    ("short_mode", "short_mode"),
    ("bucket+short_mode", "bucket+short_mode"),
    ("bucket+short_mode+long_mode", "bucket+short_mode+long_mode"),
    ("bucket+volume+short_mode+long_mode", "full"),
)
_SHORT_CHAIN: tuple[tuple[str, str], ...] = (
    ("universe", "universe"),
    ("short_mode", "short_mode"),
    ("volume+short_mode", "volume+short_mode"),
    ("volume+short_mode+long_mode", "volume+short_mode+long_mode"),
    ("bucket+volume+short_mode+long_mode", "full"),
)


@dataclass(frozen=True)
class _LookupRow:
    subset_key: str
    selector_value_key: str
    avg_return_1d: float
    avg_return_5d: float
    date_count_1d: int
    date_count_5d: int


@dataclass(frozen=True)
class Topix100Streak353SignalScorecard:
    run_id: str
    bundle_path: Path
    universe_long_score_5d: float
    universe_short_score_1d: float
    rows_by_subset: dict[str, dict[str, _LookupRow]]


@dataclass(frozen=True)
class Topix100Streak353SignalScore:
    long_score_5d: float | None
    short_score_1d: float | None


def load_topix100_streak_353_signal_scorecard() -> Topix100Streak353SignalScorecard | None:
    bundle_path = get_topix100_streak_353_multivariate_priority_latest_bundle_path()
    if bundle_path is None:
        return None
    return _load_scorecard_cached(str(bundle_path))


@lru_cache(maxsize=4)
def _load_scorecard_cached(bundle_path: str) -> Topix100Streak353SignalScorecard:
    resolved_path = Path(bundle_path)
    bundle_info = load_research_bundle_info(resolved_path)
    result = load_topix100_streak_353_multivariate_priority_research_bundle(resolved_path)
    subset_df = result.subset_candidate_scorecard_df.copy()
    if subset_df.empty:
        raise ValueError("TOPIX100 multivariate priority bundle has no candidate score rows")

    validation_df = subset_df[subset_df["sample_split"] == "validation"].copy()
    if validation_df.empty:
        raise ValueError("TOPIX100 multivariate priority bundle has no validation score rows")

    rows_by_subset: dict[str, dict[str, _LookupRow]] = {}
    for row in validation_df.to_dict(orient="records"):
        subset_key = str(row["subset_key"])
        selector_value_key = str(row["selector_value_key"])
        rows_by_subset.setdefault(subset_key, {})[selector_value_key] = _LookupRow(
            subset_key=subset_key,
            selector_value_key=selector_value_key,
            avg_return_1d=float(row["avg_return_1d"]),
            avg_return_5d=float(row["avg_return_5d"]),
            date_count_1d=int(row["date_count_1d"]),
            date_count_5d=int(row["date_count_5d"]),
        )

    universe_row = rows_by_subset.get("universe", {}).get("universe")
    if universe_row is None:
        raise ValueError("TOPIX100 multivariate priority bundle is missing the universe row")

    return Topix100Streak353SignalScorecard(
        run_id=bundle_info.run_id,
        bundle_path=resolved_path,
        universe_long_score_5d=float(universe_row.avg_return_5d),
        universe_short_score_1d=float(-universe_row.avg_return_1d),
        rows_by_subset=rows_by_subset,
    )


def score_topix100_streak_353_signal(
    *,
    price_decile: int | None,
    volume_bucket: str | None,
    short_mode: str | None,
    long_mode: str | None,
    scorecard: Topix100Streak353SignalScorecard | None = None,
) -> Topix100Streak353SignalScore:
    if (
        price_decile is None
        or volume_bucket not in {"high", "low"}
        or short_mode not in {"bullish", "bearish"}
        or long_mode not in {"bullish", "bearish"}
    ):
        return Topix100Streak353SignalScore(
            long_score_5d=None,
            short_score_1d=None,
        )

    resolved_scorecard = scorecard or load_topix100_streak_353_signal_scorecard()
    if resolved_scorecard is None:
        return Topix100Streak353SignalScore(
            long_score_5d=None,
            short_score_1d=None,
        )

    bucket_key = f"Q{price_decile}"
    volume_key = f"volume_{volume_bucket}"
    values = {
        "bucket": bucket_key,
        "short_mode": short_mode,
        "long_mode": long_mode,
        "volume": volume_key,
    }

    long_score = _blend_subset_target(
        resolved_scorecard,
        chain=_LONG_CHAIN,
        values=values,
        target="long_5d",
        prior_strength=_LONG_BLEND_PRIOR,
    )
    short_score = _blend_subset_target(
        resolved_scorecard,
        chain=_SHORT_CHAIN,
        values=values,
        target="short_1d",
        prior_strength=_SHORT_BLEND_PRIOR,
    )
    return Topix100Streak353SignalScore(
        long_score_5d=long_score,
        short_score_1d=short_score,
    )


def _blend_subset_target(
    scorecard: Topix100Streak353SignalScorecard,
    *,
    chain: tuple[tuple[str, str], ...],
    values: dict[str, str],
    target: str,
    prior_strength: float,
) -> float | None:
    base_value = (
        scorecard.universe_long_score_5d
        if target == "long_5d"
        else scorecard.universe_short_score_1d
    )
    current_value = base_value

    for subset_key, selector_kind in chain[1:]:
        row = scorecard.rows_by_subset.get(subset_key, {}).get(
            _build_selector_value_key(selector_kind, values)
        )
        if row is None:
            continue
        if target == "long_5d":
            row_value = row.avg_return_5d
            row_count = row.date_count_5d
        else:
            row_value = -row.avg_return_1d
            row_count = row.date_count_1d
        weight = row_count / (row_count + prior_strength)
        current_value = current_value * (1.0 - weight) + row_value * weight

    return current_value


def _build_selector_value_key(selector_kind: str, values: dict[str, str]) -> str:
    if selector_kind == "universe":
        return "universe"
    if selector_kind == "short_mode":
        return values["short_mode"]
    if selector_kind == "bucket+short_mode":
        return f'{values["bucket"]}|{values["short_mode"]}'
    if selector_kind == "bucket+short_mode+long_mode":
        return f'{values["bucket"]}|{values["short_mode"]}|{values["long_mode"]}'
    if selector_kind == "volume+short_mode":
        return f'{values["volume"]}|{values["short_mode"]}'
    if selector_kind == "volume+short_mode+long_mode":
        return f'{values["volume"]}|{values["short_mode"]}|{values["long_mode"]}'
    if selector_kind == "full":
        return (
            f'{values["bucket"]}|{values["volume"]}|'
            f'{values["short_mode"]}|{values["long_mode"]}'
        )
    raise ValueError(f"Unsupported selector kind: {selector_kind}")
