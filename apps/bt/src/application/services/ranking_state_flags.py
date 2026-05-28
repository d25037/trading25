"""Shared Daily Ranking state and risk flag constants."""

from __future__ import annotations

from src.entrypoints.http.schemas.ranking import RankingRiskFlag

SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
OVERHEAT_RISK_FLAG: RankingRiskFlag = "overheat"
STALE_RALLY_FADE_RISK_FLAG: RankingRiskFlag = "stale_rally_fade"
RISK_FLAG_STATE_FILTERS: frozenset[RankingRiskFlag] = frozenset(
    (OVERHEAT_RISK_FLAG, STALE_RALLY_FADE_RISK_FLAG)
)
