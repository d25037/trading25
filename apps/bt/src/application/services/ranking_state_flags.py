"""Shared Daily Ranking state and risk flag constants."""

from __future__ import annotations

from src.entrypoints.http.schemas.ranking import RankingRiskFlag, RankingTechnicalFlag

SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT = 25.0
ATR20_ACCELERATION_MAX_ATR20_TO_ATR60 = 1.25
OVERHEAT_RISK_FLAG: RankingRiskFlag = "overheat"
STALE_RALLY_FADE_RISK_FLAG: RankingRiskFlag = "stale_rally_fade"
ATR20_ACCELERATION_TECHNICAL_FLAG: RankingTechnicalFlag = "atr20_acceleration"
RISK_FLAG_STATE_FILTERS: frozenset[RankingRiskFlag] = frozenset(
    (OVERHEAT_RISK_FLAG, STALE_RALLY_FADE_RISK_FLAG)
)
