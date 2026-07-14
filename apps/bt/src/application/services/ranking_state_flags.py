"""Shared Daily Ranking state and risk flag constants."""

from __future__ import annotations

from src.application.contracts import ranking as ranking_contracts

SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT = 25.0
ATR20_ACCELERATION_MAX_ATR20_TO_ATR60 = 1.25
MOMENTUM_TOP20_PERCENTILE_THRESHOLD = 0.8
OVERHEAT_RISK_FLAG: ranking_contracts.RankingRiskFlag = "overheat"
STALE_RALLY_FADE_RISK_FLAG: ranking_contracts.RankingRiskFlag = "stale_rally_fade"
ATR20_ACCELERATION_TECHNICAL_FLAG: ranking_contracts.RankingTechnicalFlag = "atr20_acceleration"
MOMENTUM_20_60_TOP20_TECHNICAL_FLAG: ranking_contracts.RankingTechnicalFlag = "momentum_20_60_top20"
RISK_FLAG_STATE_FILTERS: frozenset[ranking_contracts.RankingRiskFlag] = frozenset(
    (OVERHEAT_RISK_FLAG, STALE_RALLY_FADE_RISK_FLAG)
)
